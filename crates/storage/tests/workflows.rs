//! Workflow persistence tests (Phase 12): migration + repository round trips.

use agentmesh_storage::{
    Database, WorkflowRepository, WorkflowRow, WorkflowStepRepository, WorkflowStepRow,
};
use sqlx::Row;
use uuid::Uuid;

fn row(id: Uuid, status: &str) -> WorkflowRow {
    let now = "2026-08-11T00:00:00+00:00".to_string();
    WorkflowRow {
        id,
        preset: "architect-implement-review".to_string(),
        goal: "goal".to_string(),
        status: status.to_string(),
        context_id: None,
        options_json: r#"{"max_review_rounds":1}"#.to_string(),
        review_rounds: 0,
        runtime_owner: Some("daemon-1".to_string()),
        runtime_heartbeat_at: None,
        error: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        completed_at: None,
        graph_revision: 1,
        parent_workflow_id: None,
        recovery_of_node_id: None,
        recovery_attempt: 0,
        source_workspace: None,
    }
}

fn step_row(workflow_id: Uuid, ordinal: i64, status: &str) -> WorkflowStepRow {
    let now = "2026-08-11T00:00:00+00:00".to_string();
    WorkflowStepRow {
        id: Uuid::new_v4(),
        workflow_id,
        ordinal,
        node_id: None,
        role: "architect".to_string(),
        intent: "architecture".to_string(),
        objective: None,
        status: status.to_string(),
        agent_id: Some("claude".to_string()),
        task_id: Some(Uuid::new_v4()),
        review_round: 0,
        summary: Some("plan".to_string()),
        result_json: Some(r#"{"step":{"id":"architect","role":"architect","intent":"architecture"},"status":"completed"}"#.to_string()),
        created_at: now.clone(),
        started_at: Some(now.clone()),
        completed_at: Some(now),
        error: None,
    }
}

#[tokio::test]
async fn migration_creates_workflow_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    for table in ["workflows", "workflow_steps"] {
        let row = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table)
            .fetch_one(db.pool())
            .await
            .expect("table exists");
        assert_eq!(row.get::<String, _>("name"), table);
    }
}

#[tokio::test]
async fn workflow_repository_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowRepository::new(db.clone());
    let id = Uuid::new_v4();

    repo.create(&row(id, "pending")).await.expect("create");
    let loaded = repo.get(id).await.expect("get").expect("exists");
    assert_eq!(loaded.preset, "architect-implement-review");
    assert_eq!(loaded.status, "pending");

    repo.update_status(id, "running", None)
        .await
        .expect("update");
    assert_eq!(repo.get(id).await.unwrap().unwrap().status, "running");

    repo.mark_completed(id, "completed", None)
        .await
        .expect("terminal");
    let done = repo.get(id).await.unwrap().unwrap();
    assert_eq!(done.status, "completed");
    assert!(done.completed_at.is_some());

    let listed = repo.list().await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
}

#[tokio::test]
async fn workflow_source_workspace_roundtrips_and_defaults_null() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowRepository::new(db.clone());

    // Phase 22 §2: the explicit source workspace persists; old rows keep NULL.
    let mut with_source = row(Uuid::new_v4(), "pending");
    with_source.source_workspace = Some("/tmp/src-repo".to_string());
    repo.create(&with_source).await.expect("create with source");
    let loaded = repo.get(with_source.id).await.unwrap().unwrap();
    assert_eq!(loaded.source_workspace.as_deref(), Some("/tmp/src-repo"));

    let mut legacy = row(Uuid::new_v4(), "pending");
    legacy.source_workspace = None;
    repo.create(&legacy).await.expect("create legacy");
    assert!(
        repo.get(legacy.id)
            .await
            .unwrap()
            .unwrap()
            .source_workspace
            .is_none()
    );
}

#[tokio::test]
async fn workflow_step_repository_upsert_and_recover() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let workflows = WorkflowRepository::new(db.clone());
    let steps = WorkflowStepRepository::new(db.clone());
    let id = Uuid::new_v4();
    workflows.create(&row(id, "running")).await.expect("create");

    steps
        .upsert(&step_row(id, 0, "running"))
        .await
        .expect("insert");
    steps
        .upsert(&step_row(id, 1, "pending"))
        .await
        .expect("insert");
    // Upsert updates the same (workflow, ordinal) row.
    steps
        .upsert(&step_row(id, 0, "completed"))
        .await
        .expect("update");

    let rows = steps.list_for(id).await.expect("list");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].status, "completed");
    assert_eq!(rows[1].status, "pending");
    // Simulate the running state at crash time.
    steps
        .upsert(&step_row(id, 0, "running"))
        .await
        .expect("back to running");
    assert_eq!(steps.list_for(id).await.unwrap()[0].status, "running");

    // Crash recovery marks running workflow + running step interrupted.
    let recovered = workflows
        .recover_interrupted("daemon died")
        .await
        .expect("recover");
    assert_eq!(recovered, 1);
    let interrupted = workflows.get(id).await.unwrap().unwrap();
    assert_eq!(interrupted.status, "interrupted");
    assert_eq!(interrupted.error.as_deref(), Some("daemon died"));

    // The daemon service also marks running steps of interrupted workflows.
    steps
        .recover_interrupted_for(&[id], "daemon died")
        .await
        .expect("recover steps");
    let rows = steps.list_for(id).await.expect("list");
    assert_eq!(rows[0].status, "interrupted");
    assert_eq!(rows[1].status, "pending");
}

#[tokio::test]
async fn dag_node_rows_and_dependency_edges_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let workflows = WorkflowRepository::new(db.clone());
    let steps = WorkflowStepRepository::new(db.clone());
    let id = Uuid::new_v4();
    workflows.create(&row(id, "running")).await.expect("create");

    // Node rows carry node_id.
    let mut arch = step_row(id, 0, "pending");
    arch.node_id = Some("architecture".to_string());
    arch.role = "architect".to_string();
    arch.intent = "architecture".to_string();
    steps.upsert(&arch).await.expect("arch");
    let mut sec = step_row(id, 1, "pending");
    sec.node_id = Some("security_review".to_string());
    sec.role = "reviewer".to_string();
    sec.intent = "review".to_string();
    steps.upsert(&sec).await.expect("sec");

    // Dependency edges.
    steps
        .set_dependencies(id, &[("security_review".into(), "architecture".into())])
        .await
        .expect("deps");
    let deps = steps.list_dependencies(id).await.expect("list");
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].node_id, "security_review");
    assert_eq!(deps[0].depends_on_node_id, "architecture");

    // Replacement clears the old edges.
    steps
        .set_dependencies(
            id,
            &[
                ("security_review".into(), "architecture".into()),
                ("implementation".into(), "architecture".into()),
            ],
        )
        .await
        .expect("replace");
    let deps = steps.list_dependencies(id).await.expect("list");
    assert_eq!(deps.len(), 2);

    // node_id roundtrips through list_for.
    let rows = steps.list_for(id).await.expect("list");
    assert_eq!(rows[0].node_id.as_deref(), Some("architecture"));
    assert_eq!(rows[1].node_id.as_deref(), Some("security_review"));
}

#[tokio::test]
async fn migration_0008_adds_node_id_and_dependency_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    // The column exists on the (pre-existing) workflow_steps table.
    let col = sqlx::query(
        "SELECT COUNT(*) AS n FROM pragma_table_info('workflow_steps') WHERE name = 'node_id'",
    )
    .fetch_one(db.pool())
    .await
    .expect("pragma");
    assert_eq!(col.get::<i64, _>("n"), 1);
    // The dependency table exists.
    let table = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_step_dependencies'",
    )
    .fetch_one(db.pool())
    .await
    .expect("table exists");
    assert_eq!(table.get::<String, _>("name"), "workflow_step_dependencies");
}
