//! Replan persistence tests (Phase 19): proposal round trips, atomic apply
//! claim, and the workflow graph_revision gate.

use agentmesh_storage::{
    Database, ReplanApplyResult, WorkflowReplanRepository, WorkflowReplanRow, WorkflowRepository,
    WorkflowRow, replan_status,
};
use uuid::Uuid;

fn row(workflow_id: Uuid, base: i64) -> WorkflowReplanRow {
    WorkflowReplanRow {
        id: Uuid::new_v4(),
        workflow_id,
        status: replan_status::READY.to_string(),
        planner_agent_id: Some("claude".to_string()),
        planner_task_id: Some(Uuid::new_v4()),
        delta_json: Some(r#"{"version":1,"summary":"s"}"#.to_string()),
        validation_error: None,
        base_graph_revision: base,
        applied_graph_revision: None,
        created_at: "2026-08-12T00:00:00+00:00".to_string(),
        applied_at: None,
    }
}

async fn workflow(db: &Database) -> Uuid {
    let repo = WorkflowRepository::new(db.clone());
    let id = Uuid::new_v4();
    repo.create(&WorkflowRow {
        id,
        preset: "plan".to_string(),
        goal: "g".to_string(),
        status: "running".to_string(),
        context_id: None,
        options_json: "{}".to_string(),
        review_rounds: 0,
        runtime_owner: None,
        runtime_heartbeat_at: None,
        error: None,
        created_at: "2026-08-12T00:00:00+00:00".to_string(),
        updated_at: "2026-08-12T00:00:00+00:00".to_string(),
        completed_at: None,
        graph_revision: 1,
        parent_workflow_id: None,
        recovery_of_node_id: None,
        recovery_attempt: 0,
        source_workspace: None,
    })
    .await
    .expect("create workflow");
    id
}

#[tokio::test]
async fn replan_rows_roundtrip_and_list_newest_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowReplanRepository::new(db.clone());
    let workflow_id = workflow(&db).await;
    let a = row(workflow_id, 1);
    let mut b = row(workflow_id, 1);
    b.id = Uuid::new_v4();
    b.created_at = "2026-08-12T00:00:01+00:00".to_string();
    repo.create(&a).await.expect("create a");
    repo.create(&b).await.expect("create b");

    let all = repo.list_for(workflow_id).await.expect("list");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, b.id, "newest first");
    let loaded = repo.get(a.id).await.expect("get").expect("exists");
    assert_eq!(
        loaded.delta_json.as_deref(),
        Some(r#"{"version":1,"summary":"s"}"#)
    );
    assert_eq!(loaded.base_graph_revision, 1);
}

#[tokio::test]
async fn replan_claim_is_atomic_and_version_gated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowReplanRepository::new(db.clone());
    let workflow_id = workflow(&db).await;
    let a = row(workflow_id, 1);
    repo.create(&a).await.expect("create");

    // Ready + base matches → Claimed.
    assert_eq!(
        repo.claim_apply(a.id, workflow_id).await.expect("claim"),
        ReplanApplyResult::Claimed
    );
    // A second claim sees the apply in progress.
    assert_eq!(
        repo.claim_apply(a.id, workflow_id).await.expect("second"),
        ReplanApplyResult::ApplyInProgress
    );
    // Applying → applied.
    repo.mark_applied(a.id, 2).await.expect("applied");
    assert_eq!(
        repo.claim_apply(a.id, workflow_id).await.expect("applied"),
        ReplanApplyResult::AlreadyApplied
    );
    let applied = repo.get(a.id).await.expect("get").expect("exists");
    assert_eq!(applied.status, replan_status::APPLIED);
    assert_eq!(applied.applied_graph_revision, Some(2));
}

#[tokio::test]
async fn replan_stale_base_revision_is_rejected_atomic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowReplanRepository::new(db.clone());
    let workflow_id = workflow(&db).await;
    // Proposal based on revision 1, but the workflow is now at revision 2.
    let repo_workflows = WorkflowRepository::new(db.clone());
    repo_workflows
        .increment_graph_revision(workflow_id)
        .await
        .expect("bump");
    let a = row(workflow_id, 1);
    repo.create(&a).await.expect("create");

    assert_eq!(
        repo.claim_apply(a.id, workflow_id).await.expect("stale"),
        ReplanApplyResult::ReplanStale
    );
    // The proposal was not mutated by a failed claim.
    let fresh = repo.get(a.id).await.expect("get").expect("exists");
    assert_eq!(fresh.status, replan_status::READY);
    assert_eq!(fresh.applied_graph_revision, None);
}

#[tokio::test]
async fn replan_not_ready_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowReplanRepository::new(db.clone());
    let workflow_id = workflow(&db).await;
    let mut a = row(workflow_id, 1);
    a.status = replan_status::INVALID.to_string();
    repo.create(&a).await.expect("create");
    assert_eq!(
        repo.claim_apply(a.id, workflow_id).await.expect("invalid"),
        ReplanApplyResult::NotReady
    );
}

#[tokio::test]
async fn workflow_graph_revision_bumps_atomically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let repo = WorkflowRepository::new(db.clone());
    let id = Uuid::new_v4();
    repo.create(&WorkflowRow {
        id,
        preset: "plan".to_string(),
        goal: "g".to_string(),
        status: "running".to_string(),
        context_id: None,
        options_json: "{}".to_string(),
        review_rounds: 0,
        runtime_owner: None,
        runtime_heartbeat_at: None,
        error: None,
        created_at: "2026-08-12T00:00:00+00:00".to_string(),
        updated_at: "2026-08-12T00:00:00+00:00".to_string(),
        completed_at: None,
        graph_revision: 1,
        parent_workflow_id: None,
        recovery_of_node_id: None,
        recovery_attempt: 0,
        source_workspace: None,
    })
    .await
    .expect("create");
    assert_eq!(repo.graph_revision(id).await.expect("rev"), 1);
    assert_eq!(repo.increment_graph_revision(id).await.expect("bump"), 2);
    assert_eq!(repo.graph_revision(id).await.expect("rev"), 2);
}

#[tokio::test]
async fn stale_applying_replan_recovers_by_graph_revision() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let replans = WorkflowReplanRepository::new(db.clone());
    let workflows = WorkflowRepository::new(db.clone());

    // Workflow still on revision 1 with a replan stuck `applying` (base 1):
    // the apply transaction never committed → retryable `ready`.
    let wf_a = Uuid::new_v4();
    workflows
        .create(&WorkflowRow {
            id: wf_a,
            preset: "plan".to_string(),
            goal: "g".to_string(),
            status: "running".to_string(),
            context_id: None,
            options_json: "{}".to_string(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: "2026-08-12T00:00:00+00:00".to_string(),
            updated_at: "2026-08-12T00:00:00+00:00".to_string(),
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create a");
    let mut ra = row(wf_a, 1);
    ra.id = Uuid::new_v4();
    ra.status = replan_status::APPLYING.to_string();
    replans.create(&ra).await.expect("create a replan");

    // Workflow advanced to revision 2 with a replan stuck `applying` (base 1):
    // the transaction committed → `applied`.
    let wf_b = Uuid::new_v4();
    workflows
        .create(&WorkflowRow {
            id: wf_b,
            preset: "plan".to_string(),
            goal: "g".to_string(),
            status: "running".to_string(),
            context_id: None,
            options_json: "{}".to_string(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: "2026-08-12T00:00:00+00:00".to_string(),
            updated_at: "2026-08-12T00:00:00+00:00".to_string(),
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create b");
    workflows
        .increment_graph_revision(wf_b)
        .await
        .expect("bump b");
    let mut rb = row(wf_b, 1);
    rb.id = Uuid::new_v4();
    rb.status = replan_status::APPLYING.to_string();
    replans.create(&rb).await.expect("create b replan");

    let (ready, applied, failed) = replans.recover_stale_applying().await.expect("recover");
    assert_eq!(ready, 1, "base == current → retryable ready");
    assert_eq!(applied, 1, "revision advanced → applied");
    assert_eq!(failed, 0, "atomic transaction leaves no unprovable state");

    let recovered_a = replans.get(ra.id).await.unwrap().unwrap();
    assert_eq!(recovered_a.status, replan_status::READY);
    let recovered_b = replans.get(rb.id).await.unwrap().unwrap();
    assert_eq!(recovered_b.status, replan_status::APPLIED);
    assert_eq!(recovered_b.applied_graph_revision, Some(2));
}
