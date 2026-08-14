//! Migration upgrade test: a Phase 4 database must survive the 0002
//! migration with all task data intact.

use agentmesh_core::TaskStatus;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

const MIGRATION_0001: &str = include_str!("../migrations/0001_initial.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_context_sessions.sql");
const MIGRATION_0003: &str = include_str!("../migrations/0003_workspaces.sql");
const MIGRATION_0004: &str = include_str!("../migrations/0004_daemon_runtime.sql");
const MIGRATION_0005: &str = include_str!("../migrations/0005_workflows.sql");
const MIGRATION_0006: &str = include_str!("../migrations/0006_applies.sql");
const MIGRATION_0007: &str = include_str!("../migrations/0007_workspace_lifecycle.sql");
const MIGRATION_0008: &str = include_str!("../migrations/0008_workflow_dag.sql");
const MIGRATION_0009: &str = include_str!("../migrations/0009_workflow_plans.sql");
const MIGRATION_0010: &str = include_str!("../migrations/0010_plan_policy.sql");
const MIGRATION_0011: &str = include_str!("../migrations/0011_workflow_replans.sql");
const MIGRATION_0012: &str = include_str!("../migrations/0012_workflow_recovery.sql");
const MIGRATION_0013: &str = include_str!("../migrations/0013_evaluations.sql");
const MIGRATION_0014: &str = include_str!("../migrations/0014_workflow_source_consensus.sql");
const MIGRATION_0015: &str = include_str!("../migrations/0015_competition_session_lanes.sql");
const MIGRATION_0016: &str = include_str!("../migrations/0016_provenance.sql");

#[tokio::test]
async fn phase4_database_upgrades_to_phase5_preserving_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");

    // 1. Build a Phase 4 database: 0001 schema + a legacy task + artifact.
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(MIGRATION_0001)
        .execute(&mut *tx)
        .await
        .expect("0001");
    let legacy_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO tasks (id, agent_id, status, prompt, workspace, error, created_at, started_at, completed_at)
         VALUES (?, 'codex', 'completed', 'legacy prompt', NULL, NULL, '2026-08-01T00:00:00+00:00', '2026-08-01T00:00:01+00:00', '2026-08-01T00:00:02+00:00')",
    )
    .bind(legacy_id.to_string())
    .execute(&mut *tx)
    .await
    .expect("insert legacy task");
    sqlx::query(
        "INSERT INTO artifacts (id, task_id, kind, name, mime_type, path, content, metadata, created_at)
         VALUES (?, ?, 'text', 'legacy.md', 'text/plain', NULL, 'hello', '{}', '2026-08-01T00:00:00+00:00')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(legacy_id.to_string())
    .execute(&mut *tx)
    .await
    .expect("insert legacy artifact");
    tx.commit().await.expect("commit");
    pool.close().await;

    // 2. Apply the 0002 migration on top.
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("reconnect");
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(MIGRATION_0002)
        .execute(&mut *tx)
        .await
        .expect("0002");
    tx.commit().await.expect("commit");

    // 3. The legacy task must survive with all data.
    let row = sqlx::query("SELECT * FROM tasks WHERE id = ?")
        .bind(legacy_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("legacy task still exists");
    assert_eq!(row.get::<String, _>("prompt"), "legacy prompt");
    assert_eq!(
        row.get::<String, _>("status"),
        TaskStatus::Completed.as_str()
    );
    assert_eq!(row.get::<String, _>("agent_id"), "codex");
    assert_eq!(
        row.get::<String, _>("created_at"),
        "2026-08-01T00:00:00+00:00"
    );
    let context_id: Option<String> = row.get("context_id");
    let session_id: Option<String> = row.get("agent_session_id");
    assert!(context_id.is_none(), "legacy tasks keep NULL context links");
    assert!(session_id.is_none());

    let artifact_count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM artifacts WHERE task_id = ?")
        .bind(legacy_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("count")
        .get("n");
    assert_eq!(artifact_count, 1);

    // 4. New tables exist and are usable.
    let tables: Vec<String> = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('contexts','agent_sessions')",
    )
    .fetch_all(&pool)
    .await
    .expect("list tables")
    .into_iter()
    .map(|row| row.get("name"))
    .collect();
    assert_eq!(tables.len(), 2);
    pool.close().await;
}

#[tokio::test]
async fn phase5_database_upgrades_to_phase6_preserving_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");

    // Build a Phase 5 database: 0001 + 0002 + data.
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(MIGRATION_0001)
        .execute(&mut *tx)
        .await
        .expect("0001");
    sqlx::query(MIGRATION_0002)
        .execute(&mut *tx)
        .await
        .expect("0002");
    let context_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contexts (id, created_at, updated_at) VALUES (?, ?, ?)")
        .bind(context_id.to_string())
        .bind("2026-08-01T00:00:00+00:00")
        .bind("2026-08-01T00:00:00+00:00")
        .execute(&mut *tx)
        .await
        .expect("context");
    sqlx::query(
        "INSERT INTO agent_sessions (id, context_id, agent_id, native_session_id, workspace, created_at, updated_at)
         VALUES (?, ?, 'claude', 'native-1', '/tmp/ws', '2026-08-01T00:00:00+00:00', '2026-08-01T00:00:00+00:00')",
    )
    .bind(session_id.to_string())
    .bind(context_id.to_string())
    .execute(&mut *tx).await.expect("session");
    sqlx::query(
        "INSERT INTO tasks (id, agent_id, status, prompt, created_at, context_id, agent_session_id)
         VALUES (?, 'claude', 'completed', 'p5 task', '2026-08-01T00:00:00+00:00', ?, ?)",
    )
    .bind(task_id.to_string())
    .bind(context_id.to_string())
    .bind(session_id.to_string())
    .execute(&mut *tx)
    .await
    .expect("task");
    tx.commit().await.expect("commit");
    pool.close().await;

    // Apply 0003.
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("reconnect");
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(MIGRATION_0003)
        .execute(&mut *tx)
        .await
        .expect("0003");
    tx.commit().await.expect("commit");

    // All Phase 5 data must survive; the workspaces table must be usable.
    let context_count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM contexts")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get("n");
    assert_eq!(context_count, 1);
    let session_count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM agent_sessions")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get("n");
    assert_eq!(session_count, 1);
    let task = sqlx::query("SELECT prompt FROM tasks WHERE id = ?")
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("task survives");
    assert_eq!(task.get::<String, _>("prompt"), "p5 task");

    sqlx::query(
        "INSERT INTO workspaces (id, agent_session_id, repository_root, path, branch, base_revision, state, created_at, updated_at)
         VALUES (?, ?, '/tmp/repo', '/tmp/ws', 'agentmesh/claude/abc', 'deadbeef', 'active', '2026-08-01T00:00:00+00:00', '2026-08-01T00:00:00+00:00')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(session_id.to_string())
    .execute(&pool).await.expect("workspace usable");
    pool.close().await;
}

#[tokio::test]
async fn fresh_database_has_all_phase6_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    for table in [
        "tasks",
        "artifacts",
        "contexts",
        "agent_sessions",
        "workspaces",
        "workflows",
        "workflow_steps",
        "applies",
    ] {
        let row = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table)
            .fetch_one(db.pool())
            .await
            .expect("table exists");
        assert_eq!(row.get::<String, _>("name"), table);
    }
}

#[tokio::test]
async fn phase17_plan_database_upgrades_to_phase18_backfilling_revision_1() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");

    // 1. Build a Phase 17 database (0001..0009) with a plan that already has
    //    a stored, validated plan JSON.
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");
    let mut tx = pool.begin().await.expect("begin");
    for migration in [
        MIGRATION_0001,
        MIGRATION_0002,
        MIGRATION_0003,
        MIGRATION_0004,
        MIGRATION_0005,
        MIGRATION_0006,
        MIGRATION_0007,
        MIGRATION_0008,
        MIGRATION_0009,
    ] {
        sqlx::query(migration)
            .execute(&mut *tx)
            .await
            .expect("migration");
    }
    let plan_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO workflow_plans
            (id, goal, status, planner_agent_id, planner_task_id, plan_json, validation_error, workflow_id, created_at, updated_at, executed_at)
         VALUES (?, 'goal', 'ready', 'claude', NULL, '{\"version\":1}', NULL, NULL, '2026-08-12T00:00:00+00:00', '2026-08-12T00:00:00+00:00', NULL)",
    )
    .bind(plan_id.to_string())
    .execute(&mut *tx)
    .await
    .expect("insert plan");
    tx.commit().await.expect("commit");
    pool.close().await;

    // 2. Apply the 0010 migration on top.
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("reconnect");
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(MIGRATION_0010)
        .execute(&mut *tx)
        .await
        .expect("0010");
    tx.commit().await.expect("commit");

    // 3. The legacy plan was backfilled as revision 1 (the original planner
    //    output) and current_revision points at it, so a later `plan edit`
    //    appends revision 2 without losing the planner output.
    let row = sqlx::query("SELECT * FROM workflow_plans WHERE id = ?")
        .bind(plan_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("plan survives");
    assert_eq!(row.get::<String, _>("status"), "ready");
    assert_eq!(row.get::<String, _>("plan_json"), "{\"version\":1}");
    assert_eq!(row.get::<i64, _>("current_revision"), 1);
    let rev: sqlx::sqlite::SqliteRow =
        sqlx::query("SELECT * FROM workflow_plan_revisions WHERE plan_id = ?")
            .bind(plan_id.to_string())
            .fetch_one(&pool)
            .await
            .expect("revision row backfilled");
    assert_eq!(rev.get::<i64, _>("revision"), 1);
    assert_eq!(rev.get::<String, _>("source"), "planner");
    assert_eq!(rev.get::<String, _>("plan_json"), "{\"version\":1}");

    // 4. The new revision table is usable (a user edit appends revision 2).
    sqlx::query(
        "INSERT INTO workflow_plan_revisions (id, plan_id, revision, plan_json, source, created_at)
         VALUES (?, ?, 2, '{\"version\":2}', 'user_edit', '2026-08-12T00:01:00+00:00')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(plan_id.to_string())
    .execute(&pool)
    .await
    .expect("append revision 2");
    pool.close().await;
}

#[tokio::test]
async fn fresh_database_has_phase18_plan_revision_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_plan_revisions'",
    )
    .fetch_one(db.pool())
    .await
    .expect("revision table exists");
    assert_eq!(row.get::<String, _>("name"), "workflow_plan_revisions");
    // The new plan columns exist and default to NULL.
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(workflow_plans)")
        .fetch_all(db.pool())
        .await
        .expect("plan columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    for column in [
        "current_revision",
        "execution_claimed_at",
        "executed_revision",
    ] {
        assert!(
            columns.iter().any(|c| c == column),
            "missing column {column}"
        );
    }
}

#[tokio::test]
async fn fresh_database_has_phase19_replan_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    // The replan table exists.
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_replans'",
    )
    .fetch_one(db.pool())
    .await
    .expect("replan table exists");
    assert_eq!(row.get::<String, _>("name"), "workflow_replans");
    // workflows.graph_revision defaults to 1; workflow_steps.objective is NULL.
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(workflows)")
        .fetch_all(db.pool())
        .await
        .expect("workflow columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert!(
        columns.iter().any(|c| c == "graph_revision"),
        "workflows.graph_revision missing"
    );
    let step_columns: Vec<String> = sqlx::query("PRAGMA table_info(workflow_steps)")
        .fetch_all(db.pool())
        .await
        .expect("step columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert!(
        step_columns.iter().any(|c| c == "objective"),
        "workflow_steps.objective missing"
    );
}

#[tokio::test]
async fn fresh_database_has_phase20_recovery_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_recoveries'",
    )
    .fetch_one(db.pool())
    .await
    .expect("recovery table exists");
    assert_eq!(row.get::<String, _>("name"), "workflow_recoveries");
    // workflows gains the parent lineage columns.
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(workflows)")
        .fetch_all(db.pool())
        .await
        .expect("workflow columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    for column in [
        "parent_workflow_id",
        "recovery_of_node_id",
        "recovery_attempt",
    ] {
        assert!(
            columns.iter().any(|c| c == column),
            "missing column {column}"
        );
    }
}

#[tokio::test]
async fn fresh_database_has_phase21_evaluation_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");
    for table in ["evaluation_groups", "evaluation_members"] {
        let row = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table)
            .fetch_one(db.pool())
            .await
            .expect("table exists");
        assert_eq!(row.get::<String, _>("name"), table);
    }
    // The members table carries the node_id mapping (Phase 21 §10).
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(evaluation_members)")
        .fetch_all(db.pool())
        .await
        .expect("member columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert!(columns.iter().any(|c| c == "node_id"), "missing node_id");
}

#[tokio::test]
async fn fresh_database_has_phase22_source_and_round_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");

    // workflows.source_workspace: explicit source project (Phase 22 §2).
    let workflow_columns: Vec<String> = sqlx::query("PRAGMA table_info(workflows)")
        .fetch_all(db.pool())
        .await
        .expect("workflow columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert!(
        workflow_columns.iter().any(|c| c == "source_workspace"),
        "workflows.source_workspace missing: {workflow_columns:?}"
    );

    // evaluation_groups.round: which consensus fix round (Phase 22 §13), with
    // a NOT NULL DEFAULT 0 so legacy rows backfill.
    let group_columns: Vec<String> = sqlx::query("PRAGMA table_info(evaluation_groups)")
        .fetch_all(db.pool())
        .await
        .expect("group columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert!(
        group_columns.iter().any(|c| c == "round"),
        "evaluation_groups.round missing: {group_columns:?}"
    );
    let dflt: String = sqlx::query(
        "SELECT dflt_value FROM pragma_table_info('evaluation_groups') WHERE name = 'round'",
    )
    .fetch_one(db.pool())
    .await
    .expect("round default")
    .get("dflt_value");
    assert_eq!(dflt, "0", "legacy groups backfill to round 0");

    // A fresh evaluation group defaults to round 0.
    let workflow_id = Uuid::new_v4();
    let row = agentmesh_storage::EvaluationGroupRow {
        id: Uuid::new_v4(),
        workflow_id,
        source_task_id: None,
        strategy: "majority".to_string(),
        quorum: 2,
        status: "pending".to_string(),
        consensus: None,
        snapshot_hash: None,
        round: 0,
        created_at: "2026-08-13T00:00:00+00:00".to_string(),
        completed_at: None,
    };
    let repo = agentmesh_storage::EvaluationRepository::new(db.clone());
    repo.create_group(&row).await.expect("create group");
    let loaded = repo.list_groups(workflow_id).await.expect("list");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].round, 0);

    // A round-1 group sorts first (newest round).
    let round1 = agentmesh_storage::EvaluationGroupRow {
        id: Uuid::new_v4(),
        round: 1,
        ..row.clone()
    };
    repo.create_group(&round1).await.expect("create round 1");
    let loaded = repo.list_groups(workflow_id).await.expect("list");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].round, 1, "round DESC ordering");
    assert_eq!(loaded[1].round, 0);
}

#[tokio::test]
async fn fresh_database_has_phase24_provenance_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = agentmesh_storage::Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open");

    let columns: Vec<String> = sqlx::query("PRAGMA table_info(provenance_events)")
        .fetch_all(db.pool())
        .await
        .expect("provenance columns")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();

    assert!(columns.iter().any(|c| c == "id"), "missing id");
    assert!(
        columns.iter().any(|c| c == "workflow_id"),
        "missing workflow_id"
    );
    assert!(columns.iter().any(|c| c == "sequence"), "missing sequence");
    assert!(
        columns.iter().any(|c| c == "event_type"),
        "missing event_type"
    );
    assert!(
        columns.iter().any(|c| c == "payload_hash"),
        "missing payload_hash"
    );
    assert!(
        columns.iter().any(|c| c == "previous_hash"),
        "missing previous_hash"
    );
    assert!(
        columns.iter().any(|c| c == "event_hash"),
        "missing event_hash"
    );
}

#[tokio::test]
async fn full_migration_chain_from_0001_to_0016_upgrades_cleanly_preserving_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");

    // Connect and run 0001
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);
    let pool = SqlitePool::connect_with(options).await.expect("connect");
    let migrations = [
        MIGRATION_0001,
        MIGRATION_0002,
        MIGRATION_0003,
        MIGRATION_0004,
        MIGRATION_0005,
        MIGRATION_0006,
        MIGRATION_0007,
        MIGRATION_0008,
        MIGRATION_0009,
        MIGRATION_0010,
        MIGRATION_0011,
        MIGRATION_0012,
        MIGRATION_0013,
        MIGRATION_0014,
        MIGRATION_0015,
        MIGRATION_0016,
    ];

    for (idx, sql) in migrations.iter().enumerate() {
        let mut tx = pool.begin().await.expect("begin");
        sqlx::query(sql)
            .execute(&mut *tx)
            .await
            .unwrap_or_else(|err| panic!("failed applying migration {:04}: {err}", idx + 1));
        tx.commit().await.expect("commit");

        if idx == 0 {
            let task_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO tasks (id, agent_id, status, prompt, workspace, error, created_at, started_at, completed_at)
                 VALUES (?, 'codex', 'completed', 'genesis prompt', NULL, NULL, '2026-08-01T00:00:00+00:00', '2026-08-01T00:00:01+00:00', '2026-08-01T00:00:02+00:00')",
            )
            .bind(task_id.to_string())
            .execute(&pool)
            .await
            .expect("insert task");
        }
    }

    // Verify all 16 migration tables exist
    let tables: Vec<String> = sqlx::query("SELECT name FROM sqlite_master WHERE type='table'")
        .fetch_all(&pool)
        .await
        .expect("tables")
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();

    for expected in [
        "tasks",
        "artifacts",
        "contexts",
        "agent_sessions",
        "workspaces",
        "workflows",
        "workflow_steps",
        "applies",
        "workflow_step_dependencies",
        "workflow_plans",
        "workflow_plan_revisions",
        "workflow_replans",
        "workflow_recoveries",
        "evaluation_groups",
        "evaluation_members",
        "competition_groups",
        "competition_candidates",
        "provenance_events",
    ] {
        assert!(
            tables.iter().any(|t| t == expected),
            "table `{expected}` must exist after full migration"
        );
    }

    let task_count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM tasks")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get("n");
    assert_eq!(task_count, 1, "genesis task must survive all 16 migrations");
}
