//! Migration upgrade test: a Phase 4 database must survive the 0002
//! migration with all task data intact.

use agentmesh_core::TaskStatus;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

const MIGRATION_0001: &str = include_str!("../migrations/0001_initial.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_context_sessions.sql");
const MIGRATION_0003: &str = include_str!("../migrations/0003_workspaces.sql");

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
    ] {
        let row = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .bind(table)
            .fetch_one(db.pool())
            .await
            .expect("table exists");
        assert_eq!(row.get::<String, _>("name"), table);
    }
}
