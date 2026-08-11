//! Context and agent session repository tests.

use agentmesh_core::{AgentMessage, AgentSession, AgentTask, Context};
use agentmesh_storage::{AgentSessionRepository, ContextRepository, Database, TaskRepository};
use uuid::Uuid;

async fn test_database() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let db = Database::open(&path).await.expect("open database");
    (db, dir)
}

#[tokio::test]
async fn create_and_get_context() {
    let (db, _dir) = test_database().await;
    let repo = ContextRepository::new(db.clone());
    let context = Context::new();
    repo.create(&context).await.expect("create");
    let loaded = repo.get(context.id).await.expect("get").expect("exists");
    assert_eq!(loaded.id, context.id);
    assert_eq!(loaded.created_at, context.created_at);
}

#[tokio::test]
async fn get_missing_context_returns_none() {
    let (db, _dir) = test_database().await;
    let repo = ContextRepository::new(db);
    assert!(repo.get(Uuid::new_v4()).await.expect("get").is_none());
}

#[tokio::test]
async fn touch_updates_updated_at() {
    let (db, _dir) = test_database().await;
    let repo = ContextRepository::new(db.clone());
    let context = Context::new();
    repo.create(&context).await.expect("create");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    repo.touch(context.id).await.expect("touch");
    let loaded = repo.get(context.id).await.expect("get").expect("exists");
    assert!(loaded.updated_at > context.updated_at);
}

#[tokio::test]
async fn session_create_get_and_native_session_id_null() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let context = Context::new();
    contexts.create(&context).await.expect("create context");

    let session = AgentSession::new(context.id, "claude");
    sessions.create(&session).await.expect("create session");
    let loaded = sessions
        .get(session.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(loaded.id, session.id);
    assert_eq!(loaded.context_id, context.id);
    assert_eq!(loaded.agent_id, "claude");
    assert!(loaded.native_session_id.is_none());
}

#[tokio::test]
async fn session_get_by_context_agent() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let context = Context::new();
    contexts.create(&context).await.expect("create context");

    let session = AgentSession::new(context.id, "codex");
    sessions.create(&session).await.expect("create session");

    let found = sessions
        .get_by_context_agent(context.id, "codex")
        .await
        .expect("query")
        .expect("exists");
    assert_eq!(found.id, session.id);
    let none = sessions
        .get_by_context_agent(context.id, "claude")
        .await
        .expect("query");
    assert!(none.is_none());
}

#[tokio::test]
async fn unique_context_agent_pair() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let context = Context::new();
    contexts.create(&context).await.expect("create context");

    let first = AgentSession::new(context.id, "claude");
    sessions.create(&first).await.expect("first create");
    let second = AgentSession::new(context.id, "claude");
    let err = sessions.create(&second).await;
    assert!(err.is_err(), "duplicate (context, agent) must fail");
}

#[tokio::test]
async fn set_native_session_id_and_update() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let context = Context::new();
    contexts.create(&context).await.expect("create context");
    let session = AgentSession::new(context.id, "claude");
    sessions.create(&session).await.expect("create session");

    sessions
        .set_native_session_id(session.id, "native-1")
        .await
        .expect("set");
    let loaded = sessions
        .get(session.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(loaded.native_session_id.as_deref(), Some("native-1"));

    // Update to a newer native id is allowed.
    sessions
        .set_native_session_id(session.id, "native-2")
        .await
        .expect("update");
    let loaded = sessions
        .get(session.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(loaded.native_session_id.as_deref(), Some("native-2"));
}

#[tokio::test]
async fn workspace_roundtrip() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let context = Context::new();
    contexts.create(&context).await.expect("create context");

    let mut session = AgentSession::new(context.id, "codex");
    session.workspace = Some(std::path::PathBuf::from("/tmp/project-a"));
    sessions.create(&session).await.expect("create session");
    let loaded = sessions
        .get(session.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        loaded.workspace.as_deref(),
        Some(std::path::Path::new("/tmp/project-a"))
    );
}

#[tokio::test]
async fn session_cascades_when_context_deleted() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let context = Context::new();
    contexts.create(&context).await.expect("create context");
    let session = AgentSession::new(context.id, "claude");
    sessions.create(&session).await.expect("create session");

    sqlx::query("DELETE FROM contexts WHERE id = ?")
        .bind(context.id.to_string())
        .execute(db.pool())
        .await
        .expect("delete context");

    assert!(sessions.get(session.id).await.expect("get").is_none());
}

#[tokio::test]
async fn create_run_setup_persists_all_three() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let tasks = TaskRepository::new(db.clone());

    let context = Context::new();
    let mut session = AgentSession::new(context.id, "mock");
    session.workspace = Some(std::path::PathBuf::from("/tmp/proj"));
    let mut task = AgentTask::new("mock", AgentMessage::user("hello"));
    task.context_id = context.id;
    task.agent_session_id = Some(session.id);

    contexts
        .create_run_setup(&context, &session, &task)
        .await
        .expect("run setup");

    assert!(contexts.get(context.id).await.expect("get").is_some());
    let session_row = sqlx::query("SELECT id FROM agent_sessions WHERE id = ?")
        .bind(session.id.to_string())
        .fetch_optional(db.pool())
        .await
        .expect("query session");
    assert!(session_row.is_some());
    let loaded = tasks.get(task.id).await.expect("get").expect("exists");
    assert_eq!(loaded.context_id, context.id);
    assert_eq!(loaded.agent_session_id, Some(session.id));
}

#[tokio::test]
async fn resume_task_creation_touches_context_and_session() {
    let (db, _dir) = test_database().await;
    let contexts = ContextRepository::new(db.clone());
    let sessions = AgentSessionRepository::new(db.clone());
    let tasks = TaskRepository::new(db.clone());

    let context = Context::new();
    contexts.create(&context).await.expect("create context");
    let session = AgentSession::new(context.id, "claude");
    sessions.create(&session).await.expect("create session");
    let original_context_updated = context.updated_at;
    let original_session_updated = session.updated_at;

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let mut task = AgentTask::new("claude", AgentMessage::user("resume prompt"));
    task.context_id = context.id;
    task.agent_session_id = Some(session.id);
    contexts
        .create_task_for_context(&context, &session, &task)
        .await
        .expect("resume task");

    let loaded_context = contexts
        .get(context.id)
        .await
        .expect("get")
        .expect("exists");
    assert!(loaded_context.updated_at > original_context_updated);
    let loaded_session = sessions
        .get(session.id)
        .await
        .expect("get")
        .expect("exists");
    assert!(loaded_session.updated_at > original_session_updated);
    assert!(tasks.get(task.id).await.expect("get").is_some());
}
