//! Repository tests against a temporary SQLite database.

use agentmesh_core::{AgentMessage, AgentTask, Artifact, ArtifactKind, TaskStatus};
use agentmesh_storage::{ArtifactRepository, Database, TaskFilter, TaskRepository};
use uuid::Uuid;

async fn test_database() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let db = Database::open(&path).await.expect("open database");
    (db, dir)
}

fn task(agent_id: &str, prompt: &str) -> AgentTask {
    AgentTask::new(agent_id, AgentMessage::user(prompt))
}

fn artifact(name: &str) -> Artifact {
    Artifact::text(name, "content")
}

#[tokio::test]
async fn create_and_get_roundtrip() {
    let (db, _dir) = test_database().await;
    let repo = TaskRepository::new(db.clone());
    let t = task("mock", "hello");

    repo.create(&t).await.expect("create");
    let loaded = repo.get(t.id).await.expect("get").expect("exists");

    assert_eq!(loaded.id, t.id);
    assert_eq!(loaded.agent_id, "mock");
    assert_eq!(loaded.status, TaskStatus::Submitted);
    assert_eq!(loaded.input.content, "hello");
    assert_eq!(loaded.created_at, t.created_at);
}

#[tokio::test]
async fn get_missing_task_returns_none() {
    let (db, _dir) = test_database().await;
    let repo = TaskRepository::new(db);
    assert!(repo.get(Uuid::new_v4()).await.expect("get").is_none());
}

#[tokio::test]
async fn list_is_newest_first_with_limit() {
    let (db, _dir) = test_database().await;
    let repo = TaskRepository::new(db.clone());
    let mut ids = Vec::new();
    for i in 0..3 {
        let t = task("mock", &format!("prompt {i}"));
        repo.create(&t).await.expect("create");
        ids.push(t.id);
    }

    let all = repo
        .list(&TaskFilter::default().limit(50))
        .await
        .expect("list");
    assert_eq!(all.len(), 3);
    // Newest first (created_at DESC).
    assert_eq!(all[0].id, ids[2]);

    let limited = repo
        .list(&TaskFilter::default().limit(2))
        .await
        .expect("list");
    assert_eq!(limited.len(), 2);

    let by_agent = repo
        .list(&TaskFilter::default().agent("nope").limit(50))
        .await
        .expect("list");
    assert!(by_agent.is_empty());
}

#[tokio::test]
async fn status_and_timestamps_update() {
    let (db, _dir) = test_database().await;
    let repo = TaskRepository::new(db.clone());
    let t = task("codex", "hi");
    repo.create(&t).await.expect("create");

    assert!(repo.mark_started(t.id).await.expect("mark started"));
    let loaded = repo.get(t.id).await.expect("get").expect("exists");
    assert_eq!(loaded.status, TaskStatus::Working);
    assert!(loaded.started_at.is_some());
    assert!(loaded.completed_at.is_none());

    assert!(repo.mark_completed(t.id).await.expect("mark completed"));
    let loaded = repo.get(t.id).await.expect("get").expect("exists");
    assert_eq!(loaded.status, TaskStatus::Completed);
    assert!(loaded.completed_at.is_some());
}

#[tokio::test]
async fn error_persists_with_failed_status() {
    let (db, _dir) = test_database().await;
    let repo = TaskRepository::new(db.clone());
    let t = task("codex", "hi");
    repo.create(&t).await.expect("create");

    assert!(repo.set_error(t.id, "boom").await.expect("set error"));
    let loaded = repo.get(t.id).await.expect("get").expect("exists");
    assert_eq!(loaded.status, TaskStatus::Failed);
    assert_eq!(loaded.error.as_deref(), Some("boom"));
    assert!(loaded.completed_at.is_some());

    let by_status = repo
        .list(&TaskFilter::default().status(TaskStatus::Failed).limit(50))
        .await
        .expect("list");
    assert_eq!(by_status.len(), 1);
}

#[tokio::test]
async fn terminal_state_cannot_be_left() {
    let (db, _dir) = test_database().await;
    let repo = TaskRepository::new(db.clone());
    let t = task("mock", "hi");
    repo.create(&t).await.expect("create");

    assert!(repo.mark_completed(t.id).await.expect("complete"));
    // Attempting to go back to working must be rejected.
    assert!(
        !repo
            .set_status(t.id, TaskStatus::Working)
            .await
            .expect("rejected")
    );
    let loaded = repo.get(t.id).await.expect("get").expect("exists");
    assert_eq!(loaded.status, TaskStatus::Completed);

    // Failed tasks cannot be overwritten by a second error either.
    let t2 = task("mock", "hi");
    repo.create(&t2).await.expect("create");
    assert!(repo.set_error(t2.id, "first").await.expect("set error"));
    assert!(!repo.set_error(t2.id, "second").await.expect("rejected"));
    let loaded = repo.get(t2.id).await.expect("get").expect("exists");
    assert_eq!(loaded.error.as_deref(), Some("first"));
}

#[tokio::test]
async fn artifacts_insert_and_list() {
    let (db, _dir) = test_database().await;
    let tasks = TaskRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let t = task("mock", "hi");
    tasks.create(&t).await.expect("create task");

    let mut a1 = artifact("summary.md");
    a1.metadata.insert("key".to_string(), "value".to_string());
    artifacts.insert(t.id, &a1).await.expect("insert artifact");

    let listed = artifacts.list_by_task(t.id).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, a1.id);
    assert_eq!(listed[0].kind, ArtifactKind::Text);
    assert_eq!(listed[0].content_as_str(), Some("content"));
    assert_eq!(
        listed[0].metadata.get("key").map(String::as_str),
        Some("value")
    );
}

#[tokio::test]
async fn artifacts_cascade_delete_with_task() {
    let (db, _dir) = test_database().await;
    let tasks = TaskRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let t = task("mock", "hi");
    tasks.create(&t).await.expect("create task");
    artifacts
        .insert(t.id, &artifact("a.txt"))
        .await
        .expect("insert");

    // Deleting the task must remove its artifacts (foreign key cascade).
    sqlx::query("DELETE FROM tasks WHERE id = ?")
        .bind(t.id.to_string())
        .execute(db.pool())
        .await
        .expect("delete task");

    let listed = artifacts.list_by_task(t.id).await.expect("list");
    assert!(listed.is_empty());
}

#[tokio::test]
async fn artifact_kind_roundtrip() {
    let (db, _dir) = test_database().await;
    let tasks = TaskRepository::new(db.clone());
    let artifacts = ArtifactRepository::new(db.clone());
    let t = task("mock", "hi");
    tasks.create(&t).await.expect("create task");

    let kinds = [
        ArtifactKind::Text,
        ArtifactKind::File,
        ArtifactKind::Patch,
        ArtifactKind::Json,
        ArtifactKind::Log,
        ArtifactKind::TestResult,
    ];
    for kind in kinds {
        let mut a = artifact("kind.txt");
        a.kind = kind;
        artifacts.insert(t.id, &a).await.expect("insert");
    }

    let listed = artifacts.list_by_task(t.id).await.expect("list");
    assert_eq!(listed.len(), kinds.len());
    let mut roundtripped: Vec<ArtifactKind> = listed.iter().map(|a| a.kind).collect();
    roundtripped.sort();
    let mut expected: Vec<ArtifactKind> = kinds.to_vec();
    expected.sort();
    assert_eq!(roundtripped, expected);
}
