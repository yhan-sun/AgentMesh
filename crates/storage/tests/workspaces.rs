//! Workspace repository and artifact store tests.

use agentmesh_storage::{
    ArtifactRepository, ArtifactStore, Database, WorkspaceRepository, WorkspaceRow, WorkspaceState,
};
use uuid::Uuid;

async fn test_database() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agentmesh.db");
    let db = Database::open(&path).await.expect("open");
    (db, dir)
}

fn workspace_row(agent_session_id: Uuid) -> WorkspaceRow {
    let now = chrono::Utc::now();
    WorkspaceRow {
        id: Uuid::new_v4(),
        agent_session_id,
        repository_root: std::path::PathBuf::from("/tmp/repo"),
        path: std::path::PathBuf::from("/tmp/ws"),
        branch: "agentmesh/claude/abc12345".to_string(),
        base_revision: "0123456789abcdef".to_string(),
        state: WorkspaceState::Active,
        created_at: now,
        updated_at: now,
    }
}

#[tokio::test]
async fn workspace_create_get_roundtrip() {
    let (db, _dir) = test_database().await;
    let repo = WorkspaceRepository::new(db.clone());
    let row = workspace_row(Uuid::new_v4());
    repo.create(&row).await.expect("create");
    let loaded = repo.get(row.id).await.expect("get").expect("exists");
    assert_eq!(loaded.id, row.id);
    assert_eq!(loaded.agent_session_id, row.agent_session_id);
    assert_eq!(loaded.branch, "agentmesh/claude/abc12345");
    assert_eq!(loaded.base_revision, "0123456789abcdef");
    assert_eq!(loaded.state, WorkspaceState::Active);
}

#[tokio::test]
async fn workspace_get_by_session_and_path() {
    let (db, _dir) = test_database().await;
    let repo = WorkspaceRepository::new(db.clone());
    let session_id = Uuid::new_v4();
    let row = workspace_row(session_id);
    repo.create(&row).await.expect("create");

    let by_session = repo
        .get_by_agent_session(session_id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(by_session.id, row.id);
    let by_path = repo
        .get_by_path(&row.path)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(by_path.id, row.id);
}

#[tokio::test]
async fn workspace_unique_session_and_path() {
    let (db, _dir) = test_database().await;
    let repo = WorkspaceRepository::new(db.clone());
    let session_id = Uuid::new_v4();
    let first = workspace_row(session_id);
    repo.create(&first).await.expect("first");

    let mut second = workspace_row(Uuid::new_v4());
    second.agent_session_id = session_id;
    assert!(
        repo.create(&second).await.is_err(),
        "unique session violated"
    );

    let mut third = workspace_row(Uuid::new_v4());
    third.path = first.path.clone();
    assert!(repo.create(&third).await.is_err(), "unique path violated");
}

#[tokio::test]
async fn workspace_state_update() {
    let (db, _dir) = test_database().await;
    let repo = WorkspaceRepository::new(db.clone());
    let row = workspace_row(Uuid::new_v4());
    repo.create(&row).await.expect("create");
    repo.set_state(row.id, WorkspaceState::Missing)
        .await
        .expect("set state");
    let loaded = repo.get(row.id).await.expect("get").expect("exists");
    assert_eq!(loaded.state, WorkspaceState::Missing);
}

#[tokio::test]
async fn artifact_store_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ArtifactStore::new(dir.path().to_path_buf());
    let task_id = Uuid::new_v4();
    let artifact_id = Uuid::new_v4();
    let path = store
        .store(task_id, artifact_id, b"large content")
        .expect("store");
    assert!(path.ends_with(format!("{artifact_id}.bin")));
    let loaded = store.load(&path).expect("load");
    assert_eq!(loaded, b"large content");
}

#[tokio::test]
async fn oversized_artifact_goes_to_file_store() {
    let (db, _dir) = test_database().await;
    let store_dir = tempfile::tempdir().expect("tempdir");
    let store = ArtifactStore::new(store_dir.path().to_path_buf());
    let artifacts = ArtifactRepository::with_store(db.clone(), store);
    let tasks = agentmesh_storage::TaskRepository::new(db.clone());
    let task = agentmesh_core::AgentTask::new("mock", agentmesh_core::AgentMessage::user("hi"));
    tasks.create(&task).await.expect("create task");

    let mut big = agentmesh_core::Artifact::text("big.patch", "");
    big.content = vec![b'x'; 300 * 1024];
    artifacts.insert(task.id, &big).await.expect("insert");

    let listed = artifacts.list_by_task(task.id).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert!(listed[0].content.is_empty(), "content must not be inline");
    let stored_path = listed[0].path.clone().expect("file path");
    assert!(stored_path.exists(), "file must exist on disk");
    let on_disk = std::fs::read(&stored_path).expect("read file");
    assert_eq!(on_disk.len(), 300 * 1024);
}
