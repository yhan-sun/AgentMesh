//! ApplyRepository tests: the Phase 13 apply persistence table.

use std::path::PathBuf;

use agentmesh_storage::{ApplyRepository, ApplyRow, ApplyStatus, ClaimResult, Database};
use uuid::Uuid;

async fn repo() -> (ApplyRepository, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("db");
    (ApplyRepository::new(db), dir)
}

fn row(id: Uuid) -> ApplyRow {
    ApplyRow {
        id,
        task_id: Some(Uuid::new_v4()),
        workflow_id: None,
        workspace_id: Uuid::new_v4(),
        source_repository: PathBuf::from("/project"),
        base_revision: "abc123".to_string(),
        status: ApplyStatus::Applying,
        error: None,
        created_at: "2026-08-01T00:00:00+00:00".to_string(),
        completed_at: None,
        workspace_snapshot_hash: None,
    }
}

#[tokio::test]
async fn create_and_get_roundtrip() {
    let (repo, _dir) = repo().await;
    let id = Uuid::new_v4();
    repo.create(&row(id)).await.expect("create");
    let loaded = repo.get(id).await.expect("get").expect("exists");
    assert_eq!(loaded.id, id);
    assert_eq!(loaded.status, ApplyStatus::Applying);
    assert_eq!(loaded.source_repository, PathBuf::from("/project"));
}

#[tokio::test]
async fn status_transitions_and_idempotency_guard() {
    let (repo, _dir) = repo().await;
    let id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let mut r = row(id);
    r.workspace_id = workspace_id;
    repo.create(&r).await.expect("create");

    // No completed apply yet: the idempotency guard reports false.
    assert!(
        !repo
            .has_completed_for_workspace(workspace_id)
            .await
            .expect("guard")
    );

    repo.mark_completed(id).await.expect("complete");
    let loaded = repo.get(id).await.unwrap().unwrap();
    assert_eq!(loaded.status, ApplyStatus::Completed);
    assert!(loaded.completed_at.is_some());

    // After a completed apply the guard blocks a second apply.
    assert!(
        repo.has_completed_for_workspace(workspace_id)
            .await
            .expect("guard")
    );

    // A different workspace stays clear.
    assert!(
        !repo
            .has_completed_for_workspace(Uuid::new_v4())
            .await
            .expect("guard")
    );
}

#[tokio::test]
async fn failed_apply_records_error() {
    let (repo, _dir) = repo().await;
    let id = Uuid::new_v4();
    repo.create(&row(id)).await.expect("create");
    repo.mark_failed(id, "git apply failed")
        .await
        .expect("fail");
    let loaded = repo.get(id).await.unwrap().unwrap();
    assert_eq!(loaded.status, ApplyStatus::Failed);
    assert_eq!(loaded.error.as_deref(), Some("git apply failed"));
    assert!(loaded.completed_at.is_some());
}

#[tokio::test]
async fn list_orders_newest_first() {
    let (repo, _dir) = repo().await;
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    repo.create(&row(a)).await.expect("create a");
    repo.create(&row(b)).await.expect("create b");
    let rows = repo.list().await.expect("list");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn claim_workspace_is_atomic_per_workspace() {
    let (repo, _dir) = repo().await;
    let workspace_id = Uuid::new_v4();

    // First claim wins.
    let mut first = row(Uuid::new_v4());
    first.workspace_id = workspace_id;
    assert_eq!(
        repo.claim_workspace(&first).await.unwrap(),
        ClaimResult::Claimed
    );

    // A concurrent claim while `applying` → InProgress.
    let mut second = row(Uuid::new_v4());
    second.workspace_id = workspace_id;
    assert_eq!(
        repo.claim_workspace(&second).await.unwrap(),
        ClaimResult::InProgress
    );

    // After completion → AlreadyCompleted.
    repo.mark_completed(first.id).await.unwrap();
    let mut third = row(Uuid::new_v4());
    third.workspace_id = workspace_id;
    assert_eq!(
        repo.claim_workspace(&third).await.unwrap(),
        ClaimResult::AlreadyCompleted
    );

    // A failed apply releases the claim (a retry can claim again).
    let other_workspace = Uuid::new_v4();
    let mut failed = row(Uuid::new_v4());
    failed.workspace_id = other_workspace;
    repo.claim_workspace(&failed).await.unwrap();
    repo.mark_failed(failed.id, "boom").await.unwrap();
    let mut retry = row(Uuid::new_v4());
    retry.workspace_id = other_workspace;
    assert_eq!(
        repo.claim_workspace(&retry).await.unwrap(),
        ClaimResult::Claimed
    );
}
