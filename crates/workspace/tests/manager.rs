//! WorkspaceManager tests against temporary Git repositories.

use std::path::Path;
use std::process::Command as StdCommand;

use agentmesh_core::AgentSession;
use agentmesh_storage::{Database, WorkspaceRepository};
use agentmesh_workspace::{WorkspaceError, WorkspaceManager};
use uuid::Uuid;

fn git(dir: &Path, args: &[&str]) {
    let status = StdCommand::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .expect("git runs");
    assert!(status.success(), "git {args:?} failed");
}

fn git_output(dir: &Path, args: &[&str]) -> String {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git runs");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Create a clean repo with a committed file; returns (tempdir, root).
fn clean_repo() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().canonicalize().expect("canonicalize");
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.name", "AgentMesh Test"]);
    git(
        &root,
        &["config", "user.email", "agentmesh@example.invalid"],
    );
    std::fs::write(root.join("foo.txt"), "base\n").expect("write");
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "-m", "initial"]);
    (dir, root)
}

async fn test_manager(root: &Path) -> (WorkspaceManager, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open db");
    let repo = WorkspaceRepository::new(db);
    let manager = WorkspaceManager::new(repo, dir.path().join("worktrees"));
    let _ = root;
    (manager, dir)
}

fn session(agent: &str) -> AgentSession {
    AgentSession::new(Uuid::new_v4(), agent)
}

#[tokio::test]
async fn discover_repository_finds_root() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let discovered = manager.discover_repository(&root).await.expect("discover");
    assert_eq!(discovered, root);
}

#[tokio::test]
async fn discover_repository_fails_outside_git() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _m) = test_manager(dir.path()).await;
    assert!(manager.discover_repository(dir.path()).await.is_err());
}

#[tokio::test]
async fn clean_repo_creates_workspace() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let s = session("claude");
    let workspace = manager.ensure_workspace(&s, &root).await.expect("ensure");

    assert!(workspace.path.exists());
    assert!(workspace.path.join(".git").exists() || workspace.path.join(".git").is_file());
    assert_eq!(workspace.repository_root, root);
    assert!(workspace.branch.starts_with("agentmesh/claude/"));
    assert_eq!(
        workspace.base_revision,
        git_output(&root, &["rev-parse", "HEAD"])
    );
}

#[tokio::test]
async fn dirty_source_repository_is_rejected() {
    let (_dir, root) = clean_repo();
    std::fs::write(root.join("foo.txt"), "dirty\n").expect("write");
    let (manager, _m) = test_manager(&root).await;
    let s = session("claude");
    let err = manager.ensure_workspace(&s, &root).await;
    assert!(
        matches!(err, Err(WorkspaceError::DirtyRepository)),
        "tracked modification must be rejected, got {err:?}"
    );

    // Untracked files also count as dirty.
    git(&root, &["checkout", "-q", "--", "foo.txt"]);
    std::fs::write(root.join("new_untracked.txt"), "x\n").expect("write");
    let err = manager.ensure_workspace(&s, &root).await;
    assert!(matches!(err, Err(WorkspaceError::DirtyRepository)));
}

#[tokio::test]
async fn existing_session_reuses_workspace() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let s = session("codex");
    let first = manager.ensure_workspace(&s, &root).await.expect("first");
    let second = manager.ensure_workspace(&s, &root).await.expect("second");
    assert_eq!(first.id, second.id);
    assert_eq!(first.path, second.path);
}

#[tokio::test]
async fn two_sessions_get_isolated_workspaces() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let a = session("claude");
    let b = session("codex");
    let wa = manager.ensure_workspace(&a, &root).await.expect("a");
    let wb = manager.ensure_workspace(&b, &root).await.expect("b");
    assert_ne!(wa.path, wb.path);
    assert_ne!(wa.branch, wb.branch);

    // Modify in A; B must still see the base content.
    std::fs::write(wa.path.join("foo.txt"), "A changed\n").expect("write in a");
    let b_content = std::fs::read_to_string(wb.path.join("foo.txt")).expect("read b");
    assert_eq!(b_content, "base\n");
}

#[tokio::test]
async fn resume_dirty_workspace_is_allowed() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let s = session("claude");
    let workspace = manager.ensure_workspace(&s, &root).await.expect("ensure");
    // Agent left the worktree dirty.
    std::fs::write(workspace.path.join("foo.txt"), "agent work\n").expect("write");
    let verified = manager.workspace_for_session(s.id).await.expect("verify");
    assert_eq!(verified.path, workspace.path);
}

#[tokio::test]
async fn missing_workspace_path_becomes_missing() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let s = session("claude");
    let workspace = manager.ensure_workspace(&s, &root).await.expect("ensure");
    std::fs::remove_dir_all(&workspace.path).expect("remove");
    let err = manager.workspace_for_session(s.id).await;
    assert!(matches!(err, Err(WorkspaceError::WorkspaceMissing(_))));
    let row = manager
        .repository()
        .get(workspace.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(row.state, agentmesh_storage::WorkspaceState::Missing);
}

#[tokio::test]
async fn diff_reports_changed_files_and_patch() {
    let (_dir, root) = clean_repo();
    // Seed an extra tracked file BEFORE creating the workspace.
    std::fs::write(root.join("extra.txt"), "to delete\n").expect("extra");
    git(&root, &["add", "extra.txt"]);
    git(&root, &["commit", "-q", "-m", "add extra"]);

    let (manager, _m) = test_manager(&root).await;
    let s = session("claude");
    let workspace = manager.ensure_workspace(&s, &root).await.expect("ensure");

    // Modify tracked, add untracked, delete another file.
    std::fs::write(workspace.path.join("foo.txt"), "modified\n").expect("modify");
    std::fs::write(workspace.path.join("untracked.txt"), "new\n").expect("untracked");
    git(&workspace.path, &["rm", "-q", "extra.txt"]);

    let diff = manager.diff(&workspace).await.expect("diff");
    assert!(
        diff.changed_files
            .iter()
            .any(|f| f.path == Path::new("foo.txt")),
        "modified file missing: {:?}",
        diff.changed_files
    );
    assert!(
        diff.changed_files
            .iter()
            .any(|f| f.path == Path::new("extra.txt")),
        "deleted file missing"
    );
    assert_eq!(
        diff.untracked_files,
        vec![Path::new("untracked.txt").to_path_buf()]
    );
    assert!(diff.patch.contains("modified"), "patch missing content");
}

#[tokio::test]
async fn remove_refuses_dirty_workspace() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let s = session("claude");
    let workspace = manager.ensure_workspace(&s, &root).await.expect("ensure");
    std::fs::write(workspace.path.join("foo.txt"), "dirty\n").expect("write");

    let err = manager.remove(workspace.id, false).await;
    assert!(matches!(err, Err(WorkspaceError::WorkspaceDirty(_))));
    // Force removal works.
    manager
        .remove(workspace.id, true)
        .await
        .expect("force remove");
    let row = manager
        .repository()
        .get(workspace.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(row.state, agentmesh_storage::WorkspaceState::Removed);
}

#[test]
fn repository_storage_key_is_stable_and_distinct() {
    let a = agentmesh_workspace::manager::repository_storage_key(Path::new("/work/proj"));
    let b = agentmesh_workspace::manager::repository_storage_key(Path::new("/work/proj"));
    let c = agentmesh_workspace::manager::repository_storage_key(Path::new("/work/other"));
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_eq!(a.len(), 16);
}

#[test]
fn sanitize_agent_id_is_ref_safe() {
    assert_eq!(
        agentmesh_workspace::manager::sanitize_agent_id("my agent!??"),
        "my_agent___"
    );
    assert_eq!(
        agentmesh_workspace::manager::sanitize_agent_id("claude"),
        "claude"
    );
}

#[tokio::test]
async fn health_check_detects_git() {
    let (_dir, root) = clean_repo();
    let (manager, _m) = test_manager(&root).await;
    let (found, version) = manager.health_check().await;
    assert!(found);
    assert!(version.is_some());
}
