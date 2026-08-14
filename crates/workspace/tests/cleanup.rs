//! Workspace lifecycle tests (Phase 14): archive, safe cleanup, branch
//! safety, snapshot fingerprint and removed-workspace semantics.

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use agentmesh_core::AgentSession;
use agentmesh_storage::{
    ApplyRepository, ApplyRow, ApplyStatus, Database, WorkspaceRepository, WorkspaceState,
};
use agentmesh_workspace::{
    CleanupContext, Workspace, WorkspaceError, WorkspaceManager, workspace_snapshot_hash,
};
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

fn branch_exists(dir: &Path, branch: &str) -> bool {
    !git_output(dir, &["branch", "--list", branch]).is_empty()
}

fn clean_repo() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().canonicalize().expect("canonicalize");
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.name", "AgentMesh Test"]);
    git(
        &root,
        &["config", "user.email", "agentmesh@example.invalid"],
    );
    std::fs::write(root.join("tracked.txt"), "base\n").expect("write");
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "-m", "initial"]);
    (dir, root)
}

struct Env {
    manager: WorkspaceManager,
    applies: ApplyRepository,
    session: AgentSession,
    workspace: Workspace,
    source_root: PathBuf,
    _dir: tempfile::TempDir,
}

/// A fresh manager + an isolated worktree for a session.
async fn setup(source_root: &Path) -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("db");
    let manager = WorkspaceManager::new(
        WorkspaceRepository::new(db.clone()),
        dir.path().join("worktrees"),
    );
    let applies = ApplyRepository::new(db.clone());
    let session = AgentSession::new(Uuid::new_v4(), "claude");
    let workspace = manager
        .ensure_workspace(&session, source_root)
        .await
        .expect("workspace");
    Env {
        manager,
        applies,
        session,
        workspace,
        source_root: source_root.to_path_buf(),
        _dir: dir,
    }
}

/// An apply-record + `Applied` state: the workspace now looks like a safely
/// applied result. Returns the snapshot hash recorded at "apply" time.
async fn mark_applied(env: &Env) -> String {
    let diff = env.manager.diff(&env.workspace).await.expect("diff");
    let hash = workspace_snapshot_hash(&env.workspace.path, &diff);
    env.applies
        .create(&ApplyRow {
            id: Uuid::new_v4(),
            task_id: Some(Uuid::new_v4()),
            workflow_id: None,
            workspace_id: env.workspace.id,
            source_repository: env.source_root.clone(),
            base_revision: env.workspace.base_revision.clone(),
            status: ApplyStatus::Completed,
            error: None,
            created_at: "2026-08-01T00:00:00+00:00".to_string(),
            completed_at: Some("2026-08-01T00:00:01+00:00".to_string()),
            workspace_snapshot_hash: Some(hash.clone()),
        })
        .await
        .expect("insert apply");
    env.manager
        .repository()
        .set_state(env.workspace.id, WorkspaceState::Applied)
        .await
        .expect("set applied");
    hash
}

fn idle_context() -> CleanupContext {
    CleanupContext {
        has_live_task: false,
        has_session_lease: false,
        has_workflow_dependency: false,
        archive_only: false,
    }
}

// ---------- archive ----------

#[tokio::test]
async fn archive_only_changes_state() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let path = env.workspace.path.clone();
    let branch = env.workspace.branch.clone();

    env.manager
        .archive(env.workspace.id)
        .await
        .expect("archive");

    let row = env
        .manager
        .repository()
        .get(env.workspace.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, WorkspaceState::Archived);
    // Files and branch untouched.
    assert!(path.exists());
    assert!(branch_exists(&root, &branch));
    // Diff stays viewable.
    let workspace = env
        .manager
        .workspace_for_session(env.session.id)
        .await
        .expect("viewable");
    env.manager.diff(&workspace).await.expect("diff");
}

// ---------- cleanup preflight never mutates ----------

#[tokio::test]
async fn cleanup_check_does_not_mutate() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    mark_applied(&env).await;
    let path = env.workspace.path.clone();
    let branch = env.workspace.branch.clone();

    let plan = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &idle_context())
        .await
        .expect("plan");
    assert!(plan.safe);
    assert!(plan.snapshot_matches);

    // Nothing was deleted.
    assert!(path.exists());
    assert!(branch_exists(&root, &branch));
    let row = env
        .manager
        .repository()
        .get(env.workspace.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, WorkspaceState::Applied);
}

// ---------- safe cleanup ----------

#[tokio::test]
async fn safe_cleanup_removes_worktree_and_managed_branch() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    mark_applied(&env).await;
    let path = env.workspace.path.clone();
    let branch = env.workspace.branch.clone();

    let outcome = env
        .manager
        .cleanup(env.workspace.id, &env.applies, &idle_context())
        .await
        .expect("cleanup");
    assert!(outcome.worktree_removed);
    assert!(outcome.branch_removed);
    assert_eq!(outcome.state, WorkspaceState::Removed);

    // Worktree and managed branch are gone; the source repo is untouched.
    assert!(!path.exists());
    assert!(!branch_exists(&root, &branch));
    assert_eq!(
        git_output(&root, &["status", "--porcelain"]).trim(),
        "",
        "source must stay clean"
    );
}

#[tokio::test]
async fn user_branch_is_never_deleted() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    mark_applied(&env).await;

    // Point the stored workspace at a user-owned branch (not `agentmesh/...`).
    let user_branch = "feature/user-thing";
    git(&root, &["branch", user_branch]);
    env.manager
        .repository()
        .set_branch(env.workspace.id, user_branch)
        .await
        .expect("set branch");

    let err = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &idle_context())
        .await;
    assert!(
        matches!(err, Err(WorkspaceError::NotManagedBranch(_))),
        "{err:?}"
    );

    // The user branch must still exist after any attempt.
    assert!(branch_exists(&root, user_branch));
}

// ---------- rejected cleanups ----------

#[tokio::test]
async fn active_dirty_unapplied_workspace_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    // Dirty, still Active, no apply record.
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();

    let err = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &idle_context())
        .await;
    assert!(
        matches!(err, Err(WorkspaceError::WorkspaceNotSafeToRemove(..))),
        "{err:?}"
    );
}

#[tokio::test]
async fn changed_after_apply_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    mark_applied(&env).await;

    // The workspace changes after the "apply".
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent + more\n").unwrap();

    let err = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &idle_context())
        .await;
    assert!(
        matches!(err, Err(WorkspaceError::WorkspaceChangedAfterApply(_))),
        "{err:?}"
    );
    // Nothing was removed.
    assert!(env.workspace.path.exists());
}

#[tokio::test]
async fn external_dependencies_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    mark_applied(&env).await;

    let ctx = CleanupContext {
        has_live_task: true,
        has_session_lease: false,
        has_workflow_dependency: false,
        archive_only: false,
    };
    let err = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &ctx)
        .await;
    assert!(matches!(
        err,
        Err(WorkspaceError::WorkspaceNotSafeToRemove(..))
    ));

    let ctx = CleanupContext {
        has_live_task: false,
        has_session_lease: true,
        has_workflow_dependency: false,
        archive_only: false,
    };
    let err = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &ctx)
        .await;
    assert!(matches!(
        err,
        Err(WorkspaceError::WorkspaceNotSafeToRemove(..))
    ));

    let ctx = CleanupContext {
        has_live_task: false,
        has_session_lease: false,
        has_workflow_dependency: true,
        archive_only: false,
    };
    let err = env
        .manager
        .plan_cleanup(env.workspace.id, &env.applies, &ctx)
        .await;
    assert!(matches!(
        err,
        Err(WorkspaceError::WorkspaceNotSafeToRemove(..))
    ));
}

// ---------- removed workspaces are never resurrected ----------

#[tokio::test]
async fn removed_workspace_cannot_be_verified_or_resumed() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    mark_applied(&env).await;
    env.manager
        .cleanup(env.workspace.id, &env.applies, &idle_context())
        .await
        .expect("cleanup");

    let err = env.manager.workspace_for_session(env.session.id).await;
    assert!(
        matches!(err, Err(WorkspaceError::WorkspaceRemoved(_))),
        "{err:?}"
    );
    // ensure_workspace must not recreate the old workspace either.
    let err = env.manager.ensure_workspace(&env.session, &root).await;
    assert!(matches!(err, Err(WorkspaceError::WorkspaceRemoved(_))));
}

// ---------- snapshot fingerprint ----------

#[tokio::test]
async fn snapshot_hash_changes_when_workspace_changes() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    let a = env.manager.diff(&env.workspace).await.unwrap();
    let hash_a = workspace_snapshot_hash(&env.workspace.path, &a);

    std::fs::write(env.workspace.path.join("tracked.txt"), "agent + more\n").unwrap();
    let b = env.manager.diff(&env.workspace).await.unwrap();
    let hash_b = workspace_snapshot_hash(&env.workspace.path, &b);

    assert_ne!(hash_a, hash_b);

    // Untracked files participate too.
    std::fs::write(env.workspace.path.join("new.txt"), "x\n").unwrap();
    let c = env.manager.diff(&env.workspace).await.unwrap();
    let hash_c = workspace_snapshot_hash(&env.workspace.path, &c);
    assert_ne!(hash_b, hash_c);
}
