//! ApplyManager integration tests (Phase 13) against temporary Git repos.
//!
//! Every test builds a real source repository, an isolated worktree, agent
//! changes inside it, then drives plan/apply through the manager. No Claude,
//! Codex or other external agent is required.

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;

use agentmesh_apply::{ApplyError, ApplyManager};
use agentmesh_core::{AgentMessage, AgentSession, AgentTask, Context};
use agentmesh_orchestrator::{
    PersistedStepResult, ReviewResult, ReviewVerdict, WorkflowRole, WorkflowStatus, WorkflowStep,
    WorkflowStepStatus,
};
use agentmesh_storage::{
    ApplyRepository, ApplyRow, ApplyStatus, ContextRepository, Database, TaskRepository,
    WorkflowRepository, WorkflowRow, WorkflowStepRepository, WorkflowStepRow, WorkspaceRepository,
    WorkspaceState,
};
use agentmesh_workspace::{Workspace, WorkspaceManager};
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

/// Create a clean repo with a committed base: `tracked.txt`, a file `a`, and
/// a tracked directory `nested/keep.txt`.
fn clean_repo() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().canonicalize().expect("canonicalize");
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.name", "AgentMesh Test"]);
    git(
        &root,
        &["config", "user.email", "agentmesh@example.invalid"],
    );
    std::fs::create_dir_all(root.join("nested")).expect("mkdir nested");
    std::fs::write(root.join("tracked.txt"), "base\n").expect("write tracked");
    std::fs::write(root.join("a"), "base-a\n").expect("write a");
    std::fs::write(root.join("nested/keep.txt"), "keep\n").expect("write keep");
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "-m", "initial"]);
    (dir, root)
}

/// Everything a test needs: the manager, the repositories, the source repo
/// and the agent session's workspace.
struct Env {
    manager: ApplyManager,
    workspaces: Arc<WorkspaceManager>,
    contexts: ContextRepository,
    workflows: WorkflowRepository,
    steps: WorkflowStepRepository,
    applies: ApplyRepository,
    task_id: Uuid,
    workspace: Workspace,
    _dir: tempfile::TempDir,
}

/// Open a database + manager and create an agent session + task + isolated
/// worktree for the (clean) source repository.
async fn setup(source_root: &Path) -> Env {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("agentmesh.db"))
        .await
        .expect("open db");
    let tasks = TaskRepository::new(db.clone());
    let contexts = ContextRepository::new(db.clone());
    let workspaces = Arc::new(WorkspaceManager::new(
        WorkspaceRepository::new(db.clone()),
        dir.path().join("worktrees"),
    ));
    let workflows = WorkflowRepository::new(db.clone());
    let steps = WorkflowStepRepository::new(db.clone());
    let applies = ApplyRepository::new(db.clone());
    let manager = ApplyManager::new(
        tasks.clone(),
        workspaces.clone(),
        workflows.clone(),
        steps.clone(),
        applies.clone(),
    );
    let (task_id, workspace) = create_chain(source_root, &contexts, &workspaces, "claude").await;
    Env {
        manager,
        workspaces,
        contexts,
        workflows,
        steps,
        applies,
        task_id,
        workspace,
        _dir: dir,
    }
}

/// Create a context + agent session + task + isolated worktree; returns the
/// task id and its workspace.
async fn create_chain(
    source_root: &Path,
    contexts: &ContextRepository,
    workspaces: &Arc<WorkspaceManager>,
    agent: &str,
) -> (Uuid, Workspace) {
    let context = Context::new();
    let session = AgentSession::new(context.id, agent);
    let mut task = AgentTask::with_workspace(agent, AgentMessage::user("work"), None);
    task.context_id = context.id;
    task.agent_session_id = Some(session.id);
    contexts
        .create_run_setup(&context, &session, &task)
        .await
        .expect("create chain");
    let workspace = workspaces
        .ensure_workspace(&session, source_root)
        .await
        .expect("ensure workspace");
    (task.id, workspace)
}

// ---------- workflow persistence helpers ----------

fn step_row(
    workflow_id: Uuid,
    ordinal: usize,
    role: &str,
    status: &str,
    task_id: Option<Uuid>,
    result_json: Option<String>,
) -> WorkflowStepRow {
    WorkflowStepRow {
        id: Uuid::new_v4(),
        workflow_id,
        ordinal: ordinal as i64,
        node_id: None,
        role: role.to_string(),
        intent: "implementation".to_string(),
        objective: None,
        status: status.to_string(),
        agent_id: Some("codex".to_string()),
        task_id,
        review_round: 0,
        summary: Some("done".to_string()),
        result_json,
        created_at: "2026-01-01T00:00:00+00:00".to_string(),
        started_at: None,
        completed_at: Some("2026-01-01T00:00:01+00:00".to_string()),
        error: None,
    }
}

fn review_json(verdict: &str) -> String {
    let verdict = match verdict {
        "approved" => ReviewVerdict::Approved,
        _ => ReviewVerdict::ChangesRequested,
    };
    let persisted = PersistedStepResult {
        step: WorkflowStep::new("reviewer", WorkflowRole::Reviewer),
        status: WorkflowStepStatus::Completed,
        agent_id: Some("claude".to_string()),
        task_id: None,
        summary: Some("review done".to_string()),
        review_result: Some(ReviewResult {
            verdict,
            summary: "ok".to_string(),
            issues: vec![],
            confidence: None,
        }),
        error: None,
    };
    serde_json::to_string(&persisted).expect("serialize review")
}

async fn insert_workflow(env: &Env, workflow_id: Uuid, status: &str, steps: Vec<WorkflowStepRow>) {
    let row = WorkflowRow {
        id: workflow_id,
        preset: "architect-implement-review".to_string(),
        goal: "goal".to_string(),
        status: status.to_string(),
        context_id: None,
        options_json: "{}".to_string(),
        review_rounds: 0,
        runtime_owner: None,
        runtime_heartbeat_at: None,
        error: None,
        created_at: "2026-01-01T00:00:00+00:00".to_string(),
        updated_at: "2026-01-01T00:00:00+00:00".to_string(),
        completed_at: Some("2026-01-01T00:00:01+00:00".to_string()),
        graph_revision: 1,
        parent_workflow_id: None,
        recovery_of_node_id: None,
        recovery_attempt: 0,
        source_workspace: None,
    };
    env.workflows.create(&row).await.expect("create workflow");
    for step in steps {
        env.steps.upsert(&step).await.expect("upsert step");
    }
}

// ---------- tracked / untracked apply ----------

#[tokio::test]
async fn tracked_modify_apply() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let head = git_output(&root, &["rev-parse", "HEAD"]);

    std::fs::write(env.workspace.path.join("tracked.txt"), "agent change\n").unwrap();
    let outcome = env.manager.apply_task(env.task_id).await.expect("apply");

    assert!(outcome.tracked_applied);
    assert_eq!(outcome.untracked_copied, 0);
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "agent change\n"
    );
    // HEAD and the agent worktree are untouched by apply.
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head);
    assert_eq!(
        std::fs::read_to_string(env.workspace.path.join("tracked.txt")).unwrap(),
        "agent change\n"
    );
}

#[tokio::test]
async fn tracked_delete_apply() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;

    std::fs::remove_file(env.workspace.path.join("a")).unwrap();
    let outcome = env.manager.apply_task(env.task_id).await.expect("apply");

    assert!(outcome.tracked_applied);
    assert!(
        !root.join("a").exists(),
        "deleted file must be removed from the source"
    );
}

#[tokio::test]
async fn untracked_create_apply() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;

    std::fs::write(env.workspace.path.join("new.txt"), "new\n").unwrap();
    let outcome = env.manager.apply_task(env.task_id).await.expect("apply");

    assert!(!outcome.tracked_applied);
    assert_eq!(outcome.untracked_copied, 1);
    assert_eq!(
        std::fs::read_to_string(root.join("new.txt")).unwrap(),
        "new\n"
    );
}

#[tokio::test]
async fn mixed_changes_apply() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;

    std::fs::write(env.workspace.path.join("tracked.txt"), "modified\n").unwrap();
    git(&env.workspace.path, &["rm", "-q", "a"]);
    std::fs::write(env.workspace.path.join("new.txt"), "new\n").unwrap();
    std::fs::create_dir_all(env.workspace.path.join("sub")).unwrap();
    std::fs::write(env.workspace.path.join("sub/other.txt"), "other\n").unwrap();

    let plan = env.manager.plan_task(env.task_id).await.expect("plan");
    let statuses: Vec<&str> = plan
        .changed_files
        .iter()
        .map(|f| f.status.as_str())
        .collect();
    assert!(statuses.contains(&"M"));
    assert!(statuses.contains(&"D"));
    assert!(plan.untracked_files.contains(&"new.txt".to_string()));
    assert!(plan.untracked_files.contains(&"sub/other.txt".to_string()));

    let outcome = env.manager.apply_task(env.task_id).await.expect("apply");
    assert!(outcome.tracked_applied);
    assert_eq!(outcome.untracked_copied, 2);
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "modified\n"
    );
    assert!(!root.join("a").exists());
    assert!(root.join("new.txt").exists());
    assert_eq!(
        std::fs::read_to_string(root.join("sub/other.txt")).unwrap(),
        "other\n"
    );
}

// ---------- preview / invariants ----------

#[tokio::test]
async fn check_preview_leaves_source_unchanged() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let head = git_output(&root, &["rev-parse", "HEAD"]);

    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    let plan = env.manager.plan_task(env.task_id).await.expect("plan");

    assert!(plan.applicable);
    assert_eq!(plan.base_revision, head);
    assert_eq!(plan.source_revision, head);
    // The source must be byte-for-byte untouched by --check.
    assert!(git_output(&root, &["status", "--porcelain"]).is_empty());
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "base\n"
    );
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head);
}

#[tokio::test]
async fn apply_keeps_head_and_workspace_unchanged() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let head = git_output(&root, &["rev-parse", "HEAD"]);
    let ws_path = env.workspace.path.clone();

    std::fs::write(ws_path.join("tracked.txt"), "agent\n").unwrap();
    std::fs::write(ws_path.join("new.txt"), "new\n").unwrap();
    let ws_state = git_output(&ws_path, &["status", "--porcelain"]);

    env.manager.apply_task(env.task_id).await.expect("apply");

    // Invariant: source HEAD unchanged, source working tree modified.
    assert_eq!(git_output(&root, &["rev-parse", "HEAD"]), head);
    assert!(!git_output(&root, &["status", "--porcelain"]).is_empty());
    // Invariant: the agent worktree is unchanged by apply.
    assert_eq!(git_output(&ws_path, &["status", "--porcelain"]), ws_state);
    assert_eq!(
        std::fs::read_to_string(ws_path.join("tracked.txt")).unwrap(),
        "agent\n"
    );
}

// ---------- source validation ----------

#[tokio::test]
async fn dirty_source_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();

    // Tracked modification in the source.
    std::fs::write(root.join("tracked.txt"), "user edit\n").unwrap();
    let err = env.manager.plan_task(env.task_id).await;
    assert!(
        matches!(err, Err(ApplyError::SourceRepositoryDirty)),
        "{err:?}"
    );

    // Untracked file in the source also counts as dirty.
    git(&root, &["checkout", "-q", "--", "tracked.txt"]);
    std::fs::write(root.join("user_file.txt"), "x\n").unwrap();
    let err = env.manager.plan_task(env.task_id).await;
    assert!(
        matches!(err, Err(ApplyError::SourceRepositoryDirty)),
        "{err:?}"
    );
}

#[tokio::test]
async fn source_head_changed_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();

    // The source advances past the workspace base commit.
    std::fs::write(root.join("extra.txt"), "x\n").unwrap();
    git(&root, &["add", "extra.txt"]);
    git(&root, &["commit", "-q", "-m", "source moved on"]);
    let err = env.manager.plan_task(env.task_id).await;
    assert!(
        matches!(err, Err(ApplyError::SourceRevisionChanged { .. })),
        "{err:?}"
    );
}

#[tokio::test]
async fn git_apply_conflict_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;

    // A well-formed patch whose hunk context does not match `tracked.txt`
    // content ("base") — `git apply --check` must reject it before any write.
    let stale = "\
diff --git a/tracked.txt b/tracked.txt
--- a/tracked.txt
+++ b/tracked.txt
@@ -1,1 +1,1 @@
-line1
+line2
";
    let err = env.manager.apply_patch(&root, stale).await;
    assert!(
        matches!(err, Err(ApplyError::ApplyCheckFailed(_))),
        "{err:?}"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "base\n"
    );
}

// ---------- untracked conflicts and path security ----------

#[tokio::test]
async fn untracked_destination_exists_conflict() {
    let (_repo, root) = clean_repo();
    // Base gets a tracked DIRECTORY `f/`.
    std::fs::create_dir_all(root.join("f")).unwrap();
    std::fs::write(root.join("f/keep.txt"), "k\n").unwrap();
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "-m", "add f dir"]);
    let env = setup(&root).await;

    // The agent replaces the tracked dir `f/` with an untracked FILE `f`:
    // its destination already exists in the source.
    git(&env.workspace.path, &["rm", "-rq", "f"]);
    std::fs::write(env.workspace.path.join("f"), "now a file\n").unwrap();
    let err = env.manager.plan_task(env.task_id).await;
    assert!(matches!(err, Err(ApplyError::ApplyConflict(_))), "{err:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn untracked_symlink_escape_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    // An untracked symlink pointing outside the workspace must be rejected.
    std::os::unix::fs::symlink("/etc/passwd", env.workspace.path.join("evil.txt")).unwrap();
    let err = env.manager.plan_task(env.task_id).await;
    assert!(
        matches!(err, Err(ApplyError::UnsafeApplyPath(_))),
        "{err:?}"
    );
}

// ---------- rollback ----------

/// Make `nested` read-only so copying into it fails with EACCES.
#[cfg(unix)]
fn make_nested_read_only(root: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root.join("nested"), std::fs::Permissions::from_mode(0o555)).unwrap();
}

#[cfg(unix)]
fn make_nested_writable(root: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root.join("nested"), std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn copy_failure_rolls_back_source() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    make_nested_read_only(&root);

    // Agent changes: tracked modify + one untracked file that copies fine +
    // one untracked file whose destination cannot be written.
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    std::fs::write(env.workspace.path.join("a_ok.txt"), "ok\n").unwrap();
    std::fs::write(env.workspace.path.join("nested/new.txt"), "blocked\n").unwrap();

    let err = env.manager.apply_task(env.task_id).await;
    make_nested_writable(&root);
    assert!(matches!(err, Err(ApplyError::CopyFailed(..))), "{err:?}");

    // Rollback restored the source: copied file removed, patch reversed.
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "base\n"
    );
    assert!(!root.join("a_ok.txt").exists());
    assert!(!root.join("nested/new.txt").exists());
    assert!(
        git_output(&root, &["status", "--porcelain"]).is_empty(),
        "source must be clean after rollback"
    );
}

// ---------- persistence and idempotency ----------

#[tokio::test]
async fn successful_apply_persisted() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();

    let outcome = env.manager.apply_task(env.task_id).await.expect("apply");
    let rows = env.applies.list().await.expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, outcome.apply_id);
    assert_eq!(rows[0].status, ApplyStatus::Completed);
    assert_eq!(rows[0].workspace_id, env.workspace.id);
    assert_eq!(rows[0].source_repository, root);
    assert_eq!(rows[0].base_revision, env.workspace.base_revision.clone());
}

#[cfg(unix)]
#[tokio::test]
async fn failed_apply_persisted() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    make_nested_read_only(&root);
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    std::fs::write(env.workspace.path.join("nested/new.txt"), "blocked\n").unwrap();

    let err = env.manager.apply_task(env.task_id).await;
    make_nested_writable(&root);
    assert!(err.is_err());
    let rows = env.applies.list().await.expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, ApplyStatus::Failed);
    assert!(rows[0].error.is_some(), "failed apply must record an error");
}

#[tokio::test]
async fn already_applied_is_rejected_and_still_viewable() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    let task_id = env.task_id;

    env.manager.apply_task(task_id).await.expect("first apply");

    // Re-apply is rejected.
    let err = env.manager.apply_task(task_id).await;
    assert!(matches!(err, Err(ApplyError::AlreadyApplied)), "{err:?}");

    // --check still works and says it is already applied.
    let plan = env
        .manager
        .plan_task(task_id)
        .await
        .expect("check after apply");
    assert!(plan.already_applied);
    assert!(!plan.applicable);
    assert!(!plan.warnings.is_empty());
}

#[tokio::test]
async fn apply_marks_workspace_applied() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    let workspace_id = env.workspace.id;

    let outcome = env.manager.apply_task(env.task_id).await.expect("apply");
    assert!(
        !outcome.workspace_snapshot_hash.is_empty(),
        "snapshot must be recorded"
    );

    // Phase 14: the workspace moves to `Applied`; worktree/branch/artifacts stay.
    let row = env
        .workspaces
        .repository()
        .get(workspace_id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(row.state, WorkspaceState::Applied);
    assert!(
        env.workspace.path.exists(),
        "apply must not remove the worktree"
    );
    let rows = env.applies.list().await.expect("list");
    assert!(rows[0].workspace_snapshot_hash.is_some());
}

#[tokio::test]
async fn apply_in_progress_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();

    // A concurrent apply already claimed the workspace (row stuck in applying).
    env.applies
        .create(&ApplyRow {
            id: Uuid::new_v4(),
            task_id: Some(env.task_id),
            workflow_id: None,
            workspace_id: env.workspace.id,
            source_repository: root.clone(),
            base_revision: env.workspace.base_revision.clone(),
            status: ApplyStatus::Applying,
            error: None,
            created_at: "2026-08-01T00:00:00+00:00".to_string(),
            completed_at: None,
            workspace_snapshot_hash: None,
        })
        .await
        .expect("insert applying");

    let err = env.manager.apply_task(env.task_id).await;
    assert!(matches!(err, Err(ApplyError::ApplyInProgress)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_apply_only_one_wins() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    std::fs::write(env.workspace.path.join("tracked.txt"), "agent\n").unwrap();
    let task_id = env.task_id;

    let a = env.manager.clone();
    let b = env.manager.clone();
    let handle_a = tokio::spawn(async move { a.apply_task(task_id).await });
    let handle_b = tokio::spawn(async move { b.apply_task(task_id).await });
    let (ra, rb) = tokio::join!(handle_a, handle_b);
    let (ra, rb) = (ra.expect("join a"), rb.expect("join b"));

    let ok_count = ra.is_ok() as usize + rb.is_ok() as usize;
    assert_eq!(ok_count, 1, "exactly one concurrent apply must win");
    let loser = if ra.is_ok() { rb } else { ra };
    match loser {
        Err(ApplyError::ApplyInProgress) | Err(ApplyError::AlreadyApplied) => {}
        other => {
            panic!("the losing request must be ApplyInProgress or AlreadyApplied, got {other:?}")
        }
    }
}

// ---------- workflow source selection ----------

#[tokio::test]
async fn workflow_selects_fixer_workspace() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let (impl_task, _impl_ws) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;
    let (fix_task, fix_ws) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;
    let (rev_task, _rev_ws) = create_chain(&root, &env.contexts, &env.workspaces, "claude").await;

    // Only the fixer workspace carries changes.
    std::fs::write(fix_ws.path.join("tracked.txt"), "fixed\n").unwrap();

    let workflow_id = Uuid::new_v4();
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![
            step_row(workflow_id, 0, "architect", "completed", None, None),
            step_row(
                workflow_id,
                1,
                "implementer",
                "completed",
                Some(impl_task),
                None,
            ),
            step_row(
                workflow_id,
                2,
                "reviewer",
                "completed",
                Some(rev_task),
                Some(review_json("approved")),
            ),
            step_row(workflow_id, 3, "fixer", "completed", Some(fix_task), None),
        ],
    )
    .await;

    let plan = env.manager.plan_workflow(workflow_id).await.expect("plan");
    assert_eq!(
        plan.workspace, fix_ws.path,
        "fixer workspace must be the apply source"
    );
    assert_eq!(plan.source_repository, root);

    let outcome = env
        .manager
        .apply_workflow(workflow_id)
        .await
        .expect("apply");
    assert!(outcome.tracked_applied);
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "fixed\n"
    );
}

#[tokio::test]
async fn workflow_falls_back_to_implementer() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let (impl_task, impl_ws) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;
    std::fs::write(impl_ws.path.join("tracked.txt"), "implemented\n").unwrap();

    let workflow_id = Uuid::new_v4();
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![
            step_row(workflow_id, 0, "architect", "completed", None, None),
            step_row(
                workflow_id,
                1,
                "implementer",
                "completed",
                Some(impl_task),
                None,
            ),
        ],
    )
    .await;

    let plan = env.manager.plan_workflow(workflow_id).await.expect("plan");
    assert_eq!(plan.workspace, impl_ws.path);
    let outcome = env
        .manager
        .apply_workflow(workflow_id)
        .await
        .expect("apply");
    assert!(outcome.tracked_applied);
    assert_eq!(
        std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
        "implemented\n"
    );
}

#[tokio::test]
async fn workflow_reviewer_workspace_is_ignored() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let (impl_task, impl_ws) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;
    let (rev_task, rev_ws) = create_chain(&root, &env.contexts, &env.workspaces, "claude").await;

    // The reviewer workspace has its own (unwanted) changes.
    std::fs::write(impl_ws.path.join("tracked.txt"), "implementer\n").unwrap();
    std::fs::write(rev_ws.path.join("rev_only.txt"), "reviewer\n").unwrap();

    let workflow_id = Uuid::new_v4();
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![
            step_row(workflow_id, 0, "architect", "completed", None, None),
            step_row(
                workflow_id,
                1,
                "implementer",
                "completed",
                Some(impl_task),
                None,
            ),
            step_row(
                workflow_id,
                2,
                "reviewer",
                "completed",
                Some(rev_task),
                Some(review_json("approved")),
            ),
        ],
    )
    .await;

    let plan = env.manager.plan_workflow(workflow_id).await.expect("plan");
    // Role-based selection: the implementer is the source, never the reviewer.
    assert_eq!(plan.workspace, impl_ws.path);
    assert!(!plan.untracked_files.contains(&"rev_only.txt".to_string()));
    let has_tracked: Vec<&str> = plan.changed_files.iter().map(|f| f.path.as_str()).collect();
    assert!(has_tracked.contains(&"tracked.txt"));
}

#[tokio::test]
async fn workflow_unapproved_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let (impl_task, _impl_ws) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;

    let workflow_id = Uuid::new_v4();
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![
            step_row(
                workflow_id,
                0,
                "implementer",
                "completed",
                Some(impl_task),
                None,
            ),
            step_row(
                workflow_id,
                1,
                "reviewer",
                "completed",
                Some(impl_task),
                Some(review_json("changes_requested")),
            ),
        ],
    )
    .await;

    let err = env.manager.plan_workflow(workflow_id).await;
    assert!(
        matches!(err, Err(ApplyError::ReviewNotApproved(_))),
        "{err:?}"
    );
}

#[tokio::test]
async fn workflow_not_completed_rejected() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let (impl_task, _impl_ws) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;

    let workflow_id = Uuid::new_v4();
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Running.as_str(),
        vec![step_row(
            workflow_id,
            0,
            "implementer",
            "running",
            Some(impl_task),
            None,
        )],
    )
    .await;

    let err = env.manager.plan_workflow(workflow_id).await;
    assert!(
        matches!(err, Err(ApplyError::WorkflowNotCompleted(..))),
        "{err:?}"
    );
}

#[tokio::test]
async fn workflow_without_implementer_is_ambiguous() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;
    let workflow_id = Uuid::new_v4();
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![step_row(
            workflow_id,
            0,
            "reviewer",
            "completed",
            None,
            Some(review_json("approved")),
        )],
    )
    .await;

    let err = env.manager.plan_workflow(workflow_id).await;
    assert!(
        matches!(err, Err(ApplyError::AmbiguousApplySource(_))),
        "{err:?}"
    );
}

#[tokio::test]
async fn dag_workflow_with_parallel_code_nodes_is_ambiguous() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;

    // A DAG workflow with TWO parallel completed implementer nodes (both
    // depend on the same architecture node) and no chain between them — the
    // apply source cannot be uniquely determined.
    let workflow_id = Uuid::new_v4();
    let mut arch = step_row(workflow_id, 0, "architect", "completed", None, None);
    arch.node_id = Some("architecture".to_string());
    arch.intent = "architecture".to_string();
    let mut impl_a = step_row(workflow_id, 1, "implementer", "completed", None, None);
    impl_a.node_id = Some("implementation_a".to_string());
    let mut impl_b = step_row(workflow_id, 2, "implementer", "completed", None, None);
    impl_b.node_id = Some("implementation_b".to_string());
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![arch, impl_a, impl_b],
    )
    .await;
    // Both code nodes depend on the architecture node (parallel fan-out).
    env.steps
        .set_dependencies(
            workflow_id,
            &[
                ("implementation_a".into(), "architecture".into()),
                ("implementation_b".into(), "architecture".into()),
            ],
        )
        .await
        .expect("deps");

    let err = env.manager.plan_workflow(workflow_id).await;
    assert!(
        matches!(err, Err(ApplyError::AmbiguousApplySource(_))),
        "parallel code nodes must be ambiguous, got {err:?}"
    );
}

#[tokio::test]
async fn dag_workflow_chain_selects_maximal_code_node() {
    let (_repo, root) = clean_repo();
    let env = setup(&root).await;

    // A DAG chain: architecture → implementation_a → implementation_b. Only
    // implementation_b is maximal, so it is the unique apply source.
    let (task_a, ws_a) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;
    let (task_b, ws_b) = create_chain(&root, &env.contexts, &env.workspaces, "codex").await;
    // Only the last code node carries changes.
    std::fs::write(ws_b.path.join("tracked.txt"), "final\n").unwrap();
    let _ = &ws_a;

    let workflow_id = Uuid::new_v4();
    let mut arch = step_row(workflow_id, 0, "architect", "completed", None, None);
    arch.node_id = Some("architecture".to_string());
    arch.intent = "architecture".to_string();
    let mut impl_a = step_row(
        workflow_id,
        1,
        "implementer",
        "completed",
        Some(task_a),
        None,
    );
    impl_a.node_id = Some("implementation_a".to_string());
    let mut impl_b = step_row(
        workflow_id,
        2,
        "implementer",
        "completed",
        Some(task_b),
        None,
    );
    impl_b.node_id = Some("implementation_b".to_string());
    insert_workflow(
        &env,
        workflow_id,
        WorkflowStatus::Completed.as_str(),
        vec![arch, impl_a, impl_b],
    )
    .await;
    env.steps
        .set_dependencies(
            workflow_id,
            &[
                ("implementation_a".into(), "architecture".into()),
                ("implementation_b".into(), "implementation_a".into()),
            ],
        )
        .await
        .expect("deps");

    let plan = env.manager.plan_workflow(workflow_id).await.expect("plan");
    assert_eq!(
        plan.workspace, ws_b.path,
        "the maximal (last) code node must be the apply source"
    );
}
