//! WorkspaceManager: Git worktree lifecycle and diff computation.

use std::path::{Path, PathBuf};

use agentmesh_core::AgentSession;
use agentmesh_storage::{ApplyRepository, WorkspaceRepository, WorkspaceRow, WorkspaceState};
use chrono::Utc;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::WorkspaceError;
use crate::git::{git, git_ok};
use crate::model::{
    ChangeStatus, ChangedFile, CleanupContext, CleanupOutcome, CleanupPlan, Workspace,
    WorkspaceDiff, workspace_snapshot_hash,
};

/// Where isolated worktrees live:
/// `<user-data>/workspaces/<repo-key>/<agent-session-id>/`.
pub fn workspace_root() -> PathBuf {
    agentmesh_storage::database::user_data_dir().join("workspaces")
}

/// Sanitize an agent id into a git-ref-safe token.
pub fn sanitize_agent_id(agent_id: &str) -> String {
    let mut result = String::with_capacity(agent_id.len());
    for c in agent_id.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
            result.push(c);
        } else {
            result.push('_');
        }
    }
    if result.is_empty() {
        "agent".to_string()
    } else {
        result
    }
}

/// Stable storage key for a repository (first 16 hex chars of SHA-256 of
/// the canonical repository root).
pub fn repository_storage_key(repository_root: &Path) -> String {
    let digest = Sha256::digest(repository_root.display().to_string().as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Git workspace manager: creates and verifies isolated worktrees.
#[derive(Clone)]
pub struct WorkspaceManager {
    workspaces: WorkspaceRepository,
    root: PathBuf,
}

impl WorkspaceManager {
    pub fn new(workspaces: WorkspaceRepository, root: PathBuf) -> Self {
        Self { workspaces, root }
    }

    pub fn with_default_root(workspaces: WorkspaceRepository) -> Self {
        Self::new(workspaces, workspace_root())
    }

    pub fn repository(&self) -> &WorkspaceRepository {
        &self.workspaces
    }

    /// Resolve `path` (usually the caller's project) to its Git repository
    /// root. Uses `git rev-parse --show-toplevel`, which handles worktrees,
    /// submodules and `.git` files.
    pub async fn discover_repository(&self, path: &Path) -> Result<PathBuf, WorkspaceError> {
        let stdout = git_ok(path, &["rev-parse", "--show-toplevel"]).await?;
        let root = PathBuf::from(stdout.trim());
        let common_dir = git_ok(&root, &["rev-parse", "--git-common-dir"]).await?;
        if common_dir.trim() == "." {
            return Err(WorkspaceError::UnsupportedRepository);
        }
        Ok(root)
    }

    /// Current HEAD commit of the repository (immutable base revision).
    pub async fn base_revision(&self, repository_root: &Path) -> Result<String, WorkspaceError> {
        let stdout = git_ok(repository_root, &["rev-parse", "HEAD"]).await?;
        let revision = stdout.trim().to_string();
        if revision.is_empty() {
            return Err(WorkspaceError::Internal(
                "repository has no HEAD commit".to_string(),
            ));
        }
        Ok(revision)
    }

    /// Whether the repository working tree is clean (tracked modifications,
    /// staged changes and untracked files all count as dirty).
    ///
    /// AgentMesh's own `.agentmesh/` directory (the local database) is
    /// excluded: it is AgentMesh state, not user work. Consider adding
    /// `.agentmesh/` to the repository `.gitignore`.
    pub async fn is_clean(&self, repository_root: &Path) -> Result<bool, WorkspaceError> {
        let stdout = git_ok(repository_root, &["status", "--porcelain"]).await?;
        let dirty = stdout.lines().any(|line| {
            let path = line.get(3..).unwrap_or("").trim();
            !path.starts_with(".agentmesh/")
        });
        Ok(!dirty)
    }

    /// Ensure the session has an active workspace; creates one when missing.
    ///
    /// For a fresh session: validates the repository is clean, creates a
    /// worktree on its own branch at the current HEAD and persists it. For
    /// an existing session: verifies and reuses the stored workspace.
    pub async fn ensure_workspace(
        &self,
        session: &AgentSession,
        source_path: &Path,
    ) -> Result<Workspace, WorkspaceError> {
        if let Some(existing) = self
            .workspaces
            .get_by_agent_session(session.id)
            .await
            .map_err(WorkspaceError::Persist)?
        {
            return self.verify_workspace(existing).await;
        }

        let repository_root = self.discover_repository(source_path).await?;
        if !self.is_clean(&repository_root).await? {
            return Err(WorkspaceError::DirtyRepository);
        }
        let base_revision = self.base_revision(&repository_root).await?;

        let branch = format!(
            "agentmesh/{}/{}",
            sanitize_agent_id(&session.agent_id),
            &session.id.to_string()[..8]
        );
        let path = self
            .root
            .join(repository_storage_key(&repository_root))
            .join(session.id.to_string());

        // git worktree add -b <branch> <path> <base-revision>
        let output = git(
            &repository_root,
            &[
                "worktree",
                "add",
                "-b",
                &branch,
                path.to_str().ok_or_else(|| {
                    WorkspaceError::Internal("non-UTF8 workspace path".to_string())
                })?,
                &base_revision,
            ],
        )
        .await?;
        if !output.success() {
            let stderr = output.stderr.trim();
            return if stderr.contains("already exists") {
                Err(WorkspaceError::WorkspaceAlreadyExists(stderr.to_string()))
            } else {
                Err(WorkspaceError::GitCommand {
                    stderr: stderr.to_string(),
                })
            };
        }

        let now = Utc::now();
        let workspace = Workspace {
            id: Uuid::new_v4(),
            agent_session_id: session.id,
            repository_root,
            path: path.clone(),
            branch,
            base_revision,
            state: WorkspaceState::Active,
            created_at: now,
            updated_at: now,
        };
        let row = WorkspaceRow {
            id: workspace.id,
            agent_session_id: workspace.agent_session_id,
            repository_root: workspace.repository_root.clone(),
            path: workspace.path.clone(),
            branch: workspace.branch.clone(),
            base_revision: workspace.base_revision.clone(),
            state: workspace.state,
            created_at: workspace.created_at,
            updated_at: workspace.updated_at,
        };
        if let Err(err) = self.workspaces.create(&row).await {
            // Roll back the worktree so we never leave an orphan behind.
            tracing::error!(error = %err, "persisting workspace failed; removing worktree");
            let _ = git(
                &workspace.repository_root,
                &[
                    "worktree",
                    "remove",
                    "--force",
                    workspace.path.to_str().unwrap_or_default(),
                ],
            )
            .await;
            return Err(WorkspaceError::Persist(err));
        }
        tracing::debug!(
            workspace_id = %workspace.id,
            session_id = %session.id,
            branch = %workspace.branch,
            "created isolated worktree"
        );
        Ok(workspace)
    }

    /// Verify a stored workspace is still a valid Git worktree of the same
    /// repository. Missing paths become `missing`; mismatched repositories
    /// become `invalid`. Workspaces removed by cleanup are never resurrected.
    pub async fn verify_workspace(
        &self,
        workspace: WorkspaceRow,
    ) -> Result<Workspace, WorkspaceError> {
        if workspace.state == WorkspaceState::Removed {
            return Err(WorkspaceError::WorkspaceRemoved(workspace.id.to_string()));
        }
        let path = workspace.path.clone();
        if !path.exists() {
            let _ = self
                .workspaces
                .set_state(workspace.id, WorkspaceState::Missing)
                .await;
            return Err(WorkspaceError::WorkspaceMissing(path.display().to_string()));
        }
        let toplevel = git_ok(&path, &["rev-parse", "--show-toplevel"]).await?;
        let actual_root = PathBuf::from(toplevel.trim());
        // In a secondary worktree `--show-toplevel` is the worktree itself;
        // compare repository identity via the common git dir instead.
        // `--git-common-dir` is relative to the invocation cwd.
        let common_here = git_ok(&path, &["rev-parse", "--git-common-dir"]).await?;
        let common_stored = git_ok(
            &workspace.repository_root,
            &["rev-parse", "--git-common-dir"],
        )
        .await?;
        let resolve = |base: &Path, rel: &str| canonical(&base.join(rel));
        let same_repo = resolve(&path, common_here.trim())
            == resolve(&workspace.repository_root, common_stored.trim());
        if !same_repo {
            let _ = self
                .workspaces
                .set_state(workspace.id, WorkspaceState::Missing)
                .await;
            return Err(WorkspaceError::WorkspaceInvalid(format!(
                "expected repository `{}` but found `{}`",
                workspace.repository_root.display(),
                actual_root.display()
            )));
        }
        let _ = self.workspaces.touch(workspace.id).await;
        Ok(Workspace {
            id: workspace.id,
            agent_session_id: workspace.agent_session_id,
            repository_root: workspace.repository_root,
            path,
            branch: workspace.branch,
            base_revision: workspace.base_revision,
            state: WorkspaceState::Active,
            created_at: workspace.created_at,
            updated_at: workspace.updated_at,
        })
    }

    /// Load and verify the workspace bound to an agent session.
    pub async fn workspace_for_session(
        &self,
        agent_session_id: Uuid,
    ) -> Result<Workspace, WorkspaceError> {
        let row = self
            .workspaces
            .get_by_agent_session(agent_session_id)
            .await
            .map_err(WorkspaceError::Persist)?
            .ok_or_else(|| WorkspaceError::WorkspaceNotFound(agent_session_id.to_string()))?;
        self.verify_workspace(row).await
    }

    /// Cumulative diff of the workspace since its base revision.
    ///
    /// Untracked files are listed in `untracked_files` but intentionally
    /// excluded from the patch (no synthetic diffs for untracked content).
    pub async fn diff(&self, workspace: &Workspace) -> Result<WorkspaceDiff, WorkspaceError> {
        let patch = git_ok(
            &workspace.path,
            &["diff", "--binary", &workspace.base_revision],
        )
        .await?;
        let status = git_ok(&workspace.path, &["status", "--porcelain=v1"]).await?;

        let mut changed_files = Vec::new();
        let mut untracked_files = Vec::new();
        for line in status.lines() {
            if line.len() < 3 {
                continue;
            }
            let (flags, file) = line.split_at(3);
            let x = flags.as_bytes()[0] as char;
            let y = flags.as_bytes()[1] as char;
            let path_str = file.trim();
            let path = PathBuf::from(path_str);
            let status = match (x, y) {
                ('?', '?') => {
                    untracked_files.push(path);
                    continue;
                }
                ('A', _) => ChangeStatus::Added,
                ('D', _) => ChangeStatus::Deleted,
                ('R', _) => ChangeStatus::Renamed,
                _ => ChangeStatus::Modified,
            };
            changed_files.push(ChangedFile { path, status });
        }

        Ok(WorkspaceDiff {
            patch,
            changed_files,
            untracked_files,
        })
    }

    /// Remove a worktree. Refuses when the workspace has changes unless
    /// `force` is set. Does not cascade to task/context rows.
    pub async fn remove(&self, workspace_id: Uuid, force: bool) -> Result<(), WorkspaceError> {
        let row = self
            .workspaces
            .get(workspace_id)
            .await
            .map_err(WorkspaceError::Persist)?
            .ok_or_else(|| WorkspaceError::WorkspaceNotFound(workspace_id.to_string()))?;

        if !force && !row.path.exists() {
            let _ = self
                .workspaces
                .set_state(workspace_id, WorkspaceState::Missing)
                .await;
            return Ok(());
        }
        if !force {
            let status = git_ok(&row.path, &["status", "--porcelain"]).await?;
            if !status.trim().is_empty() {
                return Err(WorkspaceError::WorkspaceDirty(
                    row.path.display().to_string(),
                ));
            }
        }
        let mut args = vec!["worktree", "remove"];
        if force {
            args.push("--force");
        }
        args.push(
            row.path
                .to_str()
                .ok_or_else(|| WorkspaceError::Internal("non-UTF8 workspace path".to_string()))?,
        );
        git_ok(&row.repository_root, &args).await?;
        self.workspaces
            .set_state(workspace_id, WorkspaceState::Removed)
            .await
            .map_err(WorkspaceError::Persist)?;
        Ok(())
    }

    /// Git availability health check for `doctor`.
    pub async fn health_check(&self) -> (bool, Option<String>) {
        let probe = git(Path::new("/"), &["--version"]).await;
        match probe {
            Ok(output) if output.success() => (true, Some(output.stdout.trim().to_string())),
            _ => (false, None),
        }
    }

    // ---------- Phase 14: lifecycle, archive and cleanup ----------

    /// Archive a workspace: `state → Archived` only. Never touches files or
    /// branches; the worktree stays fully viewable.
    pub async fn archive(&self, workspace_id: Uuid) -> Result<(), WorkspaceError> {
        let row = self
            .workspaces
            .get(workspace_id)
            .await
            .map_err(WorkspaceError::Persist)?
            .ok_or_else(|| WorkspaceError::WorkspaceNotFound(workspace_id.to_string()))?;
        if row.state == WorkspaceState::Removed {
            return Err(WorkspaceError::WorkspaceRemoved(workspace_id.to_string()));
        }
        self.workspaces
            .set_state(workspace_id, WorkspaceState::Archived)
            .await
            .map_err(WorkspaceError::Persist)?;
        Ok(())
    }

    /// Preflight a safe cleanup of a workspace. Never deletes anything.
    ///
    /// Every condition of Phase 14 section 7 must hold:
    /// no live task, no session lease, no active workflow dependency, state is
    /// `Applied` or `Archived`, a completed apply exists (or archive-only was
    /// requested), the workspace is unchanged since the apply (snapshot
    /// fingerprint), and the branch is AgentMesh-managed.
    ///
    /// `applies` supplies the apply history (idempotency + snapshot hash); the
    /// daemon layer supplies `context` with the live-task / lease / workflow
    /// facts it owns.
    pub async fn plan_cleanup(
        &self,
        workspace_id: Uuid,
        applies: &ApplyRepository,
        context: &CleanupContext,
    ) -> Result<CleanupPlan, WorkspaceError> {
        let row = self
            .workspaces
            .get(workspace_id)
            .await
            .map_err(WorkspaceError::Persist)?
            .ok_or_else(|| WorkspaceError::WorkspaceNotFound(workspace_id.to_string()))?;

        // 1-3. Live ownership: a live task, a session lease or an active
        // workflow would make removal unsafe.
        if context.has_live_task {
            return Err(WorkspaceError::WorkspaceNotSafeToRemove(
                workspace_id.to_string(),
                "a live task is bound to this workspace's session".to_string(),
            ));
        }
        if context.has_session_lease {
            return Err(WorkspaceError::WorkspaceNotSafeToRemove(
                workspace_id.to_string(),
                "the agent session holds an active lease".to_string(),
            ));
        }
        if context.has_workflow_dependency {
            return Err(WorkspaceError::WorkspaceNotSafeToRemove(
                workspace_id.to_string(),
                "a running or interrupted workflow depends on this workspace".to_string(),
            ));
        }

        // 4. State: only applied or archived workspaces are removed.
        if row.state != WorkspaceState::Applied && row.state != WorkspaceState::Archived {
            return Err(WorkspaceError::WorkspaceNotSafeToRemove(
                workspace_id.to_string(),
                format!(
                    "workspace state is `{}`; only applied or archived workspaces are removed",
                    row.state.as_str()
                ),
            ));
        }

        // 5. Apply record (or explicit archive-only).
        let has_completed_apply = applies.has_completed_for_workspace(workspace_id).await?;
        if !has_completed_apply && !context.archive_only {
            return Err(WorkspaceError::WorkspaceNotSafeToRemove(
                workspace_id.to_string(),
                "no successful apply record exists and archive-only was not requested".to_string(),
            ));
        }

        // 6. Git identity: the worktree must still exist and belong to the
        // stored repository.
        let workspace = self.verify_workspace(row.clone()).await?;

        // 7. Unchanged since apply: recompute the snapshot fingerprint.
        let diff = self.diff(&workspace).await?;
        let current_hash = workspace_snapshot_hash(&workspace.path, &diff);
        let stored_hash = applies.latest_snapshot_hash(workspace_id).await?;
        let snapshot_matches = match stored_hash.as_deref() {
            Some(stored) => stored == current_hash,
            // Archive-only (no apply record): no baseline to compare.
            None => true,
        };
        if has_completed_apply && !snapshot_matches {
            return Err(WorkspaceError::WorkspaceChangedAfterApply(
                workspace_id.to_string(),
            ));
        }

        // 8. Branch safety: never delete a branch AgentMesh does not own.
        if !is_managed_branch(&row.branch) {
            return Err(WorkspaceError::NotManagedBranch(row.branch.clone()));
        }

        Ok(CleanupPlan {
            workspace_id,
            workspace_path: workspace.path.clone(),
            branch: row.branch.clone(),
            agent_session_id: workspace.agent_session_id,
            state: row.state,
            base_revision: row.base_revision.clone(),
            has_completed_apply,
            snapshot_matches,
            safe: true,
        })
    }

    /// Safely remove a workspace after a full [`Self::plan_cleanup`] preflight:
    /// `git worktree remove`, then delete the AgentMesh-managed branch, then
    /// `state → Removed`.
    ///
    /// The worktree intentionally carries the applied (uncommitted) agent
    /// result, so `git worktree remove --force` is used internally — the safety
    /// checks are what matter, and they all passed in the plan. Branch removal
    /// is verified as AgentMesh-managed first; if it fails the workspace is
    /// marked `Missing`, never wrongly `Removed`.
    pub async fn cleanup(
        &self,
        workspace_id: Uuid,
        applies: &ApplyRepository,
        context: &CleanupContext,
    ) -> Result<CleanupOutcome, WorkspaceError> {
        let plan = self.plan_cleanup(workspace_id, applies, context).await?;
        if !plan.safe {
            return Err(WorkspaceError::WorkspaceNotSafeToRemove(
                workspace_id.to_string(),
                "preflight did not pass".to_string(),
            ));
        }
        let row = self
            .workspaces
            .get(workspace_id)
            .await
            .map_err(WorkspaceError::Persist)?
            .ok_or_else(|| WorkspaceError::WorkspaceNotFound(workspace_id.to_string()))?;

        // 1. Remove the worktree (the applied result lives in the source now).
        let wt = git(
            &row.repository_root,
            &[
                "worktree",
                "remove",
                "--force",
                row.path.to_str().ok_or_else(|| {
                    WorkspaceError::Internal("non-UTF8 workspace path".to_string())
                })?,
            ],
        )
        .await?;
        if !wt.success() {
            return Err(WorkspaceError::GitCommand {
                stderr: wt.stderr.trim().to_string(),
            });
        }

        // 2. Delete only the AgentMesh-managed branch.
        if !is_managed_branch(&row.branch) {
            // Worktree gone but we must not mark this a clean removal.
            self.workspaces
                .set_state(workspace_id, WorkspaceState::Missing)
                .await
                .map_err(WorkspaceError::Persist)?;
            return Err(WorkspaceError::NotManagedBranch(row.branch.clone()));
        }
        let br = git(&row.repository_root, &["branch", "-D", &row.branch]).await?;
        if !br.success() {
            // Branch deletion failed: the worktree is gone but the branch
            // remains — never report a clean `Removed`.
            self.workspaces
                .set_state(workspace_id, WorkspaceState::Missing)
                .await
                .map_err(WorkspaceError::Persist)?;
            return Err(WorkspaceError::GitCommand {
                stderr: br.stderr.trim().to_string(),
            });
        }

        self.workspaces
            .set_state(workspace_id, WorkspaceState::Removed)
            .await
            .map_err(WorkspaceError::Persist)?;
        Ok(CleanupOutcome {
            workspace_id,
            worktree_removed: true,
            branch_removed: true,
            state: WorkspaceState::Removed,
        })
    }
}

/// Whether a branch is one AgentMesh created for an isolated worktree.
///
/// AgentMesh branches look like `agentmesh/<agent>/<session-prefix>`. Anything
/// else is a user branch and must never be deleted by cleanup.
pub fn is_managed_branch(branch: &str) -> bool {
    branch.starts_with("agentmesh/")
}

/// Best-effort canonical path for comparison.
fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
