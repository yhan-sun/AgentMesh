//! Workspace domain model (vendor-neutral, storage-agnostic).

use std::path::{Path, PathBuf};

use agentmesh_storage::WorkspaceState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// A workspace bound to exactly one agent session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub id: Uuid,
    pub agent_session_id: Uuid,
    /// Repository the worktree was created from.
    pub repository_root: PathBuf,
    /// The isolated worktree path the agent actually runs in.
    pub path: PathBuf,
    /// Worktree branch, e.g. `agentmesh/claude/f25a18d1`.
    pub branch: String,
    /// Immutable commit the worktree was created from.
    pub base_revision: String,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Change status of a file relative to the workspace base revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
    Untracked,
}

impl ChangeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeStatus::Added => "A",
            ChangeStatus::Modified => "M",
            ChangeStatus::Deleted => "D",
            ChangeStatus::Renamed => "R",
            ChangeStatus::Untracked => "U",
        }
    }
}

/// A file changed inside a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedFile {
    pub path: PathBuf,
    pub status: ChangeStatus,
}

/// Cumulative diff of a workspace since its base revision.
///
/// Scope is deliberately the whole workspace: resumed tasks accumulate
/// changes (Task 1 + Task 2), never task-local patches.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceDiff {
    /// `git diff <base_revision>` output (tracked changes only).
    pub patch: String,
    pub changed_files: Vec<ChangedFile>,
    /// Untracked file paths (not part of `patch`).
    pub untracked_files: Vec<PathBuf>,
}

impl WorkspaceDiff {
    pub fn is_empty(&self) -> bool {
        self.patch.trim().is_empty() && self.untracked_files.is_empty()
    }
}

// ---------- Phase 14: cleanup ----------

/// External facts about a workspace that only the daemon layer knows, passed
/// into [`crate::WorkspaceManager::plan_cleanup`] / [`crate::WorkspaceManager::cleanup`].
#[derive(Debug, Clone, Default)]
pub struct CleanupContext {
    /// A live (non-terminal) task is bound to the workspace's session.
    pub has_live_task: bool,
    /// The session currently holds an active lease.
    pub has_session_lease: bool,
    /// A running or interrupted workflow depends on the session.
    pub has_workflow_dependency: bool,
    /// The user explicitly asked to clean up even without a successful apply
    /// record. Every other safety check still applies.
    pub archive_only: bool,
}

// The result of a cleanup preflight: everything known before anything is
/// deleted. Cleanup is preview-only by default.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupPlan {
    pub workspace_id: Uuid,
    pub workspace_path: PathBuf,
    pub branch: String,
    pub agent_session_id: Uuid,
    pub state: WorkspaceState,
    pub base_revision: String,
    /// Whether a completed apply of this workspace exists.
    pub has_completed_apply: bool,
    /// Whether the current workspace content matches the fingerprint recorded
    /// at apply time (`true` for archive-only workspaces with no apply record).
    pub snapshot_matches: bool,
    /// Whether the workspace passes every safety check and can be removed.
    pub safe: bool,
}

// What a cleanup actually removed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupOutcome {
    pub workspace_id: Uuid,
    pub worktree_removed: bool,
    pub branch_removed: bool,
    pub state: WorkspaceState,
}

/// SHA-256 fingerprint of a workspace's agent changes (Phase 14).
///
/// Based on the tracked patch plus each untracked file's relative path and
/// content. Recorded on the apply row at apply time and recomputed before a
/// cleanup so changes made *after* the apply (`WorkspaceChangedAfterApply`)
/// are detected instead of silently removed.
pub fn workspace_snapshot_hash(workspace_root: &Path, diff: &WorkspaceDiff) -> String {
    let mut hasher = Sha256::new();
    hasher.update(diff.patch.as_bytes());
    hasher.update(b"\n");
    let mut untracked: Vec<(&Path, Vec<u8>)> = diff
        .untracked_files
        .iter()
        .filter_map(|rel| {
            std::fs::read(workspace_root.join(rel))
                .ok()
                .map(|content| (rel.as_path(), content))
        })
        .collect();
    untracked.sort_by(|a, b| a.0.cmp(b.0));
    for (rel, content) in untracked {
        hasher.update(rel.to_string_lossy().as_bytes());
        hasher.update(b"\0");
        hasher.update(&content);
        hasher.update(b"\0");
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
