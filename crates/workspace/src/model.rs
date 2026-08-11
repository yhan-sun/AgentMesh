//! Workspace domain model (vendor-neutral, storage-agnostic).

use std::path::PathBuf;

use agentmesh_storage::WorkspaceState;
use chrono::{DateTime, Utc};
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
