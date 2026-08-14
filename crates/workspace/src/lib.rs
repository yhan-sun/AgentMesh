//! Git workspace manager: isolated worktrees for coding agent sessions.
//!
//! One agent session owns exactly one persistent workspace. The manager
//! creates Git worktrees, verifies them on resume, computes diffs and
//! handles cleanup — all through the system `git` CLI (no shell strings).

pub mod error;
pub mod git;
pub mod manager;
pub mod model;

pub use error::WorkspaceError;
pub use manager::{WorkspaceManager, is_managed_branch};
pub use model::{
    ChangeStatus, ChangedFile, CleanupContext, CleanupOutcome, CleanupPlan, Workspace,
    WorkspaceDiff, workspace_snapshot_hash,
};
