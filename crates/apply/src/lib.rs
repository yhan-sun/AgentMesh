//! Safe Apply (Phase 13): apply a workspace's reviewed changes back to the
//! user's source repository.
//!
//! `ApplyManager` is the only layer that writes agent results into the source
//! repository. It plans (preflight), applies (tracked patch + untracked
//! copies), detects conflicts, rolls back on failure, and persists every run.
//! Apply is not commit/merge: the source HEAD and the agent worktree are never
//! modified.
//!
//! ```text
//! CLI → Daemon → ApplyManager → Workspace/Git abstraction → source repository
//! ```

pub mod error;
pub mod manager;
pub mod model;
pub mod path;

pub use error::ApplyError;
pub use manager::ApplyManager;
pub use model::{ApplyOutcome, ApplyPlan, PlannedFile, human_size};
