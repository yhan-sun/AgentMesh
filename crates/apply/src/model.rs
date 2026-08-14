//! Apply domain model: a validated plan and the outcome of applying it.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One file a plan would touch (tracked change or untracked addition).
///
/// `status` uses the workspace's stable one-letter keys (`A`/`M`/`D`/`R`),
/// plus `U` for untracked files. Kept as a plain string so the plan is
/// serializable across the daemon boundary without leaking git details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedFile {
    pub status: String,
    pub path: String,
}

/// The result of a preflight: everything known about an apply before any
/// write to the source repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyPlan {
    /// The user's source repository the changes would be written to.
    pub source_repository: PathBuf,
    /// The isolated worktree the agent changes came from.
    pub workspace: PathBuf,
    /// The workspace's immutable base revision the source must match.
    pub base_revision: String,
    /// The source repository's current HEAD.
    pub source_revision: String,
    /// Tracked changes (`git apply` patch files), in git status order.
    pub changed_files: Vec<PlannedFile>,
    /// Untracked files to be copied from the workspace.
    pub untracked_files: Vec<String>,
    /// Size of the tracked patch in bytes.
    pub patch_size: u64,
    /// Whether the apply is safe to execute (`false` carries a warning).
    pub applicable: bool,
    /// Soft warnings (e.g. "no changes", "already applied"). Hard failures
    /// surface as errors instead.
    pub warnings: Vec<String>,
    /// `true` when this workspace's result was already applied successfully.
    pub already_applied: bool,
}

impl ApplyPlan {
    /// Total file count shown by the CLI (tracked + untracked).
    pub fn file_count(&self) -> usize {
        self.changed_files.len() + self.untracked_files.len()
    }
}

/// The outcome of a successful (or failed, via error) apply execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyOutcome {
    /// The persisted apply row id.
    pub apply_id: Uuid,
    /// The plan that was executed.
    pub plan: ApplyPlan,
    /// Whether the tracked patch was applied to the source.
    pub tracked_applied: bool,
    /// Number of untracked files copied into the source.
    pub untracked_copied: usize,
    /// SHA-256 fingerprint of the workspace at apply time (Phase 14).
    pub workspace_snapshot_hash: String,
}

/// Human-readable patch size (`18 KiB`, `1.2 MiB`, ...) for CLI output.
pub fn human_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}
