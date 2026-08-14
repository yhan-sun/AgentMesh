//! Errors produced by the apply layer (Phase 13).

use uuid::Uuid;

/// Errors produced by [`crate::ApplyManager`].
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("task `{0}` not found")]
    TaskNotFound(Uuid),

    #[error("task `{0}` has no agent session and cannot be applied")]
    TaskHasNoSession(Uuid),

    #[error("task `{0}` has no workspace to apply")]
    TaskHasNoWorkspace(Uuid),

    #[error("workflow `{0}` not found")]
    WorkflowNotFound(Uuid),

    #[error(
        "cannot uniquely determine the apply source for workflow `{0}` (no completed Fixer or Implementer step)"
    )]
    AmbiguousApplySource(Uuid),

    #[error("workflow `{0}` is not completed (status `{1}`); only completed workflows apply")]
    WorkflowNotCompleted(Uuid, String),

    #[error("workflow `{0}` final review is not approved; refusing to apply")]
    ReviewNotApproved(Uuid),

    #[error("source repository no longer exists: `{0}`")]
    SourceRepositoryMissing(String),

    #[error("source repository is dirty; commit or revert the changes before applying")]
    SourceRepositoryDirty,

    #[error(
        "source HEAD has moved since the workspace base revision; refusing to apply (base `{base}`, current `{current}`)"
    )]
    SourceRevisionChanged { base: String, current: String },

    #[error("apply conflict: target path `{0}` already exists in the source repository")]
    ApplyConflict(String),

    #[error(
        "unsafe apply path `{0}` (must stay inside the repository, no `..`, no absolute paths)"
    )]
    UnsafeApplyPath(String),

    #[error("workspace source file `{0}` is missing")]
    SourceFileMissing(String),

    #[error("git apply --check rejected the patch: {0}")]
    ApplyCheckFailed(String),

    #[error("git apply failed: {0}")]
    ApplyFailed(String),

    #[error("no changes to apply")]
    NoChanges,

    #[error("the workspace result has already been applied; use --check to inspect it")]
    AlreadyApplied,

    #[error("another apply of this workspace is currently in progress; retry after it finishes")]
    ApplyInProgress,

    #[error("failed to copy `{0}`: {1}")]
    CopyFailed(String, String),

    #[error(
        "rollback failed ({0}); the source repository may need manual recovery — inspect and revert it before continuing"
    )]
    ApplyRollbackFailed(String),

    #[error("workspace error: {0}")]
    Workspace(#[from] agentmesh_workspace::WorkspaceError),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("internal error: {0}")]
    Internal(String),
}
