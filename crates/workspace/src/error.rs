use thiserror::Error;

/// Errors produced by the workspace manager.
#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("not inside a Git repository (or git is unavailable): {0}")]
    NotAGitRepository(String),

    #[error("bare Git repositories are not supported")]
    UnsupportedRepository,

    #[error(
        "repository has uncommitted changes; commit or stash them before creating an isolated AgentMesh workspace"
    )]
    DirtyRepository,

    #[error("workspace already exists for this session: {0}")]
    WorkspaceAlreadyExists(String),

    #[error("workspace conflict: git state does not match the database ({0})")]
    WorkspaceConflict(String),

    #[error("workspace path no longer exists: {0}")]
    WorkspaceMissing(String),

    #[error("workspace is no longer a valid Git worktree: {0}")]
    WorkspaceInvalid(String),

    #[error("workspace `{0}` has uncommitted changes and cannot be removed without --force")]
    WorkspaceDirty(String),

    #[error("workspace `{0}` was removed by AgentMesh cleanup; it cannot be resumed or reused")]
    WorkspaceRemoved(String),

    #[error("workspace `{0}` is not safe to remove: {1}")]
    WorkspaceNotSafeToRemove(String, String),

    #[error(
        "workspace `{0}` changed after it was applied; refusing to remove it (the applied result would be lost)"
    )]
    WorkspaceChangedAfterApply(String),

    #[error("refusing to delete a branch AgentMesh does not manage: `{0}`")]
    NotManagedBranch(String),

    #[error("workspace `{0}` not found")]
    WorkspaceNotFound(String),

    #[error("git command failed: {stderr}")]
    GitCommand { stderr: String },

    #[error("failed to persist workspace: {0}")]
    Persist(#[from] agentmesh_storage::StorageError),

    #[error("workspace database error: {0}")]
    Storage(String),

    #[error("internal error: {0}")]
    Internal(String),
}
