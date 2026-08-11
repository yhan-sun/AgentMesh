use thiserror::Error;

/// Errors produced by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to open database at `{path}`: {source}")]
    Open { path: String, source: sqlx::Error },

    #[error("failed to run migrations: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("failed to create task {task_id}: {source}")]
    CreateTask {
        task_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load task {task_id}: {source}")]
    LoadTask {
        task_id: String,
        source: sqlx::Error,
    },

    #[error("failed to list tasks: {0}")]
    ListTasks(#[from] sqlx::Error),

    #[error("failed to update task {task_id} status to {status}: {source}")]
    UpdateTaskStatus {
        task_id: String,
        status: String,
        source: sqlx::Error,
    },

    #[error("failed to create task directory `{path}`: {source}")]
    CreateTaskDir {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to insert artifact {artifact_id} for task {task_id}: {source}")]
    InsertArtifact {
        artifact_id: String,
        task_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load artifacts for task {task_id}: {source}")]
    LoadArtifacts {
        task_id: String,
        source: sqlx::Error,
    },

    #[error("artifact metadata for {artifact_id} is not valid JSON: {source}")]
    InvalidMetadata {
        artifact_id: String,
        source: serde_json::Error,
    },

    #[error("artifact content is not valid UTF-8: {0}")]
    InvalidContent(std::str::Utf8Error),

    #[error("artifact `{0}` is too large to store inline (limit {1} bytes)")]
    ArtifactTooLarge(String, usize),

    #[error("failed to create context {context_id}: {source}")]
    CreateContext {
        context_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load context {context_id}: {source}")]
    LoadContext {
        context_id: String,
        source: sqlx::Error,
    },

    #[error("failed to touch context {context_id}: {source}")]
    TouchContext {
        context_id: String,
        source: sqlx::Error,
    },

    #[error("failed to create agent session {session_id}: {source}")]
    CreateSession {
        session_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load agent session {session_id}: {source}")]
    LoadSession {
        session_id: String,
        source: sqlx::Error,
    },

    #[error("failed to update agent session {session_id}: {source}")]
    UpdateSession {
        session_id: String,
        source: sqlx::Error,
    },

    #[error("failed to touch agent session {session_id}: {source}")]
    TouchSession {
        session_id: String,
        source: sqlx::Error,
    },

    #[error("agent session `{0}` not found")]
    SessionNotFound(String),

    #[error("failed to create workspace {workspace_id}: {source}")]
    CreateWorkspace {
        workspace_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load workspace {workspace_id}: {source}")]
    LoadWorkspace {
        workspace_id: String,
        source: sqlx::Error,
    },

    #[error("failed to update workspace {workspace_id}: {source}")]
    UpdateWorkspace {
        workspace_id: String,
        source: sqlx::Error,
    },

    #[error("workspace `{0}` not found")]
    WorkspaceNotFound(String),

    #[error("failed to write artifact file `{path}`: {source}")]
    WriteArtifactFile {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to create artifact directory `{path}`: {source}")]
    CreateArtifactDir {
        path: String,
        source: std::io::Error,
    },
}
