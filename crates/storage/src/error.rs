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

    #[error("failed to list workspaces: {0}")]
    ListWorkspaces(sqlx::Error),

    #[error("workspace `{0}` not found")]
    WorkspaceNotFound(String),

    #[error("failed to write artifact file `{path}`: {source}")]
    WriteArtifactFile {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to delete artifact file `{path}`: {source}")]
    DeleteArtifactFile {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to prune artifact files: {0}")]
    PruneArtifacts(sqlx::Error),

    #[error("failed to create artifact directory `{path}`: {source}")]
    CreateArtifactDir {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to create workflow {workflow_id}: {source}")]
    CreateWorkflow {
        workflow_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load workflow {workflow_id}: {source}")]
    LoadWorkflow {
        workflow_id: String,
        source: sqlx::Error,
    },

    #[error("failed to list workflows: {0}")]
    ListWorkflows(sqlx::Error),

    #[error("failed to update workflow {workflow_id}: {source}")]
    UpdateWorkflow {
        workflow_id: String,
        source: sqlx::Error,
    },

    #[error("failed to recover interrupted workflows: {0}")]
    RecoverWorkflows(sqlx::Error),

    #[error("workflow `{0}` not found")]
    WorkflowNotFound(String),

    #[error("failed to upsert step for workflow {workflow_id} at ordinal {ordinal}: {source}")]
    UpsertStep {
        workflow_id: String,
        ordinal: i64,
        source: sqlx::Error,
    },

    #[error("failed to load steps for workflow {workflow_id}: {source}")]
    ListSteps {
        workflow_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load step {step_id}: {source}")]
    LoadStep {
        step_id: String,
        source: sqlx::Error,
    },

    #[error("failed to recover interrupted workflow steps: {0}")]
    RecoverSteps(sqlx::Error),

    #[error("failed to set dependency edges for workflow {workflow_id}: {source}")]
    SetDependencies {
        workflow_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load dependency edges for workflow {workflow_id}: {source}")]
    ListDependencies {
        workflow_id: String,
        source: sqlx::Error,
    },

    #[error("failed to create apply {apply_id}: {source}")]
    CreateApply {
        apply_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load apply {apply_id}: {source}")]
    LoadApply {
        apply_id: String,
        source: sqlx::Error,
    },

    #[error("failed to list applies: {0}")]
    ListApplies(sqlx::Error),

    #[error("failed to update apply {apply_id}: {source}")]
    UpdateApply {
        apply_id: String,
        source: sqlx::Error,
    },

    #[error("apply `{0}` not found")]
    ApplyNotFound(String),

    #[error("failed to create workflow plan {plan_id}: {source}")]
    CreatePlan {
        plan_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load workflow plan {plan_id}: {source}")]
    LoadPlan {
        plan_id: String,
        source: sqlx::Error,
    },

    #[error("failed to list workflow plans: {0}")]
    ListPlans(sqlx::Error),

    #[error("failed to update workflow plan {plan_id}: {source}")]
    UpdatePlan {
        plan_id: String,
        source: sqlx::Error,
    },

    #[error("workflow plan `{0}` not found")]
    PlanNotFound(String),

    #[error("failed to append provenance event {event_id}: {source}")]
    AppendProvenanceEvent {
        event_id: String,
        source: sqlx::Error,
    },

    #[error("failed to load provenance event {event_id}: {source}")]
    LoadProvenanceEvent {
        event_id: String,
        source: sqlx::Error,
    },

    #[error("failed to list provenance events: {0}")]
    ListProvenanceEvents(sqlx::Error),
}
