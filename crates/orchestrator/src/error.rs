//! Errors produced by the orchestrator.

use agentmesh_a2a::client::A2AClientError;

/// Orchestrator-level errors (discovery, routing, delegation, workflows).
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("agent `{0}` is not in the directory")]
    AgentNotFound(String),

    #[error("agent `{0}` is offline")]
    AgentOffline(String),

    #[error("no capable agent found for skill `{0}`")]
    NoCapableAgent(String),

    #[error("delegation requires either an intent or an explicit agent")]
    NoIntentOrAgent,

    #[error("workflow preset `{0}` not found")]
    WorkflowPresetNotFound(String),

    #[error("workflow graph contains a dependency cycle: {0:?}")]
    WorkflowCycleDetected(Vec<String>),

    #[error("cannot resume DAG workflow: {0}")]
    InvalidDagResume(String),

    #[error("invalid review result: {0}")]
    InvalidReviewResult(String),

    #[error("A2A client error: {0}")]
    A2A(#[from] A2AClientError),
}
