//! Single-hop delegation: orchestrator → one selected agent, through A2A.
//!
//! Phase 9 delegation is `orchestrator → one agent` only (agent-to-agent
//! workflows are Phase 10). The A2A client submits the task; the real agent
//! process is owned by the daemon behind the A2A server. Adapters are never
//! called directly from here.

use std::pin::Pin;

use agentmesh_a2a::client::{A2AClient, A2AClientError, A2AClientEvent};
use agentmesh_a2a::types::Message;
use agentmesh_core::TaskIntent;
use futures::Stream;
use uuid::Uuid;

use crate::directory::AgentDirectory;
use crate::error::OrchestratorError;
use crate::router::{RouteDecision, RuleRouter};

type EventStream = Pin<Box<dyn Stream<Item = Result<A2AClientEvent, A2AClientError>> + Send>>;

/// The selected agent for a delegation, before the task starts.
#[derive(Clone)]
pub struct Delegation {
    pub agent_id: String,
    pub reason: String,
    pub client: A2AClient,
}

/// A live delegation: the selected agent, the started task and its stream.
pub struct ActiveDelegation {
    pub agent_id: String,
    pub reason: String,
    pub task_id: Uuid,
    pub context_id: Option<Uuid>,
    pub stream: EventStream,
    pub client: A2AClient,
}

/// Resolve the target agent: explicit `--agent` bypasses the router but still
/// validates existence, online status and a valid A2A card.
pub fn pick_agent(
    directory: &AgentDirectory,
    router: &RuleRouter,
    intent: Option<TaskIntent>,
    explicit_agent: Option<String>,
) -> Result<Delegation, OrchestratorError> {
    pick_agent_with_constraints(directory, router, intent, explicit_agent, &[])
}

/// Resolve the target agent, excluding the given agent ids (Phase 21 §4).
///
/// Used by parallel evaluation groups so each evaluator is a distinct agent —
/// the same session is never counted as multiple votes. Still Card + routing
/// config driven, never brand-specific.
pub fn pick_agent_with_constraints(
    directory: &AgentDirectory,
    router: &RuleRouter,
    intent: Option<TaskIntent>,
    explicit_agent: Option<String>,
    excluded: &[String],
) -> Result<Delegation, OrchestratorError> {
    let decision = match (explicit_agent, intent) {
        (Some(agent), _) => router.explicit(directory, &agent)?,
        (None, Some(intent)) => router.route_with_constraints(directory, intent, excluded),
        (None, None) => return Err(OrchestratorError::NoIntentOrAgent),
    };
    match decision {
        RouteDecision::Agent { agent_id, reason } => {
            let client = directory
                .client(&agent_id)
                .ok_or_else(|| OrchestratorError::AgentNotFound(agent_id.clone()))?;
            Ok(Delegation {
                agent_id,
                reason,
                client,
            })
        }
        RouteDecision::NoCapableAgent { skill } => Err(OrchestratorError::NoCapableAgent(skill)),
    }
}

/// Delegate a prompt to a single A2A agent and return the live stream.
///
/// Creates a fresh context / session / task on the daemon (no `contextId` is
/// sent in Phase 9), matching the Phase 9 context semantics.
pub async fn delegate(
    directory: &AgentDirectory,
    router: &RuleRouter,
    intent: Option<TaskIntent>,
    explicit_agent: Option<String>,
    prompt: &str,
) -> Result<ActiveDelegation, OrchestratorError> {
    let delegation = pick_agent(directory, router, intent, explicit_agent)?;
    let streaming = delegation
        .client
        .send_streaming_message(&Message::user_text(prompt))
        .await?;
    Ok(ActiveDelegation {
        agent_id: delegation.agent_id,
        reason: delegation.reason,
        task_id: streaming.task.id,
        context_id: streaming.task.context_id,
        stream: streaming.events,
        client: delegation.client,
    })
}
