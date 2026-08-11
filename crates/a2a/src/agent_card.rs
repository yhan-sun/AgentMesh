//! Agent Card: the machine-readable advertisement of an A2A agent.

use agentmesh_core::{AgentDescriptor, AgentSkill};
use serde::{Deserialize, Serialize};

/// Capabilities an A2A agent declares in its card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
}

impl Default for AgentCapabilities {
    fn default() -> Self {
        Self {
            streaming: true,
            push_notifications: false,
        }
    }
}

/// Advertises an agent over A2A (see the A2A protocol Agent Card spec).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCard {
    pub name: String,
    pub description: Option<String>,
    /// Where the agent can be reached, e.g. `https://host/a2a` or `agent://claude`.
    pub url: String,
    pub version: String,
    pub capabilities: AgentCapabilities,
    pub skills: Vec<AgentSkill>,
}

impl AgentCard {
    /// Build a card from a local [`AgentDescriptor`].
    pub fn from_descriptor(
        descriptor: &AgentDescriptor,
        capabilities: AgentCapabilities,
        version: impl Into<String>,
    ) -> Self {
        Self {
            name: descriptor.name.clone(),
            description: descriptor.description.clone(),
            url: descriptor.endpoint.clone(),
            version: version.into(),
            capabilities,
            skills: descriptor.skills.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmesh_core::WorkspaceRequirement;

    #[test]
    fn card_round_trips_through_json() {
        let descriptor = AgentDescriptor {
            id: "claude".into(),
            name: "Claude Code".into(),
            description: Some("coding agent".into()),
            skills: vec![AgentSkill::new("code", None)],
            endpoint: "agent://claude".into(),
            workspace_requirement: WorkspaceRequirement::None,
        };
        let card = AgentCard::from_descriptor(&descriptor, AgentCapabilities::default(), "0.1.0");
        let json = serde_json::to_string(&card).unwrap();
        let back: AgentCard = serde_json::from_str(&json).unwrap();
        assert_eq!(back, card);
        assert!(card.capabilities.streaming);
        assert_eq!(card.url, "agent://claude");
    }
}
