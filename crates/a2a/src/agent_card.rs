//! Agent Card: the machine-readable advertisement of an A2A agent.

use agentmesh_core::{AgentDescriptor, AgentSkill};
use serde::{Deserialize, Serialize};

/// Capabilities an A2A agent declares in its card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
}

/// One supported interface binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupportedInterface {
    pub url: String,
    pub protocol_binding: String,
    pub protocol_version: String,
}

/// Security scheme declared on the card (token never included).
///
/// Cards are untrusted input: an unknown scheme type is tolerated and kept
/// as [`SecurityScheme::Other`] instead of failing the whole parse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityScheme {
    HttpBearer {
        scheme: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bearer_format: Option<String>,
    },
    /// Any security scheme type this AgentMesh build does not recognize.
    #[serde(other)]
    Other,
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
///
/// Cards are untrusted input: `capabilities` and `skills` default to empty
/// when absent so a card written for a future spec version still parses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: Option<String>,
    /// Where the agent can be reached, e.g. `https://host/a2a` or `agent://claude`.
    pub url: String,
    pub version: String,
    #[serde(default)]
    pub capabilities: AgentCapabilities,
    #[serde(default)]
    pub skills: Vec<AgentSkill>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_interfaces: Option<Vec<SupportedInterface>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_schemes: Option<Vec<SecurityScheme>>,
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
            supported_interfaces: None,
            security_schemes: None,
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
        let mut card =
            AgentCard::from_descriptor(&descriptor, AgentCapabilities::default(), "0.1.0");
        card.supported_interfaces = Some(vec![SupportedInterface {
            url: "agent://claude".into(),
            protocol_binding: "JSONRPC".into(),
            protocol_version: "1.0".into(),
        }]);
        card.security_schemes = Some(vec![SecurityScheme::HttpBearer {
            scheme: "bearer".into(),
            bearer_format: Some("opaque".into()),
        }]);
        let json = serde_json::to_string(&card).unwrap();
        let back: AgentCard = serde_json::from_str(&json).unwrap();
        assert_eq!(back, card);
        assert!(card.capabilities.streaming);
        assert_eq!(card.url, "agent://claude");
    }

    #[test]
    fn card_tolerates_unknown_fields_and_unknown_schemes() {
        // A card written for a future spec version must still parse: unknown
        // top-level fields, unknown fields inside known objects, missing
        // capabilities/skills, and an unknown security scheme type.
        let json = r#"{
            "name": "Future Agent",
            "url": "http://127.0.0.1:1/",
            "version": "2.0",
            "capabilities": { "streaming": true, "pushNotifications": false, "futureCap": true },
            "skills": [{ "name": "code", "description": "x", "futureSkill": 1 }],
            "supportedInterfaces": [{
                "url": "http://127.0.0.1:1/",
                "protocolBinding": "JSONRPC",
                "protocolVersion": "1.0",
                "futureBinding": true
            }],
            "securitySchemes": [{ "type": "HTTP_DIGEST", "scheme": "digest" }],
            "futureField": { "nested": [1, 2, 3] }
        }"#;
        let card: AgentCard = serde_json::from_str(json).expect("card must parse");
        assert_eq!(card.name, "Future Agent");
        assert_eq!(card.skills[0].name, "code");
        assert!(matches!(
            card.security_schemes.as_deref().unwrap()[0],
            SecurityScheme::Other
        ));
    }

    #[test]
    fn card_missing_capabilities_defaults() {
        let json = r#"{
            "name": "Minimal",
            "url": "http://127.0.0.1:1/",
            "version": "1.0"
        }"#;
        let card: AgentCard = serde_json::from_str(json).expect("minimal card parses");
        assert_eq!(card.capabilities, AgentCapabilities::default());
        assert!(card.skills.is_empty());
    }
}
