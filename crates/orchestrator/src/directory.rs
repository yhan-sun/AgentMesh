//! AgentDirectory: discovery of local A2A agents.
//!
//! Discovery source (Phase 9 = loopback only):
//!
//! ```text
//! daemon /v1/runtime → local A2A agent URLs → GET /.well-known/agent-card.json
//! ```
//!
//! Agent Cards are untrusted input: they are parsed tolerantly (unknown
//! fields are ignored) and are never used to bypass the protocol boundary.

use std::collections::HashMap;

use agentmesh_a2a::AgentCard;
use agentmesh_a2a::client::A2AClient;
use serde::{Deserialize, Serialize};

use crate::error::OrchestratorError;

/// Health of a discovered A2A agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentHealth {
    Online,
    Offline,
}

/// Auth material for calling a discovered agent's A2A RPC endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentAuth {
    /// Bearer token for the daemon's A2A listeners.
    pub token: Option<String>,
}

/// A local A2A agent reported by the daemon runtime endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredEndpoint {
    pub agent_id: String,
    /// Listener base URL, e.g. `http://127.0.0.1:45678/`.
    pub url: String,
    /// Agent card URL.
    pub card_url: String,
}

/// One entry in the directory: the card-backed view of a local agent.
#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub agent_id: String,
    pub endpoint: String,
    pub card_url: String,
    pub card: AgentCard,
    pub health: AgentHealth,
    pub auth: AgentAuth,
}

impl DirectoryEntry {
    /// Whether the agent's card declares a skill by name.
    pub fn has_skill(&self, skill: &str) -> bool {
        self.card.skills.iter().any(|s| s.name == skill)
    }
}

/// In-memory directory of discovered A2A agents.
#[derive(Debug, Clone, Default)]
pub struct AgentDirectory {
    entries: HashMap<String, DirectoryEntry>,
}

impl AgentDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an entry directly (tests, explicit registration).
    pub fn insert(&mut self, entry: DirectoryEntry) {
        self.entries.insert(entry.agent_id.clone(), entry);
    }

    /// Entry by agent id.
    pub fn get(&self, agent_id: &str) -> Option<&DirectoryEntry> {
        self.entries.get(agent_id)
    }

    /// All entries, sorted by agent id for deterministic output.
    pub fn list(&self) -> Vec<&DirectoryEntry> {
        let mut entries: Vec<_> = self.entries.values().collect();
        entries.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        entries
    }

    /// Online entries, sorted by agent id.
    pub fn online_agents(&self) -> Vec<&DirectoryEntry> {
        let mut entries: Vec<_> = self
            .entries
            .values()
            .filter(|entry| entry.health == AgentHealth::Online)
            .collect();
        entries.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        entries
    }

    /// Online entries whose card declares a skill, sorted by agent id.
    pub fn find_by_skill(&self, skill: &str) -> Vec<&DirectoryEntry> {
        let mut entries: Vec<_> = self
            .entries
            .values()
            .filter(|entry| entry.health == AgentHealth::Online && entry.has_skill(skill))
            .collect();
        entries.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        entries
    }

    /// An A2A client for an entry, wired with its auth.
    pub fn client(&self, agent_id: &str) -> Option<A2AClient> {
        self.get(agent_id).map(|entry| {
            let mut client =
                A2AClient::new(entry.endpoint.clone()).with_card_url(entry.card_url.clone());
            if let Some(token) = &entry.auth.token {
                client = client.with_token(token.clone());
            }
            client
        })
    }

    /// Discover agents from daemon-reported endpoints by fetching each card.
    ///
    /// An agent whose card cannot be fetched is kept as [`AgentHealth::Offline`];
    /// it never fails the whole refresh.
    pub async fn refresh(
        &mut self,
        discovered: &[DiscoveredEndpoint],
        auth: &AgentAuth,
    ) -> Result<(), OrchestratorError> {
        let mut entries = HashMap::new();
        for endpoint in discovered {
            entries.insert(
                endpoint.agent_id.clone(),
                discover_entry(endpoint, auth).await,
            );
        }
        self.entries = entries;
        Ok(())
    }
}

async fn discover_entry(endpoint: &DiscoveredEndpoint, auth: &AgentAuth) -> DirectoryEntry {
    let mut client = A2AClient::new(endpoint.url.clone()).with_card_url(endpoint.card_url.clone());
    if let Some(token) = &auth.token {
        client = client.with_token(token.clone());
    }
    match client.fetch_agent_card().await {
        Ok(card) => DirectoryEntry {
            agent_id: endpoint.agent_id.clone(),
            endpoint: endpoint.url.clone(),
            card_url: endpoint.card_url.clone(),
            card,
            health: AgentHealth::Online,
            auth: auth.clone(),
        },
        Err(err) => {
            tracing::warn!(
                agent_id = %endpoint.agent_id,
                error = %err,
                "agent card fetch failed; marking agent offline"
            );
            DirectoryEntry {
                agent_id: endpoint.agent_id.clone(),
                endpoint: endpoint.url.clone(),
                card_url: endpoint.card_url.clone(),
                card: AgentCard {
                    name: endpoint.agent_id.clone(),
                    description: None,
                    url: endpoint.url.clone(),
                    version: String::new(),
                    capabilities: Default::default(),
                    skills: Vec::new(),
                    supported_interfaces: None,
                    security_schemes: None,
                },
                health: AgentHealth::Offline,
                auth: auth.clone(),
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use agentmesh_a2a::agent_card::AgentCapabilities;
    use agentmesh_core::AgentSkill;

    /// Test helper: a directory entry with the given skills and health.
    pub fn entry(agent_id: &str, skills: &[&str], health: AgentHealth) -> DirectoryEntry {
        DirectoryEntry {
            agent_id: agent_id.to_string(),
            endpoint: format!("http://127.0.0.1:1/{agent_id}"),
            card_url: format!("http://127.0.0.1:1/{agent_id}/card"),
            card: AgentCard {
                name: agent_id.to_string(),
                description: None,
                url: format!("http://127.0.0.1:1/{agent_id}/"),
                version: "1.0".to_string(),
                capabilities: AgentCapabilities::default(),
                skills: skills
                    .iter()
                    .map(|skill| AgentSkill::new(*skill, None))
                    .collect(),
                supported_interfaces: None,
                security_schemes: None,
            },
            health,
            auth: AgentAuth { token: None },
        }
    }

    fn directory(entries: &[(&str, &[&str], AgentHealth)]) -> AgentDirectory {
        let mut directory = AgentDirectory::new();
        for (agent_id, skills, health) in entries {
            directory.insert(entry(agent_id, skills, *health));
        }
        directory
    }

    #[test]
    fn get_and_list_sort_by_agent_id() {
        let directory = directory(&[
            ("beta", &["code"], AgentHealth::Online),
            ("alpha", &["code"], AgentHealth::Online),
        ]);
        assert_eq!(directory.list()[0].agent_id, "alpha");
        assert_eq!(directory.list()[1].agent_id, "beta");
        assert!(directory.get("beta").is_some());
        assert!(directory.get("nope").is_none());
    }

    #[test]
    fn find_by_skill_returns_only_online_matching_agents() {
        let directory = directory(&[
            ("alpha", &["code", "testing"], AgentHealth::Online),
            ("beta", &["code"], AgentHealth::Offline),
            ("gamma", &["testing"], AgentHealth::Online),
        ]);
        let testing = directory.find_by_skill("testing");
        let ids: Vec<_> = testing.iter().map(|e| e.agent_id.as_str()).collect();
        assert_eq!(ids, vec!["alpha", "gamma"]);
        assert!(directory.find_by_skill("review").is_empty());
    }

    #[test]
    fn has_skill_reads_the_card() {
        let alpha = entry("alpha", &["code"], AgentHealth::Online);
        assert!(alpha.has_skill("code"));
        assert!(!alpha.has_skill("debug"));
    }

    #[test]
    fn client_is_wired_with_auth() {
        let mut entry = entry("alpha", &["code"], AgentHealth::Online);
        entry.auth.token = Some("secret".into());
        let mut directory = AgentDirectory::new();
        directory.insert(entry);
        assert!(directory.client("alpha").is_some());
        assert!(directory.client("nope").is_none());
    }
}
