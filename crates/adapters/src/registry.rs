//! Registry of adapters available to the orchestrator and CLI.

use agentmesh_core::{AgentMeshConfig, CoreError};

use crate::adapter::CodingAgentAdapter;
use crate::antigravity::AntigravityAdapter;
use crate::claude::ClaudeAdapter;
use crate::codex::CodexAdapter;
use crate::mock::MockAgentAdapter;
use crate::opencode::OpenCodeAdapter;

/// Collects the adapters AgentMesh can dispatch tasks to.
#[derive(Default)]
pub struct AgentRegistry {
    agents: Vec<Box<dyn CodingAgentAdapter>>,
}

impl AgentRegistry {
    /// Registry with the built-in agents (currently the mock agent).
    pub fn builtin() -> Self {
        let mut registry = Self::default();
        registry.register(Box::new(MockAgentAdapter::new()));
        registry
    }

    /// Registry built from configuration: enabled agents only.
    ///
    /// Unknown agent ids in the config are logged and skipped, so a config
    /// written for a future AgentMesh version degrades gracefully.
    pub fn from_config(config: &AgentMeshConfig) -> Self {
        let mut registry = Self::default();
        for (id, agent_config) in &config.agents {
            if !agent_config.enabled {
                tracing::debug!(agent_id = id, "agent disabled by config, skipping");
                continue;
            }
            match id.as_str() {
                "mock" => registry.register(Box::new(MockAgentAdapter::new())),
                "claude" => registry.register(Box::new(ClaudeAdapter::from_config(agent_config))),
                "codex" => match CodexAdapter::from_config(agent_config) {
                    Ok(adapter) => registry.register(Box::new(adapter)),
                    Err(err) => {
                        tracing::warn!(agent_id = "codex", error = %err, "codex adapter misconfigured, skipping");
                    }
                },
                "opencode" => match OpenCodeAdapter::from_config(agent_config) {
                    Ok(adapter) => registry.register(Box::new(adapter)),
                    Err(err) => {
                        tracing::warn!(agent_id = "opencode", error = %err, "opencode adapter misconfigured, skipping");
                    }
                },
                "antigravity" => match AntigravityAdapter::from_config(agent_config) {
                    Ok(adapter) => registry.register(Box::new(adapter)),
                    Err(err) => {
                        tracing::warn!(agent_id = "antigravity", error = %err, "antigravity adapter misconfigured, skipping");
                    }
                },
                other => tracing::warn!(agent_id = other, "unknown agent in config, skipping"),
            }
        }
        registry
    }

    pub fn register(&mut self, agent: Box<dyn CodingAgentAdapter>) {
        self.agents.push(agent);
    }

    pub fn get(&self, id: &str) -> Result<&dyn CodingAgentAdapter, CoreError> {
        self.agents
            .iter()
            .find(|agent| agent.id() == id)
            .map(|agent| agent.as_ref())
            .ok_or_else(|| CoreError::AgentNotFound(id.to_string()))
    }

    pub fn list(&self) -> &[Box<dyn CodingAgentAdapter>] {
        &self.agents
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmesh_core::AgentMeshConfig;

    fn config_with(agents: &[&str]) -> AgentMeshConfig {
        let mut config = AgentMeshConfig::default_config();
        config.agents.retain(|id, _| agents.contains(&id.as_str()));
        config
    }

    #[test]
    fn from_config_registers_each_enabled_agent() {
        let registry = AgentRegistry::from_config(&config_with(&[
            "mock",
            "claude",
            "codex",
            "opencode",
            "antigravity",
        ]));
        let mut ids: Vec<&str> = registry.list().iter().map(|a| a.id()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec!["antigravity", "claude", "codex", "mock", "opencode"]
        );
        assert!(registry.get("opencode").is_ok());
        assert!(registry.get("antigravity").is_ok());
    }

    #[test]
    fn from_config_skips_disabled_agents() {
        let mut config = AgentMeshConfig::default_config();
        config.agents.get_mut("opencode").expect("opencode").enabled = false;
        let registry = AgentRegistry::from_config(&config);
        let ids: Vec<&str> = registry.list().iter().map(|a| a.id()).collect();
        assert!(!ids.contains(&"opencode"));
        assert!(ids.contains(&"antigravity"));
    }

    #[test]
    fn from_config_skips_unknown_agents() {
        let mut config = AgentMeshConfig::default_config();
        config
            .agents
            .insert("future-agent".to_string(), Default::default());
        let registry = AgentRegistry::from_config(&config);
        let ids: Vec<&str> = registry.list().iter().map(|a| a.id()).collect();
        assert!(!ids.contains(&"future-agent"));
    }
}
