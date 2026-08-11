//! Registry of adapters available to the orchestrator and CLI.

use agentmesh_core::{AgentMeshConfig, CoreError};

use crate::adapter::CodingAgentAdapter;
use crate::claude::ClaudeAdapter;
use crate::codex::CodexAdapter;
use crate::mock::MockAgentAdapter;

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
