//! A2A listener management: one loopback listener per enabled, online agent.

use std::sync::Arc;

use agentmesh_a2a::{A2AServerConfig, server};
use agentmesh_adapters::HealthStatus;

use crate::a2a_backend::DaemonA2ABackend;
use crate::paths::{self, Scope};
use crate::server::SharedState;

/// One running A2A listener.
pub struct A2AListener {
    pub agent_id: String,
    pub url: String,
    pub card_url: String,
    pub task: tokio::task::JoinHandle<()>,
}

/// Start an A2A listener for every enabled + online agent in the registry.
///
/// Each listener binds 127.0.0.1:0 with its own random port and its own
/// Agent Card derived from the agent descriptor.
pub async fn start_listeners(state: &SharedState, scope: &Scope) -> Vec<A2AListener> {
    let token = match auth::read_token(&paths::a2a_token_path(scope)) {
        Ok(token) => token,
        Err(_) => {
            let token = auth::generate_token();
            let _ = auth::write_token(&paths::a2a_token_path(scope), &token);
            token
        }
    };
    let backend = Arc::new(DaemonA2ABackend::new(state.clone()));
    let mut listeners = Vec::new();

    for adapter in state.task_manager.registry().list() {
        let online = matches!(
            adapter.health_check().await,
            Ok(agentmesh_adapters::AgentHealth {
                status: HealthStatus::Online,
                ..
            })
        );
        if !online {
            tracing::debug!(
                agent_id = adapter.id(),
                "a2a listener skipped: agent offline"
            );
            continue;
        }
        let descriptor = adapter.descriptor();
        let config = Arc::new(A2AServerConfig::new(
            descriptor.id.clone(),
            descriptor,
            token.clone(),
            backend.clone(),
        ));
        match server::bind(config.clone()).await {
            Ok((addr, router, listener)) => {
                let url = format!("http://{addr}/");
                config.set_url(url.clone()).await;
                let card_url = format!("http://{addr}/.well-known/agent-card.json");
                let task = tokio::spawn(server::serve(listener, router));
                tracing::info!(agent_id = %config.agent_id, url = %url, "a2a listener started");
                listeners.push(A2AListener {
                    agent_id: config.agent_id.clone(),
                    url,
                    card_url,
                    task,
                });
            }
            Err(err) => {
                tracing::warn!(agent_id = adapter.id(), error = %err, "failed to bind a2a listener");
            }
        }
    }
    let agents = runtime_agents(&listeners);
    *state.a2a_agents.lock().unwrap() = agents;
    listeners
}

/// Register the a2a_agents section of the runtime response.
pub fn runtime_agents(listeners: &[A2AListener]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for listener in listeners {
        map.insert(
            listener.agent_id.clone(),
            serde_json::json!({
                "url": listener.url,
                "card_url": listener.card_url,
            }),
        );
    }
    serde_json::Value::Object(map)
}

pub mod auth {
    pub use crate::auth::*;
}

/// Stop all listeners.
pub fn stop_listeners(listeners: Vec<A2AListener>) {
    for listener in listeners {
        listener.task.abort();
    }
}
