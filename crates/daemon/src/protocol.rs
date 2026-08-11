//! Daemon protocol types shared by server and client.

use agentmesh_core::AgentEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Protocol version between CLI and daemon.
pub const DAEMON_PROTOCOL_VERSION: u32 = 1;

/// Metadata written after a daemon binds successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonMeta {
    pub protocol_version: u32,
    pub instance_id: String,
    pub pid: u32,
    pub address: String,
    pub started_at: String,
}

/// Health endpoint payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub protocol_version: u32,
    pub instance_id: String,
    pub status: String,
}

/// Request to run a fresh task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub agent_id: String,
    pub prompt: String,
    /// Source project/repository location; `null` uses the daemon cwd scope.
    #[serde(default)]
    pub source_workspace: Option<String>,
}

/// Request to resume a previous task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeRequest {
    pub source_task_id: Uuid,
    pub prompt: String,
}

/// Task start response: the daemon now owns the live runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResponse {
    pub task_id: Uuid,
    pub context_id: Uuid,
    pub agent_session_id: Uuid,
    pub agent_id: String,
}

/// One live task as reported by the runtime endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTaskInfo {
    pub task_id: Uuid,
    pub agent_id: String,
    pub agent_session_id: Uuid,
    pub status: String,
}

/// Runtime endpoint payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeResponse {
    pub instance_id: String,
    pub live_tasks: Vec<LiveTaskInfo>,
}

/// Uniform error payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            error: ApiErrorBody {
                code: code.to_string(),
                message: message.into(),
                details: None,
            },
        }
    }
}

/// Events streamed to clients over SSE.
///
/// Vendor-neutral: adapters stay behind the AgentEvent boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonStreamEvent {
    /// Initial metadata, sent once when attaching.
    TaskInfo {
        task_id: Uuid,
        context_id: Uuid,
        agent_session_id: Uuid,
        agent_id: String,
    },
    /// A forwarded agent event.
    Agent { event: AgentEvent },
    /// Requested replay position is older than the buffer; continuing from
    /// the oldest available sequence.
    ReplayGap { oldest_available: u64 },
}

/// Shutdown request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownRequest {
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResponse {
    pub cancelled_tasks: usize,
}
