//! Unified adapter interface and built-in coding agent adapters.
//!
//! Every coding agent (Claude Code, Codex, OpenCode, ...) plugs into
//! AgentMesh through the [`CodingAgentAdapter`] trait; the orchestrator
//! never talks to agent CLIs directly.

pub mod adapter;
pub mod antigravity;
pub mod claude;
pub mod codex;
pub mod error;
pub mod mock;
pub mod opencode;
pub mod registry;

pub use adapter::{AgentHealth, AgentRunHandle, AgentRunRequest, CodingAgentAdapter, HealthStatus};
pub use antigravity::AntigravityAdapter;
pub use claude::ClaudeAdapter;
pub use codex::CodexAdapter;
pub use error::AgentError;
pub use mock::MockAgentAdapter;
pub use opencode::OpenCodeAdapter;
pub use registry::AgentRegistry;
