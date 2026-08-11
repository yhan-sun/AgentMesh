//! A2A protocol types used by AgentMesh adapters and servers.
//!
//! This crate will grow a JSON-RPC client/server on top of these types.

pub mod agent_card;

pub use agent_card::{AgentCapabilities, AgentCard};
