//! A2A v1.0 protocol server and client for AgentMesh agents.
//!
//! # Streaming Contract
//!
//! The A2A SSE transport obeys a strict frame finality contract:
//! - **StatusChanged** (`AgentEvent::StatusChanged`): `final = None`.
//!   `StatusChanged` frames convey runtime state transitions (e.g. `Working`, `Canceled`, `Completed`)
//!   without asserting stream frame completeness. Clients must never interpret non-terminal frames as
//!   anticipating additional frames when `final = None`.
//! - **Working Message** (`AgentEvent::Message`): `final = Some(false)`.
//!   Conveys intermediate progress/turn text while the task is actively executing.
//! - **Terminal Result** (`AgentEvent::Completed`, `AgentEvent::Failed`): `final = Some(true)`.
//!   Conveys task completion or failure and signals end-of-stream.
//!
//! # Start/Cancel Ownership & Transport Deadline
//!
//! - Clients enforce a 10s transport deadline on establishing the stream and receiving the initial
//!   `jsonrpc` frame containing the `task_id`.
//! - Once a start request is sent, clients must guarantee task cancellation on the server if aborted,
//!   avoiding orphan tasks or untracked background execution.

pub mod agent_card;
pub mod backend;
pub mod client;
pub mod jsonrpc;
pub mod mapping;
pub mod server;
pub mod types;

pub use agent_card::{AgentCapabilities, AgentCard};
pub use backend::{A2ABackend, A2ABackendError, A2ARun, A2AStreamEvent};
pub use client::{A2AClient, A2AClientError, A2AClientEvent, StreamingMessage, TaskStream};
pub use server::A2AServerConfig;
pub use types::A2A_PROTOCOL_VERSION;
