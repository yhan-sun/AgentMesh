//! AgentMesh daemon: project-scoped runtime owner with authenticated local IPC.
//!
//! The daemon is the only owner of live agent processes. The CLI talks to it
//! over local HTTP (127.0.0.1) with a per-instance bearer token.

pub mod auth;
pub mod client;
pub mod error;
pub mod lease;
pub mod lock;
pub mod paths;
pub mod protocol;
pub mod registry;
pub mod runtime;
pub mod server;

pub use client::{DaemonClient, connect_or_start, probe};
pub use error::DaemonError;
pub use paths::Scope;
pub use runtime::{serve, spawn_daemon_process};
