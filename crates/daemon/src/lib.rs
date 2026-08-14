//! AgentMesh daemon: project-scoped runtime owner with authenticated local IPC.
//!
//! The daemon is the only owner of live agent processes. The CLI talks to it
//! over local HTTP (127.0.0.1) with a per-instance bearer token.

pub mod a2a;
pub mod a2a_backend;
pub mod auth;
pub mod cleanup;
pub mod client;
pub mod error;
pub mod lease;
pub mod lock;
pub mod paths;
pub mod planner;
pub mod protocol;
pub mod provenance_service;
pub mod recovery;
pub mod registry;
pub mod replan;
pub mod runtime;
pub mod server;
pub mod workflow_service;

pub use client::{DaemonClient, connect_or_start, probe};
pub use error::DaemonError;
pub use paths::Scope;
pub use runtime::{serve, spawn_daemon_process};
