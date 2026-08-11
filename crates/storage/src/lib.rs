//! AgentMesh state persistence (SQLite).
//!
//! This crate stores AgentMesh's own state — tasks and artifacts. It never
//! stores agent sessions (Claude session ids, Codex thread ids), agent
//! credentials or environment secrets.

pub mod agent_session_repository;
pub mod artifact_repository;
pub mod artifact_store;
pub mod context_repository;
pub mod database;
pub mod error;
pub mod task_repository;
pub mod workspace_repository;

pub use agent_session_repository::AgentSessionRepository;
pub use artifact_repository::ArtifactRepository;
pub use artifact_store::ArtifactStore;
pub use context_repository::ContextRepository;
pub use database::Database;
pub use error::StorageError;
pub use task_repository::{TaskFilter, TaskRepository};
pub use workspace_repository::{WorkspaceRepository, WorkspaceRow, WorkspaceState};
