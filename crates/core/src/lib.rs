//! AgentMesh core domain model.
//!
//! These types are deliberately small and serializable so they can be
//! persisted and transported without translation.

pub mod config;
pub mod error;
pub mod model;

pub use config::{AgentConfig, AgentMeshConfig, ConfigError};
pub use error::CoreError;
pub use model::*;
