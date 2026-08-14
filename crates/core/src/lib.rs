//! AgentMesh core domain model.
//!
//! These types are deliberately small and serializable so they can be
//! persisted and transported without translation.

pub mod config;
pub mod error;
pub mod model;
pub mod provenance;

pub use config::{
    AgentConfig, AgentMeshConfig, CompetitionConfig, ConfigError, EvaluationConfig,
    PlanPolicyConfig, PlannerConfig, RecoveryConfig, RoutingConfig,
};
pub use error::CoreError;
pub use model::*;
pub use provenance::*;
