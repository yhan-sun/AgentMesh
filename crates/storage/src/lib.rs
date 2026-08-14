//! AgentMesh state persistence (SQLite).
//!
//! This crate stores AgentMesh's own state — tasks and artifacts. It never
//! stores agent sessions (Claude session ids, Codex thread ids), agent
//! credentials or environment secrets.

pub mod agent_session_repository;
pub mod apply_repository;
pub mod artifact_repository;
pub mod artifact_store;
pub mod competition_repository;
pub mod context_repository;
pub mod database;
pub mod error;
pub mod evaluation_repository;
pub mod provenance_repository;
pub mod task_repository;
pub mod workflow_plan_repository;
pub mod workflow_recovery_repository;
pub mod workflow_replan_repository;
pub mod workflow_repository;
pub mod workflow_step_repository;
pub mod workspace_repository;

pub use agent_session_repository::AgentSessionRepository;
pub use apply_repository::{ApplyRepository, ApplyRow, ApplyStatus, ClaimResult};
pub use artifact_repository::{ArtifactRepository, PruneResult};
pub use artifact_store::ArtifactStore;
pub use competition_repository::{
    CompetitionCandidateRow, CompetitionGroupRow, CompetitionRepository, candidate_status,
    competition_status,
};
pub use context_repository::ContextRepository;
pub use database::Database;
pub use error::StorageError;
pub use evaluation_repository::{
    EvaluationGroupRow, EvaluationMemberRow, EvaluationRepository, evaluation_status, member_status,
};
pub use provenance_repository::{ProvenanceEventRow, ProvenanceRepository};
pub use task_repository::{TaskFilter, TaskRepository};
pub use workflow_plan_repository::{
    PlanClaimResult, PlanRevisionRow, WorkflowPlanRepository, WorkflowPlanRow,
    plan_revision_source, plan_status,
};
pub use workflow_recovery_repository::{
    RecoveryClaimResult, WorkflowRecoveryRepository, WorkflowRecoveryRow, recovery_status,
};
pub use workflow_replan_repository::{
    ReplanApplyResult, WorkflowReplanRepository, WorkflowReplanRow, replan_status,
};
pub use workflow_repository::{WorkflowRepository, WorkflowRow};
pub use workflow_step_repository::{
    WorkflowStepDependencyRow, WorkflowStepRepository, WorkflowStepRow,
};
pub use workspace_repository::{WorkspaceRepository, WorkspaceRow, WorkspaceState};
