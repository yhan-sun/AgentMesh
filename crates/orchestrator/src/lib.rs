//! AgentMesh orchestrator: discovery, deterministic routing, delegation and
//! sequential multi-agent workflows.
//!
//! Phase 9 performs single-hop delegation only:
//!
//! ```text
//! RuleRouter
//!     ↓
//! AgentDirectory
//!     ↓
//! A2A Client → one selected agent
//! ```
//!
//! Phase 10 chains steps into a linear workflow (e.g. Architect → Implementer
//! → Reviewer); Phase 11 adds a review/fix loop (Reviewer → Fixer → Final
//! Reviewer). Every step still goes through the directory, the router and the
//! A2A protocol. Adapters are never called directly from this crate.

pub mod budget;
pub mod dag;
pub mod dag_scheduler;
pub mod delegate;
pub mod diff;
pub mod directory;
pub mod error;
pub mod evaluation;
pub mod handoff;
pub mod plan;
pub mod policy;
pub mod replan;
pub mod review;
pub mod router;
pub mod workflow;
pub mod workflow_state;

pub use budget::PlanBudget;
pub use dag::{PRESET_PARALLEL_REVIEW, WorkflowGraph, WorkflowNode, detect_cycle, preset_graph};
pub use dag_scheduler::{DagPersister, DagResumeSeed, DagRun, NodeStatus};
pub use delegate::{ActiveDelegation, Delegation, delegate, pick_agent};
pub use diff::{DiffField, PlanDiff};
pub use directory::{AgentAuth, AgentDirectory, AgentHealth, DirectoryEntry, DiscoveredEndpoint};
pub use error::OrchestratorError;
pub use evaluation::{
    AggregatedIssue, ConsensusOutcome, ConsensusResult, ConsensusStrategy, EvaluationGroup,
    EvaluationResult, MemberStatus, aggregate_issues, compute_consensus,
};
pub use handoff::{HandoffArtifact, HandoffPackage, build_handoff};
pub use plan::{
    MAX_OBJECTIVE_CHARS, MAX_PLAN_JSON_BYTES, MAX_PLAN_NODES, PLAN_INTENTS, PLAN_ROLES,
    PLAN_SCHEMA_VERSION, PlanParseError, PlanValidationError, PlannedNode, PlannerArtifact,
    WorkflowPlan, build_planner_prompt, parse_planner_output,
};
pub use policy::{PlanPolicy, PlanPolicyEngine, PolicyViolation};
pub use replan::{
    DeltaNode, DeltaUpdate, REPLAN_SCHEMA_VERSION, ReplanError, WorkflowPlanDelta, apply_delta,
    build_replan_prompt, is_mutable,
};
pub use review::{parse_review, render_issues};
pub use router::{RouteDecision, RuleRouter};
pub use workflow::{
    DEFAULT_MAX_PARALLEL, MAX_PARALLEL_CAP, NoopObserver, StepOutcome, WorkflowEngine,
    WorkflowObserver, WorkflowOptions, WorkflowPersister, WorkflowResumeSeed, WorkflowRun,
    stream_a2a_step,
};
pub use workflow_state::{
    PRESET_ARCHITECT_IMPLEMENT_REVIEW, PersistedStepResult, ReviewIssue, ReviewResult,
    ReviewSeverity, ReviewVerdict, Workflow, WorkflowResult, WorkflowRole, WorkflowStatus,
    WorkflowStep, WorkflowStepResult, WorkflowStepStatus, preset_steps,
};
