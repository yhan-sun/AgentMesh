//! Workflow state: roles, steps, presets, statuses and run/step results.
//!
//! Phase 10 supports a single sequential preset:
//!
//! ```text
//! Architect → Implementer → Reviewer
//! ```
//!
//! Roles are deliberately brand-agnostic ([`WorkflowRole`]); the concrete
//! agent per step is resolved by the RuleRouter from the agent's card, so
//! `claude → codex → claude` and `claude → claude → claude` are equally
//! valid as long as routing says so.

use agentmesh_core::TaskIntent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::handoff::HandoffPackage;

/// The role a workflow step plays in the pipeline.
///
/// Phase 17 adds the planner-facing roles (`SecurityReviewer`, `TestPlanner`,
/// `Tester`, `UiUx`, `Analyst`) that an AI Planner may assign. Roles are
/// deliberately brand-agnostic; the concrete agent per step is resolved by the
/// RuleRouter from the agent's card.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowRole {
    Architect,
    Implementer,
    Reviewer,
    /// Fixes the issues a review requested (reuses the implementer agent).
    Fixer,
    /// Re-reviews the implementation after a fix round.
    FinalReviewer,
    /// Security-focused review; produces a `review.json` verdict.
    SecurityReviewer,
    /// Plans the testing strategy (no code).
    TestPlanner,
    /// Writes and runs the planned tests.
    Tester,
    /// Designs / improves the user interface and experience.
    UiUx,
    /// Deep problem / requirement analysis (no code).
    Analyst,
    /// A parallel evaluator of an implementation snapshot (Phase 21). Produces
    /// a structured verdict; never modifies the implementation.
    Evaluator,
    /// The deterministic consensus gate of an evaluation group (Phase 21 §12).
    /// Not an agent task — a local computation over the evaluator results.
    ConsensusGate,
    /// A candidate implementation in a Best-of-N competition (Phase 23).
    Candidate,
    /// The deterministic selection gate of a competition group (Phase 23).
    /// Not an agent task — a local computation over candidate consensus results.
    SelectionGate,
}

impl WorkflowRole {
    /// Human-readable label (`Architect`, `Implementer`, `Reviewer`, ...).
    pub fn label(&self) -> &'static str {
        match self {
            WorkflowRole::Architect => "Architect",
            WorkflowRole::Implementer => "Implementer",
            WorkflowRole::Reviewer => "Reviewer",
            WorkflowRole::Fixer => "Fixer",
            WorkflowRole::FinalReviewer => "Final Review",
            WorkflowRole::SecurityReviewer => "Security Review",
            WorkflowRole::TestPlanner => "Test Planner",
            WorkflowRole::Tester => "Tester",
            WorkflowRole::UiUx => "UI/UX",
            WorkflowRole::Analyst => "Analyst",
            WorkflowRole::Evaluator => "Evaluator",
            WorkflowRole::ConsensusGate => "Consensus Gate",
            WorkflowRole::Candidate => "Candidate",
            WorkflowRole::SelectionGate => "Selection Gate",
        }
    }

    /// The task intent this role drives; the router maps it to a skill.
    ///
    /// A plan node may override this with an explicit intent (the planner
    /// decides WHAT, the router decides WHO).
    pub fn intent(&self) -> TaskIntent {
        match self {
            WorkflowRole::Architect => TaskIntent::Architecture,
            WorkflowRole::Implementer | WorkflowRole::Fixer | WorkflowRole::Candidate => {
                TaskIntent::Implementation
            }
            WorkflowRole::Reviewer | WorkflowRole::FinalReviewer => TaskIntent::Review,
            WorkflowRole::SecurityReviewer => TaskIntent::Review,
            WorkflowRole::TestPlanner | WorkflowRole::Tester => TaskIntent::Testing,
            WorkflowRole::UiUx => TaskIntent::UIUX,
            WorkflowRole::Analyst => TaskIntent::Architecture,
            WorkflowRole::Evaluator => TaskIntent::Review,
            WorkflowRole::ConsensusGate | WorkflowRole::SelectionGate => TaskIntent::Review,
        }
    }

    /// Whether this role ends with a machine-parseable review verdict.
    pub fn is_reviewer(&self) -> bool {
        matches!(
            self,
            WorkflowRole::Reviewer
                | WorkflowRole::FinalReviewer
                | WorkflowRole::SecurityReviewer
                | WorkflowRole::Evaluator
                | WorkflowRole::ConsensusGate
                | WorkflowRole::SelectionGate
        )
    }

    /// Stable snake_case string used for persistence and the wire.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowRole::Architect => "architect",
            WorkflowRole::Implementer => "implementer",
            WorkflowRole::Reviewer => "reviewer",
            WorkflowRole::Fixer => "fixer",
            WorkflowRole::FinalReviewer => "final_reviewer",
            WorkflowRole::SecurityReviewer => "security_review",
            WorkflowRole::TestPlanner => "test_planning",
            WorkflowRole::Tester => "testing",
            WorkflowRole::UiUx => "uiux",
            WorkflowRole::Analyst => "analysis",
            WorkflowRole::Evaluator => "evaluator",
            WorkflowRole::ConsensusGate => "consensus_gate",
            WorkflowRole::Candidate => "candidate",
            WorkflowRole::SelectionGate => "selection_gate",
        }
    }

    /// Parse a stable [`Self::as_str`] value; `None` for unknown strings.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "architect" => WorkflowRole::Architect,
            "implementer" => WorkflowRole::Implementer,
            "reviewer" => WorkflowRole::Reviewer,
            "fixer" => WorkflowRole::Fixer,
            "final_reviewer" => WorkflowRole::FinalReviewer,
            "security_review" => WorkflowRole::SecurityReviewer,
            "test_planning" => WorkflowRole::TestPlanner,
            "testing" => WorkflowRole::Tester,
            "uiux" => WorkflowRole::UiUx,
            "analysis" => WorkflowRole::Analyst,
            "evaluator" => WorkflowRole::Evaluator,
            "consensus_gate" => WorkflowRole::ConsensusGate,
            "candidate" => WorkflowRole::Candidate,
            "selection_gate" => WorkflowRole::SelectionGate,
            _ => return None,
        })
    }
}

/// One step of a workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStep {
    pub id: String,
    pub role: WorkflowRole,
    pub intent: TaskIntent,
    /// Untrusted planner-generated objective (Phase 17); `None` for preset
    /// steps. Kept on the step so persisted results round-trip it, and so the
    /// DAG scheduler can embed it in the node prompt as untrusted data.
    #[serde(default)]
    pub objective: Option<String>,
}

impl WorkflowStep {
    pub fn new(id: impl Into<String>, role: WorkflowRole) -> Self {
        Self {
            id: id.into(),
            role,
            intent: role.intent(),
            objective: None,
        }
    }

    /// Build the step of a graph node, preserving the node's explicit intent
    /// and planner objective (a plan node may route differently from the
    /// role's default intent).
    pub fn from_node(node: &crate::dag::WorkflowNode) -> Self {
        Self {
            id: node.node_id.clone(),
            role: node.role,
            intent: node.intent,
            objective: node.objective.clone(),
        }
    }
}

/// Lifecycle status of a whole workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    Pending,
    Running,
    /// A daemon crash interrupted the run; the only status resumable by
    /// default (Phase 12).
    Interrupted,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowStatus {
    /// Terminal statuses can never be left.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    }

    /// Stable snake_case string used for persistence and the wire.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowStatus::Pending => "pending",
            WorkflowStatus::Running => "running",
            WorkflowStatus::Interrupted => "interrupted",
            WorkflowStatus::Completed => "completed",
            WorkflowStatus::Failed => "failed",
            WorkflowStatus::Cancelled => "cancelled",
        }
    }

    /// Parse a stable [`Self::as_str`] value; `None` for unknown strings.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => WorkflowStatus::Pending,
            "running" => WorkflowStatus::Running,
            "interrupted" => WorkflowStatus::Interrupted,
            "completed" => WorkflowStatus::Completed,
            "failed" => WorkflowStatus::Failed,
            "cancelled" => WorkflowStatus::Cancelled,
            _ => return None,
        })
    }
}

/// Lifecycle status of a single step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStepStatus {
    Pending,
    Running,
    /// A daemon crash interrupted the step; a resumed run replaces it with a
    /// new task.
    Interrupted,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

impl WorkflowStepStatus {
    /// Stable snake_case string used for persistence and the wire.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowStepStatus::Pending => "pending",
            WorkflowStepStatus::Running => "running",
            WorkflowStepStatus::Interrupted => "interrupted",
            WorkflowStepStatus::Completed => "completed",
            WorkflowStepStatus::Failed => "failed",
            WorkflowStepStatus::Skipped => "skipped",
            WorkflowStepStatus::Cancelled => "cancelled",
        }
    }

    /// Parse a stable [`Self::as_str`] value; `None` for unknown strings.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => WorkflowStepStatus::Pending,
            "running" => WorkflowStepStatus::Running,
            "interrupted" => WorkflowStepStatus::Interrupted,
            "completed" => WorkflowStepStatus::Completed,
            "failed" => WorkflowStepStatus::Failed,
            "skipped" => WorkflowStepStatus::Skipped,
            "cancelled" => WorkflowStepStatus::Cancelled,
            _ => return None,
        })
    }
}

/// A static workflow: an id, the preset that produced it, the goal and the
/// ordered steps. Run progress lives in [`crate::workflow::WorkflowRun`],
/// not here.
#[derive(Debug, Clone)]
pub struct Workflow {
    pub id: Uuid,
    pub preset: String,
    pub goal: String,
    pub steps: Vec<WorkflowStep>,
}

/// Result of one executed step.
#[derive(Debug, Clone)]
pub struct WorkflowStepResult {
    pub step: WorkflowStep,
    pub status: WorkflowStepStatus,
    /// Agent chosen by the router for this step.
    pub agent_id: Option<String>,
    /// Routing reason recorded when the agent was chosen.
    pub reason: Option<String>,
    /// A2A task id of the step.
    pub task_id: Option<Uuid>,
    /// The handoff produced by this step (fed to the next step).
    pub handoff: Option<HandoffPackage>,
    /// Parsed review verdict, for review steps.
    pub review_result: Option<ReviewResult>,
    /// Bounded failure description for failed steps.
    pub error: Option<String>,
}

/// Terminal result of a workflow run.
#[derive(Debug, Clone)]
pub struct WorkflowResult {
    pub workflow_id: Uuid,
    pub status: WorkflowStatus,
    /// The single context shared by every step; `None` until the first step
    /// reports one.
    pub context_id: Option<Uuid>,
    pub steps: Vec<WorkflowStepResult>,
    /// The verdict of the last review step (initial or final review), when a
    /// review ran.
    pub final_review_verdict: Option<ReviewVerdict>,
    /// Workflow-level failure reason (e.g. changes still requested after the
    /// maximum number of review rounds); `None` for other terminal states.
    pub error: Option<String>,
}

// ---------- review verdicts ----------

/// Machine-parseable verdict produced by a review step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReviewVerdict {
    Approved,
    ChangesRequested,
}

impl ReviewVerdict {
    /// Stable wire key used in the reviewer's JSON output.
    pub fn key(&self) -> &'static str {
        match self {
            ReviewVerdict::Approved => "approved",
            ReviewVerdict::ChangesRequested => "changes_requested",
        }
    }

    /// Parse a stable [`Self::key`]; `None` for unknown keys.
    pub fn from_key(key: &str) -> Option<Self> {
        Some(match key.trim().to_ascii_lowercase().as_str() {
            "approved" => ReviewVerdict::Approved,
            "changes_requested" => ReviewVerdict::ChangesRequested,
            _ => return None,
        })
    }
}

/// Severity of a review issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ReviewSeverity {
    Critical,
    High,
    Medium,
    Low,
}

impl ReviewSeverity {
    /// Stable wire key.
    pub fn key(&self) -> &'static str {
        match self {
            ReviewSeverity::Critical => "critical",
            ReviewSeverity::High => "high",
            ReviewSeverity::Medium => "medium",
            ReviewSeverity::Low => "low",
        }
    }

    /// Parse a stable [`Self::key`]; unknown severities default to `Medium`.
    pub fn from_key(key: &str) -> Self {
        match key.trim().to_ascii_lowercase().as_str() {
            "critical" => ReviewSeverity::Critical,
            "high" => ReviewSeverity::High,
            "low" => ReviewSeverity::Low,
            _ => ReviewSeverity::Medium,
        }
    }
}

/// One issue raised by a review step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewIssue {
    pub severity: ReviewSeverity,
    pub title: String,
    pub description: String,
    pub file: Option<String>,
}

/// The structured output of a review step.
///
/// `confidence` is a `f64` (not `Eq`), so this type is `PartialEq` only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewResult {
    pub verdict: ReviewVerdict,
    pub summary: String,
    pub issues: Vec<ReviewIssue>,
    /// Evaluator confidence in `0.0..=1.0` (Phase 21 §6). Informational only —
    /// consensus never weights by confidence; `None` for a plain review.
    #[serde(default)]
    pub confidence: Option<f64>,
}

/// The built-in preset identifier.
pub const PRESET_ARCHITECT_IMPLEMENT_REVIEW: &str = "architect-implement-review";

/// A serializable snapshot of a completed step, used for persistence and
/// crash resume (Phase 12). Artifacts are *not* copied here: they are rebuilt
/// from the task's `ArtifactRepository` via `task_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedStepResult {
    pub step: WorkflowStep,
    pub status: WorkflowStepStatus,
    pub agent_id: Option<String>,
    pub task_id: Option<Uuid>,
    /// The step's final agent message (handoff summary).
    pub summary: Option<String>,
    pub review_result: Option<ReviewResult>,
    pub error: Option<String>,
}

impl PersistedStepResult {
    /// Reconstruct an in-memory step result. The handoff is deliberately not
    /// restored (artifact content is not persisted); a resume run supplies the
    /// previous step's handoff separately.
    pub fn to_step_result(&self) -> WorkflowStepResult {
        WorkflowStepResult {
            step: self.step.clone(),
            status: self.status,
            agent_id: self.agent_id.clone(),
            reason: None,
            task_id: self.task_id,
            handoff: None,
            review_result: self.review_result.clone(),
            error: self.error.clone(),
        }
    }
}

impl From<&WorkflowStepResult> for PersistedStepResult {
    fn from(result: &WorkflowStepResult) -> Self {
        PersistedStepResult {
            step: result.step.clone(),
            status: result.status,
            agent_id: result.agent_id.clone(),
            task_id: result.task_id,
            summary: result
                .handoff
                .as_ref()
                .map(|handoff| handoff.summary.clone()),
            review_result: result.review_result.clone(),
            error: result.error.clone(),
        }
    }
}

/// Resolve the ordered steps of a named preset; `None` for unknown presets.
pub fn preset_steps(preset: &str) -> Option<Vec<WorkflowStep>> {
    match preset {
        PRESET_ARCHITECT_IMPLEMENT_REVIEW => Some(vec![
            WorkflowStep::new("architect", WorkflowRole::Architect),
            WorkflowStep::new("implementer", WorkflowRole::Implementer),
            WorkflowStep::new("reviewer", WorkflowRole::Reviewer),
        ]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_map_to_the_phase_22_intents() {
        assert_eq!(WorkflowRole::Architect.intent(), TaskIntent::Architecture);
        assert_eq!(
            WorkflowRole::Implementer.intent(),
            TaskIntent::Implementation
        );
        assert_eq!(WorkflowRole::Reviewer.intent(), TaskIntent::Review);
        // Phase 11 roles reuse the fix/review intents so they route to the
        // same agents as the implementer / reviewer.
        assert_eq!(WorkflowRole::Fixer.intent(), TaskIntent::Implementation);
        assert_eq!(WorkflowRole::FinalReviewer.intent(), TaskIntent::Review);
        assert!(WorkflowRole::Reviewer.is_reviewer());
        assert!(WorkflowRole::FinalReviewer.is_reviewer());
        assert!(!WorkflowRole::Fixer.is_reviewer());
    }

    #[test]
    fn phase_17_roles_map_to_intents_and_reviewers() {
        assert_eq!(WorkflowRole::SecurityReviewer.intent(), TaskIntent::Review);
        assert!(WorkflowRole::SecurityReviewer.is_reviewer());
        assert_eq!(WorkflowRole::TestPlanner.intent(), TaskIntent::Testing);
        assert_eq!(WorkflowRole::Tester.intent(), TaskIntent::Testing);
        assert!(!WorkflowRole::TestPlanner.is_reviewer());
        assert!(!WorkflowRole::Tester.is_reviewer());
        assert_eq!(WorkflowRole::UiUx.intent(), TaskIntent::UIUX);
        assert_eq!(WorkflowRole::Analyst.intent(), TaskIntent::Architecture);
        assert!(!WorkflowRole::Analyst.is_reviewer());
    }

    #[test]
    fn phase_17_role_strings_roundtrip() {
        for role in [
            WorkflowRole::SecurityReviewer,
            WorkflowRole::TestPlanner,
            WorkflowRole::Tester,
            WorkflowRole::UiUx,
            WorkflowRole::Analyst,
        ] {
            assert_eq!(WorkflowRole::from_str(role.as_str()), Some(role));
            assert_eq!(
                role.label(),
                match role {
                    WorkflowRole::SecurityReviewer => "Security Review",
                    WorkflowRole::TestPlanner => "Test Planner",
                    WorkflowRole::Tester => "Tester",
                    WorkflowRole::UiUx => "UI/UX",
                    WorkflowRole::Analyst => "Analyst",
                    _ => unreachable!(),
                }
            );
        }
        assert_eq!(WorkflowRole::from_str("prompt_writer"), None);
    }

    #[test]
    fn review_verdict_keys_roundtrip() {
        assert_eq!(ReviewVerdict::Approved.key(), "approved");
        assert_eq!(ReviewVerdict::ChangesRequested.key(), "changes_requested");
        assert_eq!(
            ReviewVerdict::from_key("approved"),
            Some(ReviewVerdict::Approved)
        );
        assert_eq!(
            ReviewVerdict::from_key("CHANGES_REQUESTED"),
            Some(ReviewVerdict::ChangesRequested)
        );
        assert_eq!(ReviewVerdict::from_key("maybe"), None);
        assert_eq!(
            ReviewSeverity::from_key("critical"),
            ReviewSeverity::Critical
        );
        assert_eq!(ReviewSeverity::from_key("LOW"), ReviewSeverity::Low);
        assert_eq!(ReviewSeverity::from_key("unknown"), ReviewSeverity::Medium);
    }

    #[test]
    fn architect_implement_review_preset_is_sequential() {
        let steps = preset_steps(PRESET_ARCHITECT_IMPLEMENT_REVIEW).expect("preset");
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].role, WorkflowRole::Architect);
        assert_eq!(steps[1].role, WorkflowRole::Implementer);
        assert_eq!(steps[2].role, WorkflowRole::Reviewer);
        assert_eq!(steps[0].id, "architect");
        assert_eq!(steps[1].id, "implementer");
        assert_eq!(steps[2].id, "reviewer");
    }

    #[test]
    fn unknown_presets_are_rejected() {
        assert!(preset_steps("debate").is_none());
    }

    #[test]
    fn terminal_workflow_statuses() {
        assert!(WorkflowStatus::Completed.is_terminal());
        assert!(WorkflowStatus::Failed.is_terminal());
        assert!(WorkflowStatus::Cancelled.is_terminal());
        assert!(!WorkflowStatus::Pending.is_terminal());
        assert!(!WorkflowStatus::Running.is_terminal());
        assert!(!WorkflowStatus::Interrupted.is_terminal());
    }

    #[test]
    fn workflow_status_strings_roundtrip() {
        for status in [
            WorkflowStatus::Pending,
            WorkflowStatus::Running,
            WorkflowStatus::Interrupted,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
        ] {
            assert_eq!(WorkflowStatus::from_str(status.as_str()), Some(status));
        }
        assert_eq!(WorkflowStatus::from_str("bogus"), None);
    }

    #[test]
    fn step_status_strings_roundtrip() {
        for status in [
            WorkflowStepStatus::Pending,
            WorkflowStepStatus::Running,
            WorkflowStepStatus::Interrupted,
            WorkflowStepStatus::Completed,
            WorkflowStepStatus::Failed,
            WorkflowStepStatus::Skipped,
            WorkflowStepStatus::Cancelled,
        ] {
            assert_eq!(WorkflowStepStatus::from_str(status.as_str()), Some(status));
        }
        assert_eq!(WorkflowStepStatus::from_str("bogus"), None);
    }

    #[test]
    fn persisted_step_result_roundtrips() {
        let result = WorkflowStepResult {
            step: WorkflowStep::new("reviewer", WorkflowRole::Reviewer),
            status: WorkflowStepStatus::Completed,
            agent_id: Some("claude".into()),
            reason: Some("preferred".into()),
            task_id: Some(Uuid::new_v4()),
            handoff: Some(HandoffPackage {
                source_task_id: Uuid::new_v4(),
                source_agent_id: "claude".into(),
                summary: "looks good".into(),
                artifacts: vec![],
            }),
            review_result: Some(ReviewResult {
                verdict: ReviewVerdict::Approved,
                summary: "looks good".into(),
                issues: vec![],
                confidence: None,
            }),
            error: None,
        };
        let persisted = PersistedStepResult::from(&result);
        assert_eq!(persisted.summary.as_deref(), Some("looks good"));
        assert_eq!(
            persisted.review_result.as_ref().unwrap().verdict,
            ReviewVerdict::Approved
        );
        let rebuilt = persisted.to_step_result();
        assert_eq!(rebuilt.status, WorkflowStepStatus::Completed);
        assert_eq!(rebuilt.agent_id.as_deref(), Some("claude"));
        assert_eq!(rebuilt.task_id, result.task_id);
    }
}
