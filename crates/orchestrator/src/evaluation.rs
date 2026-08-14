//! Multi-agent evaluation + deterministic consensus (Phase 21).
//!
//! Several independent agents evaluate the same implementation snapshot. Each
//! produces a structured [`EvaluationResult`]; a deterministic local
//! [`compute_consensus`] — never an LLM judge — combines them under a
//! [`ConsensusStrategy`] and quorum.
//!
//! ```text
//! Implementation
//!   → Eval1 / Eval2 / Eval3 (parallel, distinct agents, same snapshot)
//!   → ConsensusGate (Majority / Unanimous + quorum)
//!   → Approved | ChangesRequested | Unavailable
//! ```
//!
//! Evaluators only evaluate — they never modify the implementation, never see
//! each other's results, and their workspaces are never an Apply source.

use agentmesh_core::TaskIntent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::workflow_state::{ReviewIssue, ReviewSeverity, ReviewVerdict};

/// How the evaluators' verdicts are combined (deterministic local code).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusStrategy {
    /// Approved when approved > changes_requested among valid results.
    Majority,
    /// Any `changes_requested` yields ChangesRequested.
    Unanimous,
}

impl ConsensusStrategy {
    /// Stable snake_case string used for persistence and the wire.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsensusStrategy::Majority => "majority",
            ConsensusStrategy::Unanimous => "unanimous",
        }
    }

    /// Parse a stable [`Self::as_str`]; `None` for unknown strings.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "majority" => ConsensusStrategy::Majority,
            "unanimous" => ConsensusStrategy::Unanimous,
            _ => return None,
        })
    }
}

/// The group-level outcome of an evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusOutcome {
    Approved,
    ChangesRequested,
    /// Too few valid evaluator results to form a consensus.
    Unavailable,
}

impl ConsensusOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsensusOutcome::Approved => "approved",
            ConsensusOutcome::ChangesRequested => "changes_requested",
            ConsensusOutcome::Unavailable => "unavailable",
        }
    }
}

/// One parallel evaluation of the same snapshot (Phase 21 §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationGroup {
    pub id: Uuid,
    pub workflow_id: Uuid,
    /// The task whose result is being evaluated; `None` for a workflow-level
    /// evaluation.
    pub source_task_id: Option<Uuid>,
    /// Number of evaluator members requested (1..=5).
    pub required_evaluators: usize,
    /// Minimum valid results needed to form a consensus.
    pub quorum: usize,
    pub strategy: ConsensusStrategy,
}

impl EvaluationGroup {
    /// A new group with the given workflow and configuration.
    pub fn new(
        workflow_id: Uuid,
        source_task_id: Option<Uuid>,
        required_evaluators: usize,
        quorum: usize,
        strategy: ConsensusStrategy,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            workflow_id,
            source_task_id,
            required_evaluators,
            quorum,
            strategy,
        }
    }
}

/// Lifecycle of one evaluation member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

/// One evaluator's structured result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub verdict: ReviewVerdict,
    /// 0.0..=1.0; informational only — consensus never weights by confidence.
    pub confidence: Option<f64>,
    pub summary: String,
    pub issues: Vec<ReviewIssue>,
}

impl EvaluationResult {
    pub fn is_valid(&self) -> bool {
        self.confidence
            .map(|c| (0.0..=1.0).contains(&c))
            .unwrap_or(true)
    }
}

/// An issue aggregated across evaluators (Phase 21 §9), deduplicated by
/// `severity + file + title` and attributed to its reporters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatedIssue {
    pub severity: ReviewSeverity,
    pub title: String,
    pub description: String,
    pub file: Option<String>,
    /// Agent ids that reported this issue (audit).
    pub reported_by: Vec<String>,
}

/// The deterministic consensus of an evaluation group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusResult {
    pub outcome: ConsensusOutcome,
    pub strategy: ConsensusStrategy,
    pub quorum: usize,
    pub valid_count: usize,
    pub total_count: usize,
    pub approved_count: usize,
    pub changes_requested_count: usize,
    /// Aggregated issues (ChangesRequested only).
    pub issues: Vec<AggregatedIssue>,
}

/// Compute the deterministic consensus from evaluator results (Phase 21 §7-§8).
///
/// `members` pairs each evaluator's agent id with its result (for issue
/// attribution). `total_evaluators` is the group's full evaluator count —
/// failed/absent members have no result, so `members.len()` can be smaller.
/// Valid results are those with a parseable verdict and a valid confidence.
///
/// * If `valid_count < quorum` → [`ConsensusOutcome::Unavailable`].
/// * Majority: approved > changes_requested → Approved, else ChangesRequested.
/// * Unanimous: any changes_requested → ChangesRequested, else Approved.
///
/// Never calls an LLM; never weights by confidence.
pub fn compute_consensus(
    members: &[(String, EvaluationResult)],
    strategy: ConsensusStrategy,
    quorum: usize,
    total_evaluators: usize,
) -> ConsensusResult {
    let total_count = total_evaluators;
    let valid: Vec<&(String, EvaluationResult)> =
        members.iter().filter(|(_, r)| r.is_valid()).collect();
    let valid_count = valid.len();
    let approved_count = valid
        .iter()
        .filter(|(_, r)| r.verdict == ReviewVerdict::Approved)
        .count();
    let changes_requested_count = valid_count - approved_count;

    let outcome = if valid_count < quorum {
        ConsensusOutcome::Unavailable
    } else {
        match strategy {
            ConsensusStrategy::Majority => {
                if approved_count > changes_requested_count {
                    ConsensusOutcome::Approved
                } else {
                    ConsensusOutcome::ChangesRequested
                }
            }
            ConsensusStrategy::Unanimous => {
                if changes_requested_count == 0 {
                    ConsensusOutcome::Approved
                } else {
                    ConsensusOutcome::ChangesRequested
                }
            }
        }
    };

    ConsensusResult {
        outcome,
        strategy,
        quorum,
        valid_count,
        total_count,
        approved_count,
        changes_requested_count,
        issues: if outcome == ConsensusOutcome::ChangesRequested {
            aggregate_issues(members)
        } else {
            Vec::new()
        },
    }
}

/// Aggregate + deduplicate issues across evaluators (Phase 21 §9).
///
/// Deterministic rule: issues with the same `severity + file + title` merge,
/// keeping the first description and collecting every reporter (agent id).
/// No agent summarizes another's result.
pub fn aggregate_issues(members: &[(String, EvaluationResult)]) -> Vec<AggregatedIssue> {
    let mut out: Vec<AggregatedIssue> = Vec::new();
    for (agent_id, result) in members {
        if result.verdict != ReviewVerdict::ChangesRequested || !result.is_valid() {
            continue;
        }
        for issue in &result.issues {
            let key = (issue.severity, issue.file.clone(), issue.title.clone());
            if let Some(existing) = out
                .iter_mut()
                .find(|a| (a.severity, a.file.clone(), a.title.clone()) == key)
            {
                if !existing.reported_by.iter().any(|r| r == agent_id) {
                    existing.reported_by.push(agent_id.clone());
                }
            } else {
                out.push(AggregatedIssue {
                    severity: issue.severity,
                    title: issue.title.clone(),
                    description: issue.description.clone(),
                    file: issue.file.clone(),
                    reported_by: vec![agent_id.clone()],
                });
            }
        }
    }
    out
}

/// The review intent an evaluator uses (the router maps it to a skill).
pub fn evaluator_intent() -> TaskIntent {
    TaskIntent::Review
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow_state::ReviewSeverity;

    fn result(verdict: ReviewVerdict, confidence: Option<f64>) -> EvaluationResult {
        EvaluationResult {
            verdict,
            confidence,
            summary: "s".to_string(),
            issues: vec![],
        }
    }

    fn member(agent: &str, result: EvaluationResult) -> (String, EvaluationResult) {
        (agent.to_string(), result)
    }

    fn issue(severity: ReviewSeverity, file: &str, title: &str) -> ReviewIssue {
        ReviewIssue {
            severity,
            title: title.to_string(),
            description: format!("desc {title}"),
            file: if file.is_empty() {
                None
            } else {
                Some(file.to_string())
            },
        }
    }

    #[test]
    fn majority_2_of_3_approves() {
        let results = vec![
            member("a", result(ReviewVerdict::Approved, Some(0.9))),
            member("b", result(ReviewVerdict::Approved, Some(0.8))),
            member("c", result(ReviewVerdict::ChangesRequested, Some(0.7))),
        ];
        let consensus = compute_consensus(&results, ConsensusStrategy::Majority, 2, results.len());
        assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
        assert_eq!(consensus.valid_count, 3);
        assert_eq!(consensus.approved_count, 2);
        assert_eq!(consensus.changes_requested_count, 1);
    }

    #[test]
    fn majority_tie_requests_changes() {
        let results = vec![
            member("a", result(ReviewVerdict::Approved, None)),
            member("b", result(ReviewVerdict::ChangesRequested, None)),
        ];
        let consensus = compute_consensus(&results, ConsensusStrategy::Majority, 2, results.len());
        assert_eq!(consensus.outcome, ConsensusOutcome::ChangesRequested);
    }

    #[test]
    fn unanimous_any_change_requests_changes() {
        let results = vec![
            member("a", result(ReviewVerdict::Approved, None)),
            member("b", result(ReviewVerdict::Approved, None)),
            member("c", result(ReviewVerdict::ChangesRequested, None)),
        ];
        let consensus = compute_consensus(&results, ConsensusStrategy::Unanimous, 2, results.len());
        assert_eq!(consensus.outcome, ConsensusOutcome::ChangesRequested);
    }

    #[test]
    fn unanimous_all_approve() {
        let results = vec![
            member("a", result(ReviewVerdict::Approved, None)),
            member("b", result(ReviewVerdict::Approved, None)),
            member("c", result(ReviewVerdict::Approved, None)),
        ];
        let consensus = compute_consensus(&results, ConsensusStrategy::Unanimous, 3, results.len());
        assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
    }

    #[test]
    fn quorum_met_with_one_failure() {
        // 3 evaluators, 2 valid, quorum 2 → consensus still forms.
        let mut results = vec![
            member("a", result(ReviewVerdict::Approved, None)),
            member("b", result(ReviewVerdict::Approved, None)),
            member("c", result(ReviewVerdict::ChangesRequested, None)),
        ];
        results[2].1.confidence = Some(1.5); // invalid confidence
        let consensus = compute_consensus(&results, ConsensusStrategy::Majority, 2, results.len());
        assert_eq!(consensus.outcome, ConsensusOutcome::Approved);
        assert_eq!(consensus.valid_count, 2);
        assert_eq!(consensus.total_count, 3);
    }

    #[test]
    fn below_quorum_is_unavailable() {
        let mut results = vec![
            member("a", result(ReviewVerdict::Approved, None)),
            member("b", result(ReviewVerdict::Approved, None)),
            member("c", result(ReviewVerdict::Approved, None)),
        ];
        results[1].1.confidence = Some(2.0); // invalid
        results[2].1.confidence = Some(2.0); // invalid
        let consensus = compute_consensus(&results, ConsensusStrategy::Majority, 2, results.len());
        assert_eq!(consensus.outcome, ConsensusOutcome::Unavailable);
    }

    #[test]
    fn invalid_confidence_is_not_valid() {
        assert!(!result(ReviewVerdict::Approved, Some(1.5)).is_valid());
        assert!(!result(ReviewVerdict::Approved, Some(-0.1)).is_valid());
        assert!(result(ReviewVerdict::Approved, Some(0.0)).is_valid());
        assert!(result(ReviewVerdict::Approved, Some(1.0)).is_valid());
    }

    #[test]
    fn issues_aggregate_and_dedup_by_severity_file_title() {
        let a = EvaluationResult {
            verdict: ReviewVerdict::ChangesRequested,
            confidence: Some(0.8),
            summary: "a".to_string(),
            issues: vec![issue(ReviewSeverity::High, "src/x.rs", "crash")],
        };
        let b = EvaluationResult {
            verdict: ReviewVerdict::ChangesRequested,
            confidence: Some(0.7),
            summary: "b".to_string(),
            issues: vec![issue(ReviewSeverity::High, "src/x.rs", "crash")],
        };
        let c = EvaluationResult {
            verdict: ReviewVerdict::ChangesRequested,
            confidence: Some(0.6),
            summary: "c".to_string(),
            issues: vec![issue(ReviewSeverity::Low, "", "nit")],
        };
        let aggregated = aggregate_issues(&[
            member("claude", a),
            member("codex", b),
            member("opencode", c),
        ]);
        assert_eq!(aggregated.len(), 2);
        let crash = aggregated.iter().find(|i| i.title == "crash").unwrap();
        assert_eq!(
            crash.reported_by,
            vec!["claude".to_string(), "codex".to_string()]
        );
        assert_eq!(crash.severity, ReviewSeverity::High);
    }

    #[test]
    fn approved_results_do_not_contribute_issues() {
        let a = EvaluationResult {
            verdict: ReviewVerdict::Approved,
            confidence: None,
            summary: "ok".to_string(),
            issues: vec![issue(ReviewSeverity::Medium, "x", "i")],
        };
        assert!(aggregate_issues(&[member("claude", a)]).is_empty());
    }

    #[test]
    fn strategy_strings_roundtrip() {
        assert_eq!(ConsensusStrategy::Majority.as_str(), "majority");
        assert_eq!(ConsensusStrategy::Unanimous.as_str(), "unanimous");
        assert_eq!(
            ConsensusStrategy::from_str("majority"),
            Some(ConsensusStrategy::Majority)
        );
        assert_eq!(ConsensusStrategy::from_str("debate"), None);
    }
}
