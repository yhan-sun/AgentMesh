//! Plan policy engine (Phase 18): deterministic local limits on what a plan
//! may look like and how it may execute.
//!
//! The Policy is the second gate after [`crate::plan::WorkflowPlan::validate`]:
//!
//! ```text
//! Schema Validation → DAG Validation → Policy Validation
//! ```
//!
//! A [`PlanPolicy`] is built from `[planner.policy]` config (or the safe
//! defaults) and is pure local code — the Planner cannot change it and a user
//! edit cannot bypass it. Policy only constrains `Plan → Execute`; it never
//! restricts a hand-written preset workflow.

use serde::{Deserialize, Serialize};

use agentmesh_core::config::PlanPolicyConfig;

use crate::dag::WorkflowGraph;
use crate::plan::{PLAN_INTENTS, PLAN_ROLES, WorkflowPlan};

/// A single policy limit that a plan or execution request violated.
///
/// `actual` is what the plan/request has, `limit` is the allowed maximum (or
/// `0` for the not-a-count rules such as a disallowed intent/role).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub struct PolicyViolation {
    /// Stable rule name: `max_nodes` | `max_agent_calls` | `max_parallel` |
    /// `allowed_intents` | `allowed_roles`.
    pub rule: String,
    pub actual: usize,
    pub limit: usize,
}

impl PolicyViolation {
    fn count(rule: &str, actual: usize, limit: usize) -> Self {
        Self {
            rule: rule.to_string(),
            actual,
            limit,
        }
    }
}

impl std::fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "policy violation `{}`: {actual} exceeds limit {limit}",
            self.rule,
            actual = self.actual,
            limit = self.limit
        )
    }
}

/// Policy limits. Safe defaults: 12 nodes / 12 agent calls / 8 parallel, with
/// the full legal intent and role sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanPolicy {
    pub max_nodes: usize,
    pub max_agent_calls: usize,
    pub max_parallel: usize,
    pub allowed_intents: Vec<String>,
    pub allowed_roles: Vec<String>,
}

impl Default for PlanPolicy {
    fn default() -> Self {
        Self {
            max_nodes: 12,
            max_agent_calls: 12,
            max_parallel: 8,
            allowed_intents: PLAN_INTENTS.iter().map(|s| s.to_string()).collect(),
            allowed_roles: PLAN_ROLES.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl PlanPolicy {
    /// Build a policy from `[planner.policy]`; absent fields use the defaults.
    pub fn from_config(config: &PlanPolicyConfig) -> Self {
        let default = Self::default();
        Self {
            max_nodes: config.max_nodes.unwrap_or(default.max_nodes),
            max_agent_calls: config.max_agent_calls.unwrap_or(default.max_agent_calls),
            max_parallel: config.max_parallel.unwrap_or(default.max_parallel),
            allowed_intents: config
                .allowed_intents
                .clone()
                .unwrap_or(default.allowed_intents),
            allowed_roles: config
                .allowed_roles
                .clone()
                .unwrap_or(default.allowed_roles),
        }
    }
}

/// Runs a [`PlanPolicy`] against a plan and an execution request.
#[derive(Debug, Clone)]
pub struct PlanPolicyEngine {
    policy: PlanPolicy,
}

impl PlanPolicyEngine {
    pub fn new(policy: PlanPolicy) -> Self {
        Self { policy }
    }

    pub fn policy(&self) -> &PlanPolicy {
        &self.policy
    }

    /// Check a plan's structure. For a fixed DAG one node is one agent call,
    /// so node count bounds both `max_nodes` and `max_agent_calls`.
    pub fn check_plan(&self, plan: &WorkflowPlan) -> Result<(), PolicyViolation> {
        let nodes = plan.nodes.len();
        self.check_counts(nodes)?;
        for node in &plan.nodes {
            if !self
                .policy
                .allowed_intents
                .iter()
                .any(|i| i == &node.intent)
            {
                return Err(PolicyViolation::count("allowed_intents", 1, 0));
            }
            if !self.policy.allowed_roles.iter().any(|r| r == &node.role) {
                return Err(PolicyViolation::count("allowed_roles", 1, 0));
            }
        }
        Ok(())
    }

    /// Check the *candidate* graph of a replan (Phase 19 §10): the policy
    /// applies to the full post-delta DAG — existing + added nodes — not just
    /// the added ones. One node is one agent call for a fixed DAG.
    pub fn check_graph(&self, graph: &WorkflowGraph) -> Result<(), PolicyViolation> {
        self.check_counts(graph.len())?;
        for node in &graph.nodes {
            if !self
                .policy
                .allowed_intents
                .iter()
                .any(|i| i == node.intent.key())
            {
                return Err(PolicyViolation::count("allowed_intents", 1, 0));
            }
            if !self
                .policy
                .allowed_roles
                .iter()
                .any(|r| r == node.role.as_str())
            {
                return Err(PolicyViolation::count("allowed_roles", 1, 0));
            }
        }
        Ok(())
    }

    /// Check only the node-count limits of a graph (Phase 22 §17). The
    /// consensus fix-loop candidate's roles (`fixer`, `evaluator`,
    /// `consensus_gate`) are control-plane preset nodes — the same roles the
    /// already-approved initial consensus graph used — never planner-chosen
    /// roles, so only the structural caps (`max_nodes`, `max_agent_calls`)
    /// apply to the dynamic extension.
    pub fn check_graph_counts(&self, graph: &WorkflowGraph) -> Result<(), PolicyViolation> {
        self.check_counts(graph.len())
    }

    /// Shared node-count checks so a policy limit never diverges between a
    /// plan and a replan candidate.
    fn check_counts(&self, nodes: usize) -> Result<(), PolicyViolation> {
        if nodes > self.policy.max_nodes {
            return Err(PolicyViolation::count(
                "max_nodes",
                nodes,
                self.policy.max_nodes,
            ));
        }
        if nodes > self.policy.max_agent_calls {
            return Err(PolicyViolation::count(
                "max_agent_calls",
                nodes,
                self.policy.max_agent_calls,
            ));
        }
        Ok(())
    }

    /// Check the `--max-parallel` a user requested. An explicit request above
    /// the policy limit is a hard violation — never silently clamped.
    pub fn check_parallel(&self, requested: usize) -> Result<(), PolicyViolation> {
        if requested > self.policy.max_parallel {
            return Err(PolicyViolation::count(
                "max_parallel",
                requested,
                self.policy.max_parallel,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn plan(nodes: usize) -> WorkflowPlan {
        let mut value = json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": []}
            ]
        });
        for i in 1..nodes {
            value["nodes"].as_array_mut().unwrap().push(json!({
                "id": format!("n{i}"),
                "role": "implementer",
                "intent": "implementation",
                "objective": format!("o{i}"),
                "depends_on": ["a"]
            }));
        }
        WorkflowPlan::from_json(&value.to_string()).expect("parse")
    }

    fn engine() -> PlanPolicyEngine {
        PlanPolicyEngine::new(PlanPolicy {
            max_nodes: 3,
            max_agent_calls: 3,
            max_parallel: 2,
            ..PlanPolicy::default()
        })
    }

    #[test]
    fn default_policy_allows_the_full_legal_sets() {
        let policy = PlanPolicy::default();
        assert_eq!(policy.max_nodes, 12);
        assert_eq!(policy.max_agent_calls, 12);
        assert_eq!(policy.max_parallel, 8);
        assert_eq!(policy.allowed_intents, PLAN_INTENTS);
        assert_eq!(policy.allowed_roles, PLAN_ROLES);
    }

    #[test]
    fn from_config_fills_missing_fields_with_defaults() {
        let config = PlanPolicyConfig {
            max_nodes: Some(5),
            ..PlanPolicyConfig::default()
        };
        let policy = PlanPolicy::from_config(&config);
        assert_eq!(policy.max_nodes, 5);
        assert_eq!(policy.max_agent_calls, 12, "default");
        assert_eq!(policy.max_parallel, 8, "default");
    }

    #[test]
    fn policy_rejects_over_max_nodes() {
        let err = engine().check_plan(&plan(4)).expect_err("too many");
        assert_eq!(
            err,
            PolicyViolation {
                rule: "max_nodes".into(),
                actual: 4,
                limit: 3
            }
        );
    }

    #[test]
    fn policy_rejects_over_max_agent_calls() {
        // node count == agent call count, so both caps must be hit together;
        // lower the call cap alone to observe the second rule firing.
        let eng = PlanPolicyEngine::new(PlanPolicy {
            max_nodes: 12,
            max_agent_calls: 3,
            ..PlanPolicy::default()
        });
        let err = eng.check_plan(&plan(4)).expect_err("too many calls");
        assert_eq!(
            err,
            PolicyViolation {
                rule: "max_agent_calls".into(),
                actual: 4,
                limit: 3
            }
        );
    }

    #[test]
    fn policy_rejects_disallowed_intent() {
        let mut plan = plan(1);
        plan.nodes[0].intent = "debug".to_string();
        let eng = PlanPolicyEngine::new(PlanPolicy {
            allowed_intents: vec!["architecture".to_string(), "implementation".to_string()],
            ..PlanPolicy::default()
        });
        let err = eng.check_plan(&plan).expect_err("intent");
        assert_eq!(err.rule, "allowed_intents");
    }

    #[test]
    fn policy_rejects_disallowed_role() {
        let mut plan = plan(1);
        plan.nodes[0].role = "reviewer".to_string();
        let eng = PlanPolicyEngine::new(PlanPolicy {
            allowed_roles: vec!["architect".to_string(), "implementer".to_string()],
            ..PlanPolicy::default()
        });
        let err = eng.check_plan(&plan).expect_err("role");
        assert_eq!(err.rule, "allowed_roles");
    }

    #[test]
    fn policy_rejects_requested_parallelism_over_limit() {
        let err = engine().check_parallel(4).expect_err("too parallel");
        assert_eq!(
            err,
            PolicyViolation {
                rule: "max_parallel".into(),
                actual: 4,
                limit: 2
            }
        );
        assert!(engine().check_parallel(2).is_ok());
    }

    #[test]
    fn in_scope_plan_passes() {
        assert!(engine().check_plan(&plan(3)).is_ok());
    }

    #[test]
    fn check_graph_bounds_the_full_candidate() {
        use crate::dag::{WorkflowGraph, WorkflowNode};
        use crate::workflow_state::WorkflowRole;
        // 3 nodes: within limits.
        let nodes: Vec<WorkflowNode> = (0..3)
            .map(|i| WorkflowNode::new(format!("n{i}"), WorkflowRole::Implementer))
            .collect();
        let graph = WorkflowGraph::new(nodes).expect("graph");
        assert!(engine().check_graph(&graph).is_ok());

        // 4 nodes (> max_nodes=3): rejected, even though a replan might only
        // *add* one node on top of three existing ones.
        let nodes: Vec<WorkflowNode> = (0..4)
            .map(|i| WorkflowNode::new(format!("n{i}"), WorkflowRole::Implementer))
            .collect();
        let graph = WorkflowGraph::new(nodes).expect("graph");
        let err = engine().check_graph(&graph).expect_err("over max_nodes");
        assert_eq!(err.rule, "max_nodes");
    }
}
