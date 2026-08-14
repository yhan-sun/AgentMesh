//! Structural plan budget (Phase 18): explains a plan's execution cost in
//! structure only.
//!
//! This is deliberately NOT a token/USD estimate — AgentMesh does not pretend
//! to know what Claude/Codex actually consume. For a fixed DAG each node is
//! exactly one agent call, so `estimated_agent_calls == node_count`. The
//! planner's own generation call is reported separately as `planning_calls`.

use serde::{Deserialize, Serialize};

use crate::dag::WorkflowGraph;
use crate::plan::WorkflowPlan;

/// Structural cost of executing a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanBudget {
    /// Number of nodes the plan will execute.
    pub node_count: usize,
    /// One agent call per non-gate node (Phase 22/23: deterministic
    /// ConsensusGate and SelectionGate are local computations, never agent calls).
    pub estimated_agent_calls: usize,
    /// Evaluator agent calls across ALL consensus fix rounds (Phase 22 §15):
    /// one per `Evaluator` node. Checked against `[evaluation]
    /// max_total_evaluator_calls` before execution and before a dynamic
    /// fix-loop extension.
    pub evaluation_agent_calls: usize,
    /// Candidate agent calls in a Best-of-N competition (Phase 23 §22):
    /// one per `Candidate` node. Checked against `[competition]
    /// max_total_candidate_calls` before execution.
    pub candidate_agent_calls: usize,
    /// Nodes with no dependencies (the graph can start from these).
    pub root_count: usize,
    /// Nodes nothing depends on (the graph can finish at these).
    pub terminal_count: usize,
    /// The parallelism the user requested for execution.
    pub max_parallel_requested: usize,
    /// The planner's own single generation call, reported separately.
    pub planning_calls: usize,
}

impl PlanBudget {
    /// Compute the budget from a validated plan + its graph.
    pub fn new(_plan: &WorkflowPlan, graph: &WorkflowGraph, max_parallel_requested: usize) -> Self {
        Self::from_graph(graph, max_parallel_requested)
    }

    /// Compute the budget from a graph alone (replan candidates, Phase 19 §10
    /// checks the full post-delta DAG). One agent call per agent node.
    pub fn from_graph(graph: &WorkflowGraph, max_parallel_requested: usize) -> Self {
        let node_count = graph.len();
        let terminal_count = graph
            .nodes
            .iter()
            .filter(|n| graph.is_terminal(&n.node_id))
            .count();
        Self {
            node_count,
            // ConsensusGate and SelectionGate are not agent calls (Phase 22/23).
            estimated_agent_calls: graph
                .nodes
                .iter()
                .filter(|n| {
                    n.role != crate::workflow_state::WorkflowRole::ConsensusGate
                        && n.role != crate::workflow_state::WorkflowRole::SelectionGate
                })
                .count(),
            evaluation_agent_calls: graph
                .nodes
                .iter()
                .filter(|n| n.role == crate::workflow_state::WorkflowRole::Evaluator)
                .count(),
            candidate_agent_calls: graph
                .nodes
                .iter()
                .filter(|n| n.role == crate::workflow_state::WorkflowRole::Candidate)
                .count(),
            root_count: graph.roots().len(),
            terminal_count,
            max_parallel_requested,
            planning_calls: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn budget_reports_structure_for_a_chain() {
        let plan = WorkflowPlan::from_json(&json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": []},
                {"id": "b", "role": "implementer", "intent": "implementation", "objective": "b", "depends_on": ["a"]},
                {"id": "c", "role": "reviewer", "intent": "review", "objective": "c", "depends_on": ["b"]}
            ]
        })
        .to_string())
        .expect("parse");
        let graph = plan.validate().expect("valid");
        let budget = PlanBudget::new(&plan, &graph, 2);
        assert_eq!(budget.node_count, 3);
        assert_eq!(budget.estimated_agent_calls, 3, "one call per node");
        assert_eq!(budget.root_count, 1);
        assert_eq!(budget.terminal_count, 1);
        assert_eq!(budget.max_parallel_requested, 2);
        assert_eq!(budget.planning_calls, 1, "planner call reported separately");
    }

    #[test]
    fn budget_counts_roots_and_terminals_of_a_fan() {
        let plan = WorkflowPlan::from_json(&json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": []},
                {"id": "b", "role": "implementer", "intent": "implementation", "objective": "b", "depends_on": ["a"]},
                {"id": "c", "role": "reviewer", "intent": "review", "objective": "c", "depends_on": ["a"]}
            ]
        })
        .to_string())
        .expect("parse");
        let graph = plan.validate().expect("valid");
        let budget = PlanBudget::new(&plan, &graph, 4);
        assert_eq!(budget.root_count, 1);
        assert_eq!(
            budget.terminal_count, 2,
            "both fan-out leaves are terminals"
        );
    }

    #[test]
    fn consensus_graph_counts_evaluator_calls_and_excludes_the_gate() {
        use crate::dag::{WorkflowGraph, WorkflowNode};
        use crate::workflow_state::WorkflowRole;
        // consensus-review with 3 evaluators + a fix round (Phase 22 §15):
        // Implementation=1, Eval r0=3, Fixer=1, Eval r1=3 → 6 evaluator calls;
        // the deterministic gate is never an agent call.
        let graph = WorkflowGraph::new(vec![
            WorkflowNode::new("architecture", WorkflowRole::Architect),
            WorkflowNode::with_dependencies(
                "implementation",
                WorkflowRole::Implementer,
                vec!["architecture".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "evaluator_1",
                WorkflowRole::Evaluator,
                vec!["implementation".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "evaluator_2",
                WorkflowRole::Evaluator,
                vec!["implementation".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "evaluator_3",
                WorkflowRole::Evaluator,
                vec!["implementation".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "consensus_gate",
                WorkflowRole::ConsensusGate,
                vec![
                    "evaluator_1".to_string(),
                    "evaluator_2".to_string(),
                    "evaluator_3".to_string(),
                ],
            ),
            WorkflowNode::with_dependencies(
                "fix_r1",
                WorkflowRole::Fixer,
                vec!["consensus_gate".to_string(), "implementation".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "evaluator_r1_1",
                WorkflowRole::Evaluator,
                vec!["fix_r1".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "evaluator_r1_2",
                WorkflowRole::Evaluator,
                vec!["fix_r1".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "evaluator_r1_3",
                WorkflowRole::Evaluator,
                vec!["fix_r1".to_string()],
            ),
            WorkflowNode::with_dependencies(
                "consensus_gate_r1",
                WorkflowRole::ConsensusGate,
                vec![
                    "evaluator_r1_1".to_string(),
                    "evaluator_r1_2".to_string(),
                    "evaluator_r1_3".to_string(),
                ],
            ),
        ])
        .expect("graph");
        let budget = PlanBudget::from_graph(&graph, 2);
        assert_eq!(budget.evaluation_agent_calls, 6, "3 evaluators × 2 rounds");
        assert_eq!(
            budget.estimated_agent_calls, 9,
            "all agent nodes (architecture + implementer + 6 evaluators + fixer), gates excluded"
        );
        assert_eq!(budget.node_count, 11);
    }
}
