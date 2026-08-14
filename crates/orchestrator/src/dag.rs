//! DAG model and the parallel-review preset (Phase 16).
//!
//! A [`WorkflowGraph`] is a directed acyclic graph of [`WorkflowNode`]s. It is
//! the extension point over the existing sequential [`Workflow`]: the same
//! engine drives both, and a linear preset is just a graph with a single chain.
//!
//! Nodes are brand-agnostic ([`crate::WorkflowRole`] + [`agentmesh_core::TaskIntent`]);
//! the concrete agent per node is resolved by the RuleRouter from the agent's
//! card — never hard-coded. Dependencies form the DAG; a cycle is rejected up
//! front ([`OrchestratorError::WorkflowCycleDetected`]).

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

use agentmesh_core::TaskIntent;

use crate::error::OrchestratorError;
use crate::workflow_state::WorkflowRole;

/// The built-in parallel-review preset identifier.
pub const PRESET_PARALLEL_REVIEW: &str = "parallel-review";
/// The built-in consensus-review preset identifier (Phase 21 §11).
pub const PRESET_CONSENSUS_REVIEW: &str = "consensus-review";
/// The built-in best-of-n preset identifier (Phase 23).
pub const PRESET_BEST_OF_N: &str = "best-of-n";
/// Default number of parallel evaluators in a consensus-review group.
pub const DEFAULT_EVALUATORS: usize = 3;
/// Default quorum for a consensus-review group.
pub const DEFAULT_EVALUATOR_QUORUM: usize = 2;
/// Hard cap on evaluators in one group.
pub const MAX_EVALUATORS: usize = 5;
/// Default number of candidates in a best-of-n workflow.
pub const DEFAULT_CANDIDATES: usize = 2;
/// Default number of evaluators per candidate in a best-of-n workflow.
pub const DEFAULT_CANDIDATE_EVALUATORS: usize = 2;
/// Hard cap on candidates in a best-of-n workflow.
pub const MAX_CANDIDATES: usize = 3;
/// Hard cap on evaluators per candidate in a best-of-n workflow.
pub const MAX_CANDIDATE_EVALUATORS: usize = 3;

/// One node of a workflow DAG.
///
/// Semantics mirrors [`crate::workflow_state::WorkflowStep`] but with a stable
/// `node_id` (used for persistence and fan-in handoff ordering) and an explicit
/// dependency list. A Phase 17 planner node additionally carries an untrusted
/// `objective`; preset nodes keep `objective = None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub node_id: String,
    pub role: WorkflowRole,
    pub intent: TaskIntent,
    /// `node_id`s that must all reach `Completed` before this node may run.
    /// Empty for a root node. Sorted for deterministic serialization.
    pub dependencies: Vec<String>,
    /// Untrusted planner-generated objective for this node (Phase 17); `None`
    /// for preset nodes. Embedded in the node prompt only as untrusted data.
    #[serde(default)]
    pub objective: Option<String>,
}

impl WorkflowNode {
    pub fn new(node_id: impl Into<String>, role: WorkflowRole) -> Self {
        Self::with_dependencies(node_id, role, Vec::new())
    }

    pub fn with_dependencies(
        node_id: impl Into<String>,
        role: WorkflowRole,
        dependencies: Vec<String>,
    ) -> Self {
        let mut dependencies = dependencies;
        dependencies.sort();
        dependencies.dedup();
        Self {
            node_id: node_id.into(),
            role,
            intent: role.intent(),
            dependencies,
            objective: None,
        }
    }

    /// The workflow step form of this node, preserving its explicit intent and
    /// objective (used by the DAG scheduler and the daemon persister).
    pub fn to_step(&self) -> crate::workflow_state::WorkflowStep {
        crate::workflow_state::WorkflowStep::from_node(self)
    }
}

/// A workflow DAG: the graph's nodes plus the preset/goal that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowGraph {
    pub nodes: Vec<WorkflowNode>,
}

impl WorkflowGraph {
    pub fn new(mut nodes: Vec<WorkflowNode>) -> Result<Self, OrchestratorError> {
        detect_cycle(&nodes).map_err(OrchestratorError::WorkflowCycleDetected)?;
        // Deterministic order: node_id ascending, so dependency iteration and
        // fan-out scheduling are stable across reloads.
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(Self { nodes })
    }

    /// Look up a node by id.
    pub fn get(&self, node_id: &str) -> Option<&WorkflowNode> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    /// The ids of every node that has no dependencies — the graph's roots.
    pub fn roots(&self) -> Vec<String> {
        let mut roots: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.dependencies.is_empty())
            .map(|n| n.node_id.clone())
            .collect();
        roots.sort();
        roots
    }

    /// The total number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The ids of the given node's direct dependents (nodes that list it as a
    /// dependency), sorted by node_id.
    pub fn dependents(&self, node_id: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.dependencies.iter().any(|d| d == node_id))
            .map(|n| n.node_id.clone())
            .collect();
        out.sort();
        out
    }

    /// Whether `node_id` is the graph's single terminal node (nothing depends
    /// on it). Used by apply to confirm a unique last code node.
    pub fn is_terminal(&self, node_id: &str) -> bool {
        self.dependents(node_id).is_empty()
    }
}

/// Detect a dependency cycle in a set of nodes. Returns the cycle path on
/// failure; `Ok(())` when the graph is acyclic.
pub fn detect_cycle(nodes: &[WorkflowNode]) -> Result<(), Vec<String>> {
    let ids: HashSet<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
    // A dependency that does not name an existing node is an error too — the
    // graph is malformed, not merely cyclic, but we surface it as a cycle.
    for node in nodes {
        for dep in &node.dependencies {
            if !ids.contains(dep.as_str()) {
                return Err(vec![dep.clone(), node.node_id.clone()]);
            }
        }
    }

    // Kahn's algorithm: repeatedly remove nodes with no remaining incoming
    // edges. If any nodes remain, the graph has a cycle.
    let mut indegree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for node in nodes {
        indegree.entry(node.node_id.as_str()).or_insert(0);
        for dep in &node.dependencies {
            *indegree.entry(node.node_id.as_str()).or_insert(0) += 1;
            dependents
                .entry(dep.as_str())
                .or_default()
                .push(&node.node_id);
        }
    }
    let mut queue: VecDeque<&str> = indegree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&id, _)| id)
        .collect();
    let mut removed = 0usize;
    while let Some(id) = queue.pop_front() {
        removed += 1;
        if let Some(deps) = dependents.get(id) {
            for &next in deps {
                let deg = indegree.get_mut(next).expect("node present");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(next);
                }
            }
        }
    }
    if removed == nodes.len() {
        Ok(())
    } else {
        // A node still on the cycle: walk from it via dependents to capture a
        // path, abandoning when we return to the start.
        for node in nodes {
            let mut path = vec![node.node_id.clone()];
            let mut seen = HashSet::new();
            let mut cur = node.node_id.as_str();
            seen.insert(cur);
            loop {
                let next = dependents.get(cur).and_then(|d| {
                    d.iter()
                        .find(|candidate| indegree.get(*candidate) != Some(&0))
                });
                let Some(&next) = next else { break };
                if seen.contains(next) {
                    path.push(next.to_string());
                    return Err(path);
                }
                seen.insert(next);
                path.push(next.to_string());
                cur = next;
            }
        }
        Ok(())
    }
}

/// Resolve the DAG of a named preset; `None` for unknown presets.
pub fn preset_graph(preset: &str) -> Option<WorkflowGraph> {
    match preset {
        PRESET_PARALLEL_REVIEW => {
            // Architecture -> { SecurityReview, TestPlanning } -> Implementation -> Review
            let graph = WorkflowGraph::new(vec![
                WorkflowNode::new("architecture", WorkflowRole::Architect),
                WorkflowNode::with_dependencies(
                    "security_review",
                    WorkflowRole::Reviewer,
                    vec!["architecture".to_string()],
                ),
                WorkflowNode::with_dependencies(
                    "test_planning",
                    WorkflowRole::Implementer,
                    vec!["architecture".to_string()],
                ),
                WorkflowNode::with_dependencies(
                    "implementation",
                    WorkflowRole::Implementer,
                    vec!["security_review".to_string(), "test_planning".to_string()],
                ),
                WorkflowNode::with_dependencies(
                    "review",
                    WorkflowRole::Reviewer,
                    vec!["implementation".to_string()],
                ),
            ])
            .expect("parallel-review is acyclic");
            Some(graph)
        }
        PRESET_CONSENSUS_REVIEW => Some(consensus_review_graph(DEFAULT_EVALUATORS)),
        PRESET_BEST_OF_N => Some(best_of_n_graph(
            DEFAULT_CANDIDATES,
            DEFAULT_CANDIDATE_EVALUATORS,
        )),
        _ => None,
    }
}

/// Build a `consensus-review` DAG (Phase 21 §11):
/// `Architecture → Implementation → {N evaluators in parallel} → ConsensusGate`.
///
/// The evaluators are ordinary DAG nodes (existing parallel infra); the gate
/// is a deterministic local node computed by the scheduler — never an agent.
pub fn consensus_review_graph(evaluator_count: usize) -> WorkflowGraph {
    let mut nodes = vec![
        WorkflowNode::new("architecture", WorkflowRole::Architect),
        WorkflowNode::with_dependencies(
            "implementation",
            WorkflowRole::Implementer,
            vec!["architecture".to_string()],
        ),
    ];
    let mut evaluator_ids = Vec::new();
    for i in 0..evaluator_count {
        let id = format!("evaluator_{}", i + 1);
        nodes.push(WorkflowNode::with_dependencies(
            &id,
            WorkflowRole::Evaluator,
            vec!["implementation".to_string()],
        ));
        evaluator_ids.push(id);
    }
    nodes.push(WorkflowNode::with_dependencies(
        "consensus_gate",
        WorkflowRole::ConsensusGate,
        evaluator_ids,
    ));
    WorkflowGraph::new(nodes).expect("consensus-review is acyclic")
}

/// Build a `best-of-n` DAG (Phase 23):
/// `Architecture → {N Candidates in parallel} → for each Candidate: {M evaluators in parallel} → ConsensusGate → SelectionGate`.
///
/// Candidates are parallel implementation nodes; evaluators perform blind review
/// per candidate; consensus gates compute local consensus per candidate; and the
/// selection gate deterministically selects the winning candidate.
pub fn best_of_n_graph(candidate_count: usize, evaluator_count: usize) -> WorkflowGraph {
    let mut nodes = vec![WorkflowNode::new("architecture", WorkflowRole::Architect)];
    let mut consensus_gate_ids = Vec::new();

    for c in 1..=candidate_count {
        let candidate_id = format!("candidate_{c}");
        nodes.push(WorkflowNode::with_dependencies(
            &candidate_id,
            WorkflowRole::Candidate,
            vec!["architecture".to_string()],
        ));

        let mut evaluator_ids = Vec::new();
        for e in 1..=evaluator_count {
            let eval_id = format!("eval_c{c}_{e}");
            nodes.push(WorkflowNode::with_dependencies(
                &eval_id,
                WorkflowRole::Evaluator,
                vec![candidate_id.clone()],
            ));
            evaluator_ids.push(eval_id);
        }

        let consensus_id = format!("consensus_c{c}");
        nodes.push(WorkflowNode::with_dependencies(
            &consensus_id,
            WorkflowRole::ConsensusGate,
            evaluator_ids,
        ));
        consensus_gate_ids.push(consensus_id);
    }

    nodes.push(WorkflowNode::with_dependencies(
        "selection_gate",
        WorkflowRole::SelectionGate,
        consensus_gate_ids,
    ));

    WorkflowGraph::new(nodes).expect("best-of-n is acyclic")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> WorkflowNode {
        WorkflowNode::new(id, WorkflowRole::Implementer)
    }

    fn node_with(id: &str, deps: &[&str]) -> WorkflowNode {
        WorkflowNode::with_dependencies(
            id,
            WorkflowRole::Implementer,
            deps.iter().map(|s| s.to_string()).collect(),
        )
    }

    #[test]
    fn acyclic_graph_constructs() {
        let graph = WorkflowGraph::new(vec![
            node_with("d", &["b", "c"]),
            node("a"),
            node_with("b", &["a"]),
            node_with("c", &["a"]),
        ])
        .expect("acyclic");
        // Deterministic node order.
        let ids: Vec<_> = graph.nodes.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c", "d"]);
        assert_eq!(graph.roots(), vec!["a"]);
        let mut deps = graph.dependents("a");
        deps.sort();
        assert_eq!(deps, vec!["b", "c"]);
        assert!(graph.is_terminal("d"));
        assert!(!graph.is_terminal("a"));
    }

    #[test]
    fn direct_self_cycle_is_rejected() {
        let err = WorkflowGraph::new(vec![node_with("a", &["a"])]).expect_err("cycle");
        assert!(matches!(err, OrchestratorError::WorkflowCycleDetected(_)));
    }

    #[test]
    fn indirect_cycle_is_rejected() {
        let err = WorkflowGraph::new(vec![
            node_with("a", &["b"]),
            node_with("b", &["c"]),
            node_with("c", &["a"]),
        ])
        .expect_err("cycle");
        assert!(matches!(err, OrchestratorError::WorkflowCycleDetected(_)));
    }

    #[test]
    fn missing_dependency_is_rejected() {
        let err = WorkflowGraph::new(vec![node_with("a", &["ghost"])]).expect_err("bad dep");
        assert!(matches!(err, OrchestratorError::WorkflowCycleDetected(_)));
    }

    #[test]
    fn parallel_review_preset_is_acyclic_and_shaped_right() {
        let graph = preset_graph(PRESET_PARALLEL_REVIEW).expect("preset");
        assert_eq!(graph.len(), 5);
        assert_eq!(graph.roots(), vec!["architecture"]);
        assert_eq!(graph.dependents("architecture").len(), 2);
        assert_eq!(graph.get("implementation").unwrap().dependencies.len(), 2);
        assert!(graph.is_terminal("review"));
        assert_eq!(graph.dependents("review"), Vec::<String>::new());
    }

    #[test]
    fn unknown_presets_have_no_graph() {
        assert!(preset_graph("debate").is_none());
    }
}
