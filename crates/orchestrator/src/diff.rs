//! Plan diff (Phase 18): what a user edit changed versus the original
//! planner output. Deliberately a shallow structural comparison — no text-diff
//! engine, no similarity scoring.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::plan::WorkflowPlan;

/// Structural differences between two revisions of a plan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDiff {
    /// Node ids present in `after` but not `before`.
    pub added_nodes: Vec<String>,
    /// Node ids present in `before` but not `after`.
    pub removed_nodes: Vec<String>,
    /// (node_id, before, after) for nodes whose objective text changed.
    pub changed_objective: Vec<DiffField<String>>,
    /// (node_id, before, after) for nodes whose role changed.
    pub changed_role: Vec<DiffField<String>>,
    /// (node_id, before, after) for nodes whose intent changed.
    pub changed_intent: Vec<DiffField<String>>,
    /// (node_id, before, after) for nodes whose dependency list changed.
    pub changed_dependencies: Vec<DiffField<Vec<String>>>,
}

/// One field change on a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffField<T> {
    pub node_id: String,
    pub before: T,
    pub after: T,
}

impl<T> DiffField<T> {
    fn new(node_id: &str, before: T, after: T) -> Self {
        Self {
            node_id: node_id.to_string(),
            before,
            after,
        }
    }
}

impl PlanDiff {
    /// Compare the planner's revision (`before`) to the current one (`after`).
    /// `before` is conventionally revision 1 (the planner output).
    pub fn new(before: &WorkflowPlan, after: &WorkflowPlan) -> Self {
        let before_ids: HashSet<&str> = before.nodes.iter().map(|n| n.id.as_str()).collect();
        let after_ids: HashSet<&str> = after.nodes.iter().map(|n| n.id.as_str()).collect();

        let mut added_nodes: Vec<String> = after_ids
            .difference(&before_ids)
            .map(|s| s.to_string())
            .collect();
        added_nodes.sort();
        let mut removed_nodes: Vec<String> = before_ids
            .difference(&after_ids)
            .map(|s| s.to_string())
            .collect();
        removed_nodes.sort();

        let mut changed_objective = Vec::new();
        let mut changed_role = Vec::new();
        let mut changed_intent = Vec::new();
        let mut changed_dependencies = Vec::new();
        for after_node in &after.nodes {
            let Some(before_node) = before.nodes.iter().find(|n| n.id == after_node.id) else {
                continue;
            };
            if before_node.objective != after_node.objective {
                changed_objective.push(DiffField::new(
                    &after_node.id,
                    before_node.objective.clone(),
                    after_node.objective.clone(),
                ));
            }
            if before_node.role != after_node.role {
                changed_role.push(DiffField::new(
                    &after_node.id,
                    before_node.role.clone(),
                    after_node.role.clone(),
                ));
            }
            if before_node.intent != after_node.intent {
                changed_intent.push(DiffField::new(
                    &after_node.id,
                    before_node.intent.clone(),
                    after_node.intent.clone(),
                ));
            }
            if before_node.depends_on != after_node.depends_on {
                changed_dependencies.push(DiffField::new(
                    &after_node.id,
                    before_node.depends_on.clone(),
                    after_node.depends_on.clone(),
                ));
            }
        }

        Self {
            added_nodes,
            removed_nodes,
            changed_objective,
            changed_role,
            changed_intent,
            changed_dependencies,
        }
    }

    /// True when the two revisions are structurally identical.
    pub fn is_empty(&self) -> bool {
        self.added_nodes.is_empty()
            && self.removed_nodes.is_empty()
            && self.changed_objective.is_empty()
            && self.changed_role.is_empty()
            && self.changed_intent.is_empty()
            && self.changed_dependencies.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> WorkflowPlan {
        WorkflowPlan::from_json(&json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": []},
                {"id": "b", "role": "implementer", "intent": "implementation", "objective": "b", "depends_on": ["a"]},
                {"id": "c", "role": "reviewer", "intent": "review", "objective": "c", "depends_on": ["b"]}
            ]
        })
        .to_string())
        .expect("parse")
    }

    #[test]
    fn identical_plans_diff_to_empty() {
        let a = base();
        let b = base();
        assert!(PlanDiff::new(&a, &b).is_empty());
    }

    #[test]
    fn added_and_removed_nodes_are_reported() {
        let before = base();
        let mut after = base();
        after.nodes.retain(|n| n.id != "c");
        after.nodes.push(
            serde_json::from_str(
                r#"{"id":"d","role":"testing","intent":"testing","objective":"d","depends_on":["b"]}"#,
            )
            .expect("node"),
        );
        let diff = PlanDiff::new(&before, &after);
        assert_eq!(diff.added_nodes, vec!["d"]);
        assert_eq!(diff.removed_nodes, vec!["c"]);
    }

    #[test]
    fn changed_fields_are_reported_per_node() {
        let before = base();
        let mut after = base();
        after.nodes[1].objective = "b updated".to_string();
        after.nodes[1].role = "architect".to_string();
        after.nodes[1].intent = "debug".to_string();
        after.nodes[2].depends_on = vec!["a".to_string()];
        let diff = PlanDiff::new(&before, &after);
        assert_eq!(diff.changed_objective.len(), 1);
        assert_eq!(diff.changed_objective[0].node_id, "b");
        assert_eq!(diff.changed_role.len(), 1);
        assert_eq!(diff.changed_role[0].before, "implementer");
        assert_eq!(diff.changed_role[0].after, "architect");
        assert_eq!(diff.changed_intent.len(), 1);
        assert_eq!(diff.changed_dependencies.len(), 1);
        assert_eq!(diff.changed_dependencies[0].after, vec!["a".to_string()]);
    }
}
