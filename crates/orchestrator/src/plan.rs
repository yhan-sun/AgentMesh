//! Phase 17: AI Planner output model, strict validation and parsing.
//!
//! The Planner decides **WHAT** (the task structure and its dependencies)
//! and nothing else. A [`WorkflowPlan`] is pure structure — roles, intents,
//! objectives and dependency edges. It must never carry an agent/provider/
//! model/workspace/permission/command, so the schema is closed
//! ([`serde(deny_unknown_fields)`]): any unknown control field is rejected,
//! never silently accepted.
//!
//! ```text
//! WorkflowPlan (planner output)
//!   → PlanValidator
//!   → WorkflowGraph::new (Phase 16 Kahn / cycle validation — the only DAG
//!     validator; there is no second runtime)
//! ```
//!
//! Parsing rules (Planner output §3):
//! * prefer a JSON/Data artifact;
//! * fallback: the final agent message is itself valid JSON;
//! * reject markdown fences, regex extraction, auto-comma-fixing and guessing
//!   a plan from prose — never be lenient.

use std::collections::HashSet;

use agentmesh_a2a::types::A2AArtifact;
use agentmesh_core::{ArtifactKind, TaskIntent};
use serde::{Deserialize, Serialize};

use crate::dag::{WorkflowGraph, WorkflowNode};
use crate::error::OrchestratorError;
use crate::handoff::{artifact_kind, extract_content};
use crate::workflow_state::WorkflowRole;

/// The only accepted plan schema version.
pub const PLAN_SCHEMA_VERSION: u32 = 1;
/// Hard cap on plan nodes (1..=MAX_PLAN_NODES); also bounds parallelism risk.
pub const MAX_PLAN_NODES: usize = 12;
/// Hard cap on a single node objective (characters).
pub const MAX_OBJECTIVE_CHARS: usize = 2048;
/// Hard cap on the serialized plan JSON (bytes) — bounds what gets persisted
/// and re-parsed at execute time.
pub const MAX_PLAN_JSON_BYTES: usize = 64 * 1024;

/// Roles a planner may assign. Deliberately finite: the planner cannot invent
/// a role to control the system prompt.
pub const PLAN_ROLES: &[&str] = &[
    "architect",
    "implementer",
    "reviewer",
    "security_review",
    "test_planning",
    "testing",
    "uiux",
    "analysis",
];

/// Intents a planner may assign (the existing [`TaskIntent`] set).
pub const PLAN_INTENTS: &[&str] = &[
    "architecture",
    "implementation",
    "debug",
    "review",
    "testing",
    "uiux",
    "general",
];

/// A structured execution plan produced by the AI planner.
///
/// The schema is closed (`deny_unknown_fields`): `agent_id`, `provider`,
/// `model`, `permissions`, `workspace`, `environment`, `commands`,
/// `max_parallel` or any other unknown control field is rejected at parse time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPlan {
    pub version: u32,
    pub summary: String,
    pub nodes: Vec<PlannedNode>,
}

impl WorkflowPlan {
    /// Parse a plan from its raw JSON string, enforcing the overall size cap.
    pub fn from_json(text: &str) -> Result<WorkflowPlan, PlanParseError> {
        if text.len() > MAX_PLAN_JSON_BYTES {
            return Err(PlanParseError::PlanTooLarge {
                max: MAX_PLAN_JSON_BYTES,
            });
        }
        serde_json::from_str(text).map_err(|err| PlanParseError::MalformedJson(err.to_string()))
    }

    /// Strictly validate the plan and convert it into a [`WorkflowGraph`].
    ///
    /// All semantic checks live here; the graph's own constructor runs the
    /// Phase 16 Kahn/cycle validation (there is no second DAG validator).
    pub fn validate(&self) -> Result<WorkflowGraph, PlanValidationError> {
        if self.version != PLAN_SCHEMA_VERSION {
            return Err(PlanValidationError::UnsupportedVersion {
                version: self.version,
                expected: PLAN_SCHEMA_VERSION,
            });
        }
        if self.nodes.is_empty() {
            return Err(PlanValidationError::EmptyPlan);
        }
        if self.nodes.len() > MAX_PLAN_NODES {
            return Err(PlanValidationError::TooManyNodes {
                max: MAX_PLAN_NODES,
                got: self.nodes.len(),
            });
        }

        // Collect the full id set first so dependency checks below can
        // reference nodes declared later in the array.
        let mut ids = HashSet::new();
        for node in &self.nodes {
            if !ids.insert(node.id.clone()) {
                return Err(PlanValidationError::DuplicateNodeId(node.id.clone()));
            }
        }

        for node in &self.nodes {
            if node.objective.trim().is_empty() {
                return Err(PlanValidationError::EmptyObjective {
                    id: node.id.clone(),
                });
            }
            if node.objective.chars().count() > MAX_OBJECTIVE_CHARS {
                return Err(PlanValidationError::ObjectiveTooLong {
                    id: node.id.clone(),
                    max: MAX_OBJECTIVE_CHARS,
                });
            }
            if WorkflowRole::from_str(&node.role).is_none()
                || !PLAN_ROLES.contains(&node.role.as_str())
            {
                return Err(PlanValidationError::UnsupportedRole {
                    id: node.id.clone(),
                    role: node.role.clone(),
                });
            }
            if TaskIntent::from_key(&node.intent).is_none()
                || !PLAN_INTENTS.contains(&node.intent.as_str())
            {
                return Err(PlanValidationError::UnsupportedIntent {
                    id: node.id.clone(),
                    intent: node.intent.clone(),
                });
            }
            if node.depends_on.iter().any(|d| d == &node.id) {
                return Err(PlanValidationError::SelfDependency(node.id.clone()));
            }
            for dep in &node.depends_on {
                if !ids.contains(dep) {
                    return Err(PlanValidationError::MissingDependency {
                        id: node.id.clone(),
                        dep: dep.clone(),
                    });
                }
            }
        }

        // At least one root (no incoming edges) and one terminal (no outgoing
        // edges); otherwise the plan can never start or never finish.
        if !self.nodes.iter().any(|n| n.depends_on.is_empty()) {
            return Err(PlanValidationError::NoRoot);
        }
        if !self
            .nodes
            .iter()
            .any(|n| !self.nodes.iter().any(|o| o.depends_on.contains(&n.id)))
        {
            return Err(PlanValidationError::NoTerminal);
        }

        let graph_nodes: Vec<WorkflowNode> = self
            .nodes
            .iter()
            .map(|node| WorkflowNode {
                node_id: node.id.clone(),
                role: WorkflowRole::from_str(&node.role).expect("role validated above"),
                intent: TaskIntent::from_key(&node.intent).expect("intent validated above"),
                dependencies: node.depends_on.clone(),
                objective: Some(node.objective.clone()),
            })
            .collect();
        WorkflowGraph::new(graph_nodes).map_err(|err| match err {
            OrchestratorError::WorkflowCycleDetected(path) => PlanValidationError::Cycle(path),
            other => PlanValidationError::Invalid(other.to_string()),
        })
    }
}

/// One node of a [`WorkflowPlan`].
///
/// `role`/`intent` are stable snake_case strings (validated against
/// [`PLAN_ROLES`] / [`PLAN_INTENTS`]). The objective is untrusted
/// planner-generated text — the execution prompt treats it as data only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedNode {
    pub id: String,
    pub role: String,
    pub intent: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// A failed attempt to extract a plan from the planner's output.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanParseError {
    #[error("planner produced no usable plan JSON: {0}")]
    NoJsonOutput(String),
    #[error("planner output is wrapped in a markdown code fence; only pure JSON is accepted")]
    MarkdownFenced,
    #[error("planner output is not valid JSON: {0}")]
    MalformedJson(String),
    #[error("plan JSON exceeds {max} bytes")]
    PlanTooLarge { max: usize },
}

/// A semantic violation found by [`WorkflowPlan::validate`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanValidationError {
    #[error("unsupported plan schema version {version}; expected {expected}")]
    UnsupportedVersion { version: u32, expected: u32 },
    #[error("plan has no nodes")]
    EmptyPlan,
    #[error("plan has more than {max} nodes ({got})")]
    TooManyNodes { max: usize, got: usize },
    #[error("duplicate node id `{0}`")]
    DuplicateNodeId(String),
    #[error("node `{id}` has an empty objective")]
    EmptyObjective { id: String },
    #[error("node `{id}` objective exceeds {max} characters")]
    ObjectiveTooLong { id: String, max: usize },
    #[error("node `{id}` references a missing dependency `{dep}`")]
    MissingDependency { id: String, dep: String },
    #[error("node `{0}` depends on itself")]
    SelfDependency(String),
    #[error("node `{id}` uses an unsupported role `{role}`")]
    UnsupportedRole { id: String, role: String },
    #[error("node `{id}` uses an unsupported intent `{intent}`")]
    UnsupportedIntent { id: String, intent: String },
    #[error("plan has no root node (every node depends on another)")]
    NoRoot,
    #[error("plan has no terminal node (every node is a dependency of another)")]
    NoTerminal,
    #[error("plan contains a dependency cycle: {0:?}")]
    Cycle(Vec<String>),
    #[error("invalid plan: {0}")]
    Invalid(String),
}

/// A normalized artifact the planner produced, for plan extraction.
///
/// The daemon maps the A2A artifacts of the planner task onto this shape; the
/// parser itself stays free of A2A/daemon concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerArtifact {
    pub name: String,
    pub kind: ArtifactKind,
    /// Inline text content (for a JSON/Data part this is the JSON text).
    pub content: Option<String>,
}

impl From<&A2AArtifact> for PlannerArtifact {
    fn from(artifact: &A2AArtifact) -> Self {
        let (content, _, _) = extract_content(artifact);
        Self {
            name: artifact.name.clone(),
            kind: artifact_kind(artifact),
            content,
        }
    }
}

/// Extract a [`WorkflowPlan`] from the planner's final message + artifacts.
///
/// Order (Planner output §3):
/// 1. a JSON/Data artifact is preferred and, when present, is authoritative —
///    a fenced/malformed artifact is rejected, never silently skipped;
/// 2. fallback: the final agent message must itself be valid JSON;
/// 3. otherwise the output is unusable ([`PlanParseError`]).
pub fn parse_planner_output(
    summary: Option<&str>,
    artifacts: &[PlannerArtifact],
) -> Result<WorkflowPlan, PlanParseError> {
    let json_artifacts: Vec<&PlannerArtifact> = artifacts
        .iter()
        .filter(|a| a.kind == ArtifactKind::Json || a.name.to_ascii_lowercase().ends_with(".json"))
        .collect();
    if !json_artifacts.is_empty() {
        let artifact = json_artifacts[0];
        let text = artifact.content.as_deref().ok_or_else(|| {
            PlanParseError::NoJsonOutput(format!(
                "json artifact `{}` has no inline content",
                artifact.name
            ))
        })?;
        if looks_markdown(text) {
            return Err(PlanParseError::MarkdownFenced);
        }
        return WorkflowPlan::from_json(text);
    }
    if let Some(message) = summary {
        if looks_markdown(message) {
            return Err(PlanParseError::MarkdownFenced);
        }
        return WorkflowPlan::from_json(message);
    }
    Err(PlanParseError::NoJsonOutput(
        "planner produced no JSON artifact and no final message".to_string(),
    ))
}

/// Whether text is wrapped in a markdown code fence (` ``` ` or a bare tick).
/// Fenced JSON is rejected — we never strip the fence and parse inside it.
fn looks_markdown(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("```") || trimmed.starts_with('`')
}

/// The prompt sent to the planner agent over A2A.
///
/// The planner is a normal coding agent reached through the RuleRouter (intent
/// `architecture`); the prompt forces strict JSON output and forbids every
/// control field that the Router/Daemon must decide.
pub fn build_planner_prompt(goal: &str) -> String {
    format!(
        "You are the planning agent of AgentMesh. Given the user's goal, produce a \
         structured multi-agent execution plan as STRICT JSON.\n\n\
         RULES\n\
         - Respond with ONLY valid JSON. No markdown fences, no explanations, no prose \
         outside the JSON.\n\
         - Prefer to emit the JSON as a data/JSON artifact named `plan.json`. If your \
         platform cannot emit artifacts, your final message must be the JSON itself.\n\
         - The JSON must match this schema exactly:\n\
         {{\n  \"version\": 1,\n  \"summary\": \"one-line plan summary\",\n  \
         \"nodes\": [\n    {{\n      \"id\": \"short-snake-case-unique-id\",\n      \
         \"role\": \"one of the allowed roles\",\n      \"intent\": \"one of the allowed \
         intents\",\n      \"objective\": \"what this node should accomplish (non-empty, \
         <= {MAX_OBJECTIVE_CHARS} chars)\",\n      \"depends_on\": [\"ids of other nodes \
         that must complete first\"]\n    }}\n  ]\n}}\n\n\
         ALLOWED ROLES\n{}\n\n\
         ALLOWED INTENTS\n{}\n\n\
         STRUCTURE RULES\n\
         - Between 1 and {MAX_PLAN_NODES} nodes.\n\
         - Node ids are unique.\n\
         - `depends_on` may only reference existing node ids, never itself.\n\
         - At least one node has no dependencies (a root) and at least one node is \
         terminal (nothing depends on it).\n\
         - The plan must be acyclic.\n\n\
         FORBIDDEN\n\
         - Never include agent/provider/model/workspace/permissions/commands/\
         parallelism fields. Agents are chosen by AgentMesh routing, not by the plan.\n\n\
         USER GOAL\n{goal}",
        PLAN_ROLES.join("\n"),
        PLAN_INTENTS.join("\n"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A minimal valid plan JSON (architect → implementer → reviewer).
    fn valid_plan_json() -> serde_json::Value {
        json!({
            "version": 1,
            "summary": "auth refactor plan",
            "nodes": [
                {
                    "id": "architecture",
                    "role": "architect",
                    "intent": "architecture",
                    "objective": "Design the auth refactor",
                    "depends_on": []
                },
                {
                    "id": "implementation",
                    "role": "implementer",
                    "intent": "implementation",
                    "objective": "Implement the approved design",
                    "depends_on": ["architecture"]
                },
                {
                    "id": "security",
                    "role": "security_review",
                    "intent": "review",
                    "objective": "Security-review the implementation",
                    "depends_on": ["implementation"]
                }
            ]
        })
    }

    fn parse_ok(value: &serde_json::Value) -> WorkflowPlan {
        WorkflowPlan::from_json(&value.to_string()).expect("parse ok")
    }

    #[test]
    fn valid_plan_parses_and_validates_to_a_graph() {
        let plan = parse_ok(&valid_plan_json());
        assert_eq!(plan.version, PLAN_SCHEMA_VERSION);
        let graph = plan.validate().expect("valid");
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.roots(), vec!["architecture"]);
        assert!(graph.is_terminal("security"));
        // The objective is carried onto the node.
        assert_eq!(
            graph.get("implementation").unwrap().objective.as_deref(),
            Some("Implement the approved design")
        );
    }

    #[test]
    fn plan_artifact_is_preferred() {
        let artifact = PlannerArtifact {
            name: "plan.json".to_string(),
            kind: ArtifactKind::Json,
            content: Some(valid_plan_json().to_string()),
        };
        let plan = parse_planner_output(Some("prose summary"), &[artifact]).expect("parse");
        assert_eq!(plan.nodes.len(), 3);
    }

    #[test]
    fn final_message_json_is_a_fallback() {
        let plan = parse_planner_output(Some(&valid_plan_json().to_string()), &[]).expect("parse");
        assert_eq!(plan.nodes.len(), 3);
    }

    #[test]
    fn malformed_json_is_rejected() {
        let artifact = PlannerArtifact {
            name: "plan.json".to_string(),
            kind: ArtifactKind::Json,
            content: Some("{\"version\": 1, \"nodes\": [".to_string()),
        };
        let err = parse_planner_output(None, &[artifact]).expect_err("reject");
        assert!(matches!(err, PlanParseError::MalformedJson(_)));
    }

    #[test]
    fn markdown_fenced_json_is_rejected() {
        let fenced = format!("```json\n{}\n```", valid_plan_json());
        let artifact = PlannerArtifact {
            name: "plan.json".to_string(),
            kind: ArtifactKind::Json,
            content: Some(fenced),
        };
        let err = parse_planner_output(None, &[artifact]).expect_err("reject");
        assert!(matches!(err, PlanParseError::MarkdownFenced));
    }

    #[test]
    fn prose_message_without_artifact_is_rejected() {
        let err =
            parse_planner_output(Some("I think we should refactor auth"), &[]).expect_err("reject");
        assert!(matches!(err, PlanParseError::MalformedJson(_)));
    }

    #[test]
    fn no_output_at_all_is_rejected() {
        let err = parse_planner_output(None, &[]).expect_err("reject");
        assert!(matches!(err, PlanParseError::NoJsonOutput(_)));
    }

    #[test]
    fn duplicate_node_id_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][1]["id"] = json!("architecture");
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::DuplicateNodeId(_)));
    }

    #[test]
    fn missing_dependency_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][1]["depends_on"] = json!(["ghost"]);
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(
            err,
            PlanValidationError::MissingDependency { dep, .. } if dep == "ghost"
        ));
    }

    #[test]
    fn self_dependency_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][0]["depends_on"] = json!(["architecture"]);
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(
            err,
            PlanValidationError::SelfDependency(id) if id == "architecture"
        ));
    }

    #[test]
    fn cycle_is_rejected() {
        // a (root) → b ↔ c (cycle) → d (terminal): root/terminal checks pass,
        // so the cycle validator must be the one to reject it.
        let value = json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": []},
                {"id": "b", "role": "implementer", "intent": "implementation", "objective": "b", "depends_on": ["a", "c"]},
                {"id": "c", "role": "reviewer", "intent": "review", "objective": "c", "depends_on": ["b"]},
                {"id": "d", "role": "testing", "intent": "testing", "objective": "d", "depends_on": ["a"]}
            ]
        });
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::Cycle(_)));
    }

    #[test]
    fn invalid_role_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][0]["role"] = json!("prompt_engineer");
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(
            err,
            PlanValidationError::UnsupportedRole { role, .. } if role == "prompt_engineer"
        ));
    }

    #[test]
    fn invalid_intent_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][0]["intent"] = json!("surfing");
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(
            err,
            PlanValidationError::UnsupportedIntent { intent, .. } if intent == "surfing"
        ));
    }

    #[test]
    fn too_many_nodes_is_rejected() {
        let mut value = valid_plan_json();
        let extra = value["nodes"][2].clone();
        for _ in 0..MAX_PLAN_NODES {
            value["nodes"].as_array_mut().unwrap().push(extra.clone());
        }
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::TooManyNodes { .. }));
    }

    #[test]
    fn agent_id_field_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][1]["agent_id"] = json!("claude");
        let err = WorkflowPlan::from_json(&value.to_string()).expect_err("reject");
        assert!(matches!(err, PlanParseError::MalformedJson(_)));
    }

    #[test]
    fn permission_field_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][1]["permissions"] = json!(["root"]);
        let err = WorkflowPlan::from_json(&value.to_string()).expect_err("reject");
        assert!(matches!(err, PlanParseError::MalformedJson(_)));
    }

    #[test]
    fn provider_and_commands_fields_are_rejected() {
        for field in [
            "provider",
            "model",
            "workspace",
            "environment",
            "commands",
            "max_parallel",
        ] {
            let mut value = valid_plan_json();
            value["nodes"][1][field] = json!("anything");
            let err = WorkflowPlan::from_json(&value.to_string()).expect_err("reject");
            assert!(
                matches!(err, PlanParseError::MalformedJson(_)),
                "field `{field}` must be rejected"
            );
        }
    }

    #[test]
    fn top_level_control_fields_are_rejected() {
        let mut value = valid_plan_json();
        value["agent_id"] = json!("claude");
        let err = WorkflowPlan::from_json(&value.to_string()).expect_err("reject");
        assert!(matches!(err, PlanParseError::MalformedJson(_)));
    }

    #[test]
    fn objective_size_limit_is_enforced() {
        let mut value = valid_plan_json();
        value["nodes"][0]["objective"] = json!("x".repeat(MAX_OBJECTIVE_CHARS + 1));
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::ObjectiveTooLong { .. }));
    }

    #[test]
    fn empty_objective_is_rejected() {
        let mut value = valid_plan_json();
        value["nodes"][0]["objective"] = json!("   ");
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::EmptyObjective { .. }));
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let mut value = valid_plan_json();
        value["version"] = json!(2);
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(
            err,
            PlanValidationError::UnsupportedVersion { version: 2, .. }
        ));
    }

    #[test]
    fn plan_too_large_is_rejected() {
        let big = format!(
            "{{\"version\":1,\"summary\":\"{}\",\"nodes\":[]}}",
            "y".repeat(MAX_PLAN_JSON_BYTES)
        );
        let err = WorkflowPlan::from_json(&big).expect_err("reject");
        assert!(matches!(err, PlanParseError::PlanTooLarge { .. }));
    }

    #[test]
    fn no_root_is_rejected() {
        // Every node depends on another → the plan can never start.
        let value = json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": ["b"]},
                {"id": "b", "role": "implementer", "intent": "implementation", "objective": "b", "depends_on": ["a"]}
            ]
        });
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::NoRoot));
    }

    #[test]
    fn no_terminal_is_rejected() {
        // a is a root, but every node is a dependency of another (b↔c), so the
        // plan can never finish.
        let value = json!({
            "version": 1,
            "summary": "s",
            "nodes": [
                {"id": "a", "role": "architect", "intent": "architecture", "objective": "a", "depends_on": []},
                {"id": "b", "role": "implementer", "intent": "implementation", "objective": "b", "depends_on": ["a", "c"]},
                {"id": "c", "role": "reviewer", "intent": "review", "objective": "c", "depends_on": ["b"]}
            ]
        });
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::NoTerminal));
    }

    #[test]
    fn empty_plan_is_rejected() {
        let value = json!({ "version": 1, "summary": "s", "nodes": [] });
        let err = parse_ok(&value).validate().expect_err("reject");
        assert!(matches!(err, PlanValidationError::EmptyPlan));
    }

    #[test]
    fn planner_prompt_forbids_control_fields() {
        let prompt = build_planner_prompt("Refactor auth");
        assert!(prompt.contains("Refactor auth"));
        assert!(prompt.contains("architect"));
        assert!(prompt.contains("security_review"));
        assert!(prompt.contains("No markdown fences"));
        assert!(
            prompt
                .to_lowercase()
                .contains("never include agent/provider")
        );
    }

    #[test]
    fn plan_roundtrips_through_json() {
        let plan = parse_ok(&valid_plan_json());
        let json = serde_json::to_string(&plan).unwrap();
        let back = WorkflowPlan::from_json(&json).unwrap();
        assert_eq!(back, plan);
    }
}
