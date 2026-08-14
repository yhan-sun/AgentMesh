//! Phase 24: Provenance, Audit, and Hash-Chain Integrity.
//!
//! Provides typed provenance event payloads, canonical serialization,
//! SHA-256 hash-chaining, policy snapshots, and centralized secret/reasoning
//! redaction (`AuditRedactor`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Stable schema version for exported provenance events and audit streams.
pub const PROVENANCE_SCHEMA_VERSION: u32 = 1;

/// Actor category for provenance events.
pub mod actor_type {
    pub const USER: &str = "user";
    pub const AGENT: &str = "agent";
    pub const SYSTEM: &str = "system";
}

/// Entity types tracked in the provenance ledger.
pub mod entity_type {
    pub const WORKFLOW: &str = "workflow";
    pub const NODE: &str = "node";
    pub const PLAN: &str = "plan";
    pub const REPLAN: &str = "replan";
    pub const RECOVERY: &str = "recovery";
    pub const EVALUATION_GROUP: &str = "evaluation_group";
    pub const EVALUATION_MEMBER: &str = "evaluation_member";
    pub const COMPETITION_GROUP: &str = "competition_group";
    pub const COMPETITION_CANDIDATE: &str = "competition_candidate";
    pub const APPLY: &str = "apply";
    pub const WORKSPACE: &str = "workspace";
}

/// Stable provenance event types.
pub mod event_type {
    pub const PLAN_GENERATED: &str = "PlanGenerated";
    pub const PLAN_REVISED: &str = "PlanRevised";
    pub const PLAN_EXECUTED: &str = "PlanExecuted";

    pub const WORKFLOW_STARTED: &str = "WorkflowStarted";
    pub const WORKFLOW_RESUMED: &str = "WorkflowResumed";
    pub const WORKFLOW_CANCELLED: &str = "WorkflowCancelled";
    pub const WORKFLOW_COMPLETED: &str = "WorkflowCompleted";
    pub const WORKFLOW_FAILED: &str = "WorkflowFailed";

    pub const NODE_STARTED: &str = "NodeStarted";
    pub const NODE_COMPLETED: &str = "NodeCompleted";
    pub const NODE_FAILED: &str = "NodeFailed";

    pub const REPLAN_PROPOSED: &str = "ReplanProposed";
    pub const REPLAN_APPLIED: &str = "ReplanApplied";

    pub const RECOVERY_PROPOSED: &str = "RecoveryProposed";
    pub const RECOVERY_WORKFLOW_CREATED: &str = "RecoveryWorkflowCreated";

    pub const EVALUATION_STARTED: &str = "EvaluationStarted";
    pub const EVALUATION_COMPLETED: &str = "EvaluationCompleted";
    pub const CONSENSUS_COMPUTED: &str = "ConsensusComputed";

    pub const CANDIDATE_STARTED: &str = "CandidateStarted";
    pub const CANDIDATE_COMPLETED: &str = "CandidateCompleted";
    pub const CANDIDATE_CONSENSUS_COMPUTED: &str = "CandidateConsensusComputed";
    pub const WINNER_SELECTED: &str = "WinnerSelected";

    pub const APPLY_PLANNED: &str = "ApplyPlanned";
    pub const APPLY_COMPLETED: &str = "ApplyCompleted";

    pub const WORKSPACE_ARCHIVED: &str = "WorkspaceArchived";
    pub const WORKSPACE_REMOVED: &str = "WorkspaceRemoved";

    pub const SYNTHETIC_SNAPSHOT: &str = "SyntheticSnapshot";
}

// ---------- Audit Redactor ----------

/// Centralized redactor that strips confidential credentials, environment variables,
/// authorization headers, tokens, and prohibited hidden reasoning/chain-of-thought traces.
pub struct AuditRedactor;

impl AuditRedactor {
    /// Case-insensitive key substrings that indicate confidential data.
    const SENSITIVE_KEY_PATTERNS: &'static [&'static str] = &[
        "token",
        "authorization",
        "api_key",
        "apikey",
        "secret",
        "password",
        "passwd",
        "cookie",
        "credential",
        "auth_header",
        "bearer",
        "private_key",
        "privkey",
    ];

    /// Prohibited reasoning/CoT fields.
    const REASONING_KEY_PATTERNS: &'static [&'static str] = &[
        "reasoning",
        "chain_of_thought",
        "cot",
        "hidden_thoughts",
        "thinking_trace",
        "model_trace",
        "hidden_reasoning",
    ];

    /// Check if a JSON key is sensitive.
    pub fn is_sensitive_key(key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        // Exact match on "env" or substring matches for patterns
        if lower == "env" || lower == "environment" {
            return true;
        }
        Self::SENSITIVE_KEY_PATTERNS
            .iter()
            .any(|pat| lower.contains(pat))
    }

    /// Check if a JSON key represents prohibited reasoning/chain-of-thought.
    pub fn is_reasoning_key(key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        Self::REASONING_KEY_PATTERNS
            .iter()
            .any(|pat| lower == *pat || lower.contains(pat))
    }

    /// Redact secrets and strip prohibited reasoning fields from a serde_json Value.
    pub fn redact_value(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut out = serde_json::Map::new();
                for (k, v) in map {
                    // Reject reasoning keys completely
                    if Self::is_reasoning_key(k) {
                        continue;
                    }
                    // Redact sensitive keys
                    if Self::is_sensitive_key(k) {
                        out.insert(
                            k.clone(),
                            serde_json::Value::String("[REDACTED]".to_string()),
                        );
                    } else {
                        out.insert(k.clone(), Self::redact_value(v));
                    }
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(arr) => {
                let out = arr.iter().map(Self::redact_value).collect();
                serde_json::Value::Array(out)
            }
            serde_json::Value::String(s) => {
                // Redact bearer tokens or basic auth strings if embedded
                if s.to_ascii_lowercase().starts_with("bearer ")
                    || s.to_ascii_lowercase().starts_with("basic ")
                {
                    serde_json::Value::String("[REDACTED_AUTH_HEADER]".to_string())
                } else {
                    serde_json::Value::String(s.clone())
                }
            }
            other => other.clone(),
        }
    }
}

// ---------- Canonical Serialization & Hashing ----------

/// Recursively sorts all JSON object keys deterministically into a BTreeMap.
pub fn canonicalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.clone(), canonicalize_json(v));
            }
            serde_json::to_value(sorted).unwrap_or_else(|_| serde_json::Value::Object(map.clone()))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<serde_json::Value> = arr.iter().map(canonicalize_json).collect();
            serde_json::Value::Array(items)
        }
        other => other.clone(),
    }
}

/// Serializes a JSON value to its deterministic canonical string representation.
pub fn canonical_json_string(value: &serde_json::Value) -> String {
    let canonical = canonicalize_json(value);
    serde_json::to_string(&canonical).unwrap_or_default()
}

/// Computes SHA-256 hex digest of a byte slice.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Computes the canonical payload string and its SHA-256 payload hash.
pub fn compute_payload_hash(payload: &serde_json::Value) -> (String, String) {
    let redacted = AuditRedactor::redact_value(payload);
    let canonical_str = canonical_json_string(&redacted);
    let hash = sha256_hex(canonical_str.as_bytes());
    (canonical_str, hash)
}

/// Computes the tamper-evident event hash across previous hash, canonical event fields, and payload hash.
#[allow(clippy::too_many_arguments)]
pub fn compute_event_hash(
    previous_hash: Option<&str>,
    workflow_id: Option<&str>,
    sequence: i64,
    event_type: &str,
    entity_type: &str,
    entity_id: &str,
    actor_type: &str,
    actor_id: Option<&str>,
    payload_hash: &str,
) -> String {
    let raw = format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}",
        previous_hash.unwrap_or("GENESIS"),
        workflow_id.unwrap_or("NONE"),
        sequence,
        event_type,
        entity_type,
        entity_id,
        actor_type,
        actor_id.unwrap_or("NONE"),
        payload_hash
    );
    sha256_hex(raw.as_bytes())
}

// ---------- Typed Provenance Payloads ----------

/// Execution policy snapshot recorded at decision/execution time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicySnapshot {
    pub max_nodes: usize,
    pub max_agent_calls: usize,
    pub max_parallel: usize,
    pub max_review_rounds: usize,
    pub max_replan_rounds: usize,
    pub max_recovery_attempts: usize,
    pub max_candidates: usize,
    pub max_evaluators: usize,
}

impl Default for PolicySnapshot {
    fn default() -> Self {
        Self {
            max_nodes: 50,
            max_agent_calls: 100,
            max_parallel: 8,
            max_review_rounds: 3,
            max_replan_rounds: 2,
            max_recovery_attempts: 1,
            max_candidates: 3,
            max_evaluators: 3,
        }
    }
}

/// Metadata reference to an artifact without storing full raw content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub size_bytes: Option<i64>,
    pub hash: Option<String>,
}

/// Candidate ranking entry in selection decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateRankingEntry {
    pub candidate_id: String,
    pub agent_id: String,
    pub is_approved: bool,
    pub approved_count: usize,
    pub valid_count: usize,
    pub issue_count: usize,
}

/// Deterministic selection ranking (Phase 23 & Phase 24):
/// 1. Approved only (filtered)
/// 2. approved_count DESC
/// 3. valid_count DESC
/// 4. aggregated_issue_count ASC
/// 5. candidate_id lexical ASC
pub fn rank_candidates(candidates: &[CandidateRankingEntry]) -> Vec<CandidateRankingEntry> {
    let mut eligible: Vec<CandidateRankingEntry> = candidates
        .iter()
        .filter(|c| c.is_approved)
        .cloned()
        .collect();
    eligible.sort_by(|a, b| {
        b.approved_count
            .cmp(&a.approved_count)
            .then_with(|| b.valid_count.cmp(&a.valid_count))
            .then_with(|| a.issue_count.cmp(&b.issue_count))
            .then_with(|| a.candidate_id.cmp(&b.candidate_id))
    });
    eligible
}

// Specific Payloads

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanGeneratedPayload {
    pub plan_id: Uuid,
    pub revision: i64,
    pub goal: String,
    pub node_count: usize,
    pub policy: PolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanExecutedPayload {
    pub plan_id: Uuid,
    pub revision: i64,
    pub workflow_id: Uuid,
    pub node_count: usize,
    pub policy: PolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowStartedPayload {
    pub workflow_id: Uuid,
    pub preset: String,
    pub goal: String,
    pub source_workspace: Option<String>,
    pub base_revision: Option<String>,
    pub policy: PolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowResumedPayload {
    pub workflow_id: Uuid,
    pub from_status: String,
    pub resumed_nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowCancelledPayload {
    pub workflow_id: Uuid,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowCompletedPayload {
    pub workflow_id: Uuid,
    pub final_review_verdict: Option<String>,
    pub winner_candidate_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowFailedPayload {
    pub workflow_id: Uuid,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeStartedPayload {
    pub node_id: String,
    pub role: String,
    pub intent: String,
    pub agent_id: String,
    pub session_lane: String,
    pub workspace_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeCompletedPayload {
    pub node_id: String,
    pub role: String,
    pub agent_id: String,
    pub task_id: Option<Uuid>,
    pub summary: Option<String>,
    pub snapshot_hash: Option<String>,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeFailedPayload {
    pub node_id: String,
    pub role: String,
    pub agent_id: Option<String>,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplanProposedPayload {
    pub workflow_id: Uuid,
    pub base_graph_revision: i64,
    pub new_graph_revision: i64,
    pub reason: String,
    pub added_nodes: Vec<String>,
    pub removed_nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplanAppliedPayload {
    pub workflow_id: Uuid,
    pub graph_revision: i64,
    pub node_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecoveryProposedPayload {
    pub workflow_id: Uuid,
    pub failed_node_id: String,
    pub attempt: usize,
    pub strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecoveryWorkflowCreatedPayload {
    pub parent_workflow_id: Uuid,
    pub child_workflow_id: Uuid,
    pub recovery_of_node_id: String,
    pub attempt: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationStartedPayload {
    pub group_id: Uuid,
    pub workflow_id: Uuid,
    pub candidate_id: Option<String>,
    pub round: usize,
    pub strategy: String,
    pub quorum: usize,
    pub evaluators: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationCompletedPayload {
    pub member_id: Uuid,
    pub group_id: Uuid,
    pub node_id: String,
    pub agent_id: String,
    pub verdict: String,
    pub confidence: Option<f64>,
    pub issue_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsensusComputedPayload {
    pub group_id: Uuid,
    pub workflow_id: Uuid,
    pub candidate_id: Option<String>,
    pub round: usize,
    pub outcome: String,
    pub approved_count: usize,
    pub changes_requested_count: usize,
    pub total_issues: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandidateStartedPayload {
    pub group_id: Uuid,
    pub candidate_id: String,
    pub agent_id: String,
    pub session_lane: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandidateCompletedPayload {
    pub group_id: Uuid,
    pub candidate_id: String,
    pub agent_id: String,
    pub task_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub snapshot_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandidateConsensusComputedPayload {
    pub group_id: Uuid,
    pub candidate_id: String,
    pub outcome: String,
    pub approved_count: usize,
    pub valid_count: usize,
    pub issue_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WinnerSelectedPayload {
    pub group_id: Uuid,
    pub workflow_id: Uuid,
    pub winner_candidate_id: String,
    pub winner_agent_id: String,
    pub winner_task_id: Option<Uuid>,
    pub winner_workspace_id: Option<Uuid>,
    pub winner_snapshot_hash: Option<String>,
    pub candidate_rankings: Vec<CandidateRankingEntry>,
    pub selection_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplyPlannedPayload {
    pub apply_id: Uuid,
    pub workflow_id: Option<Uuid>,
    pub source_workspace: String,
    pub source_task_id: Option<Uuid>,
    pub snapshot_hash: Option<String>,
    pub target_branch: String,
    pub base_revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplyCompletedPayload {
    pub apply_id: Uuid,
    pub workflow_id: Option<Uuid>,
    pub source_workspace_id: Option<Uuid>,
    pub applied_commit: Option<String>,
    pub applied_files_count: usize,
    pub snapshot_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkspaceArchivedPayload {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkspaceRemovedPayload {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyntheticSnapshotPayload {
    pub workflow_id: Uuid,
    pub synthetic: bool,
    pub schema_version: u32,
    pub note: String,
}

// ---------- Full Provenance Event DTO ----------

/// Fully resolved provenance event object for audit views, exports, and integrity verification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProvenanceEvent {
    pub id: Uuid,
    pub workflow_id: Option<Uuid>,
    pub sequence: i64,
    pub event_type: String,
    pub entity_type: String,
    pub entity_id: String,
    pub parent_event_id: Option<Uuid>,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub payload: serde_json::Value,
    pub payload_hash: String,
    pub previous_hash: Option<String>,
    pub event_hash: String,
    pub created_at: String,
    pub schema_version: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redactor_masks_tokens_secrets_and_env() {
        let raw = serde_json::json!({
            "agent_id": "claude",
            "api_token": "secret-12345",
            "Authorization": "Bearer super-secret-jwt",
            "nested": {
                "credential_value": "pass123",
                "ENV": "production_key",
                "safe_key": "safe_value"
            }
        });

        let redacted = AuditRedactor::redact_value(&raw);
        assert_eq!(redacted["agent_id"], "claude");
        assert_eq!(redacted["api_token"], "[REDACTED]");
        assert_eq!(redacted["Authorization"], "[REDACTED]");
        assert_eq!(redacted["nested"]["credential_value"], "[REDACTED]");
        assert_eq!(redacted["nested"]["ENV"], "[REDACTED]");
        assert_eq!(redacted["nested"]["safe_key"], "safe_value");
    }

    #[test]
    fn redactor_strips_reasoning_and_chain_of_thought() {
        let raw = serde_json::json!({
            "verdict": "approved",
            "summary": "all tests pass",
            "reasoning": "I thought about step 1 then step 2...",
            "chain_of_thought": "hidden thoughts...",
            "nested": {
                "thinking_trace": "internal model tokens",
                "issue_count": 0
            }
        });

        let redacted = AuditRedactor::redact_value(&raw);
        assert_eq!(redacted["verdict"], "approved");
        assert_eq!(redacted["summary"], "all tests pass");
        assert!(redacted.get("reasoning").is_none());
        assert!(redacted.get("chain_of_thought").is_none());
        assert!(redacted["nested"].get("thinking_trace").is_none());
        assert_eq!(redacted["nested"]["issue_count"], 0);
    }

    #[test]
    fn canonical_json_and_hash_deterministic_across_key_orders() {
        let v1 = serde_json::json!({
            "z_key": 1,
            "a_key": "first",
            "m_key": [2, 1]
        });

        let v2 = serde_json::json!({
            "a_key": "first",
            "m_key": [2, 1],
            "z_key": 1
        });

        let (c1, h1) = compute_payload_hash(&v1);
        let (c2, h2) = compute_payload_hash(&v2);

        assert_eq!(c1, c2);
        assert_eq!(h1, h2);
    }

    #[test]
    fn event_hash_chain_computes_and_verifies() {
        let w_id = Uuid::new_v4();
        let payload1 = serde_json::json!({"action": "start"});
        let (_, p_hash1) = compute_payload_hash(&payload1);
        let e_hash1 = compute_event_hash(
            None,
            Some(&w_id.to_string()),
            1,
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &w_id.to_string(),
            actor_type::SYSTEM,
            Some("Scheduler"),
            &p_hash1,
        );

        let payload2 = serde_json::json!({"node_id": "architect", "status": "completed"});
        let (_, p_hash2) = compute_payload_hash(&payload2);
        let e_hash2 = compute_event_hash(
            Some(&e_hash1),
            Some(&w_id.to_string()),
            2,
            event_type::NODE_COMPLETED,
            entity_type::NODE,
            "architect",
            actor_type::AGENT,
            Some("claude"),
            &p_hash2,
        );

        assert_ne!(e_hash1, e_hash2);
        assert!(!e_hash1.is_empty());
        assert!(!e_hash2.is_empty());
    }

    #[test]
    fn deterministic_candidate_ranking() {
        let candidates = vec![
            CandidateRankingEntry {
                candidate_id: "candidate_3".to_string(),
                agent_id: "claude".to_string(),
                is_approved: true,
                approved_count: 2,
                valid_count: 3,
                issue_count: 1,
            },
            CandidateRankingEntry {
                candidate_id: "candidate_1".to_string(),
                agent_id: "codex".to_string(),
                is_approved: true,
                approved_count: 3,
                valid_count: 3,
                issue_count: 0,
            },
            CandidateRankingEntry {
                candidate_id: "candidate_2".to_string(),
                agent_id: "opencode".to_string(),
                is_approved: false,
                approved_count: 1,
                valid_count: 3,
                issue_count: 4,
            },
        ];

        let ranked = rank_candidates(&candidates);

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].candidate_id, "candidate_1");
        assert_eq!(ranked[1].candidate_id, "candidate_3");
    }
}
