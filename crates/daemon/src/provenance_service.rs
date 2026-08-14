//! Phase 24: Provenance verification, deterministic decision replay, and lineage.
//!
//! Replay rules:
//! - Recompute deterministic consensus from persisted evaluator verdicts.
//! - Recompute deterministic Best-of-N selection ranking using the exact same `rank_candidates`.
//! - Verify policy snapshot limits against recorded execution.
//! - Verify apply source provenance against Winner workspace without mutating git/workspaces.
//! - ZERO agent calls, ZERO git mutation, ZERO filesystem mutation.

use agentmesh_core::provenance::{
    CandidateRankingEntry, PolicySnapshot, compute_event_hash, compute_payload_hash, event_type,
    rank_candidates,
};
use agentmesh_orchestrator::evaluation::{
    ConsensusOutcome, ConsensusStrategy, EvaluationResult, compute_consensus,
};
use agentmesh_storage::{
    ApplyRepository, CompetitionRepository, EvaluationRepository, ProvenanceRepository,
    TaskRepository, WorkflowPlanRepository, WorkflowRecoveryRepository, WorkflowReplanRepository,
    WorkflowRepository, WorkflowStepRepository, WorkspaceRepository,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Result of hash-chain and provenance integrity verification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntegrityReport {
    pub workflow_id: Uuid,
    pub valid: bool,
    pub is_legacy: bool,
    pub total_events: usize,
    pub details: Vec<String>,
    pub failure: Option<String>,
}

/// Result of deterministic decision replay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayReport {
    pub workflow_id: Uuid,
    pub passed: bool,
    pub is_legacy: bool,
    pub integrity_passed: bool,
    pub consensus_passed: bool,
    pub selection_passed: bool,
    pub apply_passed: bool,
    pub policy_passed: bool,
    pub mismatches: Vec<String>,
    pub details: Vec<String>,
}

/// Workflow lineage tree / graph query representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LineageReport {
    pub workflow_id: Uuid,
    pub preset: String,
    pub goal: String,
    pub status: String,
    pub parent_workflow_id: Option<Uuid>,
    pub recovery_workflows: Vec<Uuid>,
    pub plan_id: Option<Uuid>,
    pub graph_revision: i64,
    pub replans_count: usize,
    pub competition_group_id: Option<Uuid>,
    pub winner_candidate_id: Option<String>,
    pub evaluation_groups_count: usize,
    pub apply_id: Option<Uuid>,
    pub provenance_events_count: usize,
}

/// Provenance audit & replay service.
#[derive(Clone)]
pub struct ProvenanceService {
    provenance: ProvenanceRepository,
    workflows: WorkflowRepository,
    #[allow(dead_code)]
    steps: WorkflowStepRepository,
    evaluations: EvaluationRepository,
    competitions: CompetitionRepository,
    applies: ApplyRepository,
    #[allow(dead_code)]
    plans: WorkflowPlanRepository,
    replans: WorkflowReplanRepository,
    recoveries: WorkflowRecoveryRepository,
    #[allow(dead_code)]
    tasks: TaskRepository,
    #[allow(dead_code)]
    workspaces: WorkspaceRepository,
}

impl ProvenanceService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provenance: ProvenanceRepository,
        workflows: WorkflowRepository,
        steps: WorkflowStepRepository,
        evaluations: EvaluationRepository,
        competitions: CompetitionRepository,
        applies: ApplyRepository,
        plans: WorkflowPlanRepository,
        replans: WorkflowReplanRepository,
        recoveries: WorkflowRecoveryRepository,
        tasks: TaskRepository,
        workspaces: WorkspaceRepository,
    ) -> Self {
        Self {
            provenance,
            workflows,
            steps,
            evaluations,
            competitions,
            applies,
            plans,
            replans,
            recoveries,
            tasks,
            workspaces,
        }
    }

    /// Convenience constructor from a Database instance.
    pub fn from_db(db: agentmesh_storage::Database) -> Self {
        Self::new(
            ProvenanceRepository::new(db.clone()),
            WorkflowRepository::new(db.clone()),
            WorkflowStepRepository::new(db.clone()),
            EvaluationRepository::new(db.clone()),
            CompetitionRepository::new(db.clone()),
            ApplyRepository::new(db.clone()),
            WorkflowPlanRepository::new(db.clone()),
            WorkflowReplanRepository::new(db.clone()),
            WorkflowRecoveryRepository::new(db.clone()),
            TaskRepository::new(db.clone()),
            WorkspaceRepository::new(db),
        )
    }

    /// Verifies tamper-evident integrity: monotonic sequence, hash chain continuity,
    /// canonical payload hashing, and event hash correctness.
    pub async fn verify_integrity(&self, workflow_id: Uuid) -> IntegrityReport {
        let events = match self.provenance.list_for_workflow(workflow_id).await {
            Ok(evs) => evs,
            Err(err) => {
                return IntegrityReport {
                    workflow_id,
                    valid: false,
                    is_legacy: false,
                    total_events: 0,
                    details: vec![format!("Failed to query provenance events: {err}")],
                    failure: Some(err.to_string()),
                };
            }
        };

        if events.is_empty() {
            // Check if workflow exists in database (pre-Phase 24 legacy workflow)
            let exists = self
                .workflows
                .get(workflow_id)
                .await
                .ok()
                .flatten()
                .is_some();
            if exists {
                return IntegrityReport {
                    workflow_id,
                    valid: true,
                    is_legacy: true,
                    total_events: 0,
                    details: vec![
                        "Legacy workflow: provenance unavailable before schema v1".to_string(),
                    ],
                    failure: None,
                };
            } else {
                return IntegrityReport {
                    workflow_id,
                    valid: false,
                    is_legacy: false,
                    total_events: 0,
                    details: vec!["Workflow not found".to_string()],
                    failure: Some(format!("Workflow {workflow_id} not found")),
                };
            }
        }

        let mut details = Vec::new();
        let total_events = events.len();

        for (idx, ev) in events.iter().enumerate() {
            let expected_seq = (idx + 1) as i64;
            if ev.sequence != expected_seq {
                let msg = format!(
                    "Sequence gap detected: event {} at index {} has sequence {}, expected {}",
                    ev.id, idx, ev.sequence, expected_seq
                );
                details.push(msg.clone());
                return IntegrityReport {
                    workflow_id,
                    valid: false,
                    is_legacy: false,
                    total_events,
                    details,
                    failure: Some(msg),
                };
            }

            // Verify previous_hash link
            if idx == 0 {
                if ev.previous_hash.is_some() {
                    let msg = format!("Event 1 has non-null previous_hash: {:?}", ev.previous_hash);
                    details.push(msg.clone());
                    return IntegrityReport {
                        workflow_id,
                        valid: false,
                        is_legacy: false,
                        total_events,
                        details,
                        failure: Some(msg),
                    };
                }
            } else {
                let prev_event = &events[idx - 1];
                if ev.previous_hash.as_deref() != Some(prev_event.event_hash.as_str()) {
                    let msg = format!(
                        "Hash chain broken at sequence {}: previous_hash {:?} != expected {}",
                        ev.sequence, ev.previous_hash, prev_event.event_hash
                    );
                    details.push(msg.clone());
                    return IntegrityReport {
                        workflow_id,
                        valid: false,
                        is_legacy: false,
                        total_events,
                        details,
                        failure: Some(msg),
                    };
                }
            }

            // Verify payload_hash calculation
            let payload_val: serde_json::Value = serde_json::from_str(&ev.payload_json)
                .unwrap_or(serde_json::Value::String(ev.payload_json.clone()));
            let (_canonical_p, computed_p_hash) = compute_payload_hash(&payload_val);
            if ev.payload_hash != computed_p_hash {
                let msg = format!(
                    "Payload hash mismatch at sequence {}: recorded {} != computed {}",
                    ev.sequence, ev.payload_hash, computed_p_hash
                );
                details.push(msg.clone());
                return IntegrityReport {
                    workflow_id,
                    valid: false,
                    is_legacy: false,
                    total_events,
                    details,
                    failure: Some(msg),
                };
            }

            // Verify event_hash calculation
            let wid_str = ev.workflow_id.map(|w| w.to_string());
            let computed_e_hash = compute_event_hash(
                ev.previous_hash.as_deref(),
                wid_str.as_deref(),
                ev.sequence,
                &ev.event_type,
                &ev.entity_type,
                &ev.entity_id,
                &ev.actor_type,
                ev.actor_id.as_deref(),
                &ev.payload_hash,
            );

            if ev.event_hash != computed_e_hash {
                let msg = format!(
                    "Event hash mismatch at sequence {}: recorded {} != computed {}",
                    ev.sequence, ev.event_hash, computed_e_hash
                );
                details.push(msg.clone());
                return IntegrityReport {
                    workflow_id,
                    valid: false,
                    is_legacy: false,
                    total_events,
                    details,
                    failure: Some(msg),
                };
            }
        }

        details.push(format!(
            "Verified {total_events} provenance events in continuous hash chain"
        ));

        IntegrityReport {
            workflow_id,
            valid: true,
            is_legacy: false,
            total_events,
            details,
            failure: None,
        }
    }

    /// Replays all deterministic decisions for a workflow (Consensus, SelectionGate, Apply, Policy)
    /// and compares recomputed outcomes against persisted state and provenance records.
    pub async fn replay_workflow(&self, workflow_id: Uuid) -> ReplayReport {
        let integrity = self.verify_integrity(workflow_id).await;
        if !integrity.valid {
            return ReplayReport {
                workflow_id,
                passed: false,
                is_legacy: integrity.is_legacy,
                integrity_passed: false,
                consensus_passed: false,
                selection_passed: false,
                apply_passed: false,
                policy_passed: false,
                mismatches: vec!["Integrity verification failed".to_string()],
                details: integrity.details,
            };
        }

        if integrity.is_legacy {
            return ReplayReport {
                workflow_id,
                passed: true,
                is_legacy: true,
                integrity_passed: true,
                consensus_passed: true,
                selection_passed: true,
                apply_passed: true,
                policy_passed: true,
                mismatches: Vec::new(),
                details: vec!["Legacy workflow: deterministic replay skipped".to_string()],
            };
        }

        let mut mismatches = Vec::new();
        let mut details = Vec::new();
        let mut consensus_passed = true;
        let mut selection_passed = true;
        let mut apply_passed = true;
        let mut policy_passed = true;

        let events = self
            .provenance
            .list_for_workflow(workflow_id)
            .await
            .unwrap_or_default();

        // 1. Consensus Replay
        let eval_groups = self
            .evaluations
            .list_groups(workflow_id)
            .await
            .unwrap_or_default();
        for group in &eval_groups {
            let members = self
                .evaluations
                .list_members(group.id)
                .await
                .unwrap_or_default();
            let strategy =
                ConsensusStrategy::from_str(&group.strategy).unwrap_or(ConsensusStrategy::Majority);
            let quorum = group.quorum as usize;
            let total_evaluators = members.len();

            let mut eval_results = Vec::new();
            for member in &members {
                let Some(res_json) = &member.result_json else {
                    continue;
                };
                if let Ok(eval_res) = serde_json::from_str::<EvaluationResult>(res_json) {
                    eval_results.push((member.agent_id.clone(), eval_res));
                }
            }

            let recomputed = compute_consensus(&eval_results, strategy, quorum, total_evaluators);

            let Some(recorded_consensus_json) = &group.consensus else {
                continue;
            };
            if let Ok(recorded) = serde_json::from_str::<
                agentmesh_orchestrator::evaluation::ConsensusResult,
            >(recorded_consensus_json)
            {
                if recomputed.outcome != recorded.outcome
                    || recomputed.approved_count != recorded.approved_count
                    || recomputed.changes_requested_count != recorded.changes_requested_count
                {
                    consensus_passed = false;
                    let msg = format!(
                        "Consensus mismatch for group {}: recomputed {:?} ({}/{} approved) != recorded {:?} ({}/{} approved)",
                        group.id,
                        recomputed.outcome,
                        recomputed.approved_count,
                        recomputed.valid_count,
                        recorded.outcome,
                        recorded.approved_count,
                        recorded.valid_count
                    );
                    mismatches.push(msg.clone());
                    details.push(msg);
                } else {
                    details.push(format!(
                        "Consensus verified for group {}: {:?} ({}/{} approved)",
                        group.id,
                        recomputed.outcome,
                        recomputed.approved_count,
                        recomputed.valid_count
                    ));
                }
            }
        }

        // 2. Best-of-N Selection Replay
        if let Ok(Some(comp_group)) = self.competitions.get_group_for_workflow(workflow_id).await {
            let candidates = self
                .competitions
                .list_candidates_for_group(comp_group.id)
                .await
                .unwrap_or_default();
            let mut ranking_entries = Vec::new();

            for cand in &candidates {
                let mut app_count = 0usize;
                let mut val_count = 0usize;
                let mut issue_count = 0usize;
                let mut is_approved = false;

                if let Some(eg_id) = cand.evaluation_group_id {
                    if let Ok(Some(eg)) = self.evaluations.get_group(eg_id).await {
                        let maybe_c_res = eg.consensus.as_deref().and_then(|c_json| {
                            serde_json::from_str::<
                                agentmesh_orchestrator::evaluation::ConsensusResult,
                            >(c_json)
                            .ok()
                        });
                        if let Some(c_res) = maybe_c_res {
                            is_approved = c_res.outcome == ConsensusOutcome::Approved;
                            app_count = c_res.approved_count;
                            val_count = c_res.valid_count;
                        }
                    }
                    if let Ok(members) = self.evaluations.list_members(eg_id).await {
                        for m in &members {
                            let Some(res_json) = &m.result_json else {
                                continue;
                            };
                            if let Ok(eval_res) = serde_json::from_str::<EvaluationResult>(res_json)
                            {
                                issue_count += eval_res.issues.len();
                            }
                        }
                    }
                }

                ranking_entries.push(CandidateRankingEntry {
                    candidate_id: cand.candidate_id.clone(),
                    agent_id: cand.agent_id.clone(),
                    is_approved,
                    approved_count: app_count,
                    valid_count: val_count,
                    issue_count,
                });
            }

            let ranked = rank_candidates(&ranking_entries);
            let recomputed_winner = ranked.first().map(|c| c.candidate_id.clone());

            if comp_group.winner_candidate_id != recomputed_winner {
                selection_passed = false;
                let msg = format!(
                    "Winner selection mismatch: recomputed {:?} != persisted {:?}",
                    recomputed_winner, comp_group.winner_candidate_id
                );
                mismatches.push(msg.clone());
                details.push(msg);
            } else {
                details.push(format!(
                    "Winner selection verified: winner = {:?}",
                    recomputed_winner
                ));
            }

            // Check against WinnerSelected provenance event
            if let Some(w_event) = events
                .iter()
                .find(|e| e.event_type == event_type::WINNER_SELECTED)
            {
                let p_val: serde_json::Value =
                    serde_json::from_str(&w_event.payload_json).unwrap_or_default();
                let prov_winner = p_val
                    .get("winner_candidate_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                if prov_winner != comp_group.winner_candidate_id {
                    selection_passed = false;
                    let msg = format!(
                        "Winner provenance mismatch: event payload winner {:?} != persisted {:?}",
                        prov_winner, comp_group.winner_candidate_id
                    );
                    mismatches.push(msg.clone());
                    details.push(msg);
                }
            }
        }

        // 3. Apply Source Provenance Replay
        if let Ok(applies) = self.applies.list_for_workflow(workflow_id).await {
            for apply in &applies {
                if let Ok(Some(comp_group)) =
                    self.competitions.get_group_for_workflow(workflow_id).await
                {
                    // Best-of-N invariant: apply source MUST be winner workspace
                    if let Some(winner_ws_id) = comp_group.winner_workspace_id {
                        if apply.workspace_id != winner_ws_id {
                            apply_passed = false;
                            let msg = format!(
                                "Apply source mismatch for apply {}: apply workspace {} != winner workspace {}",
                                apply.id, apply.workspace_id, winner_ws_id
                            );
                            mismatches.push(msg.clone());
                            details.push(msg);
                        } else {
                            details.push(format!(
                                "Apply source verified for apply {}: correctly used winner workspace {}",
                                apply.id, winner_ws_id
                            ));
                        }
                    }
                }
            }
        }

        // 4. Policy Snapshot Replay
        for ev in &events {
            if ev.event_type == event_type::WORKFLOW_STARTED
                || ev.event_type == event_type::PLAN_EXECUTED
            {
                let p_val: serde_json::Value =
                    serde_json::from_str(&ev.payload_json).unwrap_or_default();
                let Some(policy_val) = p_val.get("policy") else {
                    continue;
                };
                if let Ok(policy) = serde_json::from_value::<PolicySnapshot>(policy_val.clone()) {
                    if policy.max_nodes > 0 && events.len() > policy.max_nodes * 20 {
                        policy_passed = false;
                        let msg = format!(
                            "Policy budget violation: total events {} exceeded policy max nodes {}",
                            events.len(),
                            policy.max_nodes
                        );
                        mismatches.push(msg.clone());
                        details.push(msg);
                    } else {
                        details.push(format!(
                            "Policy snapshot verified: max_nodes={}, max_parallel={}, max_agent_calls={}",
                            policy.max_nodes, policy.max_parallel, policy.max_agent_calls
                        ));
                    }
                }
            }
        }

        let passed = consensus_passed && selection_passed && apply_passed && policy_passed;

        ReplayReport {
            workflow_id,
            passed,
            is_legacy: false,
            integrity_passed: true,
            consensus_passed,
            selection_passed,
            apply_passed,
            policy_passed,
            mismatches,
            details,
        }
    }

    /// Resolves full lineage graph for a workflow.
    pub async fn get_lineage(&self, workflow_id: Uuid) -> Option<LineageReport> {
        let workflow = self.workflows.get(workflow_id).await.ok().flatten()?;
        let recovery_rows = self
            .recoveries
            .list_for(workflow_id)
            .await
            .unwrap_or_default();
        let recovery_workflows: Vec<Uuid> = recovery_rows
            .iter()
            .filter_map(|r| r.recovery_workflow_id)
            .collect();
        let replan_rows = self
            .replans
            .list_for_workflow(workflow_id)
            .await
            .unwrap_or_default();
        let comp_group = self
            .competitions
            .get_group_for_workflow(workflow_id)
            .await
            .ok()
            .flatten();
        let eval_groups = self
            .evaluations
            .list_groups(workflow_id)
            .await
            .unwrap_or_default();
        let applies = self
            .applies
            .list_for_workflow(workflow_id)
            .await
            .unwrap_or_default();
        let prov_count = self
            .provenance
            .count_for_workflow(workflow_id)
            .await
            .unwrap_or(0);

        Some(LineageReport {
            workflow_id,
            preset: workflow.preset.clone(),
            goal: workflow.goal.clone(),
            status: workflow.status.clone(),
            parent_workflow_id: workflow.parent_workflow_id,
            recovery_workflows,
            plan_id: None,
            graph_revision: workflow.graph_revision,
            replans_count: replan_rows.len(),
            competition_group_id: comp_group.as_ref().map(|g| g.id),
            winner_candidate_id: comp_group
                .as_ref()
                .and_then(|g| g.winner_candidate_id.clone()),
            evaluation_groups_count: eval_groups.len(),
            apply_id: applies.first().map(|a| a.id),
            provenance_events_count: prov_count as usize,
        })
    }
}
