//! Integration tests for Phase 24: Provenance Ledger, Decision Audit & Deterministic Replay.

use agentmesh_core::provenance::{
    CandidateRankingEntry, ConsensusComputedPayload, EvaluationCompletedPayload,
    WinnerSelectedPayload, WorkflowCompletedPayload, WorkflowStartedPayload, actor_type,
    entity_type, event_type,
};
use agentmesh_daemon::provenance_service::ProvenanceService;
use agentmesh_orchestrator::WorkflowStatus;
use agentmesh_orchestrator::evaluation::{ConsensusOutcome, ConsensusResult, ConsensusStrategy};
use agentmesh_storage::{
    ApplyRepository, CompetitionCandidateRow, CompetitionRepository, Database,
    EvaluationRepository, ProvenanceRepository, TaskRepository, WorkflowPlanRepository,
    WorkflowRecoveryRepository, WorkflowReplanRepository, WorkflowRepository,
    WorkflowStepRepository, WorkspaceRepository,
};
use uuid::Uuid;

async fn test_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db");
    let db = Database::open(&path).await.expect("connect");
    (db, dir)
}

fn make_provenance_service(db: &Database) -> ProvenanceService {
    ProvenanceService::new(
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
        WorkspaceRepository::new(db.clone()),
    )
}

#[tokio::test]
async fn test_provenance_hash_chain_integrity() {
    let (db, _dir) = test_db().await;
    let prov_repo = ProvenanceRepository::new(db.clone());
    let service = make_provenance_service(&db);

    let workflow_id = Uuid::new_v4();
    let wf_repo = WorkflowRepository::new(db.clone());
    let now = chrono::Utc::now().to_rfc3339();
    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: workflow_id,
            preset: "consensus-review".into(),
            goal: "build a feature".into(),
            status: WorkflowStatus::Completed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create workflow");

    // Event 1: WorkflowStarted
    let start_payload = serde_json::to_value(WorkflowStartedPayload {
        workflow_id,
        preset: "consensus-review".into(),
        goal: "build a feature".into(),
        source_workspace: None,
        base_revision: None,
        policy: Default::default(),
    })
    .unwrap();
    let ev1 = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &workflow_id.to_string(),
            None,
            actor_type::SYSTEM,
            Some("WorkflowService"),
            &start_payload,
        )
        .await
        .expect("ev1");

    assert_eq!(ev1.sequence, 1);
    assert!(ev1.previous_hash.is_none());

    // Event 2: EvaluationCompleted
    let eval_payload = serde_json::to_value(EvaluationCompletedPayload {
        member_id: Uuid::new_v4(),
        group_id: Uuid::new_v4(),
        node_id: "evaluator_1".into(),
        agent_id: "claude".into(),
        verdict: "approved".into(),
        confidence: Some(0.95),
        issue_count: 0,
    })
    .unwrap();
    let ev2 = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::EVALUATION_COMPLETED,
            entity_type::EVALUATION_MEMBER,
            "evaluator_1",
            None,
            actor_type::AGENT,
            Some("claude"),
            &eval_payload,
        )
        .await
        .expect("ev2");

    assert_eq!(ev2.sequence, 2);
    assert_eq!(ev2.previous_hash, Some(ev1.event_hash.clone()));

    // Event 3: WorkflowCompleted
    let term_payload = serde_json::to_value(WorkflowCompletedPayload {
        workflow_id,
        final_review_verdict: Some("approved".into()),
        winner_candidate_id: None,
    })
    .unwrap();
    let ev3 = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::WORKFLOW_COMPLETED,
            entity_type::WORKFLOW,
            &workflow_id.to_string(),
            None,
            actor_type::SYSTEM,
            Some("WorkflowService"),
            &term_payload,
        )
        .await
        .expect("ev3");

    assert_eq!(ev3.sequence, 3);
    assert_eq!(ev3.previous_hash, Some(ev2.event_hash.clone()));

    // Verify integrity
    let report = service.verify_integrity(workflow_id).await;
    assert!(report.valid);
    assert_eq!(report.total_events, 3);
    assert!(!report.is_legacy);
    assert!(report.failure.is_none());
}

#[tokio::test]
async fn test_provenance_tampering_detection() {
    let (db, _dir) = test_db().await;
    let prov_repo = ProvenanceRepository::new(db.clone());
    let service = make_provenance_service(&db);

    let workflow_id = Uuid::new_v4();
    let wf_repo = WorkflowRepository::new(db.clone());
    let now = chrono::Utc::now().to_rfc3339();
    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: workflow_id,
            preset: "plan".into(),
            goal: "tamper test".into(),
            status: WorkflowStatus::Completed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create workflow");

    let p1 = serde_json::json!({"action": "step1"});
    let ev1 = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &workflow_id.to_string(),
            None,
            actor_type::SYSTEM,
            None,
            &p1,
        )
        .await
        .expect("ev1");

    let p2 = serde_json::json!({"action": "step2"});
    let _ev2 = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::NODE_COMPLETED,
            entity_type::NODE,
            "step2",
            None,
            actor_type::AGENT,
            None,
            &p2,
        )
        .await
        .expect("ev2");

    // Tamper directly with the SQLite row of event 1
    sqlx::query("UPDATE provenance_events SET payload_json = ? WHERE id = ?")
        .bind("{\"action\":\"malicious_injection\"}")
        .bind(ev1.id.to_string())
        .execute(db.pool())
        .await
        .expect("tamper sql");

    // Verification must detect the tampering
    let report = service.verify_integrity(workflow_id).await;
    assert!(!report.valid);
    assert!(report.failure.is_some());
    assert!(
        report
            .failure
            .as_ref()
            .unwrap()
            .contains("Payload hash mismatch")
    );
}

#[tokio::test]
async fn test_provenance_secret_and_reasoning_redaction() {
    let (db, _dir) = test_db().await;
    let prov_repo = ProvenanceRepository::new(db.clone());

    let sensitive_payload = serde_json::json!({
        "api_key": "sk-ant-api03-secretkey12345",
        "Authorization": "Bearer super-secret-jwt-token",
        "cookie": "session=secretcookie987",
        "reasoning": "This is private internal chain-of-thought that should never be audited",
        "chain_of_thought": "hidden reasoning trace",
        "agent_name": "claude-3-7-sonnet",
        "verdict": "approved"
    });

    let ev = prov_repo
        .append_event(
            None,
            event_type::EVALUATION_COMPLETED,
            entity_type::EVALUATION_MEMBER,
            "eval_1",
            None,
            actor_type::AGENT,
            Some("claude"),
            &sensitive_payload,
        )
        .await
        .expect("append event");

    let parsed: serde_json::Value = serde_json::from_str(&ev.payload_json).unwrap();
    // Secrets must be redacted
    assert_eq!(parsed["api_key"], "[REDACTED]");
    assert_eq!(parsed["Authorization"], "[REDACTED]");
    assert_eq!(parsed["cookie"], "[REDACTED]");
    // Prohibited reasoning must be completely stripped
    assert!(parsed.get("reasoning").is_none());
    assert!(parsed.get("chain_of_thought").is_none());
    // Safe fields must remain intact
    assert_eq!(parsed["agent_name"], "claude-3-7-sonnet");
    assert_eq!(parsed["verdict"], "approved");
}

#[tokio::test]
async fn test_deterministic_consensus_replay() {
    let (db, _dir) = test_db().await;
    let service = make_provenance_service(&db);
    let prov_repo = ProvenanceRepository::new(db.clone());
    let eval_repo = EvaluationRepository::new(db.clone());
    let wf_repo = WorkflowRepository::new(db.clone());

    let workflow_id = Uuid::new_v4();
    let now = chrono::Utc::now().to_rfc3339();
    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: workflow_id,
            preset: "consensus-review".into(),
            goal: "consensus replay test".into(),
            status: WorkflowStatus::Completed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create workflow");

    // Evaluation Group
    let group_id = Uuid::new_v4();
    eval_repo
        .create_group(&agentmesh_storage::EvaluationGroupRow {
            id: group_id,
            workflow_id,
            source_task_id: None,
            strategy: ConsensusStrategy::Majority.as_str().into(),
            quorum: 2,
            status: "completed".into(),
            consensus: Some(
                serde_json::to_string(&ConsensusResult {
                    outcome: ConsensusOutcome::Approved,
                    strategy: ConsensusStrategy::Majority,
                    quorum: 2,
                    valid_count: 3,
                    total_count: 3,
                    approved_count: 2,
                    changes_requested_count: 1,
                    issues: Vec::new(),
                })
                .unwrap(),
            ),
            snapshot_hash: Some("hash123".into()),
            round: 0,
            created_at: now.clone(),
            completed_at: Some(now.clone()),
        })
        .await
        .expect("create group");

    // 3 Members: 2 approved, 1 changes_requested
    for (i, (verdict, agent)) in [
        ("approved", "claude"),
        ("approved", "codex"),
        ("changes_requested", "opencode"),
    ]
    .iter()
    .enumerate()
    {
        let member_id = Uuid::new_v4();
        let eval_res = agentmesh_orchestrator::evaluation::EvaluationResult {
            verdict: if *verdict == "approved" {
                agentmesh_orchestrator::ReviewVerdict::Approved
            } else {
                agentmesh_orchestrator::ReviewVerdict::ChangesRequested
            },
            confidence: Some(0.9),
            summary: format!("Member {i}"),
            issues: Vec::new(),
        };
        eval_repo
            .create_member(&agentmesh_storage::EvaluationMemberRow {
                id: member_id,
                group_id,
                node_id: format!("evaluator_{}", i + 1),
                agent_id: agent.to_string(),
                task_id: None,
                status: "completed".into(),
                result_json: Some(serde_json::to_string(&eval_res).unwrap()),
                error: None,
                created_at: now.clone(),
                completed_at: Some(now.clone()),
            })
            .await
            .expect("create member");
    }

    // Record ConsensusComputed Provenance event
    let consensus_payload = serde_json::to_value(ConsensusComputedPayload {
        group_id,
        workflow_id,
        candidate_id: None,
        round: 0,
        outcome: ConsensusOutcome::Approved.as_str().into(),
        approved_count: 2,
        changes_requested_count: 1,
        total_issues: 0,
    })
    .unwrap();

    let _ = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::CONSENSUS_COMPUTED,
            entity_type::EVALUATION_GROUP,
            &group_id.to_string(),
            None,
            actor_type::SYSTEM,
            Some("ConsensusGate"),
            &consensus_payload,
        )
        .await
        .expect("append consensus");

    // Run deterministic replay
    let replay_report = service.replay_workflow(workflow_id).await;
    assert!(replay_report.passed);
    assert!(replay_report.consensus_passed);
    assert!(replay_report.mismatches.is_empty());
}

#[tokio::test]
async fn test_deterministic_best_of_n_selection_replay() {
    let (db, _dir) = test_db().await;
    let service = make_provenance_service(&db);
    let prov_repo = ProvenanceRepository::new(db.clone());
    let comp_repo = CompetitionRepository::new(db.clone());
    let eval_repo = EvaluationRepository::new(db.clone());
    let wf_repo = WorkflowRepository::new(db.clone());

    let workflow_id = Uuid::new_v4();
    let now = chrono::Utc::now().to_rfc3339();
    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: workflow_id,
            preset: "best-of-n".into(),
            goal: "best of n replay test".into(),
            status: WorkflowStatus::Completed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create workflow");

    let comp_group_id = Uuid::new_v4();
    comp_repo
        .create_group(&agentmesh_storage::CompetitionGroupRow {
            id: comp_group_id,
            workflow_id,
            source_workspace: None,
            base_revision: "HEAD".into(),
            candidate_count: 2,
            status: "completed".into(),
            winner_candidate_id: Some("candidate_1".into()),
            winner_task_id: None,
            winner_workspace_id: None,
            winner_snapshot_hash: Some("cand1_hash".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .await
        .expect("create comp group");

    // Candidate 1 (Winner)
    let cand1_eval_group = Uuid::new_v4();
    eval_repo
        .create_group(&agentmesh_storage::EvaluationGroupRow {
            id: cand1_eval_group,
            workflow_id,
            source_task_id: None,
            strategy: ConsensusStrategy::Majority.as_str().into(),
            quorum: 2,
            status: "completed".into(),
            consensus: Some(
                serde_json::to_string(&ConsensusResult {
                    outcome: ConsensusOutcome::Approved,
                    strategy: ConsensusStrategy::Majority,
                    quorum: 2,
                    valid_count: 3,
                    total_count: 3,
                    approved_count: 3,
                    changes_requested_count: 0,
                    issues: Vec::new(),
                })
                .unwrap(),
            ),
            snapshot_hash: Some("cand1_hash".into()),
            round: 0,
            created_at: now.clone(),
            completed_at: Some(now.clone()),
        })
        .await
        .expect("cand1 eval group");

    for i in 1..=3 {
        let eval_res = agentmesh_orchestrator::evaluation::EvaluationResult {
            verdict: agentmesh_orchestrator::ReviewVerdict::Approved,
            confidence: Some(0.95),
            summary: format!("Eval {i}"),
            issues: Vec::new(),
        };
        eval_repo
            .create_member(&agentmesh_storage::EvaluationMemberRow {
                id: Uuid::new_v4(),
                group_id: cand1_eval_group,
                node_id: format!("eval_{i}"),
                agent_id: format!("evaluator_{i}"),
                task_id: None,
                status: "completed".into(),
                result_json: Some(serde_json::to_string(&eval_res).unwrap()),
                error: None,
                created_at: now.clone(),
                completed_at: Some(now.clone()),
            })
            .await
            .expect("create member cand1");
    }

    comp_repo
        .create_candidate(&CompetitionCandidateRow {
            id: Uuid::new_v4(),
            group_id: comp_group_id,
            candidate_id: "candidate_1".into(),
            agent_id: "codex".into(),
            session_lane: "candidate:candidate_1".into(),
            task_id: None,
            workspace_id: None,
            status: "completed".into(),
            snapshot_hash: Some("cand1_hash".into()),
            summary: Some("Candidate 1 summary".into()),
            evaluation_group_id: Some(cand1_eval_group),
            patch_path: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .await
        .expect("create cand 1");

    // Candidate 2
    let cand2_eval_group = Uuid::new_v4();
    eval_repo
        .create_group(&agentmesh_storage::EvaluationGroupRow {
            id: cand2_eval_group,
            workflow_id,
            source_task_id: None,
            strategy: ConsensusStrategy::Majority.as_str().into(),
            quorum: 2,
            status: "completed".into(),
            consensus: Some(
                serde_json::to_string(&ConsensusResult {
                    outcome: ConsensusOutcome::ChangesRequested,
                    strategy: ConsensusStrategy::Majority,
                    quorum: 2,
                    valid_count: 3,
                    total_count: 3,
                    approved_count: 1,
                    changes_requested_count: 2,
                    issues: Vec::new(),
                })
                .unwrap(),
            ),
            snapshot_hash: Some("cand2_hash".into()),
            round: 0,
            created_at: now.clone(),
            completed_at: Some(now.clone()),
        })
        .await
        .expect("cand2 eval group");

    for (i, v) in [
        agentmesh_orchestrator::ReviewVerdict::Approved,
        agentmesh_orchestrator::ReviewVerdict::ChangesRequested,
        agentmesh_orchestrator::ReviewVerdict::ChangesRequested,
    ]
    .iter()
    .enumerate()
    {
        let eval_res = agentmesh_orchestrator::evaluation::EvaluationResult {
            verdict: *v,
            confidence: Some(0.9),
            summary: format!("Eval {i}"),
            issues: Vec::new(),
        };
        eval_repo
            .create_member(&agentmesh_storage::EvaluationMemberRow {
                id: Uuid::new_v4(),
                group_id: cand2_eval_group,
                node_id: format!("eval_{i}"),
                agent_id: format!("evaluator_{i}"),
                task_id: None,
                status: "completed".into(),
                result_json: Some(serde_json::to_string(&eval_res).unwrap()),
                error: None,
                created_at: now.clone(),
                completed_at: Some(now.clone()),
            })
            .await
            .expect("create member cand2");
    }

    comp_repo
        .create_candidate(&CompetitionCandidateRow {
            id: Uuid::new_v4(),
            group_id: comp_group_id,
            candidate_id: "candidate_2".into(),
            agent_id: "opencode".into(),
            session_lane: "candidate:candidate_2".into(),
            task_id: None,
            workspace_id: None,
            status: "completed".into(),
            snapshot_hash: Some("cand2_hash".into()),
            summary: Some("Candidate 2 summary".into()),
            evaluation_group_id: Some(cand2_eval_group),
            patch_path: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .await
        .expect("create cand 2");

    // Record WinnerSelected Provenance Event
    let win_payload = serde_json::to_value(WinnerSelectedPayload {
        group_id: comp_group_id,
        workflow_id,
        winner_candidate_id: "candidate_1".into(),
        winner_agent_id: "codex".into(),
        winner_task_id: None,
        winner_workspace_id: None,
        winner_snapshot_hash: Some("cand1_hash".into()),
        candidate_rankings: vec![
            CandidateRankingEntry {
                candidate_id: "candidate_1".into(),
                agent_id: "codex".into(),
                is_approved: true,
                approved_count: 3,
                valid_count: 3,
                issue_count: 0,
            },
            CandidateRankingEntry {
                candidate_id: "candidate_2".into(),
                agent_id: "opencode".into(),
                is_approved: false,
                approved_count: 1,
                valid_count: 3,
                issue_count: 1,
            },
        ],
        selection_reason: "winner candidate_1".into(),
    })
    .unwrap();

    let _ = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::WINNER_SELECTED,
            entity_type::COMPETITION_GROUP,
            &comp_group_id.to_string(),
            None,
            actor_type::SYSTEM,
            Some("SelectionGate"),
            &win_payload,
        )
        .await
        .expect("append winner");

    // Replay Best-of-N selection
    let replay_report = service.replay_workflow(workflow_id).await;
    assert!(replay_report.passed);
    assert!(replay_report.selection_passed);
    assert!(replay_report.mismatches.is_empty());
}

#[tokio::test]
async fn test_legacy_workflow_provenance_handling() {
    let (db, _dir) = test_db().await;
    let service = make_provenance_service(&db);
    let wf_repo = WorkflowRepository::new(db.clone());

    let workflow_id = Uuid::new_v4();
    let now = chrono::Utc::now().to_rfc3339();
    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: workflow_id,
            preset: "default".into(),
            goal: "legacy workflow before schema v1".into(),
            status: WorkflowStatus::Completed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create legacy workflow");

    // Verify integrity for legacy workflow (0 events)
    let report = service.verify_integrity(workflow_id).await;
    assert!(report.valid);
    assert!(report.is_legacy);
    assert_eq!(report.total_events, 0);
    assert!(
        report
            .details
            .iter()
            .any(|d| d.contains("Legacy workflow: provenance unavailable before schema v1"))
    );

    // Replay for legacy workflow
    let replay = service.replay_workflow(workflow_id).await;
    assert!(replay.passed);
    assert!(replay.is_legacy);
}

#[tokio::test]
async fn test_lineage_graph_resolution() {
    let (db, _dir) = test_db().await;
    let service = make_provenance_service(&db);
    let wf_repo = WorkflowRepository::new(db.clone());
    let recovery_repo = WorkflowRecoveryRepository::new(db.clone());
    let prov_repo = ProvenanceRepository::new(db.clone());

    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = chrono::Utc::now().to_rfc3339();

    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: parent_id,
            preset: "plan".into(),
            goal: "parent goal".into(),
            status: WorkflowStatus::Failed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: Some("node failed".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create parent wf");

    recovery_repo
        .create(&agentmesh_storage::WorkflowRecoveryRow {
            id: Uuid::new_v4(),
            workflow_id: parent_id,
            failed_node_id: "implementer".into(),
            status: "executed".into(),
            planner_agent_id: Some("planner".into()),
            planner_task_id: None,
            plan_json: Some("{}".into()),
            validation_error: None,
            recovery_workflow_id: Some(child_id),
            attempt: 1,
            created_at: now.clone(),
            executed_at: Some(now.clone()),
        })
        .await
        .expect("create recovery proposal");

    let p = serde_json::json!({"action": "parent_start"});
    let _ = prov_repo
        .append_event(
            Some(parent_id),
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &parent_id.to_string(),
            None,
            actor_type::SYSTEM,
            None,
            &p,
        )
        .await
        .expect("parent prov");

    let lineage = service.get_lineage(parent_id).await.expect("lineage");
    assert_eq!(lineage.workflow_id, parent_id);
    assert_eq!(lineage.preset, "plan");
    assert_eq!(lineage.status, WorkflowStatus::Failed.as_str());
    assert_eq!(lineage.recovery_workflows, vec![child_id]);
    assert_eq!(lineage.provenance_events_count, 1);
}

#[tokio::test]
async fn test_apply_source_provenance_verification() {
    let (db, _dir) = test_db().await;
    let service = make_provenance_service(&db);
    let wf_repo = WorkflowRepository::new(db.clone());
    let comp_repo = CompetitionRepository::new(db.clone());
    let apply_repo = ApplyRepository::new(db.clone());
    let prov_repo = ProvenanceRepository::new(db.clone());

    let workflow_id = Uuid::new_v4();
    let winner_ws_id = Uuid::new_v4();
    let wrong_ws_id = Uuid::new_v4();
    let now = chrono::Utc::now().to_rfc3339();

    wf_repo
        .create(&agentmesh_storage::WorkflowRow {
            id: workflow_id,
            preset: "best-of-n".into(),
            goal: "apply test".into(),
            status: WorkflowStatus::Completed.as_str().into(),
            context_id: None,
            options_json: "{}".into(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: Some(now.clone()),
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create wf");

    let p = serde_json::json!({"action": "start"});
    let _ = prov_repo
        .append_event(
            Some(workflow_id),
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &workflow_id.to_string(),
            None,
            actor_type::SYSTEM,
            None,
            &p,
        )
        .await
        .expect("prov");

    let comp_group_id = Uuid::new_v4();
    comp_repo
        .create_group(&agentmesh_storage::CompetitionGroupRow {
            id: comp_group_id,
            workflow_id,
            source_workspace: None,
            base_revision: "HEAD".into(),
            candidate_count: 1,
            status: "completed".into(),
            winner_candidate_id: Some("candidate_1".into()),
            winner_task_id: None,
            winner_workspace_id: Some(winner_ws_id),
            winner_snapshot_hash: Some("hash1".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .await
        .expect("comp group");

    // Case 1: Apply with wrong workspace (not winner)
    let bad_apply_id = Uuid::new_v4();
    apply_repo
        .create(&agentmesh_storage::ApplyRow {
            id: bad_apply_id,
            task_id: None,
            workflow_id: Some(workflow_id),
            workspace_id: wrong_ws_id,
            source_repository: "repo".into(),
            base_revision: "HEAD".into(),
            status: agentmesh_storage::ApplyStatus::Completed,
            error: None,
            workspace_snapshot_hash: Some("wrong_hash".into()),
            created_at: now.clone(),
            completed_at: Some(now.clone()),
        })
        .await
        .expect("create bad apply");

    let replay_bad = service.replay_workflow(workflow_id).await;
    assert!(!replay_bad.passed);
    assert!(!replay_bad.apply_passed);
    assert!(
        replay_bad
            .mismatches
            .iter()
            .any(|m| m.contains("Apply source mismatch"))
    );
}
