//! Phase 24: ProvenanceRepository storage tests.

use std::sync::Arc;

use agentmesh_core::provenance::{
    actor_type, compute_event_hash, compute_payload_hash, entity_type, event_type,
};
use agentmesh_storage::{Database, ProvenanceRepository, WorkflowRepository, WorkflowRow};
use uuid::Uuid;

async fn setup_db() -> (Database, ProvenanceRepository, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db");
    let db = Database::open(&path).await.expect("open db");
    let repo = ProvenanceRepository::new(db.clone());
    (db, repo, dir)
}

async fn create_workflow(db: &Database, id: Uuid) {
    let workflows = WorkflowRepository::new(db.clone());
    let now = chrono::Utc::now().to_rfc3339();
    workflows
        .create(&WorkflowRow {
            id,
            preset: "best-of-n".to_string(),
            goal: "Test goal".to_string(),
            status: "pending".to_string(),
            context_id: None,
            options_json: "{}".to_string(),
            review_rounds: 0,
            runtime_owner: None,
            runtime_heartbeat_at: None,
            error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            completed_at: None,
            graph_revision: 1,
            parent_workflow_id: None,
            recovery_of_node_id: None,
            recovery_attempt: 0,
            source_workspace: None,
        })
        .await
        .expect("create workflow");
}

#[tokio::test]
async fn append_and_list_sequential_hash_chain() {
    let (db, repo, _dir) = setup_db().await;
    let workflow_id = Uuid::new_v4();
    create_workflow(&db, workflow_id).await;

    let p1 = serde_json::json!({
        "goal": "Build payment gateway",
        "preset": "best-of-n",
        "token": "secret_123" // must be redacted
    });

    let e1 = repo
        .append_event(
            Some(workflow_id),
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &workflow_id.to_string(),
            None,
            actor_type::SYSTEM,
            Some("Scheduler"),
            &p1,
        )
        .await
        .expect("append e1");

    assert_eq!(e1.sequence, 1);
    assert!(e1.previous_hash.is_none());
    assert_eq!(e1.workflow_id, Some(workflow_id));
    assert!(e1.payload_json.contains("[REDACTED]"));
    assert!(!e1.payload_json.contains("secret_123"));

    let p2 = serde_json::json!({
        "node_id": "candidate_1",
        "agent_id": "claude"
    });

    let e2 = repo
        .append_event(
            Some(workflow_id),
            event_type::CANDIDATE_STARTED,
            entity_type::COMPETITION_CANDIDATE,
            "candidate_1",
            Some(e1.id),
            actor_type::AGENT,
            Some("claude"),
            &p2,
        )
        .await
        .expect("append e2");

    assert_eq!(e2.sequence, 2);
    assert_eq!(e2.previous_hash.as_deref(), Some(e1.event_hash.as_str()));

    let p3 = serde_json::json!({
        "winner_candidate_id": "candidate_1",
        "selection_reason": "winner candidate_1 (approved 2/2)"
    });

    let e3 = repo
        .append_event(
            Some(workflow_id),
            event_type::WINNER_SELECTED,
            entity_type::COMPETITION_GROUP,
            "group_1",
            None,
            actor_type::SYSTEM,
            Some("SelectionGate"),
            &p3,
        )
        .await
        .expect("append e3");

    assert_eq!(e3.sequence, 3);
    assert_eq!(e3.previous_hash.as_deref(), Some(e2.event_hash.as_str()));

    // Verify list_for_workflow orders by sequence ASC
    let events = repo
        .list_for_workflow(workflow_id)
        .await
        .expect("list events");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].id, e1.id);
    assert_eq!(events[1].id, e2.id);
    assert_eq!(events[2].id, e3.id);

    // Verify last_for_workflow
    let last = repo
        .last_for_workflow(workflow_id)
        .await
        .expect("last event")
        .expect("event exists");
    assert_eq!(last.id, e3.id);

    // Verify count_for_workflow
    let count = repo
        .count_for_workflow(workflow_id)
        .await
        .expect("count events");
    assert_eq!(count, 3);
}

#[tokio::test]
async fn concurrent_appends_allocate_unique_monotonic_sequences() {
    let (db, repo, _dir) = setup_db().await;
    let workflow_id = Uuid::new_v4();
    create_workflow(&db, workflow_id).await;
    let repo_arc = Arc::new(repo);

    let mut handles = Vec::new();
    for i in 0..20 {
        let r = repo_arc.clone();
        let wid = workflow_id;
        handles.push(tokio::spawn(async move {
            let payload = serde_json::json!({"item": i});
            r.append_event(
                Some(wid),
                event_type::NODE_COMPLETED,
                entity_type::NODE,
                &format!("node_{i}"),
                None,
                actor_type::AGENT,
                Some("worker"),
                &payload,
            )
            .await
            .expect("append event")
        }));
    }

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.expect("join handle"));
    }

    // Verify all 20 events have distinct sequences 1..=20
    let mut sequences: Vec<i64> = results.iter().map(|r| r.sequence).collect();
    sequences.sort();
    let expected: Vec<i64> = (1..=20).collect();
    assert_eq!(sequences, expected);

    // Verify hash chain consistency for all 20 events
    let events = repo_arc
        .list_for_workflow(workflow_id)
        .await
        .expect("list events");
    assert_eq!(events.len(), 20);

    for (idx, ev) in events.iter().enumerate() {
        assert_eq!(ev.sequence, (idx + 1) as i64);
        if idx == 0 {
            assert!(ev.previous_hash.is_none());
        } else {
            assert_eq!(
                ev.previous_hash.as_deref(),
                Some(events[idx - 1].event_hash.as_str())
            );
        }
    }
}

#[tokio::test]
async fn tampered_payload_or_chain_is_detectable() {
    let (db, repo, _dir) = setup_db().await;
    let workflow_id = Uuid::new_v4();
    create_workflow(&db, workflow_id).await;

    let p1 = serde_json::json!({"action": "initial"});
    let e1 = repo
        .append_event(
            Some(workflow_id),
            event_type::WORKFLOW_STARTED,
            entity_type::WORKFLOW,
            &workflow_id.to_string(),
            None,
            actor_type::SYSTEM,
            Some("Scheduler"),
            &p1,
        )
        .await
        .expect("append e1");

    // Manually verify computed hash against row
    let (c1, p_hash) = compute_payload_hash(&p1);
    assert_eq!(e1.payload_json, c1);
    assert_eq!(e1.payload_hash, p_hash);

    let expected_e_hash = compute_event_hash(
        None,
        Some(&workflow_id.to_string()),
        1,
        event_type::WORKFLOW_STARTED,
        entity_type::WORKFLOW,
        &workflow_id.to_string(),
        actor_type::SYSTEM,
        Some("Scheduler"),
        &p_hash,
    );
    assert_eq!(e1.event_hash, expected_e_hash);

    // If payload is modified, payload hash changes
    let tampered_payload = serde_json::json!({"action": "tampered"});
    let (_, tampered_p_hash) = compute_payload_hash(&tampered_payload);
    assert_ne!(p_hash, tampered_p_hash);
}
