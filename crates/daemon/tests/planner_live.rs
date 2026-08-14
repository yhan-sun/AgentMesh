//! Live Phase 17 test (ignored by default): the full AI-planner chain against
//! the real configured agents.
//!
//! ```text
//! Planner A2A → Plan → Validation → WorkflowGraph → DagScheduler → RuleRouter → A2A Agents
//! ```
//!
//! Requires at least one online agent with the `architecture` skill (routed
//! for the planner) and agents for the plan's node intents. When the external
//! agents are unavailable the test skips — the architecture is never changed
//! to make the live chain pass.

use std::sync::Arc;

use agentmesh_core::TaskIntent;
use agentmesh_daemon::a2a;
use agentmesh_daemon::paths::Scope;
use agentmesh_daemon::runtime::{build_state, parse_scope_arg};
use agentmesh_daemon::server::SharedState;
use agentmesh_daemon::workflow_service::WorkflowService;
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::{WorkflowStatus, pick_agent};
use uuid::Uuid;

async fn wait_for_terminal(workflows: &Arc<WorkflowService>, id: Uuid) -> WorkflowStatus {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(900);
    loop {
        let status = workflows
            .get(id)
            .await
            .ok()
            .flatten()
            .map(|d| d.status)
            .unwrap_or(WorkflowStatus::Pending);
        if status.is_terminal() {
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "live workflow did not reach a terminal state"
        );
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn build_directory(state: &SharedState, token: &str) -> AgentDirectory {
    let agents = state.a2a_agents.lock().unwrap().clone();
    let mut discovered = Vec::new();
    if let Some(object) = agents.as_object() {
        for (agent_id, info) in object {
            let url = info["url"].as_str().unwrap_or("").to_string();
            let card_url = info["card_url"].as_str().unwrap_or("").to_string();
            if url.is_empty() || card_url.is_empty() {
                continue;
            }
            discovered.push(DiscoveredEndpoint {
                agent_id: agent_id.clone(),
                url,
                card_url,
            });
        }
    }
    let mut directory = AgentDirectory::new();
    // A failed refresh means no agents are reachable — leave it empty so the
    // test can skip.
    let _ = directory
        .refresh(
            &discovered,
            &AgentAuth {
                token: Some(token.into()),
            },
        )
        .await;
    directory
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the real configured agents (claude/codex/opencode/antigravity) to be online"]
async fn live_plan_generate_validate_and_execute() {
    let scope = match std::env::args().nth(1) {
        Some(arg) => parse_scope_arg(&arg).expect("scope"),
        None => Scope::User,
    };
    let instance_id = Uuid::new_v4();
    let token = format!("live-{instance_id}");
    let state = build_state(&scope, instance_id, token.clone())
        .await
        .expect("build state");

    // Start A2A listeners for every online, enabled agent.
    let listeners = a2a::start_listeners(&state, &scope).await;
    if listeners.is_empty() {
        eprintln!("no online agents; skipping live plan test");
        return;
    }
    let directory = build_directory(&state, &token).await;
    if directory.list().is_empty() {
        eprintln!("no A2A agents discovered; skipping live plan test");
        return;
    }
    state.workflows.set_directory(directory.clone());

    // Routing must find an architecture-capable planner; otherwise skip.
    let router = state.workflows.router();
    if pick_agent(&directory, &router, Some(TaskIntent::Architecture), None).is_err() {
        eprintln!("no architecture-capable agent online; skipping live plan test");
        return;
    }

    let goal = "Design and implement an authentication module with security review and tests";
    let plan_id = state
        .plans
        .create_plan(goal, None)
        .await
        .expect("create plan");
    let detail = state
        .plans
        .get(plan_id)
        .await
        .unwrap()
        .expect("plan exists");
    assert_eq!(detail.status, "ready", "live plan must validate");
    assert!(!detail.nodes.is_empty(), "live plan must have nodes");

    // The plan decides structure only: no agent/provider/model in the JSON.
    let raw = state
        .plans
        .stored_plan_json(plan_id)
        .await
        .unwrap()
        .unwrap();
    for forbidden in [
        "agent_id",
        "provider",
        "model",
        "permissions",
        "commands",
        "max_parallel",
    ] {
        assert!(
            !raw.contains(forbidden),
            "live plan must not contain `{forbidden}`"
        );
    }

    // Execute: the only step that runs agents.
    let workflow_id = state
        .plans
        .execute(plan_id, 2, None)
        .await
        .expect("execute plan");
    let executed = state.plans.get(plan_id).await.unwrap().unwrap();
    assert_eq!(executed.status, "executed");
    assert_eq!(executed.workflow_id, Some(workflow_id));

    let status = wait_for_terminal(&state.workflows, workflow_id).await;
    eprintln!("live workflow {workflow_id} finished with {status:?}");
    assert_eq!(
        status,
        WorkflowStatus::Completed,
        "live full chain must complete"
    );
}
