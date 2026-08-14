//! Orchestrator delegation tests: discovery + routing + A2A streaming against
//! controllable mock A2A agents (no real Claude/Codex).

mod common;

use agentmesh_a2a::client::A2AClientEvent;
use agentmesh_a2a::types::TaskState;
use agentmesh_core::{AgentEvent, RoutingConfig, TaskIntent};
use agentmesh_orchestrator::OrchestratorError;
use agentmesh_orchestrator::delegate::{delegate, pick_agent};
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::{RouteDecision, RuleRouter};
use common::{MockAgent, ScriptedBackend, mock_agent};
use futures::StreamExt;
use std::time::Duration;

async fn directory_from(agents: &[&MockAgent]) -> AgentDirectory {
    let discovered: Vec<DiscoveredEndpoint> = agents
        .iter()
        .map(|agent| DiscoveredEndpoint {
            agent_id: agent.agent_id.clone(),
            url: agent.url.clone(),
            card_url: agent.card_url.clone(),
        })
        .collect();
    let mut directory = AgentDirectory::new();
    directory
        .refresh(
            &discovered,
            &AgentAuth {
                token: Some(agents[0].token.clone()),
            },
        )
        .await
        .expect("refresh");
    directory
}

/// Build a directory with a codex-like (testing) and claude-like (code only)
/// mock agent, plus the default routing config.
async fn default_env() -> (AgentDirectory, RuleRouter) {
    let codex = mock_agent("codex", &["code", "testing"], ScriptedBackend::new(vec![])).await;
    let claude = mock_agent("claude", &["code"], ScriptedBackend::new(vec![])).await;
    let directory = directory_from(&[&codex, &claude]).await;
    let router = RuleRouter::new(RoutingConfig::default());
    (directory, router)
}

#[tokio::test]
async fn directory_refresh_discovers_cards_and_skills() {
    let codex = mock_agent("codex", &["code", "testing"], ScriptedBackend::new(vec![])).await;
    let directory = directory_from(&[&codex]).await;
    let entry = directory.get("codex").expect("codex discovered");
    assert_eq!(entry.health, agentmesh_orchestrator::AgentHealth::Online);
    assert!(entry.has_skill("testing"));
    assert!(!entry.has_skill("review"));
}

#[tokio::test]
async fn routing_prefers_config_capable_agent() {
    let (directory, router) = default_env().await;
    // `testing` prefers codex and codex declares the skill.
    match router.route(&directory, TaskIntent::Testing) {
        RouteDecision::Agent { agent_id, reason } => {
            assert_eq!(agent_id, "codex");
            assert!(reason.contains("preferred agent with skill `testing`"));
        }
        other => panic!("expected an agent, got {other:?}"),
    }
    // `implementation` prefers codex first; both declare `code`, so codex wins.
    match router.route(&directory, TaskIntent::Implementation) {
        RouteDecision::Agent { agent_id, .. } => assert_eq!(agent_id, "codex"),
        other => panic!("expected an agent, got {other:?}"),
    }
}

#[tokio::test]
async fn routing_falls_back_when_preferred_lacks_skill() {
    // Config prefers codex then "ghost" for debug; codex lacks the skill and
    // ghost is absent, so the router falls back to any capable agent.
    let claude = mock_agent("claude", &["debug"], ScriptedBackend::new(vec![])).await;
    let codex = mock_agent("codex", &["code"], ScriptedBackend::new(vec![])).await;
    let directory = directory_from(&[&codex, &claude]).await;
    let config = RoutingConfig {
        debug: vec!["codex".into(), "ghost".into()],
        ..RoutingConfig::default()
    };
    let router = RuleRouter::new(config);
    match router.route(&directory, TaskIntent::Debug) {
        RouteDecision::Agent { agent_id, reason } => {
            assert_eq!(agent_id, "claude");
            assert!(reason.contains("fallback agent with skill `debug`"));
        }
        other => panic!("expected a fallback agent, got {other:?}"),
    }
}

#[tokio::test]
async fn no_capable_agent_when_nothing_declares_the_skill() {
    let (directory, router) = default_env().await;
    match router.route(&directory, TaskIntent::Review) {
        RouteDecision::NoCapableAgent { skill } => assert_eq!(skill, "review"),
        other => panic!("expected no capable agent, got {other:?}"),
    }
}

#[tokio::test]
async fn explicit_agent_is_validated_and_chosen() {
    let (directory, router) = default_env().await;
    let decision = router
        .explicit(&directory, "claude")
        .expect("explicit claude");
    assert_eq!(
        decision,
        RouteDecision::Agent {
            agent_id: "claude".into(),
            reason: "explicit --agent override".into()
        }
    );
    assert!(matches!(
        router.explicit(&directory, "ghost"),
        Err(OrchestratorError::AgentNotFound(_))
    ));
}

#[tokio::test]
async fn delegate_streams_through_a2a() {
    let codex = mock_agent(
        "codex",
        &["code", "testing"],
        ScriptedBackend::new(vec![
            AgentEvent::Message("reviewing tests".into()),
            AgentEvent::Completed,
        ]),
    )
    .await;
    let claude = mock_agent("claude", &["code"], ScriptedBackend::new(vec![])).await;
    let directory = directory_from(&[&codex, &claude]).await;
    let router = RuleRouter::new(RoutingConfig::default());

    let mut delegation = delegate(
        &directory,
        &router,
        Some(TaskIntent::Testing),
        None,
        "review the tests",
    )
    .await
    .expect("delegate");
    assert_eq!(delegation.agent_id, "codex");
    assert_ne!(delegation.task_id, uuid::Uuid::nil());
    assert!(delegation.context_id.is_some());

    let mut messages = Vec::new();
    let mut completed = false;
    while let Some(event) = delegation.stream.next().await {
        match event.expect("event") {
            A2AClientEvent::Status(status) => {
                if let Some(message) = status.status.message {
                    messages.push(message);
                }
                if status.status.state == TaskState::Completed {
                    completed = true;
                    break;
                }
            }
            A2AClientEvent::Artifact(_) => {}
        }
    }
    assert!(completed, "delegation must complete: {messages:?}");
    assert!(messages.contains(&"reviewing tests".to_string()));
}

#[tokio::test]
async fn delegate_with_explicit_agent_bypasses_router() {
    let codex = mock_agent(
        "codex",
        &["code", "testing"],
        ScriptedBackend::new(vec![
            AgentEvent::Message("from codex".into()),
            AgentEvent::Completed,
        ]),
    )
    .await;
    let claude = mock_agent(
        "claude",
        &["code"],
        ScriptedBackend::new(vec![
            AgentEvent::Message("from claude".into()),
            AgentEvent::Completed,
        ]),
    )
    .await;
    let directory = directory_from(&[&codex, &claude]).await;
    let router = RuleRouter::new(RoutingConfig::default());

    // Explicit claude even though `testing` would route to codex.
    let mut delegation = delegate(&directory, &router, None, Some("claude".into()), "hello")
        .await
        .expect("delegate");
    assert_eq!(delegation.agent_id, "claude");

    let mut saw = false;
    while let Some(event) = delegation.stream.next().await {
        let event = event.expect("event");
        if let A2AClientEvent::Status(status) = &event {
            if status.status.message.as_deref() == Some("from claude") {
                saw = true;
            }
            if status.status.state.is_terminal() {
                break;
            }
        }
    }
    assert!(saw, "claude's message must stream");
}

#[tokio::test]
async fn delegate_no_capable_agent_errors() {
    let codex = mock_agent("codex", &["code"], ScriptedBackend::new(vec![])).await;
    let directory = directory_from(&[&codex]).await;
    let router = RuleRouter::new(RoutingConfig::default());
    let err = delegate(
        &directory,
        &router,
        Some(TaskIntent::Review),
        None,
        "review",
    )
    .await;
    assert!(matches!(err, Err(OrchestratorError::NoCapableAgent(_))));
}

#[tokio::test]
async fn delegate_requires_an_intent_or_agent() {
    let (directory, router) = default_env().await;
    let err = pick_agent(&directory, &router, None, None);
    assert!(matches!(err, Err(OrchestratorError::NoIntentOrAgent)));
}

#[tokio::test]
async fn cancel_via_a2a_marks_task_cancelled() {
    // An empty script keeps the task live (no terminal event).
    let codex = mock_agent("codex", &["code", "testing"], ScriptedBackend::new(vec![])).await;
    let directory = directory_from(&[&codex]).await;
    let router = RuleRouter::new(RoutingConfig::default());

    let delegation = delegate(
        &directory,
        &router,
        Some(TaskIntent::Testing),
        None,
        "long task",
    )
    .await
    .expect("delegate");
    let task_id = delegation.task_id;
    delegation
        .client
        .cancel_task(task_id)
        .await
        .expect("cancel");
    let task = delegation.client.get_task(task_id).await.expect("get");
    assert_eq!(task.state, TaskState::Canceled);
}

#[tokio::test]
async fn reconnect_via_subscribe_completes_a_dropped_stream() {
    let codex = mock_agent(
        "codex",
        &["code", "testing"],
        ScriptedBackend::new(vec![
            AgentEvent::Message("first".into()),
            AgentEvent::Message("second".into()),
            AgentEvent::Completed,
        ])
        .with_step(Duration::from_millis(40)),
    )
    .await;
    let directory = directory_from(&[&codex]).await;
    let router = RuleRouter::new(RoutingConfig::default());

    let mut delegation = delegate(&directory, &router, Some(TaskIntent::Testing), None, "go")
        .await
        .expect("delegate");
    let task_id = delegation.task_id;

    // Read the first message, then drop the stream.
    let mut saw_first = false;
    while let Some(event) = delegation.stream.next().await {
        if let A2AClientEvent::Status(status) = event.expect("event")
            && status.status.message.as_deref() == Some("first")
        {
            saw_first = true;
            break;
        }
    }
    assert!(saw_first);
    // Simulate a connection drop by replacing the stream with a subscribe.
    let subscription = delegation
        .client
        .subscribe_to_task(task_id)
        .await
        .expect("subscribe");
    delegation.stream = subscription.events;

    let mut messages = Vec::new();
    let mut completed = false;
    while let Some(event) = delegation.stream.next().await {
        match event.expect("event") {
            A2AClientEvent::Status(status) => {
                if let Some(message) = status.status.message {
                    messages.push(message);
                }
                if status.status.state == TaskState::Completed {
                    completed = true;
                    break;
                }
            }
            A2AClientEvent::Artifact(_) => {}
        }
    }
    assert!(completed, "reconnected stream must complete");
    assert!(
        messages.contains(&"second".to_string()),
        "reconnect must observe later events: {messages:?}"
    );
}
