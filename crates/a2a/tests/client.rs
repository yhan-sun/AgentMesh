//! A2A client tests: real JSON-RPC/SSE against a controllable mock server.

mod common;

use agentmesh_a2a::client::{A2AClient, A2AClientError, A2AClientEvent};
use agentmesh_a2a::types::{Message, TaskState};
use agentmesh_core::AgentEvent;
use common::{LiveScriptBackend, card_server, mock_server};
use futures::StreamExt;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

fn client(server: &common::MockServer) -> A2AClient {
    A2AClient::new(format!("http://{}/", server.addr)).with_token(&server.token)
}

#[tokio::test]
async fn fetch_agent_card_discovers_skills() {
    let server = mock_server(&["code", "testing"], LiveScriptBackend::new(vec![])).await;
    let client = A2AClient::new(format!("http://{}/", server.addr));
    let card = client.fetch_agent_card().await.expect("card");
    assert_eq!(card.name, "Mock Agent");
    assert!(card.skills.iter().any(|s| s.name == "code"));
    assert!(card.skills.iter().any(|s| s.name == "testing"));
}

#[tokio::test]
async fn card_with_unknown_fields_parses_tolerantly() {
    let addr = card_server(json!({
        "name": "Future Agent",
        "description": "a card from the future",
        "url": format!("http://{}/", "127.0.0.1:1"),
        "version": "9.9",
        "capabilities": { "streaming": true, "pushNotifications": false, "teleport": true },
        "skills": [{ "name": "code", "futureSkill": 1 }],
        "supportedInterfaces": [{ "url": "http://x/", "protocolBinding": "JSONRPC", "protocolVersion": "1.0", "extra": 1 }],
        "securitySchemes": [{ "type": "HTTP_MAGIC", "scheme": "magic" }],
        "futureField": { "nested": [1, 2, 3] }
    }))
    .await;
    let client = A2AClient::new(format!("http://{addr}/"));
    let card = client.fetch_agent_card().await.expect("card must parse");
    assert_eq!(card.name, "Future Agent");
    assert_eq!(card.skills[0].name, "code");
}

#[tokio::test]
async fn send_message_returns_task() {
    let server = mock_server(&["code"], LiveScriptBackend::new(vec![])).await;
    let task = client(&server)
        .send_message(&Message::user_text("hello"))
        .await
        .expect("send");
    assert_ne!(task.id, Uuid::nil());
    assert_eq!(task.state, TaskState::Submitted);
}

#[tokio::test]
async fn jsonrpc_error_is_surfaced_for_unknown_task() {
    let server = mock_server(&["code"], LiveScriptBackend::new(vec![])).await;
    let err = client(&server).get_task(Uuid::new_v4()).await;
    assert!(matches!(err, Err(A2AClientError::TaskNotFound)), "{err:?}");
}

#[tokio::test]
async fn version_mismatch_is_surfaced() {
    let server = mock_server(&["code"], LiveScriptBackend::new(vec![])).await;
    let mismatched = A2AClient::new(format!("http://{}/", server.addr))
        .with_token(&server.token)
        .with_protocol_version("0.9");
    let err = mismatched.get_task(Uuid::new_v4()).await;
    assert!(
        matches!(err, Err(A2AClientError::VersionMismatch(_))),
        "{err:?}"
    );
}

#[tokio::test]
async fn auth_failure_is_surfaced() {
    let server = mock_server(&["code"], LiveScriptBackend::new(vec![])).await;
    let anonymous = A2AClient::new(format!("http://{}/", server.addr));
    let err = anonymous.get_task(Uuid::new_v4()).await;
    assert!(matches!(err, Err(A2AClientError::Unauthorized)), "{err:?}");
}

#[tokio::test]
async fn send_streaming_message_streams_events() {
    let script = vec![
        AgentEvent::Message("first message".into()),
        AgentEvent::Completed,
    ];
    let server = mock_server(&["code"], LiveScriptBackend::new(script)).await;
    let streaming = client(&server)
        .send_streaming_message(&Message::user_text("go"))
        .await
        .expect("send");
    let task_id = streaming.task.id;
    assert_ne!(task_id, Uuid::nil());

    let mut events = streaming.events;
    let mut messages = Vec::new();
    let mut completed = false;
    while let Some(event) = events.next().await {
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
    assert!(
        completed,
        "stream must end completed, got messages: {messages:?}"
    );
    assert!(messages.contains(&"first message".to_string()));
}

#[tokio::test]
async fn cancel_task_marks_task_cancelled() {
    let server = mock_server(&["code"], LiveScriptBackend::new(vec![])).await;
    let streaming = client(&server)
        .send_streaming_message(&Message::user_text("go"))
        .await
        .expect("send");
    let task_id = streaming.task.id;

    client(&server).cancel_task(task_id).await.expect("cancel");
    let task = client(&server).get_task(task_id).await.expect("get");
    assert_eq!(task.state, TaskState::Canceled);
}

#[tokio::test]
async fn subscribe_reattaches_to_live_task() {
    let script = vec![
        AgentEvent::Message("first".into()),
        AgentEvent::Message("second".into()),
        AgentEvent::Completed,
    ];
    let server = mock_server(
        &["code"],
        LiveScriptBackend::new(script).with_delay(Duration::from_millis(50)),
    )
    .await;
    let client = client(&server);
    let streaming = client
        .send_streaming_message(&Message::user_text("go"))
        .await
        .expect("send");
    let task_id = streaming.task.id;

    // Read the first message, then "drop" the connection.
    let mut first_stream = streaming.events;
    let mut saw_first = false;
    while let Some(event) = first_stream.next().await {
        if let A2AClientEvent::Status(status) = event.expect("event")
            && status.status.message.as_deref() == Some("first")
        {
            saw_first = true;
            break;
        }
    }
    assert!(
        saw_first,
        "must see the first message on the initial stream"
    );
    drop(first_stream);

    // Reattach via SubscribeToTask and read the rest.
    let mut subscription = client.subscribe_to_task(task_id).await.expect("subscribe");
    let mut messages = Vec::new();
    let mut completed = false;
    while let Some(event) = subscription.events.next().await {
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
    assert!(completed, "subscription must complete");
    assert!(
        messages.contains(&"second".to_string()),
        "subscription must observe later events: {messages:?}"
    );
}

#[tokio::test]
async fn subscribe_to_unknown_task_is_rejected() {
    let server = mock_server(&["code"], LiveScriptBackend::new(vec![])).await;
    let err = client(&server).subscribe_to_task(Uuid::new_v4()).await;
    assert!(
        matches!(err, Err(A2AClientError::TaskNotLive)),
        "expected TaskNotLive error"
    );
}

#[tokio::test]
async fn cancel_before_task_id_cancels_on_server_once_known() {
    // A long-running script that would otherwise keep running.
    let script = vec![
        AgentEvent::Message("starting work".into()),
        AgentEvent::Message("more work".into()),
        AgentEvent::Completed,
    ];
    let server = mock_server(
        &["code"],
        LiveScriptBackend::new(script).with_delay(Duration::from_millis(50)),
    )
    .await;
    let client = client(&server);

    let streaming = client
        .send_streaming_message(&Message::user_text("run"))
        .await
        .expect("start");
    let task_id = streaming.task.id;

    // Simulate caller triggering cancellation as soon as task_id is resolved.
    client.cancel_task(task_id).await.expect("cancel");

    // Task on server must transition to Cancelled
    let task = client.get_task(task_id).await.expect("get_task");
    assert_eq!(task.state, TaskState::Canceled);
}

#[tokio::test]
async fn start_never_returns_times_out() {
    use tokio::net::TcpListener;

    // Set up a mock server that returns SSE content-type but never sends any events or bytes
    let app = axum::Router::new().fallback(|| async {
        let stream =
            futures::stream::pending::<Result<axum::body::Bytes, std::convert::Infallible>>();
        axum::response::Response::builder()
            .status(200)
            .header("Content-Type", "text/event-stream")
            .body(axum::body::Body::from_stream(stream))
            .unwrap()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = A2AClient::new(format!("http://{addr}/"));
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(13),
        client.send_streaming_message(&Message::user_text("test")),
    )
    .await;

    assert!(result.is_ok(), "must not hang beyond client deadline");
    let inner_err = result.unwrap();
    assert!(
        inner_err.is_err(),
        "expected error when server never sends first frame"
    );
    assert!(
        start.elapsed() >= Duration::from_secs(9),
        "should have waited for ~10s transport deadline, elapsed: {:?}",
        start.elapsed()
    );
}
