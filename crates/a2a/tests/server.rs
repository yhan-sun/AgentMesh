//! A2A server tests: real JSON-RPC over HTTP with a controllable backend.

use std::sync::Arc;

use agentmesh_a2a::{
    A2A_PROTOCOL_VERSION, A2ABackend, A2ABackendError, A2ARun, A2AServerConfig, A2AStreamEvent,
};
use agentmesh_core::{AgentDescriptor, AgentEvent, AgentSkill, AgentTask, Artifact, TaskStatus};
use async_trait::async_trait;
use futures::Stream;
use serde_json::json;
use uuid::Uuid;

/// Controllable backend: replays a script per run.
#[derive(Clone)]
struct ScriptBackend {
    script: Vec<AgentEvent>,
    context_session: Option<Uuid>,
    artifact: Option<Artifact>,
}

impl ScriptBackend {
    fn new(script: Vec<AgentEvent>) -> Self {
        Self {
            script,
            context_session: None,
            artifact: None,
        }
    }
}

#[async_trait]
impl A2ABackend for ScriptBackend {
    async fn start(
        &self,
        agent_id: &str,
        prompt: &str,
        _workspace: Option<std::path::PathBuf>,
    ) -> Result<A2ARun, A2ABackendError> {
        let task_id = Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let script = self.script.clone();
        let artifact = self.artifact.clone();
        let agent_id_owned = agent_id.to_string();
        tokio::spawn(async move {
            let _ = tx
                .send(A2AStreamEvent::TaskInfo {
                    task_id,
                    context_id: Uuid::new_v4(),
                    agent_session_id: None,
                    agent_id: agent_id_owned,
                })
                .await;
            for event in script {
                let _ = tx.send(A2AStreamEvent::Agent(event)).await;
                if let Some(artifact) = &artifact {
                    let _ = tx
                        .send(A2AStreamEvent::Agent(AgentEvent::ArtifactUpdated(
                            artifact.clone(),
                        )))
                        .await;
                }
            }
            let _ = prompt;
        });
        Ok(A2ARun {
            task_id,
            context_id: Uuid::new_v4(),
            agent_session_id: None,
            agent_id: agent_id.to_string(),
            events: rx,
        })
    }

    async fn start_in_context(
        &self,
        context_id: Uuid,
        agent_id: &str,
        _prompt: &str,
    ) -> Result<A2ARun, A2ABackendError> {
        if self.context_session.is_none() {
            return Err(A2ABackendError::SessionForAgentNotFound);
        }
        let task_id = Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let script = self.script.clone();
        let agent_id_owned = agent_id.to_string();
        let context_session = self.context_session;
        tokio::spawn(async move {
            let _ = tx
                .send(A2AStreamEvent::TaskInfo {
                    task_id,
                    context_id,
                    agent_session_id: context_session,
                    agent_id: agent_id_owned,
                })
                .await;
            for event in script {
                let _ = tx.send(A2AStreamEvent::Agent(event)).await;
            }
        });
        Ok(A2ARun {
            task_id,
            context_id,
            agent_session_id: self.context_session,
            agent_id: agent_id.to_string(),
            events: rx,
        })
    }

    async fn get_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        let mut task = AgentTask::new("mock", agentmesh_core::AgentMessage::user("hi"));
        task.id = task_id;
        task.status = TaskStatus::Completed;
        Ok(Some((task, self.artifact.clone().into_iter().collect())))
    }

    async fn list_tasks(
        &self,
        _context_id: Option<Uuid>,
        _status: Option<TaskStatus>,
        _limit: usize,
    ) -> Result<Vec<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        Ok(vec![])
    }

    async fn cancel(&self, task_id: Uuid) -> Result<(), A2ABackendError> {
        if task_id == Uuid::nil() {
            Err(A2ABackendError::TaskNotFound(task_id))
        } else {
            Ok(())
        }
    }

    async fn subscribe(
        &self,
        _task_id: Uuid,
        _after: u64,
    ) -> Result<std::pin::Pin<Box<dyn Stream<Item = A2AStreamEvent> + Send>>, A2ABackendError> {
        Err(A2ABackendError::TaskNotLive)
    }
}

struct TestServer {
    addr: std::net::SocketAddr,
    token: String,
}

async fn test_server(backend: ScriptBackend) -> TestServer {
    let descriptor = AgentDescriptor {
        id: "mock".into(),
        name: "Mock Agent".into(),
        description: Some("test".into()),
        skills: vec![AgentSkill::new("code", None)],
        endpoint: "agent://mock".into(),
        workspace_requirement: agentmesh_core::WorkspaceRequirement::None,
    };
    let token = "a2a-test-token-1234".to_string();
    let config = Arc::new(A2AServerConfig::new(
        "mock".into(),
        descriptor,
        token.clone(),
        Arc::new(backend),
    ));
    let (addr, router, listener) = agentmesh_a2a::server::bind(config.clone())
        .await
        .expect("bind");
    config.set_url(format!("http://{addr}/")).await;
    tokio::spawn(agentmesh_a2a::server::serve(listener, router));
    TestServer { addr, token }
}

async fn rpc(server: &TestServer, body: serde_json::Value, with_auth: bool) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("http://{}/", server.addr))
        .header("A2A-Version", A2A_PROTOCOL_VERSION)
        .json(&body);
    if with_auth {
        request = request.bearer_auth(&server.token);
    }
    request.send().await.expect("request")
}

#[tokio::test]
async fn agent_card_is_anonymous_and_complete() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/.well-known/agent-card.json",
            server.addr
        ))
        .send()
        .await
        .expect("card");
    assert_eq!(response.status(), 200);
    let card: serde_json::Value = response.json().await.expect("json");
    assert_eq!(card["name"], "Mock Agent");
    assert_eq!(card["capabilities"]["streaming"], true);
    assert_eq!(card["supportedInterfaces"][0]["protocolBinding"], "JSONRPC");
    assert_eq!(card["supportedInterfaces"][0]["protocolVersion"], "1.0");
    assert!(
        card["securitySchemes"][0]["type"]
            .as_str()
            .unwrap()
            .contains("BEARER")
    );
    // Token must never appear in the card.
    assert!(!card.to_string().contains(&server.token));
}

#[tokio::test]
async fn version_validation() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/", server.addr))
        .header("A2A-Version", "0.9")
        .bearer_auth(&server.token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"taskId":"x"}}))
        .send()
        .await
        .expect("request");
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32002);
}

#[tokio::test]
async fn auth_is_required() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"taskId":"x"}}),
        false,
    )
    .await;
    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn unknown_method_returns_method_not_found() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({"jsonrpc":"2.0","id":1,"method":"Bogus","params":{}}),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32601);
}

#[tokio::test]
async fn malformed_json_returns_invalid_request() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/", server.addr))
        .header("A2A-Version", A2A_PROTOCOL_VERSION)
        .bearer_auth(&server.token)
        .body("not json {{{")
        .send()
        .await
        .expect("request");
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32600);
}

#[tokio::test]
async fn send_message_returns_task() {
    let script = vec![
        AgentEvent::Started,
        AgentEvent::Message("hello".into()),
        AgentEvent::Completed,
    ];
    let server = test_server(ScriptBackend::new(script)).await;
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"SendMessage",
            "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"hi"}]}}
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    eprintln!("send_message body: {body}");
    assert_eq!(body["result"]["state"], "TASK_STATE_COMPLETED");
    assert!(body["result"]["id"].as_str().is_some());
}

#[tokio::test]
async fn file_part_is_rejected() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"SendMessage",
            "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"FilePart","file":{"name":"a.txt"}}]}}
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32005);
}

#[tokio::test]
async fn task_id_followup_is_unsupported() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"SendMessage",
            "params":{
                "taskId": format!("{}", Uuid::new_v4()),
                "message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"hi"}]}
            }
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32001);
}

#[tokio::test]
async fn send_streaming_message_streams_events() {
    let artifact = Artifact::text("note.txt", "hello artifact");
    let script = vec![
        AgentEvent::Started,
        AgentEvent::Message("first message".into()),
        AgentEvent::ArtifactUpdated(artifact),
        AgentEvent::Completed,
    ];
    let mut backend = ScriptBackend::new(script);
    backend.artifact = Some(Artifact::text("note.txt", "hello artifact"));
    let server = test_server(backend).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/", server.addr))
        .header("A2A-Version", A2A_PROTOCOL_VERSION)
        .bearer_auth(&server.token)
        .json(&json!({
            "jsonrpc":"2.0","id":7,"method":"SendStreamingMessage",
            "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"go"}]}}
        }))
        .send()
        .await
        .expect("request");
    assert!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/event-stream")
    );
    let text = response.text().await.expect("text");
    assert!(text.contains("\"jsonrpc\""));
    assert!(
        text.contains("TASK_STATE_COMPLETED"),
        "must end with completed: {text}"
    );
    assert!(text.contains("first message"), "must contain agent message");
    assert!(text.contains("artifact"), "must contain artifact update");
}

#[tokio::test]
async fn get_task_and_cancel() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let task_id = Uuid::new_v4();
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"GetTask",
            "params":{"taskId": task_id.to_string()}
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["result"]["id"], task_id.to_string());
    assert_eq!(body["result"]["state"], "TASK_STATE_COMPLETED");

    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":2,"method":"CancelTask",
            "params":{"taskId": task_id.to_string()}
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["result"]["cancelled"], true);
}

#[tokio::test]
async fn subscribe_to_unknown_task_is_rejected() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"SubscribeToTask",
            "params":{"taskId": Uuid::new_v4().to_string()}
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32004);
}

#[tokio::test]
async fn list_tasks_is_supported() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"ListTasks",
            "params":{"pageSize": 20}
        }),
        true,
    )
    .await;
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn context_id_continuation_rejects_missing_session() {
    let server = test_server(ScriptBackend::new(vec![])).await;
    let response = rpc(
        &server,
        json!({
            "jsonrpc":"2.0","id":1,"method":"SendMessage",
            "params":{
                "contextId": Uuid::new_v4().to_string(),
                "message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"hi"}]}
            }
        }),
        true,
    )
    .await;
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], -32602);
}

fn parse_status_events(sse_text: &str) -> Vec<agentmesh_a2a::types::TaskStatusUpdateEvent> {
    let mut out = Vec::new();
    for block in sse_text.split("\n\n") {
        let mut is_status = false;
        let mut data_str = None;
        for line in block.lines() {
            if line.starts_with("event: status") {
                is_status = true;
            } else if let Some(data) = line.strip_prefix("data: ") {
                data_str = Some(data);
            }
        }
        if is_status
            && let Some(data) = data_str
            && let Ok(ev) =
                serde_json::from_str::<agentmesh_a2a::types::TaskStatusUpdateEvent>(data)
        {
            out.push(ev);
        }
    }
    out
}

#[tokio::test]
async fn test_a_status_changed_frame_semantics_final_is_none() {
    use agentmesh_a2a::types::TaskState;

    // StatusChanged frames (Cancelled, Failed, Completed) must have final_ == None
    // and explicitly NOT final_ == Some(false).
    let script = vec![
        AgentEvent::Started,
        AgentEvent::StatusChanged(TaskStatus::Cancelled),
        AgentEvent::StatusChanged(TaskStatus::Failed),
        AgentEvent::StatusChanged(TaskStatus::Completed),
    ];
    let server = test_server(ScriptBackend::new(script)).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/", server.addr))
        .header("A2A-Version", A2A_PROTOCOL_VERSION)
        .bearer_auth(&server.token)
        .json(&json!({
            "jsonrpc":"2.0","id":1,"method":"SendStreamingMessage",
            "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"test-status-changed"}]}}
        }))
        .send()
        .await
        .expect("request");
    let text = response.text().await.expect("text");
    let events = parse_status_events(&text);

    // Initial event is the initial status with final_: Some(false).
    assert!(!events.is_empty(), "must have received status events");
    assert_eq!(events[0].final_, Some(false));

    // The stream must have exactly 4 status events:
    // [0] initial status frame (final: false)
    // [1] StatusChanged(Cancelled) -> final: None
    // [2] StatusChanged(Failed) -> final: None
    // [3] StatusChanged(Completed) -> final: None
    assert_eq!(
        events.len(),
        4,
        "must contain initial + 3 StatusChanged frames: {events:?}"
    );

    // [1] StatusChanged(Cancelled)
    assert_eq!(events[1].status.state, TaskState::Canceled);
    assert_eq!(
        events[1].final_, None,
        "StatusChanged(Cancelled) must have final_ == None"
    );
    assert_ne!(
        events[1].final_,
        Some(false),
        "StatusChanged(Cancelled) must never have final_ == Some(false)"
    );

    // [2] StatusChanged(Failed)
    assert_eq!(events[2].status.state, TaskState::Failed);
    assert_eq!(
        events[2].final_, None,
        "StatusChanged(Failed) must have final_ == None"
    );
    assert_ne!(
        events[2].final_,
        Some(false),
        "StatusChanged(Failed) must never have final_ == Some(false)"
    );

    // [3] StatusChanged(Completed)
    assert_eq!(events[3].status.state, TaskState::Completed);
    assert_eq!(
        events[3].final_, None,
        "StatusChanged(Completed) must have final_ == None"
    );
    assert_ne!(
        events[3].final_,
        Some(false),
        "StatusChanged(Completed) must never have final_ == Some(false)"
    );
}

#[tokio::test]
async fn test_d_terminal_frames_have_final_true() {
    use agentmesh_a2a::types::TaskState;

    // 1. Terminal Completed event
    {
        let script = vec![
            AgentEvent::Started,
            AgentEvent::Message("all good".into()),
            AgentEvent::Completed,
        ];
        let server = test_server(ScriptBackend::new(script)).await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("http://{}/", server.addr))
            .header("A2A-Version", A2A_PROTOCOL_VERSION)
            .bearer_auth(&server.token)
            .json(&json!({
                "jsonrpc":"2.0","id":1,"method":"SendStreamingMessage",
                "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"test-completed"}]}}
            }))
            .send()
            .await
            .expect("request");
        let text = response.text().await.expect("text");
        let events = parse_status_events(&text);
        let completed_terminal = events
            .iter()
            .find(|e| e.status.state == TaskState::Completed && e.final_ == Some(true))
            .expect("must contain Completed event with final_ == Some(true)");
        assert_eq!(completed_terminal.final_, Some(true));
        assert_eq!(
            completed_terminal.status.message.as_deref(),
            Some("all good")
        );
    }

    // 2. Terminal Failed event
    {
        let script = vec![
            AgentEvent::Started,
            AgentEvent::Failed("critical failure".into()),
        ];
        let server = test_server(ScriptBackend::new(script)).await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("http://{}/", server.addr))
            .header("A2A-Version", A2A_PROTOCOL_VERSION)
            .bearer_auth(&server.token)
            .json(&json!({
                "jsonrpc":"2.0","id":2,"method":"SendStreamingMessage",
                "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"test-failed"}]}}
            }))
            .send()
            .await
            .expect("request");
        let text = response.text().await.expect("text");
        let events = parse_status_events(&text);
        let failed_terminal = events
            .iter()
            .find(|e| e.status.state == TaskState::Failed && e.final_ == Some(true))
            .expect("must contain Failed event with final_ == Some(true)");
        assert_eq!(failed_terminal.final_, Some(true));
        assert_eq!(
            failed_terminal.status.message.as_deref(),
            Some("critical failure")
        );
    }
}

#[tokio::test]
async fn test_working_messages_have_final_false() {
    use agentmesh_a2a::types::TaskState;

    let script = vec![
        AgentEvent::Started,
        AgentEvent::Message("in progress step 1".into()),
        AgentEvent::Message("in progress step 2".into()),
        AgentEvent::Completed,
    ];
    let server = test_server(ScriptBackend::new(script)).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/", server.addr))
        .header("A2A-Version", A2A_PROTOCOL_VERSION)
        .bearer_auth(&server.token)
        .json(&json!({
            "jsonrpc":"2.0","id":1,"method":"SendStreamingMessage",
            "params":{"message":{"role":"ROLE_USER","parts":[{"kind":"TextPart","text":"test-working"}]}}
        }))
        .send()
        .await
        .expect("request");
    let text = response.text().await.expect("text");
    let events = parse_status_events(&text);
    let working_events: Vec<_> = events
        .iter()
        .filter(|e| e.status.state == TaskState::Working)
        .collect();
    assert_eq!(working_events.len(), 2);
    for w in working_events {
        assert_eq!(w.final_, Some(false));
    }
}
