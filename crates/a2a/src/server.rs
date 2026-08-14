//! A2A JSON-RPC server: one listener per agent.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::agent_card::AgentCard;
use crate::backend::{A2ABackend, A2ABackendError, A2ARun, A2AStreamEvent};
use crate::jsonrpc::{error_response, parse_request};
use crate::mapping::{task_state, to_artifact, to_task};
use crate::types::{A2A_PROTOCOL_VERSION, JsonRpcResponse, TaskStatusUpdateEvent, error_code};
use agentmesh_core::{AgentDescriptor, TaskStatus};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{Value, json};

/// Server configuration for one agent listener.
pub struct A2AServerConfig {
    pub agent_id: String,
    pub descriptor: AgentDescriptor,
    pub token: String,
    pub backend: Arc<dyn A2ABackend>,
    /// Listener base URL, set by the daemon after binding.
    pub url: tokio::sync::RwLock<String>,
}

impl A2AServerConfig {
    pub fn new(
        agent_id: String,
        descriptor: AgentDescriptor,
        token: String,
        backend: Arc<dyn A2ABackend>,
    ) -> Self {
        Self {
            agent_id,
            descriptor,
            token,
            backend,
            url: tokio::sync::RwLock::new(String::new()),
        }
    }

    pub async fn set_url(&self, url: String) {
        *self.url.write().await = url;
    }

    async fn base_url(&self) -> String {
        self.url.read().await.clone()
    }
}

impl A2AServerConfig {
    /// The agent card for this listener (anonymous read).
    pub fn card(&self, url: String, _card_url: String) -> AgentCard {
        let interface_url = url.clone();
        AgentCard {
            name: self.descriptor.name.clone(),
            description: self.descriptor.description.clone(),
            url,
            version: A2A_PROTOCOL_VERSION.to_string(),
            capabilities: crate::agent_card::AgentCapabilities {
                streaming: true,
                push_notifications: false,
            },
            skills: self.descriptor.skills.clone(),
            supported_interfaces: Some(vec![crate::agent_card::SupportedInterface {
                url: interface_url,
                protocol_binding: "JSONRPC".to_string(),
                protocol_version: A2A_PROTOCOL_VERSION.to_string(),
            }]),
            security_schemes: Some(vec![crate::agent_card::SecurityScheme::HttpBearer {
                scheme: "bearer".to_string(),
                bearer_format: Some("opaque".to_string()),
            }]),
        }
    }
}

type SharedConfig = Arc<A2AServerConfig>;

/// Build the router for one agent listener.
pub fn router(config: SharedConfig) -> Router {
    Router::new()
        .route("/.well-known/agent-card.json", get(agent_card))
        .route("/", post(rpc))
        .with_state(config)
}

async fn agent_card(State(config): State<SharedConfig>) -> Response {
    let base = config.base_url().await;
    let card = config.card(base.clone(), base);
    Json(card).into_response()
}

async fn rpc(
    State(config): State<SharedConfig>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // 1. Version check (A2A-Version header).
    let version = headers
        .get("A2A-Version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if version != A2A_PROTOCOL_VERSION {
        let response = JsonRpcResponse::error(
            None,
            JsonRpcError {
                code: error_code::VERSION_NOT_SUPPORTED,
                message: format!(
                    "unsupported A2A version `{version}`; expected `{A2A_PROTOCOL_VERSION}`"
                ),
                data: None,
            },
        );
        return Json(response).into_response();
    }

    // 2. Auth: Bearer <a2a-token> for all RPC calls.
    let authorized = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|token| token == config.token)
        .unwrap_or(false);
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(error_response(
                None,
                error_code::INVALID_REQUEST,
                "unauthorized",
            )),
        )
            .into_response();
    }

    // 3. Parse JSON-RPC.
    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(response) => return Json(*response).into_response(),
    };
    let id = request.id.clone();

    // 4. Dispatch.
    match request.method.as_str() {
        "SendMessage" => match handle_send_message(&config, &request.params, false).await {
            Ok(response) => response,
            Err(response) => Json(response).into_response(),
        },
        "SendStreamingMessage" => match handle_send_message(&config, &request.params, true).await {
            Ok(stream) => stream.into_response(),
            Err(response) => Json(response).into_response(),
        },
        "GetTask" => {
            let result = handle_get_task(&config, &request.params, id).await;
            Json(result).into_response()
        }
        "ListTasks" => {
            let result = handle_list_tasks(&config, &request.params, id).await;
            Json(result).into_response()
        }
        "CancelTask" => {
            let result = handle_cancel_task(&config, &request.params, id).await;
            Json(result).into_response()
        }
        "SubscribeToTask" => {
            let result = handle_subscribe(&config, &request.params, id).await;
            match result {
                Ok(stream) => stream.into_response(),
                Err(response) => Json(response).into_response(),
            }
        }
        _ => Json(error_response(
            id,
            error_code::METHOD_NOT_FOUND,
            &format!("method not found: {}", request.method),
        ))
        .into_response(),
    }
}

use crate::types::{JsonRpcError, Part, Task};

// ---------- SendMessage / SendStreamingMessage ----------

async fn handle_send_message(
    config: &SharedConfig,
    params: &Value,
    streaming: bool,
) -> Result<Response, JsonRpcResponse> {
    let id = params.get("id").cloned();
    let task_id = params.get("taskId").and_then(|v| v.as_str());
    if task_id.is_some() {
        // Follow-up on the same task is not supported yet: AgentMesh creates
        // a new task per invocation.
        return Err(error_response(
            id.clone(),
            error_code::UNSUPPORTED_OPERATION,
            "follow-up messages on an existing taskId are not supported yet",
        ));
    }

    let context_id = params.get("contextId").and_then(|v| v.as_str());
    let message: Option<crate::types::Message> = params
        .get("message")
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let prompt = match extract_prompt(message.as_ref()) {
        Ok(prompt) => prompt,
        Err(response) => return Err(*response),
    };
    // Phase 22: the source project/repository the caller wants the agent to
    // operate on (used to provision the first session's isolated worktree).
    // Only the contextless `start` path carries it; context continuations
    // derive the repository from the context's existing workspace.
    let source_workspace = params
        .get("sourceWorkspace")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from);
    let session_lane = params
        .get("sessionLane")
        .and_then(|v| v.as_str())
        .or_else(|| params.get("lane").and_then(|v| v.as_str()));

    let run = match context_id {
        Some(context) => {
            let context_id = parse_uuid(context).ok_or_else(|| {
                error_response(id.clone(), error_code::INVALID_PARAMS, "invalid contextId")
            })?;
            config
                .backend
                .start_in_context_with_lane(context_id, &config.agent_id, &prompt, session_lane)
                .await
        }
        None => {
            config
                .backend
                .start(&config.agent_id, &prompt, source_workspace)
                .await
        }
    }
    .map_err(backend_error)?;

    let task = task_from_run(config, &run).await;
    if streaming {
        Ok(streaming_response(config, run, task, id).await)
    } else {
        Ok(Json(JsonRpcResponse::result(
            id,
            serde_json::to_value(task).unwrap_or(json!({})),
        ))
        .into_response())
    }
}

/// Build the initial A2A Task from a started run (uses backend get_task).
async fn task_from_run(config: &SharedConfig, run: &A2ARun) -> Task {
    match config.backend.get_task(run.task_id).await {
        Ok(Some((task, artifacts))) => to_task(&task, &artifacts),
        _ => Task {
            id: run.task_id,
            context_id: Some(run.context_id),
            state: task_state(TaskStatus::Submitted),
            messages: None,
            artifacts: None,
            status: None,
            history: None,
            metadata: None,
        },
    }
}

/// SSE response for streaming methods: first the JSON-RPC result (Task),
/// then status/artifact events.
async fn streaming_response(
    _config: &SharedConfig,
    mut run: A2ARun,
    task: Task,
    id: Option<Value>,
) -> Response {
    let first = Event::default().event("jsonrpc").data(
        serde_json::to_string(&JsonRpcResponse::result(
            id,
            serde_json::to_value(&task).unwrap_or(json!({})),
        ))
        .unwrap_or_default(),
    );
    let stream = async_stream::stream! {
        yield Ok::<_, std::convert::Infallible>(first);
        // Initial status event.
        {
            let init = status_event(task.id, task.status.as_ref().map(|s| s.state).unwrap_or(task_state(TaskStatus::Submitted)), Some(false));
            tracing::debug!(task_id = %task.id, "a2a sse yield initial frame");
            yield Ok::<_, std::convert::Infallible>(init);
        }
        let mut message_count: usize = 0;
        let mut artifact_count: usize = 0;
        let mut last_message: Option<String> = None;
        let mut event_log: Vec<String> = Vec::new();
        while let Some(event) = run.events.recv().await {
            match event {
                A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Message(content)) => {
                    // Agent messages are surfaced as status updates with a message.
                    message_count += 1;
                    last_message = Some(content.clone());
                    event_log.push("message".to_string());
                    let s = TaskStatusUpdateEvent {
                        id: task.id,
                        status: crate::types::TaskStatus {
                            state: task_state(TaskStatus::Working),
                            message: Some(content),
                            timestamp: None,
                        },
                        final_: Some(false),
                    };
                    tracing::debug!(task_id = %task.id, "a2a sse yield message frame");
                    yield Ok::<_, std::convert::Infallible>(Event::default().event("status").data(serde_json::to_string(&s).unwrap_or_default()));
                }
                A2AStreamEvent::Agent(agentmesh_core::AgentEvent::ArtifactUpdated(artifact)) => {
                    artifact_count += 1;
                    event_log.push(format!("artifact({})", artifact.name));
                    let event = crate::types::TaskArtifactUpdateEvent {
                        id: task.id,
                        artifact: to_artifact(&artifact),
                    };
                    yield Ok::<_, std::convert::Infallible>(Event::default().event("artifact").data(serde_json::to_string(&event).unwrap_or_default()));
                }
                A2AStreamEvent::Agent(agentmesh_core::AgentEvent::StatusChanged(status)) => {
                    event_log.push(format!("status({status:?}"));
                    let state = task_state(status);
                    let s = crate::types::TaskStatusUpdateEvent {
                        id: task.id,
                        status: crate::types::TaskStatus {
                            state,
                            message: None,
                            timestamp: None,
                        },
                        final_: None,
                    };
                    let data = serde_json::to_string(&s).unwrap_or_default();
                    tracing::debug!(task_id = %task.id, state = ?state, len = data.len(), "a2a sse yield status changed frame");
                    yield Ok::<_, std::convert::Infallible>(Event::default().event("status").data(data));
                }
                A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Completed) => {
                    event_log.push("completed".to_string());
                    tracing::debug!(
                        task_id = %task.id,
                        message_count,
                        artifact_count,
                        "a2a streaming response completed"
                    );
                    // The final status frame carries the agent's last message so
                    // a client that lost intermediate frames (transport race
                    // under load) still receives the summary.
                    let s = TaskStatusUpdateEvent {
                        id: task.id,
                        status: crate::types::TaskStatus {
                            state: task_state(TaskStatus::Completed),
                            message: last_message.clone(),
                            timestamp: None,
                        },
                        final_: Some(true),
                    };
                    let data = serde_json::to_string(&s).unwrap_or_default();
                    tracing::debug!(task_id = %task.id, "a2a sse yield completed frame");
                    yield Ok::<_, std::convert::Infallible>(Event::default().event("status").data(data));
                    break;
                }
                A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Failed(message)) => {
                    let s = TaskStatusUpdateEvent {
                        id: task.id,
                        status: crate::types::TaskStatus {
                            state: task_state(TaskStatus::Failed),
                            message: Some(message),
                            timestamp: None,
                        },
                        final_: Some(true),
                    };
                    yield Ok::<_, std::convert::Infallible>(Event::default().event("status").data(serde_json::to_string(&s).unwrap_or_default()));
                    break;
                }
                A2AStreamEvent::ReplayGap { .. } => {}
                A2AStreamEvent::TaskInfo { .. } => {}
                A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Started) => {}
            }
        }
        if message_count == 0 && artifact_count == 0 {
            tracing::debug!(
                task_id = %task.id,
                events = ?event_log,
                "a2a streaming response ended without terminal event and with no message/artifacts"
            );
        }
    };
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("ping"),
        )
        .into_response()
}

fn status_event(
    task_id: uuid::Uuid,
    state: crate::types::TaskState,
    final_: Option<bool>,
) -> Event {
    let s = TaskStatusUpdateEvent {
        id: task_id,
        status: crate::types::TaskStatus {
            state,
            message: None,
            timestamp: None,
        },
        final_,
    };
    Event::default()
        .event("status")
        .data(serde_json::to_string(&s).unwrap_or_default())
}

// ---------- GetTask / ListTasks / CancelTask / SubscribeToTask ----------

async fn handle_get_task(
    config: &SharedConfig,
    params: &Value,
    id: Option<Value>,
) -> JsonRpcResponse {
    let Some(task_id) = params.get("taskId").and_then(|v| v.as_str()) else {
        return error_response(id, error_code::INVALID_PARAMS, "taskId is required");
    };
    let Ok(task_id) = uuid::Uuid::parse_str(task_id) else {
        return error_response(id, error_code::INVALID_PARAMS, "invalid taskId");
    };
    match config.backend.get_task(task_id).await {
        Ok(Some((task, artifacts))) => JsonRpcResponse::result(
            id,
            serde_json::to_value(to_task(&task, &artifacts)).unwrap_or(json!({})),
        ),
        Ok(None) => error_response(id, error_code::TASK_NOT_FOUND, "task not found"),
        Err(err) => backend_error_jsonrpc(id, err),
    }
}

async fn handle_list_tasks(
    config: &SharedConfig,
    params: &Value,
    id: Option<Value>,
) -> JsonRpcResponse {
    let context_id = params
        .get("contextId")
        .and_then(|v| v.as_str())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());
    let status = params
        .get("status")
        .and_then(|v| v.as_str())
        .and_then(|s| match s {
            "TASK_STATE_SUBMITTED" => Some(TaskStatus::Submitted),
            "TASK_STATE_WORKING" => Some(TaskStatus::Working),
            "TASK_STATE_INPUT_REQUIRED" => Some(TaskStatus::InputRequired),
            "TASK_STATE_COMPLETED" => Some(TaskStatus::Completed),
            "TASK_STATE_FAILED" => Some(TaskStatus::Failed),
            "TASK_STATE_CANCELED" => Some(TaskStatus::Cancelled),
            _ => None,
        });
    let page_size = params
        .get("pageSize")
        .and_then(|v| v.as_u64())
        .unwrap_or(50)
        .clamp(1, 100) as usize;

    match config
        .backend
        .list_tasks(context_id, status, page_size)
        .await
    {
        Ok(tasks) => {
            let tasks: Vec<Task> = tasks
                .iter()
                .map(|(task, artifacts)| to_task(task, artifacts))
                .collect();
            JsonRpcResponse::result(id, serde_json::to_value(tasks).unwrap_or(json!([])))
        }
        Err(err) => backend_error_jsonrpc(id, err),
    }
}

async fn handle_cancel_task(
    config: &SharedConfig,
    params: &Value,
    id: Option<Value>,
) -> JsonRpcResponse {
    let Some(task_id) = params.get("taskId").and_then(|v| v.as_str()) else {
        return error_response(id, error_code::INVALID_PARAMS, "taskId is required");
    };
    let Ok(task_id) = uuid::Uuid::parse_str(task_id) else {
        return error_response(id, error_code::INVALID_PARAMS, "invalid taskId");
    };
    match config.backend.cancel(task_id).await {
        Ok(()) => JsonRpcResponse::result(id, json!({ "cancelled": true })),
        Err(err) => backend_error_jsonrpc(id, err),
    }
}

async fn handle_subscribe(
    config: &SharedConfig,
    params: &Value,
    id: Option<Value>,
) -> Result<Response, JsonRpcResponse> {
    let Some(task_id) = params.get("taskId").and_then(|v| v.as_str()) else {
        return Err(error_response(
            id,
            error_code::INVALID_PARAMS,
            "taskId is required",
        ));
    };
    let Ok(task_id) = uuid::Uuid::parse_str(task_id) else {
        return Err(error_response(
            id,
            error_code::INVALID_PARAMS,
            "invalid taskId",
        ));
    };
    match config.backend.subscribe(task_id, 0).await {
        Ok(mut stream) => {
            // Terminal tasks cannot be subscribed (backend rejects).
            let first = Event::default().event("jsonrpc").data(
                serde_json::to_string(&JsonRpcResponse::result(
                    id,
                    json!({ "taskId": task_id.to_string() }),
                ))
                .unwrap_or_default(),
            );
            let stream = async_stream::stream! {
                yield Ok::<_, std::convert::Infallible>(first);
                while let Some(event) = stream.next().await {
                    match event {
                        A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Message(content)) => {
                            let s = TaskStatusUpdateEvent {
                                id: task_id,
                                status: crate::types::TaskStatus {
                                    state: task_state(TaskStatus::Working),
                                    message: Some(content),
                                    timestamp: None,
                                },
                                final_: Some(false),
                            };
                            yield Ok::<_, std::convert::Infallible>(Event::default().event("status").data(serde_json::to_string(&s).unwrap_or_default()));
                        }
                        A2AStreamEvent::Agent(agentmesh_core::AgentEvent::ArtifactUpdated(artifact)) => {
                            let event = crate::types::TaskArtifactUpdateEvent {
                                id: task_id,
                                artifact: to_artifact(&artifact),
                            };
                            yield Ok::<_, std::convert::Infallible>(Event::default().event("artifact").data(serde_json::to_string(&event).unwrap_or_default()));
                        }
                        A2AStreamEvent::Agent(agentmesh_core::AgentEvent::StatusChanged(status)) => {
                            yield Ok::<_, std::convert::Infallible>(status_event(task_id, task_state(status), None));
                        }
                        A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Completed) => {
                            yield Ok::<_, std::convert::Infallible>(status_event(task_id, task_state(TaskStatus::Completed), Some(true)));
                            break;
                        }
                        A2AStreamEvent::Agent(agentmesh_core::AgentEvent::Failed(message)) => {
                            let s = TaskStatusUpdateEvent {
                                id: task_id,
                                status: crate::types::TaskStatus {
                                    state: task_state(TaskStatus::Failed),
                                    message: Some(message),
                                    timestamp: None,
                                },
                                final_: Some(true),
                            };
                            yield Ok::<_, std::convert::Infallible>(Event::default().event("status").data(serde_json::to_string(&s).unwrap_or_default()));
                            break;
                        }
                        _ => {}
                    }
                }
            };
            Ok(Sse::new(stream)
                .keep_alive(
                    KeepAlive::new()
                        .interval(std::time::Duration::from_secs(15))
                        .text("ping"),
                )
                .into_response())
        }
        Err(err) => Err(backend_error_jsonrpc(id, err)),
    }
}

// ---------- helpers ----------

/// Extract a plain-text prompt from an A2A message; only ROLE_USER +
/// TextPart are accepted in Phase 8.
fn extract_prompt(message: Option<&crate::types::Message>) -> Result<String, Box<JsonRpcResponse>> {
    let Some(message) = message else {
        return Err(Box::new(error_response(
            None,
            error_code::INVALID_PARAMS,
            "message is required",
        )));
    };
    if message.role != crate::types::Role::User {
        return Err(Box::new(error_response(
            None,
            error_code::INVALID_PARAMS,
            "only ROLE_USER messages are accepted",
        )));
    }
    let mut text = String::new();
    for part in &message.parts {
        match part {
            Part::Text(part) => {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&part.text);
            }
            _ => {
                return Err(Box::new(error_response(
                    None,
                    error_code::CONTENT_TYPE_NOT_SUPPORTED,
                    "only TextPart input is supported",
                )));
            }
        }
    }
    if text.is_empty() {
        return Err(Box::new(error_response(
            None,
            error_code::INVALID_PARAMS,
            "message text must not be empty",
        )));
    }
    Ok(text)
}

fn parse_uuid(value: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(value).ok()
}

fn backend_error(err: A2ABackendError) -> JsonRpcResponse {
    backend_error_jsonrpc(None, err)
}

fn backend_error_jsonrpc(id: Option<Value>, err: A2ABackendError) -> JsonRpcResponse {
    let (code, message) = match &err {
        A2ABackendError::AgentNotFound(_) => (error_code::INVALID_PARAMS, err.to_string()),
        A2ABackendError::TaskNotFound(_) => (error_code::TASK_NOT_FOUND, err.to_string()),
        A2ABackendError::SessionBusy => (error_code::SESSION_BUSY, err.to_string()),
        A2ABackendError::SessionForAgentNotFound => (error_code::INVALID_PARAMS, err.to_string()),
        A2ABackendError::TaskNotLive => (error_code::TASK_NOT_CANCELABLE, err.to_string()),
        A2ABackendError::Unsupported => (error_code::UNSUPPORTED_OPERATION, err.to_string()),
        A2ABackendError::Internal(_) => (error_code::INTERNAL_ERROR, "internal error".to_string()),
    };
    error_response(id, code, &message)
}

/// Bind an A2A listener on 127.0.0.1:0.
pub async fn bind(
    config: SharedConfig,
) -> std::io::Result<(SocketAddr, Router, tokio::net::TcpListener)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    Ok((addr, router(config), listener))
}

/// Serve the A2A listener until the server is aborted.
pub async fn serve(listener: tokio::net::TcpListener, router: Router) {
    axum::serve(listener, router)
        .await
        .expect("a2a server error");
}
