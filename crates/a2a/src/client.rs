//! A2A v1.0 JSON-RPC client over HTTP/SSE.
//!
//! Consumes the same protocol types the server produces ([`crate::types`],
//! [`crate::agent_card`]) — there is no second DTO layer. RPC calls carry the
//! `A2A-Version` header and an optional bearer token; agent cards are fetched
//! anonymously.

use std::sync::atomic::{AtomicU64, Ordering};

use futures::{Stream, StreamExt};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::agent_card::AgentCard;
use crate::types::{
    A2A_PROTOCOL_VERSION, JsonRpcError, JsonRpcRequest, JsonRpcResponse, Message, Task,
    TaskArtifactUpdateEvent, TaskState, TaskStatusUpdateEvent, error_code,
};

type BoxStream<T> = std::pin::Pin<Box<dyn Stream<Item = T> + Send>>;

/// Client-side A2A errors (protocol-level, never provider-specific).
#[derive(Debug, thiserror::Error)]
pub enum A2AClientError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("A2A authentication failed")]
    Unauthorized,

    #[error("A2A protocol version mismatch: {0}")]
    VersionMismatch(String),

    #[error("A2A protocol error ({code}): {message}")]
    Protocol { code: i64, message: String },

    #[error("task not found")]
    TaskNotFound,

    #[error("task is not live in the agent runtime")]
    TaskNotLive,

    #[error("agent session is busy with another task; retry when it is free")]
    SessionBusy,

    #[error("operation not supported by the agent: {0}")]
    Unsupported(String),

    #[error("invalid response from agent: {0}")]
    InvalidResponse(String),
}

/// Map a server JSON-RPC error onto a client error.
fn jsonrpc_to_client_error(error: JsonRpcError) -> A2AClientError {
    match error.code {
        error_code::TASK_NOT_FOUND => A2AClientError::TaskNotFound,
        error_code::TASK_NOT_CANCELABLE => A2AClientError::TaskNotLive,
        error_code::SESSION_BUSY => A2AClientError::SessionBusy,
        error_code::UNSUPPORTED_OPERATION => A2AClientError::Unsupported(error.message),
        error_code::VERSION_NOT_SUPPORTED => A2AClientError::VersionMismatch(error.message),
        _ => A2AClientError::Protocol {
            code: error.code,
            message: error.message,
        },
    }
}

/// A streaming event emitted by `SendStreamingMessage` / `SubscribeToTask`.
#[derive(Debug, Clone)]
pub enum A2AClientEvent {
    Status(TaskStatusUpdateEvent),
    Artifact(TaskArtifactUpdateEvent),
}

/// Result of `SendStreamingMessage`: the initial task plus the event stream.
pub struct StreamingMessage {
    pub task: Task,
    pub events: BoxStream<Result<A2AClientEvent, A2AClientError>>,
}

/// Result of `SubscribeToTask`: the task id plus the live event stream.
pub struct TaskStream {
    pub task_id: Uuid,
    pub events: BoxStream<Result<A2AClientEvent, A2AClientError>>,
}

/// Typed A2A client for one agent listener.
pub struct A2AClient {
    base_url: String,
    card_url: String,
    token: Option<String>,
    protocol_version: String,
    http: reqwest::Client,
    next_id: AtomicU64,
}

impl Clone for A2AClient {
    fn clone(&self) -> Self {
        Self {
            base_url: self.base_url.clone(),
            card_url: self.card_url.clone(),
            token: self.token.clone(),
            protocol_version: self.protocol_version.clone(),
            http: self.http.clone(),
            next_id: AtomicU64::new(self.next_id.load(Ordering::Relaxed)),
        }
    }
}

impl A2AClient {
    /// Client for an agent listener base URL, e.g. `http://127.0.0.1:45678/`.
    /// The agent card URL is derived from it.
    pub fn new(endpoint: impl Into<String>) -> Self {
        let trimmed = endpoint.into().trim_end_matches('/').to_string();
        Self {
            base_url: format!("{trimmed}/"),
            card_url: format!("{trimmed}/.well-known/agent-card.json"),
            token: None,
            protocol_version: A2A_PROTOCOL_VERSION.to_string(),
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            next_id: AtomicU64::new(1),
        }
    }

    /// Override the agent card URL (the daemon reports it explicitly).
    pub fn with_card_url(mut self, card_url: impl Into<String>) -> Self {
        self.card_url = card_url.into();
        self
    }

    /// Attach the bearer token used for RPC calls.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Override the `A2A-Version` header (used for version-mismatch tests).
    pub fn with_protocol_version(mut self, version: impl Into<String>) -> Self {
        self.protocol_version = version.into();
        self
    }

    /// Fetch the agent card (anonymous).
    pub async fn fetch_agent_card(&self) -> Result<AgentCard, A2AClientError> {
        let response = self
            .http
            .get(&self.card_url)
            .send()
            .await
            .map_err(|err| A2AClientError::Transport(err.to_string()))?;
        if !response.status().is_success() {
            return Err(A2AClientError::Protocol {
                code: response.status().as_u16() as i64,
                message: format!("agent card fetch failed with HTTP {}", response.status()),
            });
        }
        response
            .json()
            .await
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))
    }

    // ---------- plain JSON-RPC methods ----------

    /// Start a task and wait for the initial task object.
    pub async fn send_message(&self, message: &Message) -> Result<Task, A2AClientError> {
        let result = self
            .rpc("SendMessage", json!({ "message": message }))
            .await?;
        serde_json::from_value(result)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))
    }

    /// Send a message in an existing context (continues that agent session).
    pub async fn send_message_in_context(
        &self,
        context_id: Uuid,
        message: &Message,
    ) -> Result<Task, A2AClientError> {
        let result = self
            .rpc(
                "SendMessage",
                json!({ "contextId": context_id.to_string(), "message": message }),
            )
            .await?;
        serde_json::from_value(result)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))
    }

    /// Fetch a task by id.
    pub async fn get_task(&self, task_id: Uuid) -> Result<Task, A2AClientError> {
        let result = self
            .rpc("GetTask", json!({ "taskId": task_id.to_string() }))
            .await?;
        serde_json::from_value(result)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))
    }

    /// Cancel a live task (kills the real agent process via the daemon).
    pub async fn cancel_task(&self, task_id: Uuid) -> Result<(), A2AClientError> {
        self.rpc("CancelTask", json!({ "taskId": task_id.to_string() }))
            .await?;
        Ok(())
    }

    /// List tasks, optionally filtered by context, status and page size.
    pub async fn list_tasks(
        &self,
        context_id: Option<Uuid>,
        status: Option<TaskState>,
        page_size: usize,
    ) -> Result<Vec<Task>, A2AClientError> {
        let mut params = json!({ "pageSize": page_size.clamp(1, 100) });
        if let Some(context_id) = context_id {
            params["contextId"] = json!(context_id.to_string());
        }
        if let Some(status) = status {
            params["status"] = serde_json::to_value(status)
                .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))?;
        }
        let result = self.rpc("ListTasks", params).await?;
        serde_json::from_value(result)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))
    }

    // ---------- streaming methods ----------

    /// Start a task and stream its events over SSE.
    pub async fn send_streaming_message(
        &self,
        message: &Message,
    ) -> Result<StreamingMessage, A2AClientError> {
        self.send_streaming_message_with_workspace(message, None)
            .await
    }

    /// Start a task and stream its events over SSE, provisioning the first
    /// agent session's isolated worktree from `source_workspace` (Phase 22).
    /// The daemon backend uses it as the source repository; `None` keeps the
    /// legacy daemon-cwd behavior.
    pub async fn send_streaming_message_with_workspace(
        &self,
        message: &Message,
        source_workspace: Option<std::path::PathBuf>,
    ) -> Result<StreamingMessage, A2AClientError> {
        let mut params = json!({ "message": message });
        if let Some(workspace) = source_workspace {
            params["sourceWorkspace"] = json!(workspace.to_string_lossy().to_string());
        }
        let (result, events) = self.open_sse("SendStreamingMessage", params).await?;
        let task = serde_json::from_value(result)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))?;
        Ok(StreamingMessage { task, events })
    }

    /// Start a task in an existing context with an optional session lane and stream its events over SSE.
    pub async fn send_streaming_message_in_context_with_lane(
        &self,
        context_id: Uuid,
        message: &Message,
        session_lane: Option<&str>,
    ) -> Result<StreamingMessage, A2AClientError> {
        let mut params = json!({ "contextId": context_id.to_string(), "message": message });
        if let Some(lane) = session_lane {
            params["sessionLane"] = json!(lane);
        }
        let (result, events) = self.open_sse("SendStreamingMessage", params).await?;
        let task = serde_json::from_value(result)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))?;
        Ok(StreamingMessage { task, events })
    }

    /// Start a task in an existing context and stream its events over SSE.
    ///
    /// The context continuation semantics are implemented by the agent's
    /// backend (the daemon reuses the context's session for the same agent
    /// and creates a new session when the agent joins the context for the
    /// first time).
    pub async fn send_streaming_message_in_context(
        &self,
        context_id: Uuid,
        message: &Message,
    ) -> Result<StreamingMessage, A2AClientError> {
        self.send_streaming_message_in_context_with_lane(context_id, message, None)
            .await
    }

    /// Attach to a live task and stream its events over SSE.
    pub async fn subscribe_to_task(&self, task_id: Uuid) -> Result<TaskStream, A2AClientError> {
        let (result, events) = self
            .open_sse("SubscribeToTask", json!({ "taskId": task_id.to_string() }))
            .await?;
        let task_id = result
            .get("taskId")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| {
                A2AClientError::InvalidResponse("subscribe result missing taskId".to_string())
            })?;
        Ok(TaskStream { task_id, events })
    }

    // ---------- internals ----------

    fn request(&self, method: &str, params: Value) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(self.next_id.fetch_add(1, Ordering::Relaxed))),
            method: method.to_string(),
            params,
        }
    }

    fn build_post(&self, body: &JsonRpcRequest) -> reqwest::RequestBuilder {
        let mut request = self
            .http
            .post(&self.base_url)
            .header("A2A-Version", &self.protocol_version);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        request.json(body)
    }

    async fn post_jsonrpc(
        &self,
        method: &str,
        params: Value,
    ) -> Result<JsonRpcResponse, A2AClientError> {
        let request = self.request(method, params);
        let response = self
            .build_post(&request)
            .send()
            .await
            .map_err(|err| A2AClientError::Transport(err.to_string()))?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(A2AClientError::Unauthorized);
        }
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(A2AClientError::Protocol {
                code: status.as_u16() as i64,
                message: format!("HTTP {status}: {text}"),
            });
        }
        let body: JsonRpcResponse =
            tokio::time::timeout(std::time::Duration::from_secs(10), response.json())
                .await
                .map_err(|_| A2AClientError::Transport("jsonrpc response timed out".to_string()))?
                .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))?;
        if body.jsonrpc != "2.0" {
            return Err(A2AClientError::InvalidResponse(
                "response is not JSON-RPC 2.0".to_string(),
            ));
        }
        Ok(body)
    }

    /// JSON-RPC round trip; `Ok(result)` or the mapped error.
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, A2AClientError> {
        let response = self.post_jsonrpc(method, params).await?;
        if let Some(error) = response.error {
            return Err(jsonrpc_to_client_error(error));
        }
        Ok(response.result.unwrap_or(Value::Null))
    }

    /// Open an SSE stream for a streaming method. Returns the JSON-RPC
    /// result from the first `jsonrpc` SSE event plus the remaining event
    /// stream (status / artifact updates).
    async fn open_sse(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(Value, BoxStream<Result<A2AClientEvent, A2AClientError>>), A2AClientError> {
        let request = self.request(method, params);
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.build_post(&request).send(),
        )
        .await
        .map_err(|_| A2AClientError::Transport("request timed out".to_string()))?
        .map_err(|err| A2AClientError::Transport(err.to_string()))?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(A2AClientError::Unauthorized);
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !content_type.contains("text/event-stream") {
            // A JSON-RPC error response (e.g. version mismatch, invalid params).
            let body: JsonRpcResponse = response
                .json()
                .await
                .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))?;
            if let Some(error) = body.error {
                return Err(jsonrpc_to_client_error(error));
            }
            return Err(A2AClientError::InvalidResponse(
                "expected an SSE response, got a non-stream response".to_string(),
            ));
        }

        let mut frames = sse_frames(response.bytes_stream());
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), frames.next())
            .await
            .map_err(|_| A2AClientError::Transport("handshake first frame timed out".to_string()))?
            .ok_or_else(|| A2AClientError::InvalidResponse("empty SSE stream".to_string()))??;
        if first.event.as_deref() != Some("jsonrpc") {
            return Err(A2AClientError::InvalidResponse(
                "expected a `jsonrpc` SSE event first".to_string(),
            ));
        }
        let rpc: JsonRpcResponse = serde_json::from_str(&first.data)
            .map_err(|err| A2AClientError::InvalidResponse(err.to_string()))?;
        if let Some(error) = rpc.error {
            return Err(jsonrpc_to_client_error(error));
        }
        let result = rpc.result.unwrap_or(Value::Null);
        let task_id = result
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        let events = frames
            .filter_map(move |frame| {
                let task_id = task_id.clone();
                async move {
                    match frame {
                        Ok(frame) => {
                            tracing::debug!(task_id = task_id.as_str(), event = ?frame.event, len = frame.data.len(), preview = &frame.data[..frame.data.len().min(80)], "a2a sse frame received");
                            map_client_event(frame).await
                        }
                        Err(err) => Some(Err(err)),
                    }
                }
            })
            .boxed();
        Ok((result, events))
    }
}

/// One parsed SSE frame.
struct SseFrame {
    event: Option<String>,
    data: String,
}

/// Map a frame onto a client event; `None` for comments, pings and frames
/// that are not status/artifact updates.
async fn map_client_event(frame: SseFrame) -> Option<Result<A2AClientEvent, A2AClientError>> {
    let event = match frame.event.as_deref() {
        Some("status") => {
            serde_json::from_str::<TaskStatusUpdateEvent>(&frame.data).map(A2AClientEvent::Status)
        }
        Some("artifact") => serde_json::from_str::<TaskArtifactUpdateEvent>(&frame.data)
            .map(A2AClientEvent::Artifact),
        _ => return None,
    };
    Some(event.map_err(|err| A2AClientError::InvalidResponse(err.to_string())))
}

/// Incrementally parse an SSE byte stream into frames.
fn sse_frames(
    bytes: impl Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static,
) -> BoxStream<Result<SseFrame, A2AClientError>> {
    let stream = async_stream::stream! {
        let mut bytes = Box::pin(bytes);
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk.map_err(|err| A2AClientError::Transport(err.to_string()))?;
            tracing::debug!(len = chunk.len(), "a2a sse chunk");
            buf.extend_from_slice(&chunk);
            while let Some(pos) = find_frame_boundary(&buf) {
                let frame = buf.drain(..pos).collect::<Vec<_>>();
                tracing::debug!(len = frame.len(), "a2a sse boundary frame");
                if let Some(parsed) = parse_sse_frame(&frame) {
                    yield Ok(parsed);
                }
            }
        }
        tracing::debug!(len = buf.len(), "a2a sse eof residue");
        if !buf.is_empty() && let Some(parsed) = parse_sse_frame(&buf) {
            yield Ok(parsed);
        }
    };
    Box::pin(stream)
}

/// Split a byte buffer at the first blank line (`\n\n`) between SSE frames.
fn find_frame_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n").map(|pos| pos + 2)
}

/// Parse one SSE frame (`event:` / `data:` lines, comments ignored).
fn parse_sse_frame(frame: &[u8]) -> Option<SseFrame> {
    let text = String::from_utf8_lossy(frame);
    let mut event = None;
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value).to_string());
        }
        // Comments (`: ...`) and unknown fields are ignored.
    }
    if data.is_empty() {
        return None;
    }
    Some(SseFrame {
        event,
        data: data.join("\n"),
    })
}
