//! DaemonClient: typed HTTP/SSE client used by the CLI.

use std::path::PathBuf;
use std::time::Duration;

use futures::{Stream, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::auth;
use crate::error::DaemonError;
use crate::paths::{self, Scope};
use crate::protocol::{
    DAEMON_PROTOCOL_VERSION, DaemonMeta, DaemonStreamEvent, HealthResponse, ResumeRequest,
    RunRequest, RunResponse, RuntimeResponse, ShutdownRequest, ShutdownResponse,
};
use crate::runtime::read_metadata;

/// One decoded SSE event.
#[derive(Debug, Clone)]
pub struct SseEvent {
    pub id: Option<u64>,
    pub data: DaemonStreamEvent,
}

/// Typed client for a running daemon.
#[derive(Clone)]
pub struct DaemonClient {
    base: String,
    token: String,
    http: Client,
}

impl DaemonClient {
    pub fn new(meta: &DaemonMeta, token: String) -> Self {
        Self {
            base: format!("http://{}", meta.address),
            token,
            http: Client::builder()
                .timeout(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Read metadata + token for a scope.
    pub fn from_scope(scope: &Scope) -> Result<Self, DaemonError> {
        let meta = read_metadata(scope).ok_or(DaemonError::NotRunning)?;
        let token = auth::read_token(&paths::daemon_token_path(scope))?;
        Ok(Self::new(&meta, token))
    }

    pub fn instance_id(&self) -> Result<String, DaemonError> {
        let meta = read_metadata(&Scope::resolve()).ok_or(DaemonError::NotRunning)?;
        Ok(meta.instance_id)
    }

    pub async fn health(&self) -> Result<HealthResponse, DaemonError> {
        self.get("/health").await
    }

    pub async fn run(
        &self,
        agent_id: &str,
        prompt: &str,
        workspace: Option<&PathBuf>,
    ) -> Result<RunResponse, DaemonError> {
        self.post(
            "/v1/tasks/run",
            &RunRequest {
                agent_id: agent_id.to_string(),
                prompt: prompt.to_string(),
                source_workspace: workspace.map(|p| p.display().to_string()),
            },
        )
        .await
    }

    pub async fn resume(
        &self,
        source_task_id: Uuid,
        prompt: &str,
    ) -> Result<RunResponse, DaemonError> {
        self.post(
            "/v1/tasks/resume",
            &ResumeRequest {
                source_task_id,
                prompt: prompt.to_string(),
            },
        )
        .await
    }

    pub async fn cancel(&self, task_id: Uuid) -> Result<(), DaemonError> {
        self.post_raw(
            &format!("/v1/tasks/{task_id}/cancel"),
            &serde_json::json!({}),
        )
        .await
    }

    pub async fn get_task(&self, task_id: Uuid) -> Result<Option<serde_json::Value>, DaemonError> {
        self.get_optional(&format!("/v1/tasks/{task_id}")).await
    }

    pub async fn runtime(&self) -> Result<RuntimeResponse, DaemonError> {
        self.get("/v1/runtime").await
    }

    pub async fn shutdown(&self, force: bool) -> Result<ShutdownResponse, DaemonError> {
        self.post("/v1/shutdown", &ShutdownRequest { force }).await
    }

    /// Open the SSE event stream for a task, replaying from `after`.
    pub fn events(
        &self,
        task_id: Uuid,
        after: u64,
    ) -> impl Stream<Item = Result<SseEvent, DaemonError>> + Send + 'static {
        let url = format!("{}/v1/tasks/{task_id}/events?after={after}", self.base);
        let token = self.token.clone();
        let client = self.http.clone();
        async_stream::stream! {
            let response = match client
                .get(&url)
                .bearer_auth(&token)
                .send()
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    yield Err(DaemonError::Http(err.to_string()));
                    return;
                }
            };
            if !response.status().is_success() {
                let err = parse_error_response(response).await;
                yield Err(err);
                return;
            }
            let mut stream = response.bytes_stream();
            let mut buf = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        yield Err(DaemonError::Http(err.to_string()));
                        return;
                    }
                };
                buf.extend_from_slice(&chunk);
                while let Some(pos) = find_event_boundary(&buf) {
                    let frame = buf.drain(..pos).collect::<Vec<_>>();
                    match parse_sse_frame(&frame) {
                        Some(event) => yield Ok(event),
                        None => {}
                    }
                }
            }
        }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, DaemonError> {
        let response = self
            .http
            .get(format!("{}{}", self.base, path))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| DaemonError::Http(err.to_string()))?;
        if !response.status().is_success() {
            return Err(parse_error_response(response).await);
        }
        response
            .json::<T>()
            .await
            .map_err(|err| DaemonError::Http(err.to_string()))
    }

    async fn get_optional(&self, path: &str) -> Result<Option<serde_json::Value>, DaemonError> {
        let response = self
            .http
            .get(format!("{}{}", self.base, path))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| DaemonError::Http(err.to_string()))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(parse_error_response(response).await);
        }
        response
            .json::<serde_json::Value>()
            .await
            .map(Some)
            .map_err(|err| DaemonError::Http(err.to_string()))
    }

    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T, DaemonError> {
        let response = self
            .http
            .post(format!("{}{}", self.base, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|err| DaemonError::Http(err.to_string()))?;
        if !response.status().is_success() {
            return Err(parse_error_response(response).await);
        }
        response
            .json::<T>()
            .await
            .map_err(|err| DaemonError::Http(err.to_string()))
    }

    async fn post_raw(&self, path: &str, body: &impl serde::Serialize) -> Result<(), DaemonError> {
        let response = self
            .http
            .post(format!("{}{}", self.base, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|err| DaemonError::Http(err.to_string()))?;
        if !response.status().is_success() {
            return Err(parse_error_response(response).await);
        }
        Ok(())
    }
}

async fn parse_error_response(response: reqwest::Response) -> DaemonError {
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap_or(serde_json::json!({}));
    let code = body
        .pointer("/error/code")
        .and_then(|v| v.as_str())
        .unwrap_or("daemon_internal")
        .to_string();
    let message = body
        .pointer("/error/message")
        .and_then(|v| v.as_str())
        .unwrap_or("daemon error")
        .to_string();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return DaemonError::Unauthorized;
    }
    DaemonError::api(code, message)
}

/// Split an SSE stream at the blank line between frames.
fn find_event_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n").map(|pos| pos + 2)
}

/// Parse a single SSE frame (`id:`, `event:`, `data:` lines).
fn parse_sse_frame(frame: &[u8]) -> Option<SseEvent> {
    let text = String::from_utf8_lossy(frame);
    let mut id = None;
    let mut data = String::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            id = value.trim().parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim());
        }
    }
    if data.is_empty() {
        return None;
    }
    let parsed = serde_json::from_str(&data).ok()?;
    Some(SseEvent { id, data: parsed })
}

/// Probe the daemon for a scope: metadata + authenticated health.
pub async fn probe(scope: &Scope) -> Result<DaemonClient, DaemonError> {
    let meta = read_metadata(scope).ok_or(DaemonError::NotRunning)?;
    if meta.protocol_version != DAEMON_PROTOCOL_VERSION {
        return Err(DaemonError::ProtocolMismatch);
    }
    let token = auth::read_token(&paths::daemon_token_path(scope))?;
    let client = DaemonClient::new(&meta, token);
    client.health().await?;
    Ok(client)
}

/// Connect to the running daemon for the current scope, or start one and
/// wait until it is healthy. Returns the client.
pub async fn connect_or_start(scope: Scope) -> Result<DaemonClient, DaemonError> {
    // Try to connect first.
    if let Ok(client) = probe(&scope).await {
        return Ok(client);
    }
    // Lock attempt: start a daemon process; the winner acquires the lock.
    let mut child = crate::runtime::spawn_daemon_process(&scope)
        .map_err(|err| DaemonError::Spawn(err.to_string()))?;
    // The child may exit immediately if another daemon holds the lock.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(client) = probe(&scope).await {
            return Ok(client);
        }
        if std::time::Instant::now() > deadline {
            let _ = child.wait();
            return Err(DaemonError::StartupTimeout(10));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
