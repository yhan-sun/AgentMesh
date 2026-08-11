//! Axum HTTP server for the AgentMesh daemon.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use agentmesh_adapters::AgentRunRequest;
use agentmesh_core::{AgentEvent, AgentMessage, TaskStatus};
use agentmesh_tasks::{ExecutionMetadata, ManagedTaskRun, TaskError, TaskManager};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::{Notify, broadcast};
use tracing::instrument;
use uuid::Uuid;

use crate::lease::SessionLeaseManager;
use crate::protocol::{
    ApiError, DAEMON_PROTOCOL_VERSION, DaemonStreamEvent, HealthResponse, LiveTaskInfo,
    ResumeRequest, RunRequest, RunResponse, RuntimeResponse, ShutdownRequest, ShutdownResponse,
};
use crate::registry::{LiveTask, LiveTaskRegistry};

/// Shared daemon state injected into handlers.
pub struct DaemonState {
    pub instance_id: Uuid,
    pub token: String,
    pub task_manager: TaskManager,
    pub registry: LiveTaskRegistry,
    pub leases: Arc<SessionLeaseManager>,
    pub scope: crate::paths::Scope,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub shutdown: Arc<Notify>,
    /// Set once graceful shutdown has been requested (for the shutdown API).
    pub shutting_down: std::sync::atomic::AtomicBool,
    /// Live-task repository handle for heartbeats.
    pub task_repo: agentmesh_storage::TaskRepository,
}

pub type SharedState = Arc<DaemonState>;

/// Build the daemon router.
pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/tasks/run", post(run_task))
        .route("/v1/tasks/resume", post(resume_task))
        .route("/v1/tasks/{id}/events", get(task_events))
        .route("/v1/tasks/{id}/cancel", post(cancel_task))
        .route("/v1/tasks", get(list_tasks))
        .route("/v1/tasks/{id}", get(get_task))
        .route("/v1/runtime", get(runtime_info))
        .route("/v1/shutdown", post(shutdown))
        .with_state(state)
        .fallback(not_found)
}

// ---------- auth ----------

fn authorized(headers: &HeaderMap, state: &DaemonState) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|token| token == state.token)
        .unwrap_or(false)
}

fn unauthorized() -> ApiError {
    ApiError::new("unauthorized", "invalid or missing bearer token")
}

async fn auth_or_err(headers: &HeaderMap, state: &SharedState) -> Result<(), ApiError> {
    if authorized(headers, state) {
        Ok(())
    } else {
        Err(unauthorized())
    }
}

// ---------- handlers ----------

async fn health(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    Json(HealthResponse {
        protocol_version: DAEMON_PROTOCOL_VERSION,
        instance_id: state.instance_id.to_string(),
        status: "ok".to_string(),
    })
    .into_response()
}

#[instrument(skip_all, fields(agent = %request.agent_id))]
async fn run_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(request): Json<RunRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let mut run_request = AgentRunRequest::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        AgentMessage::user(&request.prompt),
    );
    if let Some(source) = &request.source_workspace {
        run_request.workspace = Some(PathBuf::from(source));
    }
    let metadata = ExecutionMetadata {
        runtime_owner: Some(state.instance_id.to_string()),
    };
    match state
        .task_manager
        .start_with_metadata(&request.agent_id, run_request, metadata)
        .await
    {
        Ok(run) => {
            let agent_session_id = run.agent_session_id().unwrap_or_default();
            let lease = state
                .leases
                .acquire(agent_session_id, run.task_id())
                .expect("fresh session lease must succeed");
            register_run(&state, run, request.agent_id.clone(), lease).await
        }
        Err(err) => api_error_from_task(err),
    }
}

async fn resume_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(request): Json<ResumeRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    // Resolve the target session, acquire its lease, then resume.
    let (_, session_id) = match state
        .task_manager
        .resolve_resume_target(request.source_task_id)
        .await
    {
        Ok(target) => target,
        Err(err) => return api_error_from_task(err),
    };
    // The manager creates the real task id; hold a placeholder binding while
    // starting so a concurrent resume of the same session is rejected.
    let pending_task_id = Uuid::new_v4();
    let lease = match state.leases.acquire(session_id, pending_task_id) {
        Ok(lease) => lease,
        Err(err) => {
            return (
                StatusCode::CONFLICT,
                Json(ApiError::new("session_busy", err.to_string())),
            )
                .into_response();
        }
    };
    let run_request = AgentRunRequest::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        AgentMessage::user(&request.prompt),
    );
    let metadata = ExecutionMetadata {
        runtime_owner: Some(state.instance_id.to_string()),
    };
    let result = state
        .task_manager
        .resume_with_metadata(request.source_task_id, run_request, metadata)
        .await;
    match result {
        Ok(run) => {
            let task_id = run.task_id();
            let agent_id = run.agent_id().to_string();
            // Rebind the lease to the real task id (the placeholder guard
            // released the pending binding on drop).
            drop(lease);
            let lease = state
                .leases
                .acquire(session_id, task_id)
                .expect("rebind must succeed after placeholder release");
            register_run(&state, run, agent_id, lease).await
        }
        Err(err) => {
            drop(lease);
            api_error_from_task(err)
        }
    }
}

/// Register a ManagedTaskRun in the registry and spawn the forwarder.
async fn register_run(
    state: &SharedState,
    mut run: ManagedTaskRun,
    agent_id: String,
    lease: crate::lease::SessionLease,
) -> Response {
    let task_id = run.task_id();
    let context_id = run.context_id();
    let agent_session_id = run.agent_session_id().unwrap_or_default();

    let (broadcast_tx, _) = broadcast::channel(256);
    let live = Arc::new(LiveTask {
        task_id,
        context_id,
        agent_session_id,
        agent_id: agent_id.clone(),
        status: tokio::sync::RwLock::new(TaskStatus::Submitted),
        replay: tokio::sync::RwLock::new(crate::registry::ReplayBuffer::new(512, 2 * 1024 * 1024)),
        broadcaster: broadcast_tx,
        manager: state.task_manager.clone(),
        run_id: run.run_id(),
    });
    state.registry.insert(live.clone()).await;

    let registry = state.registry.clone();
    let task_repo = state.task_repo.clone();

    // Send TaskInfo first so every stream starts with metadata.
    let info = DaemonStreamEvent::TaskInfo {
        task_id,
        context_id,
        agent_session_id,
        agent_id: agent_id.clone(),
    };
    live.push(info).await;

    tokio::spawn(async move {
        let mut lease = Some(lease);
        while let Some(event) = run.next_event().await {
            let status = match &event {
                AgentEvent::Started => TaskStatus::Working,
                AgentEvent::StatusChanged(status) => *status,
                AgentEvent::Completed => TaskStatus::Completed,
                AgentEvent::Failed(_) => TaskStatus::Failed,
                _ => {
                    let _ = live.push(DaemonStreamEvent::Agent { event }).await;
                    continue;
                }
            };
            *live.status.write().await = status;
            let _ = live.push(DaemonStreamEvent::Agent { event }).await;
            if status.is_terminal() {
                // Releasing the lease guard also unbinds the session.
                drop(lease.take());
                let _ = task_repo.heartbeat(task_id).await;
            }
        }
        drop(lease);
        // Stream exhausted without terminal event: keep the live record as-is;
        // status reflects what the database says.
        let _ = task_repo.heartbeat(task_id).await;
        // Keep the LiveTask in the registry so attach can report terminal state.
        let _ = registry;
    });

    Json(RunResponse {
        task_id,
        context_id,
        agent_session_id,
        agent_id,
    })
    .into_response()
}

fn api_error_from_task(err: TaskError) -> Response {
    let (status, code) = match &err {
        TaskError::AgentNotFound(_) => (StatusCode::NOT_FOUND, "agent_not_found"),
        TaskError::TaskNotFound(_) => (StatusCode::NOT_FOUND, "task_not_found"),
        TaskError::AgentSessionNotFound(_) => (StatusCode::NOT_FOUND, "task_not_found"),
        TaskError::NativeSessionUnavailable(_) => {
            (StatusCode::CONFLICT, "native_session_unavailable")
        }
        TaskError::WorkspaceUnavailable(_) => (StatusCode::CONFLICT, "workspace_unavailable"),
        TaskError::Workspace(_) => (StatusCode::CONFLICT, "workspace_unavailable"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    // Sanitized: do not leak internal details.
    let message = match err {
        TaskError::WorkspaceUnavailable(path) => format!("workspace unavailable: {path}"),
        TaskError::AgentNotFound(agent) => format!("agent `{agent}` not found"),
        _ => err.to_string(),
    };
    (status, Json(ApiError::new(code, message))).into_response()
}

async fn task_events(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(task_id): Path<Uuid>,
    Query(query): Query<EventsQuery>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let after = query.after.unwrap_or(0);
    let Some(live) = state.registry.get(task_id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("task_not_found", "task is not live")),
        )
            .into_response();
    };
    // Replay buffer + live broadcast, deduplicated by sequence.
    let mut receiver = live.subscribe();
    let replay = live.replay_after(after).await;
    let oldest = live.oldest_available().await;

    let stream = async_stream::stream! {
        if after > 0 && oldest > after {
            yield Ok::<Event, std::convert::Infallible>(
                sse_event(DaemonStreamEvent::ReplayGap { oldest_available: oldest }),
            );
        }
        for (seq, event) in replay {
            yield Ok::<Event, std::convert::Infallible>(sse_event(event).id(seq.to_string()));
        }
        loop {
            match receiver.recv().await {
                Ok((seq, event)) => {
                    if seq <= after {
                        continue;
                    }
                    yield Ok::<Event, std::convert::Infallible>(sse_event(event).id(seq.to_string()));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Slow client: the gap is real; continue from the live
                    // stream and let the client re-sync on reconnect.
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
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

fn sse_event(event: DaemonStreamEvent) -> Event {
    Event::default()
        .event("agent")
        .data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string()))
}

#[derive(Debug, serde::Deserialize)]
pub struct EventsQuery {
    pub after: Option<u64>,
}

async fn cancel_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(task_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let Some(live) = state.registry.get(task_id).await else {
        // Terminal or unknown: report gracefully.
        return (
            StatusCode::CONFLICT,
            Json(ApiError::new(
                "task_not_live",
                "task is not owned by the current daemon runtime",
            )),
        )
            .into_response();
    };
    if live.status.read().await.is_terminal() {
        // Idempotent: already terminal.
        return Json(serde_json::json!({"cancelled": false, "already_terminal": true}))
            .into_response();
    }
    match live.cancel().await {
        Ok(()) => Json(serde_json::json!({"cancelled": true})).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::new("daemon_internal", err.to_string())),
        )
            .into_response(),
    }
}

async fn list_tasks(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let live = state.registry.list().await;
    let info: Vec<LiveTaskInfo> = live
        .into_iter()
        .map(
            |(task_id, agent_id, agent_session_id, status)| LiveTaskInfo {
                task_id,
                agent_id,
                agent_session_id,
                status: status.as_str().to_string(),
            },
        )
        .collect();
    Json(info).into_response()
}

async fn get_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(task_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.task_manager.get_task(task_id).await {
        Ok(Some(task)) => Json(task).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("task_not_found", "task not found")),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::new("daemon_internal", "failed to load task")),
        )
            .into_response(),
    }
}

async fn runtime_info(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let live = state.registry.list().await;
    Json(RuntimeResponse {
        instance_id: state.instance_id.to_string(),
        live_tasks: live
            .into_iter()
            .map(
                |(task_id, agent_id, agent_session_id, status)| LiveTaskInfo {
                    task_id,
                    agent_id,
                    agent_session_id,
                    status: status.as_str().to_string(),
                },
            )
            .collect(),
    })
    .into_response()
}

async fn shutdown(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(request): Json<ShutdownRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let live = state.registry.list().await;
    if !request.force && !live.is_empty() {
        return (
            StatusCode::CONFLICT,
            Json(ApiError::new(
                "daemon_busy",
                format!(
                    "daemon has {} running tasks; use force to cancel them before shutdown",
                    live.len()
                ),
            )),
        )
            .into_response();
    }
    let mut cancelled = 0usize;
    for (task_id, ..) in live {
        if let Some(task) = state.registry.get(task_id).await
            && task.cancel().await.is_ok()
        {
            cancelled += 1;
        }
    }
    // Give terminal events a moment to persist.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    state.shutdown.notify_one();
    Json(ShutdownResponse {
        cancelled_tasks: cancelled,
    })
    .into_response()
}

async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiError::new("invalid_request", "unknown endpoint")),
    )
        .into_response()
}

/// Bind the server on 127.0.0.1 with an OS-assigned port.
pub async fn bind(
    state: SharedState,
) -> std::io::Result<(SocketAddr, Router, tokio::net::TcpListener)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = router(state);
    Ok((addr, router, listener))
}

pub async fn serve(listener: tokio::net::TcpListener, router: Router, shutdown: Arc<Notify>) {
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.notified().await;
        })
        .await
        .expect("daemon server error");
}
