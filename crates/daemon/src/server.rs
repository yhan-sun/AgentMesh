//! Axum HTTP server for the AgentMesh daemon.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use agentmesh_adapters::AgentRunRequest;
use agentmesh_apply::ApplyManager;
use agentmesh_core::{AgentEvent, AgentMessage, TaskStatus};
use agentmesh_tasks::{ExecutionMetadata, ManagedTaskRun, TaskError, TaskManager};
use agentmesh_workspace::{WorkspaceError, WorkspaceManager};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::{Notify, broadcast};
use tracing::instrument;
use uuid::Uuid;

use crate::cleanup::{self, CleanupError};
use crate::lease::SessionLeaseManager;
use crate::planner::PlanService;
use crate::protocol::{
    ApiError, ApplyInfo, ApplyRequest, ApplyResponse, CleanupRequest, CleanupResponse,
    DAEMON_PROTOCOL_VERSION, DaemonStreamEvent, EvaluationDetail, EvaluationInfo,
    EvaluationMemberInfo, EvaluationStartRequest, EvaluationStartResponse, HealthResponse,
    LiveTaskInfo, PlanCreateRequest, PlanCreateResponse, PlanEditRequest, PlanEditResponse,
    PlanExecuteRequest, PlanExecuteResponse, PruneRequest, PruneResponse, RecoveryApplyRequest,
    RecoveryApplyResponse, RecoveryCreateRequest, RecoveryDetail, RecoveryInfo, ReplanApplyRequest,
    ReplanApplyResponse, ReplanCreateRequest, ReplanCreateResponse, ReplanDetail, ReplanInfo,
    ResumeRequest, RunRequest, RunResponse, ShutdownRequest, ShutdownResponse,
    WorkflowStartRequest, WorkflowStartResponse, WorkflowStreamEvent, WorkspaceInfo,
};
use crate::registry::{LiveTask, LiveTaskRegistry};
use crate::workflow_service::{WorkflowError, WorkflowService};

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
    /// Daemon-owned workflow runtime.
    pub workflows: Arc<WorkflowService>,
    /// Phase 17: AI-planner plans (generate / preview / execute).
    pub plans: Arc<PlanService>,
    /// Phase 19: user-triggered runtime replans (propose / preview / apply).
    pub replans: Arc<crate::replan::ReplanService>,
    /// Phase 20: failure recovery (analyze / propose / execute).
    pub recoveries: Arc<crate::recovery::RecoveryService>,
    /// Safe-apply layer for task / workflow results (Phase 13).
    pub apply: Arc<ApplyManager>,
    /// Workspace lifecycle + cleanup (Phase 14).
    pub workspaces: Arc<WorkspaceManager>,
    /// Apply history (also used for the cleanup snapshot fingerprint).
    pub applies: agentmesh_storage::ApplyRepository,
    /// Workflow/step history (also used for cleanup dependency checks).
    pub workflows_repo: agentmesh_storage::WorkflowRepository,
    pub steps: agentmesh_storage::WorkflowStepRepository,
    /// Competition groups and candidates (Phase 23).
    pub competitions: agentmesh_storage::CompetitionRepository,
    /// Artifact persistence (used by `artifacts prune`).
    pub artifacts: agentmesh_storage::ArtifactRepository,
    /// Provenance event repository (Phase 24).
    pub provenance_repo: agentmesh_storage::ProvenanceRepository,
    /// Provenance verifier and deterministic replay service (Phase 24).
    pub provenance: Arc<crate::provenance_service::ProvenanceService>,
    /// A2A listeners (agent_id -> urls), set after startup.
    pub a2a_agents: std::sync::Mutex<serde_json::Value>,
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
        .route("/v1/tasks/{id}/apply", post(apply_task))
        .route("/v1/tasks/{id}/archive", post(archive_task))
        .route("/v1/tasks/{id}/cleanup", post(cleanup_task))
        .route("/v1/applies", get(list_applies))
        .route("/v1/applies/{id}", get(get_apply))
        .route("/v1/workspaces", get(list_workspaces))
        .route("/v1/artifacts/prune", post(prune_artifacts))
        .route("/v1/runtime", get(runtime_info))
        .route("/v1/workflows", post(start_workflow))
        .route("/v1/workflows", get(list_workflows))
        .route("/v1/workflows/{id}", get(get_workflow))
        .route("/v1/workflows/{id}/events", get(workflow_events))
        .route("/v1/workflows/{id}/cancel", post(cancel_workflow))
        .route("/v1/workflows/{id}/resume", post(resume_workflow))
        .route("/v1/workflows/{id}/apply", post(apply_workflow))
        .route("/v1/workflows/{id}/cleanup", post(cleanup_workflow))
        .route("/v1/workflows/{id}/replan", post(create_replan))
        .route("/v1/workflows/{id}/replans", get(list_workflow_replans))
        .route("/v1/replans", get(list_replans))
        .route("/v1/replans/{id}", get(get_replan))
        .route("/v1/replans/{id}/apply", post(apply_replan))
        .route("/v1/workflows/{id}/recover", post(create_recovery))
        .route(
            "/v1/workflows/{id}/recoveries",
            get(list_workflow_recoveries),
        )
        .route("/v1/recoveries", get(list_recoveries))
        .route("/v1/recoveries/{id}", get(get_recovery))
        .route("/v1/recoveries/{id}/execute", post(execute_recovery))
        .route("/v1/workflows/{id}/lineage", get(workflow_lineage))
        .route("/v1/workflows/{id}/audit", get(workflow_audit))
        .route("/v1/workflows/{id}/replay", post(workflow_replay))
        .route("/v1/workflows/{id}/evaluate", post(start_evaluation))
        .route(
            "/v1/workflows/{id}/evaluations",
            get(list_workflow_evaluations),
        )
        .route("/v1/evaluations/{id}", get(get_evaluation))
        .route(
            "/v1/workflows/{id}/competitions",
            get(list_workflow_competitions),
        )
        .route("/v1/competitions/{id}", get(get_competition))
        .route("/v1/plans", post(create_plan))
        .route("/v1/plans", get(list_plans))
        .route("/v1/plans/{id}", get(get_plan))
        .route("/v1/plans/{id}/execute", post(execute_plan))
        .route("/v1/plans/{id}/edit", post(edit_plan))
        .route("/v1/plans/{id}/diff", get(diff_plan))
        .route("/v1/plans/{id}/revisions", get(list_plan_revisions))
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

    let (inherited_context_id, prior_summaries) =
        match collect_prior_tasks(&state, request.from_task_id, request.from_context_id).await {
            Ok(res) => res,
            Err(err) => return (StatusCode::NOT_FOUND, Json(err)).into_response(),
        };

    let final_prompt = agentmesh_core::format_cross_agent_prompt(&request.prompt, &prior_summaries);
    let context_id = inherited_context_id.unwrap_or_else(Uuid::new_v4);

    let mut run_request = AgentRunRequest::new(
        Uuid::new_v4(),
        context_id,
        AgentMessage::user(&final_prompt),
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
            let response = register_live_run(&state, run, request.agent_id.clone(), lease).await;
            Json(response).into_response()
        }
        Err(err) => api_error_from_task(err),
    }
}

async fn collect_prior_tasks(
    state: &SharedState,
    from_task_id: Option<Uuid>,
    from_context_id: Option<Uuid>,
) -> Result<(Option<Uuid>, Vec<agentmesh_core::PriorTaskSummary>), ApiError> {
    if let Some(task_id) = from_task_id {
        let task = state
            .task_repo
            .get(task_id)
            .await
            .map_err(|e| ApiError::new("storage_error", e.to_string()))?
            .ok_or_else(|| {
                ApiError::new("not_found", format!("prior task `{task_id}` not found"))
            })?;

        let artifacts = state
            .artifacts
            .list_by_task(task.id)
            .await
            .map_err(|e| ApiError::new("storage_error", e.to_string()))?;

        let art_summaries = artifacts
            .into_iter()
            .map(|a| {
                let size_bytes = a.content.len();
                let preview = String::from_utf8(a.content).ok();
                agentmesh_core::PriorArtifactSummary {
                    name: a.name,
                    kind: a.kind,
                    content_preview: preview,
                    size_bytes,
                }
            })
            .collect();

        let summary = agentmesh_core::PriorTaskSummary {
            task_id: task.id,
            agent_id: task.agent_id,
            status: task.status,
            prompt: task.input.content,
            error: task.error,
            artifacts: art_summaries,
            created_at: task.created_at.to_rfc3339(),
            completed_at: task.completed_at.map(|t| t.to_rfc3339()),
        };

        Ok((Some(task.context_id), vec![summary]))
    } else if let Some(context_id) = from_context_id {
        let filter = agentmesh_storage::TaskFilter::default().context(context_id);
        let tasks = state
            .task_repo
            .list(&filter)
            .await
            .map_err(|e| ApiError::new("storage_error", e.to_string()))?;

        if tasks.is_empty() {
            return Err(ApiError::new(
                "not_found",
                format!("no tasks found for prior context `{context_id}`"),
            ));
        }

        let mut summaries = Vec::new();
        for task in tasks {
            let artifacts = state
                .artifacts
                .list_by_task(task.id)
                .await
                .map_err(|e| ApiError::new("storage_error", e.to_string()))?;

            let art_summaries = artifacts
                .into_iter()
                .map(|a| {
                    let size_bytes = a.content.len();
                    let preview = String::from_utf8(a.content).ok();
                    agentmesh_core::PriorArtifactSummary {
                        name: a.name,
                        kind: a.kind,
                        content_preview: preview,
                        size_bytes,
                    }
                })
                .collect();

            summaries.push(agentmesh_core::PriorTaskSummary {
                task_id: task.id,
                agent_id: task.agent_id,
                status: task.status,
                prompt: task.input.content,
                error: task.error,
                artifacts: art_summaries,
                created_at: task.created_at.to_rfc3339(),
                completed_at: task.completed_at.map(|t| t.to_rfc3339()),
            });
        }
        Ok((Some(context_id), summaries))
    } else {
        Ok((None, Vec::new()))
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
            let response = register_live_run(&state, run, agent_id, lease).await;
            Json(response).into_response()
        }
        Err(err) => {
            drop(lease);
            api_error_from_task(err)
        }
    }
}

/// Register a ManagedTaskRun in the registry and spawn the forwarder.
///
/// `lease` keeps the session lease alive for the whole run and releases it
/// (via Drop) once the task reaches a terminal state or the stream ends.
/// Returns the run metadata; used by both the internal API and the A2A
/// backend.
pub async fn register_live_run(
    state: &SharedState,
    mut run: ManagedTaskRun,
    agent_id: String,
    lease: crate::lease::SessionLease,
) -> crate::protocol::RunResponse {
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

    RunResponse {
        task_id,
        context_id,
        agent_session_id,
        agent_id,
    }
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
    let a2a_agents = state.a2a_agents.lock().unwrap().clone();
    let mut response = serde_json::json!({
        "instance_id": state.instance_id.to_string(),
        "live_tasks": live
            .into_iter()
            .map(
                |(task_id, agent_id, agent_session_id, status)| LiveTaskInfo {
                    task_id,
                    agent_id,
                    agent_session_id,
                    status: status.as_str().to_string(),
                },
            )
            .collect::<Vec<_>>(),
    });
    response["a2a_agents"] = a2a_agents;
    Json(response).into_response()
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
    // Phase 13: interrupt every live workflow so its running step and the
    // workflow itself persist `Interrupted` (resumable later), not Cancelled.
    state.workflows.shutdown_interrupt().await;
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

// ---------- workflow handlers (Phase 12) ----------

async fn start_workflow(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(request): Json<WorkflowStartRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let options = agentmesh_orchestrator::WorkflowOptions {
        max_review_rounds: request.max_review_rounds,
        max_parallel: request.max_parallel,
    };
    match state
        .workflows
        .start_with_source(
            &request.preset,
            &request.goal,
            options,
            request.source_workspace.clone(),
        )
        .await
    {
        Ok(workflow_id) => Json(WorkflowStartResponse { workflow_id }).into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn list_workflows(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.list().await {
        Ok(workflows) => Json(workflows).into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn get_workflow(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.get(workflow_id).await {
        Ok(Some(detail)) => Json(detail).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("workflow_not_found", "workflow not found")),
        )
            .into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn cancel_workflow(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.cancel(workflow_id).await {
        Ok(()) => Json(serde_json::json!({ "cancelled": true })).into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn resume_workflow(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.resume(workflow_id).await {
        Ok(()) => Json(serde_json::json!({ "resumed": true })).into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn workflow_events(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    // Replay the persisted state, then follow the live broadcast if the
    // workflow is still running in this daemon.
    let events = match state.workflows.replay(workflow_id).await {
        Ok(events) => events,
        Err(err) => return workflow_error_response(err),
    };
    let receiver = state.workflows.subscribe(workflow_id).await;
    let stream = async_stream::stream! {
        for event in events {
            yield Ok::<_, std::convert::Infallible>(workflow_sse_event(event));
        }
        if let Some(mut receiver) = receiver {
            while let Ok(event) = receiver.recv().await {
                yield Ok::<_, std::convert::Infallible>(workflow_sse_event(event));
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

fn workflow_sse_event(event: WorkflowStreamEvent) -> Event {
    Event::default()
        .event("workflow")
        .data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string()))
}

fn workflow_error_response(err: WorkflowError) -> Response {
    let (status, code) = match &err {
        WorkflowError::NotFound(_) => (StatusCode::NOT_FOUND, "workflow_not_found"),
        WorkflowError::NotRunning(_) => (StatusCode::CONFLICT, "workflow_not_running"),
        WorkflowError::NotResumable(..) => (StatusCode::CONFLICT, "workflow_not_resumable"),
        WorkflowError::DirectoryUninitialized => {
            (StatusCode::SERVICE_UNAVAILABLE, "daemon_not_ready")
        }
        WorkflowError::InvalidState(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "workflow_invalid_state")
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    (status, Json(ApiError::new(code, err.to_string()))).into_response()
}

// ---------- Phase 17: AI planner plans ----------

async fn create_plan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(request): Json<PlanCreateRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state
        .plans
        .create_plan(&request.goal, request.agent.as_deref())
        .await
    {
        Ok(plan_id) => Json(PlanCreateResponse { plan_id }).into_response(),
        Err(err) => plan_error_response(err),
    }
}

async fn list_plans(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.plans.list().await {
        Ok(plans) => Json(plans).into_response(),
        Err(err) => plan_error_response(err),
    }
}

async fn get_plan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(plan_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.plans.get(plan_id).await {
        Ok(Some(detail)) => Json(detail).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("plan_not_found", "plan not found")),
        )
            .into_response(),
        Err(err) => plan_error_response(err),
    }
}

async fn execute_plan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(plan_id): Path<Uuid>,
    Json(request): Json<PlanExecuteRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        state
            .plans
            .preview(plan_id, request.max_parallel)
            .await
            .map(|preview| PlanExecuteResponse::Preview { preview })
    } else {
        state
            .plans
            .execute(
                plan_id,
                request.max_parallel,
                request.source_workspace.clone(),
            )
            .await
            .map(|workflow_id| PlanExecuteResponse::Workflow { workflow_id })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => plan_error_response(err),
    }
}

async fn edit_plan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(plan_id): Path<Uuid>,
    Json(request): Json<PlanEditRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.plans.edit(plan_id, &request.plan_json).await {
        Ok(revision) => Json(PlanEditResponse { plan_id, revision }).into_response(),
        Err(err) => plan_error_response(err),
    }
}

async fn diff_plan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(plan_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.plans.diff(plan_id).await {
        Ok(Some(diff)) => Json(diff).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("plan_not_found", "plan not found")),
        )
            .into_response(),
        Err(err) => plan_error_response(err),
    }
}

async fn list_plan_revisions(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(plan_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.plans.revisions(plan_id).await {
        Ok(revisions) => Json(revisions).into_response(),
        Err(err) => plan_error_response(err),
    }
}

fn plan_error_response(err: crate::planner::PlanError) -> Response {
    let (status, code) = match &err {
        crate::planner::PlanError::NotFound(_) => (StatusCode::NOT_FOUND, "plan_not_found"),
        crate::planner::PlanError::NotReady(..) => (StatusCode::CONFLICT, "plan_not_ready"),
        crate::planner::PlanError::AlreadyExecuted(_) => {
            (StatusCode::CONFLICT, "plan_already_executed")
        }
        crate::planner::PlanError::ExecutionInProgress(_) => {
            (StatusCode::CONFLICT, "plan_execution_in_progress")
        }
        crate::planner::PlanError::NotEditable(..) => (StatusCode::CONFLICT, "plan_not_editable"),
        crate::planner::PlanError::PolicyViolation(..) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "plan_policy_violation")
        }
        crate::planner::PlanError::DirectoryUninitialized => {
            (StatusCode::SERVICE_UNAVAILABLE, "daemon_not_ready")
        }
        crate::planner::PlanError::InvalidPlannerOutput(_)
        | crate::planner::PlanError::InvalidPlan(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "plan_invalid")
        }
        crate::planner::PlanError::PlannerTaskFailed(_) => {
            (StatusCode::BAD_GATEWAY, "planner_task_failed")
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    (status, Json(ApiError::new(code, err.to_string()))).into_response()
}

// ---------- Phase 19: replan ----------

async fn create_replan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
    Json(request): Json<ReplanCreateRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state
        .replans
        .create_proposal(workflow_id, &request.prompt, request.agent.as_deref())
        .await
    {
        Ok(replan_id) => Json(ReplanCreateResponse { replan_id }).into_response(),
        Err(err) => replan_error_response(err),
    }
}

async fn list_workflow_replans(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.replans.list_for(workflow_id).await {
        Ok(rows) => {
            Json(rows.into_iter().map(ReplanInfo::from).collect::<Vec<_>>()).into_response()
        }
        Err(err) => replan_error_response(err),
    }
}

async fn list_replans(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.replans.list().await {
        Ok(rows) => {
            Json(rows.into_iter().map(ReplanInfo::from).collect::<Vec<_>>()).into_response()
        }
        Err(err) => replan_error_response(err),
    }
}

async fn get_replan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(replan_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.replans.get(replan_id).await {
        Ok(Some(row)) => Json(replan_detail(&row)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("replan_not_found", "replan not found")),
        )
            .into_response(),
        Err(err) => replan_error_response(err),
    }
}

async fn apply_replan(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(replan_id): Path<Uuid>,
    Json(request): Json<ReplanApplyRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        state
            .replans
            .preview_detail(replan_id)
            .await
            .map(|preview| ReplanApplyResponse::Preview { preview })
    } else {
        state
            .replans
            .apply(replan_id)
            .await
            .map(|revision| ReplanApplyResponse::Applied {
                applied_graph_revision: revision,
            })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => replan_error_response(err),
    }
}

fn replan_detail(row: &agentmesh_storage::WorkflowReplanRow) -> ReplanDetail {
    let parsed = row.delta_json.as_deref().and_then(|json| {
        serde_json::from_str::<agentmesh_orchestrator::replan::WorkflowPlanDelta>(json).ok()
    });
    ReplanDetail {
        replan_id: row.id,
        workflow_id: row.workflow_id,
        status: row.status.clone(),
        summary: parsed.as_ref().map(|d| d.summary.clone()),
        delta: parsed,
        validation_error: row.validation_error.clone(),
        base_graph_revision: row.base_graph_revision,
        applied_graph_revision: row.applied_graph_revision,
        created_at: row.created_at.clone(),
        applied_at: row.applied_at.clone(),
    }
}

fn replan_error_response(err: crate::replan::ReplanError) -> Response {
    let (status, code) = match &err {
        crate::replan::ReplanError::NotFound(_) => (StatusCode::NOT_FOUND, "replan_not_found"),
        crate::replan::ReplanError::WorkflowNotFound(_) => {
            (StatusCode::NOT_FOUND, "workflow_not_found")
        }
        crate::replan::ReplanError::NotDag(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "workflow_not_dag")
        }
        crate::replan::ReplanError::NotReplannable(..) => {
            (StatusCode::CONFLICT, "workflow_not_replannable")
        }
        crate::replan::ReplanError::NotReady(..) => (StatusCode::CONFLICT, "replan_not_ready"),
        crate::replan::ReplanError::AlreadyApplied(_) => {
            (StatusCode::CONFLICT, "replan_already_applied")
        }
        crate::replan::ReplanError::ApplyInProgress(_) => {
            (StatusCode::CONFLICT, "replan_apply_in_progress")
        }
        crate::replan::ReplanError::ReplanStale { .. } => (StatusCode::CONFLICT, "replan_stale"),
        crate::replan::ReplanError::InvalidDelta(_)
        | crate::replan::ReplanError::InvalidCandidate(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "replan_invalid")
        }
        crate::replan::ReplanError::PolicyViolation(..) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "replan_policy_violation")
        }
        crate::replan::ReplanError::DirectoryUninitialized => {
            (StatusCode::SERVICE_UNAVAILABLE, "daemon_not_ready")
        }
        crate::replan::ReplanError::PlannerTaskFailed(_) => {
            (StatusCode::BAD_GATEWAY, "replan_planner_failed")
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    (status, Json(ApiError::new(code, err.to_string()))).into_response()
}

// ---------- Phase 20: failure recovery + lineage ----------

async fn create_recovery(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
    Json(request): Json<RecoveryCreateRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state
        .recoveries
        .propose(workflow_id, request.agent.as_deref())
        .await
    {
        Ok(recovery_id) => {
            let detail = recovery_detail(&state, recovery_id).await;
            Json(detail).into_response()
        }
        Err(err) => recovery_error_response(err),
    }
}

async fn list_workflow_recoveries(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.recoveries.list_for(workflow_id).await {
        Ok(rows) => {
            Json(rows.into_iter().map(RecoveryInfo::from).collect::<Vec<_>>()).into_response()
        }
        Err(err) => recovery_error_response(err),
    }
}

async fn list_recoveries(State(state): State<SharedState>, headers: HeaderMap) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.recoveries.list().await {
        Ok(rows) => {
            Json(rows.into_iter().map(RecoveryInfo::from).collect::<Vec<_>>()).into_response()
        }
        Err(err) => recovery_error_response(err),
    }
}

async fn get_recovery(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(recovery_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.recoveries.get(recovery_id).await {
        Ok(Some(row)) => Json(recovery_detail_from_row(&row)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("recovery_not_found", "recovery not found")),
        )
            .into_response(),
        Err(err) => recovery_error_response(err),
    }
}

async fn execute_recovery(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(recovery_id): Path<Uuid>,
    Json(request): Json<RecoveryApplyRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        state
            .recoveries
            .preview_detail(recovery_id)
            .await
            .map(|preview| RecoveryApplyResponse::Preview { preview })
    } else {
        state
            .recoveries
            .execute(recovery_id)
            .await
            .map(|recovery_workflow_id| RecoveryApplyResponse::Executed {
                recovery_workflow_id,
            })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => recovery_error_response(err),
    }
}

async fn workflow_lineage(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.lineage(workflow_id).await {
        Ok(Some(lineage)) => Json(lineage).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("workflow_not_found", "workflow not found")),
        )
            .into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn workflow_audit(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let report = state.provenance.verify_integrity(workflow_id).await;
    if !report.valid && report.total_events == 0 && !report.is_legacy {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("workflow_not_found", "workflow not found")),
        )
            .into_response();
    }
    let raw_events = match state.provenance_repo.list_for_workflow(workflow_id).await {
        Ok(evs) => evs,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("storage_error", err.to_string())),
            )
                .into_response();
        }
    };
    let events = raw_events.into_iter().map(|r| r.to_dto()).collect();
    Json(crate::protocol::WorkflowAuditResponse {
        workflow_id,
        schema_version: agentmesh_core::provenance::PROVENANCE_SCHEMA_VERSION,
        is_legacy: report.is_legacy,
        integrity_valid: report.valid,
        events_count: report.total_events,
        events,
        details: report.details,
    })
    .into_response()
}

async fn workflow_replay(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
    Json(req): Json<crate::protocol::WorkflowReplayRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let report = if req.verify_only {
        let integrity = state.provenance.verify_integrity(workflow_id).await;
        if !integrity.valid && integrity.total_events == 0 && !integrity.is_legacy {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiError::new("workflow_not_found", "workflow not found")),
            )
                .into_response();
        }
        crate::provenance_service::ReplayReport {
            workflow_id,
            passed: integrity.valid,
            is_legacy: integrity.is_legacy,
            integrity_passed: integrity.valid,
            consensus_passed: integrity.valid,
            selection_passed: integrity.valid,
            apply_passed: integrity.valid,
            policy_passed: integrity.valid,
            mismatches: integrity.failure.into_iter().collect(),
            details: integrity.details,
        }
    } else {
        state.provenance.replay_workflow(workflow_id).await
    };

    Json(crate::protocol::WorkflowReplayResponse {
        workflow_id: report.workflow_id,
        passed: report.passed,
        is_legacy: report.is_legacy,
        integrity_passed: report.integrity_passed,
        consensus_passed: report.consensus_passed,
        selection_passed: report.selection_passed,
        apply_passed: report.apply_passed,
        policy_passed: report.policy_passed,
        mismatches: report.mismatches,
        details: report.details,
    })
    .into_response()
}

async fn recovery_detail(
    state: &SharedState,
    recovery_id: Uuid,
) -> crate::protocol::RecoveryDetail {
    match state.recoveries.get(recovery_id).await {
        Ok(Some(row)) => recovery_detail_from_row(&row),
        Ok(None) | Err(_) => crate::protocol::RecoveryDetail {
            recovery_id,
            workflow_id: Uuid::nil(),
            failed_node_id: String::new(),
            status: "not_found".to_string(),
            summary: None,
            plan: None,
            validation_error: Some("recovery not found".to_string()),
            recovery_workflow_id: None,
            attempt: 0,
            created_at: String::new(),
            executed_at: None,
        },
    }
}

fn recovery_detail_from_row(row: &agentmesh_storage::WorkflowRecoveryRow) -> RecoveryDetail {
    let parsed = row.plan_json.as_deref().and_then(|json| {
        serde_json::from_str::<agentmesh_orchestrator::plan::WorkflowPlan>(json).ok()
    });
    RecoveryDetail {
        recovery_id: row.id,
        workflow_id: row.workflow_id,
        failed_node_id: row.failed_node_id.clone(),
        status: row.status.clone(),
        summary: parsed.as_ref().map(|p| p.summary.clone()),
        plan: parsed,
        validation_error: row.validation_error.clone(),
        recovery_workflow_id: row.recovery_workflow_id,
        attempt: row.attempt,
        created_at: row.created_at.clone(),
        executed_at: row.executed_at.clone(),
    }
}

fn recovery_error_response(err: crate::recovery::RecoveryError) -> Response {
    let (status, code) = match &err {
        crate::recovery::RecoveryError::NotFound(_) => {
            (StatusCode::NOT_FOUND, "recovery_not_found")
        }
        crate::recovery::RecoveryError::WorkflowNotFound(_) => {
            (StatusCode::NOT_FOUND, "workflow_not_found")
        }
        crate::recovery::RecoveryError::WorkflowNotFailed(..) => {
            (StatusCode::CONFLICT, "workflow_not_failed")
        }
        crate::recovery::RecoveryError::NoFailedNode(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "no_failed_node")
        }
        crate::recovery::RecoveryError::RecoveryLimitReached { .. } => {
            (StatusCode::CONFLICT, "recovery_limit_reached")
        }
        crate::recovery::RecoveryError::RecoveryBudgetExceeded { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, "recovery_budget_exceeded")
        }
        crate::recovery::RecoveryError::NotReady(..) => {
            (StatusCode::CONFLICT, "recovery_not_ready")
        }
        crate::recovery::RecoveryError::AlreadyExecuted(_) => {
            (StatusCode::CONFLICT, "recovery_already_executed")
        }
        crate::recovery::RecoveryError::AlreadyPending { .. } => {
            (StatusCode::CONFLICT, "recovery_already_pending")
        }
        crate::recovery::RecoveryError::ExecutionInProgress(_) => {
            (StatusCode::CONFLICT, "recovery_execution_in_progress")
        }
        crate::recovery::RecoveryError::InvalidPlan(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "recovery_invalid")
        }
        crate::recovery::RecoveryError::PolicyViolation(..) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "recovery_policy_violation",
        ),
        crate::recovery::RecoveryError::DirectoryUninitialized => {
            (StatusCode::SERVICE_UNAVAILABLE, "daemon_not_ready")
        }
        crate::recovery::RecoveryError::AnalyzerTaskFailed(_) => {
            (StatusCode::BAD_GATEWAY, "failure_analyzer_failed")
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    (status, Json(ApiError::new(code, err.to_string()))).into_response()
}

// ---------- Phase 21: multi-agent evaluation ----------

async fn start_evaluation(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
    Json(request): Json<EvaluationStartRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state
        .workflows
        .start_evaluation(
            workflow_id,
            request.evaluators,
            request.strategy.as_deref(),
            request.quorum,
        )
        .await
    {
        Ok((workflow_id, group_id)) => Json(EvaluationStartResponse {
            workflow_id,
            group_id,
        })
        .into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn list_workflow_evaluations(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.evaluation_groups(workflow_id).await {
        Ok(rows) => Json(
            rows.into_iter()
                .map(EvaluationInfo::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(err) => workflow_error_response(err),
    }
}

async fn get_evaluation(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(group_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let Ok(Some(group)) = state.workflows.evaluation_group(group_id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiError::new(
                "evaluation_not_found",
                "evaluation group not found",
            )),
        )
            .into_response();
    };
    let members = state.workflows.evaluation_members(group_id).await;
    let Ok(members) = members else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::new(
                "daemon_internal",
                "failed to load evaluation members",
            )),
        )
            .into_response();
    };
    Json(EvaluationDetail {
        group_id: group.id,
        workflow_id: group.workflow_id,
        strategy: group.strategy.clone(),
        quorum: group.quorum as usize,
        status: group.status.clone(),
        consensus: group
            .consensus
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        snapshot_hash: group.snapshot_hash.clone(),
        round: group.round as usize,
        created_at: group.created_at.clone(),
        completed_at: group.completed_at.clone(),
        members: members
            .into_iter()
            .map(EvaluationMemberInfo::from)
            .collect(),
    })
    .into_response()
}

// ---------- Phase 23: Best-of-N competition ----------

async fn list_workflow_competitions(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.competition_groups(workflow_id).await {
        Ok(groups) => {
            let mut list = Vec::new();
            for group in groups {
                let candidates = state
                    .workflows
                    .competition_candidates(group.id)
                    .await
                    .unwrap_or_default();
                list.push(crate::protocol::CompetitionGroupInfo {
                    id: group.id,
                    workflow_id: group.workflow_id,
                    source_workspace: group.source_workspace,
                    base_revision: group.base_revision,
                    candidate_count: group.candidate_count,
                    status: group.status,
                    winner_candidate_id: group.winner_candidate_id,
                    winner_task_id: group.winner_task_id,
                    winner_workspace_id: group.winner_workspace_id,
                    winner_snapshot_hash: group.winner_snapshot_hash,
                    created_at: group.created_at,
                    updated_at: group.updated_at,
                    candidates: candidates
                        .into_iter()
                        .map(|c| crate::protocol::CompetitionCandidateInfo {
                            id: c.id,
                            group_id: c.group_id,
                            candidate_id: c.candidate_id,
                            agent_id: c.agent_id,
                            session_lane: c.session_lane,
                            task_id: c.task_id,
                            workspace_id: c.workspace_id,
                            snapshot_hash: c.snapshot_hash,
                            evaluation_group_id: c.evaluation_group_id,
                            status: c.status,
                            summary: c.summary,
                            patch_path: c.patch_path,
                            consensus: None,
                            approved_count: None,
                            valid_count: None,
                            created_at: c.created_at,
                            updated_at: c.updated_at,
                        })
                        .collect(),
                });
            }
            Json(list).into_response()
        }
        Err(err) => workflow_error_response(err),
    }
}

async fn get_competition(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(group_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.workflows.competition_group(group_id).await {
        Ok(Some(group)) => {
            let candidates = state
                .workflows
                .competition_candidates(group.id)
                .await
                .unwrap_or_default();
            Json(crate::protocol::CompetitionGroupInfo {
                id: group.id,
                workflow_id: group.workflow_id,
                source_workspace: group.source_workspace,
                base_revision: group.base_revision,
                candidate_count: group.candidate_count,
                status: group.status,
                winner_candidate_id: group.winner_candidate_id,
                winner_task_id: group.winner_task_id,
                winner_workspace_id: group.winner_workspace_id,
                winner_snapshot_hash: group.winner_snapshot_hash,
                created_at: group.created_at,
                updated_at: group.updated_at,
                candidates: candidates
                    .into_iter()
                    .map(|c| crate::protocol::CompetitionCandidateInfo {
                        id: c.id,
                        group_id: c.group_id,
                        candidate_id: c.candidate_id,
                        agent_id: c.agent_id,
                        session_lane: c.session_lane,
                        task_id: c.task_id,
                        workspace_id: c.workspace_id,
                        snapshot_hash: c.snapshot_hash,
                        evaluation_group_id: c.evaluation_group_id,
                        status: c.status,
                        summary: c.summary,
                        patch_path: c.patch_path,
                        consensus: None,
                        approved_count: None,
                        valid_count: None,
                        created_at: c.created_at,
                        updated_at: c.updated_at,
                    })
                    .collect(),
            })
            .into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new(
                "competition_not_found",
                "competition group not found",
            )),
        )
            .into_response(),
        Err(err) => workflow_error_response(err),
    }
}

// ---------- Phase 13: safe apply ----------

async fn apply_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(task_id): Path<Uuid>,
    Json(request): Json<ApplyRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        state
            .apply
            .plan_task(task_id)
            .await
            .map(|plan| ApplyResponse::Plan { plan })
    } else {
        state
            .apply
            .apply_task(task_id)
            .await
            .map(|outcome| ApplyResponse::Applied { outcome })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => apply_error_response(err),
    }
}

async fn apply_workflow(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
    Json(request): Json<ApplyRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        state
            .apply
            .plan_workflow(workflow_id)
            .await
            .map(|plan| ApplyResponse::Plan { plan })
    } else {
        state
            .apply
            .apply_workflow(workflow_id)
            .await
            .map(|outcome| ApplyResponse::Applied { outcome })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => apply_error_response(err),
    }
}

fn apply_error_response(err: agentmesh_apply::ApplyError) -> Response {
    use agentmesh_apply::ApplyError as E;
    let (status, code) = match &err {
        E::TaskNotFound(_) => (StatusCode::NOT_FOUND, "task_not_found"),
        E::TaskHasNoSession(_) | E::TaskHasNoWorkspace(_) => {
            (StatusCode::CONFLICT, "task_not_applicable")
        }
        E::WorkflowNotFound(_) => (StatusCode::NOT_FOUND, "workflow_not_found"),
        E::AmbiguousApplySource(_) => (StatusCode::CONFLICT, "ambiguous_apply_source"),
        E::WorkflowNotCompleted(..) => (StatusCode::CONFLICT, "workflow_not_completed"),
        E::ReviewNotApproved(_) => (StatusCode::CONFLICT, "review_not_approved"),
        E::SourceRepositoryMissing(_) => (StatusCode::CONFLICT, "source_repository_missing"),
        E::SourceRepositoryDirty => (StatusCode::CONFLICT, "source_repository_dirty"),
        E::SourceRevisionChanged { .. } => (StatusCode::CONFLICT, "source_revision_changed"),
        E::ApplyConflict(_) => (StatusCode::CONFLICT, "apply_conflict"),
        E::UnsafeApplyPath(_) => (StatusCode::CONFLICT, "unsafe_apply_path"),
        E::SourceFileMissing(_) => (StatusCode::CONFLICT, "source_file_missing"),
        E::ApplyCheckFailed(_) => (StatusCode::CONFLICT, "apply_check_failed"),
        E::ApplyFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "apply_failed"),
        E::NoChanges => (StatusCode::CONFLICT, "no_changes"),
        E::AlreadyApplied => (StatusCode::CONFLICT, "already_applied"),
        E::ApplyInProgress => (StatusCode::CONFLICT, "apply_in_progress"),
        E::CopyFailed(..) => (StatusCode::INTERNAL_SERVER_ERROR, "copy_failed"),
        E::ApplyRollbackFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "apply_rollback_failed"),
        E::Workspace(_) => (StatusCode::CONFLICT, "workspace_unavailable"),
        E::Storage(_) | E::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    // Internal failures must not leak storage details; keep the message
    // user-actionable for everything else.
    let message = match &err {
        E::Storage(_) | E::Internal(_) => "apply failed (internal error)".to_string(),
        other => other.to_string(),
    };
    (status, Json(ApiError::new(code, message))).into_response()
}

// ---------- Phase 14: apply history + workspace lifecycle + cleanup ----------

#[derive(Debug, serde::Deserialize)]
pub struct AppliesQuery {
    pub limit: Option<usize>,
    pub status: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct WorkspacesQuery {
    pub state: Option<String>,
    pub limit: Option<usize>,
}

async fn list_applies(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<AppliesQuery>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let status = match query.status.as_deref() {
        None => None,
        Some(raw) => match agentmesh_storage::ApplyStatus::from_str(raw) {
            Some(status) => Some(status),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::new(
                        "invalid_status",
                        format!(
                            "invalid apply status `{raw}` (expected: planned, applying, completed, failed)"
                        ),
                    )),
                )
                    .into_response();
            }
        },
    };
    match state
        .applies
        .list_with_filter(query.limit.unwrap_or(20), status)
        .await
    {
        Ok(rows) => Json(rows.into_iter().map(ApplyInfo::from).collect::<Vec<_>>()).into_response(),
        Err(err) => storage_error_response(err),
    }
}

async fn get_apply(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(apply_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match state.applies.get(apply_id).await {
        Ok(Some(row)) => Json(ApplyInfo::from(row)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("apply_not_found", "apply not found")),
        )
            .into_response(),
        Err(err) => storage_error_response(err),
    }
}

async fn list_workspaces(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Query(query): Query<WorkspacesQuery>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let state_filter = match query.state.as_deref() {
        None => None,
        Some(raw) => match agentmesh_storage::WorkspaceState::from_str(raw) {
            Some(state) => Some(state),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::new(
                        "invalid_state",
                        format!(
                            "invalid workspace state `{raw}` (expected: active, applied, archived, missing, removed)"
                        ),
                    )),
                )
                    .into_response();
            }
        },
    };
    match state
        .workspaces
        .repository()
        .list_with_agent(state_filter, query.limit.unwrap_or(20))
        .await
    {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|(row, agent_id)| WorkspaceInfo {
                    id: row.id,
                    agent_id,
                    state: row.state.as_str().to_string(),
                    repository: row.repository_root.display().to_string(),
                    branch: row.branch,
                    base_revision: row.base_revision,
                    created_at: row.created_at.to_rfc3339(),
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(err) => storage_error_response(err),
    }
}

async fn archive_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(task_id): Path<Uuid>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    match cleanup::archive_task(&state, task_id).await {
        Ok(()) => Json(serde_json::json!({ "archived": true })).into_response(),
        Err(err) => cleanup_error_response(err),
    }
}

async fn cleanup_task(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(task_id): Path<Uuid>,
    Json(request): Json<CleanupRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        cleanup::plan_cleanup_task(&state, task_id)
            .await
            .map(|plan| CleanupResponse::Plan { plan })
    } else {
        cleanup::cleanup_task(&state, task_id)
            .await
            .map(|outcome| CleanupResponse::Removed { outcome })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => cleanup_error_response(err),
    }
}

async fn cleanup_workflow(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(workflow_id): Path<Uuid>,
    Json(request): Json<CleanupRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let result = if request.check {
        cleanup::plan_cleanup_workflow(&state, workflow_id)
            .await
            .map(|plans| CleanupResponse::Plans { plans })
    } else {
        cleanup::cleanup_workflow(&state, workflow_id)
            .await
            .map(|outcomes| CleanupResponse::RemovedAll { outcomes })
    };
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => cleanup_error_response(err),
    }
}

async fn prune_artifacts(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(request): Json<PruneRequest>,
) -> Response {
    if let Err(err) = auth_or_err(&headers, &state).await {
        return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
    }
    let older_than = chrono::Utc::now() - chrono::Duration::days(request.older_than_days as i64);
    match state
        .artifacts
        .prune_files(&older_than, request.check)
        .await
    {
        Ok(result) => Json(PruneResponse {
            candidates: result.candidates,
            pruned: result.pruned,
        })
        .into_response(),
        Err(err) => storage_error_response(err),
    }
}

fn storage_error_response(_err: agentmesh_storage::StorageError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiError::new("daemon_internal", "storage error")),
    )
        .into_response()
}

fn cleanup_error_response(err: CleanupError) -> Response {
    use CleanupError as E;
    let (status, code) = match &err {
        E::TaskNotFound(_) => (StatusCode::NOT_FOUND, "task_not_found"),
        E::TaskHasNoWorkspace(_) => (StatusCode::CONFLICT, "task_not_applicable"),
        E::WorkflowNotFound(_) => (StatusCode::NOT_FOUND, "workflow_not_found"),
        E::WorkflowStillActive(..) => (StatusCode::CONFLICT, "workflow_still_active"),
        E::Workspace(WorkspaceError::WorkspaceNotSafeToRemove(..)) => {
            (StatusCode::CONFLICT, "workspace_not_safe_to_remove")
        }
        E::Workspace(WorkspaceError::WorkspaceChangedAfterApply(_)) => {
            (StatusCode::CONFLICT, "workspace_changed_after_apply")
        }
        E::Workspace(WorkspaceError::WorkspaceRemoved(_)) => {
            (StatusCode::CONFLICT, "workspace_removed")
        }
        E::Workspace(WorkspaceError::NotManagedBranch(_)) => {
            (StatusCode::CONFLICT, "not_managed_branch")
        }
        E::Workspace(_) => (StatusCode::CONFLICT, "workspace_unavailable"),
        E::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
        E::Task(_) => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
        E::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "daemon_internal"),
    };
    // Internal failures must not leak details.
    let message = match &err {
        E::Storage(_) | E::Task(_) | E::Internal(_) => {
            "cleanup failed (internal error)".to_string()
        }
        other => other.to_string(),
    };
    (status, Json(ApiError::new(code, message))).into_response()
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
