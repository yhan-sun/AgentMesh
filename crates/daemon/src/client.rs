//! DaemonClient: typed HTTP/SSE client used by the CLI.

use std::path::PathBuf;
use std::time::Duration;

use futures::{Stream, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use agentmesh_orchestrator::diff::PlanDiff;

use crate::auth;
use crate::error::DaemonError;
use crate::paths::{self, Scope};
use crate::protocol::{
    ApplyInfo, ApplyRequest, ApplyResponse, CleanupRequest, CleanupResponse, CompetitionGroupInfo,
    DAEMON_PROTOCOL_VERSION, DaemonMeta, DaemonStreamEvent, EvaluationDetail, EvaluationInfo,
    EvaluationStartRequest, EvaluationStartResponse, HealthResponse, PlanCreateRequest,
    PlanCreateResponse, PlanDetail, PlanEditRequest, PlanEditResponse, PlanExecuteRequest,
    PlanExecuteResponse, PlanInfo, PlanRevisionInfo, PruneRequest, PruneResponse,
    RecoveryApplyRequest, RecoveryApplyResponse, RecoveryCreateRequest, RecoveryDetail,
    RecoveryInfo, ReplanApplyRequest, ReplanApplyResponse, ReplanCreateRequest,
    ReplanCreateResponse, ReplanDetail, ReplanInfo, ResumeRequest, RunRequest, RunResponse,
    ShutdownRequest, ShutdownResponse, WorkflowAuditResponse, WorkflowDetail, WorkflowInfo,
    WorkflowLineage, WorkflowReplayRequest, WorkflowReplayResponse, WorkflowStartRequest,
    WorkflowStartResponse, WorkflowStreamEvent, WorkspaceInfo,
};
use crate::runtime::read_metadata;

/// One decoded SSE event.
#[derive(Debug, Clone)]
pub struct SseEvent {
    pub id: Option<u64>,
    pub data: DaemonStreamEvent,
}

/// One decoded workflow SSE event.
#[derive(Debug, Clone)]
pub struct WorkflowSseEvent {
    pub id: Option<u64>,
    pub data: WorkflowStreamEvent,
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
        self.run_with_options(agent_id, prompt, workspace, None, None)
            .await
    }

    pub async fn run_with_options(
        &self,
        agent_id: &str,
        prompt: &str,
        workspace: Option<&PathBuf>,
        from_task_id: Option<Uuid>,
        from_context_id: Option<Uuid>,
    ) -> Result<RunResponse, DaemonError> {
        self.post(
            "/v1/tasks/run",
            &RunRequest {
                agent_id: agent_id.to_string(),
                prompt: prompt.to_string(),
                source_workspace: workspace.map(|p| p.display().to_string()),
                from_task_id,
                from_context_id,
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

    pub async fn runtime(&self) -> Result<serde_json::Value, DaemonError> {
        self.get("/v1/runtime").await
    }

    pub async fn shutdown(&self, force: bool) -> Result<ShutdownResponse, DaemonError> {
        self.post("/v1/shutdown", &ShutdownRequest { force }).await
    }

    // ---------- Phase 12: daemon-owned workflows ----------

    /// Start a daemon-owned workflow in the background; returns its id.
    pub async fn start_workflow(
        &self,
        preset: &str,
        goal: &str,
        max_review_rounds: usize,
        max_parallel: usize,
    ) -> Result<WorkflowStartResponse, DaemonError> {
        self.start_workflow_with_source(preset, goal, max_review_rounds, max_parallel, None)
            .await
    }

    /// Start a workflow with an explicit source workspace (Phase 22 §1); the
    /// daemon canonicalizes + validates it, never guessing from cwd.
    pub async fn start_workflow_with_source(
        &self,
        preset: &str,
        goal: &str,
        max_review_rounds: usize,
        max_parallel: usize,
        source_workspace: Option<String>,
    ) -> Result<WorkflowStartResponse, DaemonError> {
        self.post(
            "/v1/workflows",
            &WorkflowStartRequest {
                preset: preset.to_string(),
                goal: goal.to_string(),
                max_review_rounds,
                max_parallel,
                source_workspace,
            },
        )
        .await
    }

    /// List persisted workflows, newest first.
    pub async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, DaemonError> {
        self.get("/v1/workflows").await
    }

    /// Load one workflow (including its steps).
    pub async fn get_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<WorkflowDetail>, DaemonError> {
        self.get_optional(&format!("/v1/workflows/{workflow_id}"))
            .await
    }

    /// Cancel a running workflow.
    pub async fn cancel_workflow(&self, workflow_id: Uuid) -> Result<(), DaemonError> {
        self.post_raw(
            &format!("/v1/workflows/{workflow_id}/cancel"),
            &serde_json::json!({}),
        )
        .await
    }

    /// Resume an interrupted workflow in the background.
    pub async fn resume_workflow(&self, workflow_id: Uuid) -> Result<(), DaemonError> {
        self.post_raw(
            &format!("/v1/workflows/{workflow_id}/resume"),
            &serde_json::json!({}),
        )
        .await
    }

    // ---------- Phase 19: replan ----------

    /// Generate a replan proposal for a workflow (waits for the replan planner
    /// agent, which may take minutes).
    pub async fn create_replan(
        &self,
        workflow_id: Uuid,
        prompt: &str,
        agent: Option<&str>,
    ) -> Result<ReplanCreateResponse, DaemonError> {
        self.post_with_timeout(
            &format!("/v1/workflows/{workflow_id}/replan"),
            &ReplanCreateRequest {
                prompt: prompt.to_string(),
                agent: agent.map(str::to_string),
            },
            Duration::from_secs(600),
        )
        .await
    }

    /// List proposals of a workflow, newest first.
    pub async fn list_workflow_replans(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<ReplanInfo>, DaemonError> {
        self.get(&format!("/v1/workflows/{workflow_id}/replans"))
            .await
    }

    /// Load one proposal with its parsed delta.
    pub async fn get_replan(&self, replan_id: Uuid) -> Result<Option<ReplanDetail>, DaemonError> {
        self.get_optional(&format!("/v1/replans/{replan_id}")).await
    }

    /// Preview (`check = true`) or apply (`check = false`) a proposal.
    pub async fn apply_replan(
        &self,
        replan_id: Uuid,
        check: bool,
    ) -> Result<ReplanApplyResponse, DaemonError> {
        self.post(
            &format!("/v1/replans/{replan_id}/apply"),
            &ReplanApplyRequest { check },
        )
        .await
    }

    // ---------- Phase 20: failure recovery ----------

    /// Generate a recovery proposal for a failed workflow (waits for the
    /// Failure Analyzer agent). Returns the proposal detail.
    pub async fn create_recovery(&self, workflow_id: Uuid) -> Result<RecoveryDetail, DaemonError> {
        self.post_with_timeout(
            &format!("/v1/workflows/{workflow_id}/recover"),
            &RecoveryCreateRequest { agent: None },
            Duration::from_secs(600),
        )
        .await
    }

    /// List recovery proposals of a workflow, newest first.
    pub async fn list_workflow_recoveries(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<RecoveryInfo>, DaemonError> {
        self.get(&format!("/v1/workflows/{workflow_id}/recoveries"))
            .await
    }

    /// Load one recovery proposal with its parsed plan.
    pub async fn get_recovery(
        &self,
        recovery_id: Uuid,
    ) -> Result<Option<RecoveryDetail>, DaemonError> {
        self.get_optional(&format!("/v1/recoveries/{recovery_id}"))
            .await
    }

    /// Preview (`check = true`) or execute (`check = false`) a recovery.
    pub async fn execute_recovery(
        &self,
        recovery_id: Uuid,
        check: bool,
    ) -> Result<RecoveryApplyResponse, DaemonError> {
        self.post(
            &format!("/v1/recoveries/{recovery_id}/execute"),
            &RecoveryApplyRequest { check },
        )
        .await
    }

    /// A workflow's lineage (parent + recovery children).
    pub async fn workflow_lineage(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<WorkflowLineage>, DaemonError> {
        self.get_optional(&format!("/v1/workflows/{workflow_id}/lineage"))
            .await
    }

    // ---------- Phase 21: evaluation ----------

    /// Start a standalone evaluation of a workflow's latest implementation.
    pub async fn start_evaluation(
        &self,
        workflow_id: Uuid,
        evaluators: Option<usize>,
        strategy: Option<&str>,
        quorum: Option<usize>,
    ) -> Result<EvaluationStartResponse, DaemonError> {
        self.post(
            &format!("/v1/workflows/{workflow_id}/evaluate"),
            &EvaluationStartRequest {
                evaluators,
                strategy: strategy.map(str::to_string),
                quorum,
            },
        )
        .await
    }

    /// Evaluation groups of a workflow, newest first.
    pub async fn list_workflow_evaluations(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<EvaluationInfo>, DaemonError> {
        self.get(&format!("/v1/workflows/{workflow_id}/evaluations"))
            .await
    }

    /// One evaluation group with its members + consensus.
    pub async fn get_evaluation(
        &self,
        group_id: Uuid,
    ) -> Result<Option<EvaluationDetail>, DaemonError> {
        self.get_optional(&format!("/v1/evaluations/{group_id}"))
            .await
    }

    /// Competition groups of a workflow (Phase 23).
    pub async fn list_workflow_competitions(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<CompetitionGroupInfo>, DaemonError> {
        self.get(&format!("/v1/workflows/{workflow_id}/competitions"))
            .await
    }

    /// One competition group with its candidates and winner provenance (Phase 23).
    pub async fn get_competition(
        &self,
        group_id: Uuid,
    ) -> Result<Option<CompetitionGroupInfo>, DaemonError> {
        self.get_optional(&format!("/v1/competitions/{group_id}"))
            .await
    }

    // ---------- Phase 17: AI planner plans ----------

    /// Generate a plan from a goal (waits for the planner agent task, which
    /// may take minutes; uses a longer timeout than the default 60s).
    pub async fn create_plan(
        &self,
        goal: &str,
        agent: Option<&str>,
    ) -> Result<PlanCreateResponse, DaemonError> {
        self.post_with_timeout(
            "/v1/plans",
            &PlanCreateRequest {
                goal: goal.to_string(),
                agent: agent.map(str::to_string),
            },
            Duration::from_secs(600),
        )
        .await
    }

    /// List plans, newest first.
    pub async fn list_plans(&self) -> Result<Vec<PlanInfo>, DaemonError> {
        self.get("/v1/plans").await
    }

    /// Load one plan (including its preview nodes).
    pub async fn get_plan(&self, plan_id: Uuid) -> Result<Option<PlanDetail>, DaemonError> {
        self.get_optional(&format!("/v1/plans/{plan_id}")).await
    }

    /// Execute (`check = false`) or preview (`check = true`) a plan. A preview
    /// never claims the plan or creates a workflow.
    pub async fn execute_plan(
        &self,
        plan_id: Uuid,
        max_parallel: usize,
        check: bool,
    ) -> Result<PlanExecuteResponse, DaemonError> {
        self.execute_plan_with_source(plan_id, max_parallel, check, None)
            .await
    }

    /// [`Self::execute_plan`] with an explicit source workspace (Phase 22 §4).
    pub async fn execute_plan_with_source(
        &self,
        plan_id: Uuid,
        max_parallel: usize,
        check: bool,
        source_workspace: Option<String>,
    ) -> Result<PlanExecuteResponse, DaemonError> {
        self.post(
            &format!("/v1/plans/{plan_id}/execute"),
            &PlanExecuteRequest {
                max_parallel,
                check,
                source_workspace,
            },
        )
        .await
    }

    /// Replace the current revision with an edited plan (Phase 18). The edit
    /// uses the same WorkflowPlan schema as the planner output.
    pub async fn edit_plan(
        &self,
        plan_id: Uuid,
        plan_json: &str,
    ) -> Result<PlanEditResponse, DaemonError> {
        self.post(
            &format!("/v1/plans/{plan_id}/edit"),
            &PlanEditRequest {
                plan_json: plan_json.to_string(),
            },
        )
        .await
    }

    /// Structural diff between the planner output and the current revision.
    pub async fn diff_plan(&self, plan_id: Uuid) -> Result<PlanDiff, DaemonError> {
        self.get(&format!("/v1/plans/{plan_id}/diff")).await
    }

    /// Revision history of a plan, oldest first.
    pub async fn plan_revisions(
        &self,
        plan_id: Uuid,
    ) -> Result<Vec<PlanRevisionInfo>, DaemonError> {
        self.get(&format!("/v1/plans/{plan_id}/revisions")).await
    }

    // ---------- Phase 13: safe apply ----------

    /// Plan (`check = true`) or execute (`check = false`) an apply of a
    /// task's workspace result.
    pub async fn apply_task(
        &self,
        task_id: Uuid,
        check: bool,
    ) -> Result<ApplyResponse, DaemonError> {
        self.post(
            &format!("/v1/tasks/{task_id}/apply"),
            &ApplyRequest { check },
        )
        .await
    }

    /// Plan (`check = true`) or execute (`check = false`) an apply of a
    /// completed workflow's implementer/fixer result.
    pub async fn apply_workflow(
        &self,
        workflow_id: Uuid,
        check: bool,
    ) -> Result<ApplyResponse, DaemonError> {
        self.post(
            &format!("/v1/workflows/{workflow_id}/apply"),
            &ApplyRequest { check },
        )
        .await
    }

    // ---------- Phase 14: apply history + workspace lifecycle ----------

    /// List apply history, newest first.
    pub async fn list_applies(
        &self,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<ApplyInfo>, DaemonError> {
        let path = match status {
            Some(status) => format!("/v1/applies?limit={limit}&status={status}"),
            None => format!("/v1/applies?limit={limit}"),
        };
        self.get(&path).await
    }

    /// Show one apply by id.
    pub async fn get_apply(&self, apply_id: Uuid) -> Result<Option<ApplyInfo>, DaemonError> {
        self.get_optional(&format!("/v1/applies/{apply_id}")).await
    }

    /// List workspaces, optionally filtered by state.
    pub async fn list_workspaces(
        &self,
        state: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WorkspaceInfo>, DaemonError> {
        let path = match state {
            Some(state) => format!("/v1/workspaces?limit={limit}&state={state}"),
            None => format!("/v1/workspaces?limit={limit}"),
        };
        self.get(&path).await
    }

    /// Archive a task's workspace (`state → Archived`, nothing deleted).
    pub async fn archive_task(&self, task_id: Uuid) -> Result<(), DaemonError> {
        self.post_raw(
            &format!("/v1/tasks/{task_id}/archive"),
            &serde_json::json!({}),
        )
        .await
    }

    /// Plan (`check = true`) or execute (`check = false`) a cleanup of a
    /// task's workspace.
    pub async fn cleanup_task(
        &self,
        task_id: Uuid,
        check: bool,
    ) -> Result<CleanupResponse, DaemonError> {
        self.post(
            &format!("/v1/tasks/{task_id}/cleanup"),
            &CleanupRequest { check },
        )
        .await
    }

    /// Plan (`check = true`) or execute (`check = false`) a cleanup of every
    /// workspace a workflow used.
    pub async fn cleanup_workflow(
        &self,
        workflow_id: Uuid,
        check: bool,
    ) -> Result<CleanupResponse, DaemonError> {
        self.post(
            &format!("/v1/workflows/{workflow_id}/cleanup"),
            &CleanupRequest { check },
        )
        .await
    }

    /// Prune old file-backed artifacts of terminal tasks.
    pub async fn prune_artifacts(
        &self,
        older_than_days: u64,
        check: bool,
    ) -> Result<PruneResponse, DaemonError> {
        self.post(
            "/v1/artifacts/prune",
            &PruneRequest {
                older_than_days,
                check,
            },
        )
        .await
    }

    // ---------- Phase 24: Provenance, Audit, Replay & Lineage ----------

    /// Fetch the provenance audit trail and integrity verification for a workflow.
    pub async fn workflow_audit(
        &self,
        workflow_id: Uuid,
    ) -> Result<WorkflowAuditResponse, DaemonError> {
        self.get(&format!("/v1/workflows/{workflow_id}/audit"))
            .await
    }

    /// Run deterministic decision replay for a workflow.
    pub async fn workflow_replay(
        &self,
        workflow_id: Uuid,
        verify_only: bool,
    ) -> Result<WorkflowReplayResponse, DaemonError> {
        self.post(
            &format!("/v1/workflows/{workflow_id}/replay"),
            &WorkflowReplayRequest { verify_only },
        )
        .await
    }

    /// Open the workflow event stream (persisted replay + live events).
    pub fn workflow_events(
        &self,
        workflow_id: Uuid,
        after: u64,
    ) -> impl Stream<Item = Result<WorkflowSseEvent, DaemonError>> + Send + 'static {
        let url = format!(
            "{}/v1/workflows/{workflow_id}/events?after={after}",
            self.base
        );
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
                    match parse_workflow_sse_frame(&frame) {
                        Some(event) => yield Ok(event),
                        None => {}
                    }
                }
            }
        }
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

    async fn get_optional<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<Option<T>, DaemonError> {
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
            .json::<T>()
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

    /// POST with an explicit request timeout (for long-running daemon calls
    /// such as plan generation, which waits on the planner agent).
    async fn post_with_timeout<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
        timeout: Duration,
    ) -> Result<T, DaemonError> {
        let response = self
            .http
            .post(format!("{}{}", self.base, path))
            .timeout(timeout)
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

/// Parse a single workflow SSE frame (`id:`, `data:` lines).
fn parse_workflow_sse_frame(frame: &[u8]) -> Option<WorkflowSseEvent> {
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
    Some(WorkflowSseEvent { id, data: parsed })
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
