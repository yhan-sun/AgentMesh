//! TaskManager: task lifecycle orchestration with persistence.
//!
//! Owns the mapping between agent events and persisted task state, and the
//! context / agent session lifecycle:
//!
//! ```text
//! adapter stream
//!      ↓
//! TaskManager (persists state + native session, forwards events)
//!      ↓
//! consumer stream → CLI / API
//! ```

use std::sync::Arc;

use agentmesh_adapters::{AgentRegistry, AgentRunHandle, AgentRunRequest};
use agentmesh_core::{
    AgentEvent, AgentSession, AgentTask, Artifact, ArtifactKind, Context, TaskStatus,
    WorkspaceRequirement,
};
use agentmesh_storage::{
    AgentSessionRepository, ArtifactRepository, ContextRepository, TaskFilter, TaskRepository,
};
use agentmesh_workspace::{Workspace, WorkspaceError, WorkspaceManager};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Vendor-neutral execution context passed to task creation.
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetadata {
    /// Daemon instance id that owns the live runtime of this task.
    pub runtime_owner: Option<String>,
}

/// Errors produced by the TaskManager.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("agent `{0}` not found")]
    AgentNotFound(String),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("failed to start agent `{0}`: {1}")]
    AdapterStart(String, agentmesh_adapters::AgentError),

    #[error("invalid status transition {from:?} -> {to:?} for task {task_id}")]
    InvalidTransition {
        task_id: String,
        from: TaskStatus,
        to: TaskStatus,
    },

    #[error("task `{0}` not found")]
    TaskNotFound(Uuid),

    #[error("context `{0}` not found")]
    ContextNotFound(Uuid),

    #[error("agent session `{0}` not found")]
    AgentSessionNotFound(Uuid),

    #[error("task `{0}` has no persisted agent session and cannot be resumed")]
    NativeSessionUnavailable(Uuid),

    #[error("cannot resume session because its workspace no longer exists: {0}")]
    WorkspaceUnavailable(String),

    #[error("workspace error: {0}")]
    Workspace(#[from] agentmesh_workspace::WorkspaceError),

    #[error("agent `{agent_id}` has no session in context {context_id}")]
    SessionForAgentNotFound { context_id: Uuid, agent_id: String },
}

/// A running task: the persisted task ids plus a live event stream.
pub struct ManagedTaskRun {
    task_id: Uuid,
    context_id: Uuid,
    agent_session_id: Option<Uuid>,
    agent_id: String,
    run_id: Uuid,
    events: mpsc::Receiver<AgentEvent>,
    manager: TaskManager,
}

impl ManagedTaskRun {
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }

    pub fn context_id(&self) -> Uuid {
        self.context_id
    }

    pub fn agent_session_id(&self) -> Option<Uuid> {
        self.agent_session_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Receive the next streaming event; `None` once the stream is exhausted.
    pub async fn next_event(&mut self) -> Option<AgentEvent> {
        self.events.recv().await
    }

    /// Cancel the underlying agent process (kills the live process).
    pub async fn cancel(&self) -> Result<(), TaskError> {
        self.manager.cancel_run(&self.agent_id, &self.run_id).await
    }
}

/// Task manager: creates persisted tasks, runs them through adapters and
/// streams events back to the caller.
#[derive(Clone)]
pub struct TaskManager {
    registry: Arc<AgentRegistry>,
    tasks: TaskRepository,
    artifacts: ArtifactRepository,
    contexts: ContextRepository,
    sessions: AgentSessionRepository,
    workspaces: Arc<WorkspaceManager>,
}

impl TaskManager {
    pub fn new(
        registry: Arc<AgentRegistry>,
        tasks: TaskRepository,
        artifacts: ArtifactRepository,
        contexts: ContextRepository,
        sessions: AgentSessionRepository,
        workspaces: Arc<WorkspaceManager>,
    ) -> Self {
        Self {
            registry,
            tasks,
            artifacts,
            contexts,
            sessions,
            workspaces,
        }
    }

    /// Start a fresh task on `agent_id`: creates a new context, an agent
    /// session and the task (in one transaction) before the adapter is
    /// contacted, so failures still leave a consistent record.
    pub async fn start(
        &self,
        agent_id: &str,
        request: AgentRunRequest,
    ) -> Result<ManagedTaskRun, TaskError> {
        self.start_with_metadata(agent_id, request, ExecutionMetadata::default())
            .await
    }

    /// Start with execution metadata (e.g. the owning daemon instance).
    pub async fn start_with_metadata(
        &self,
        agent_id: &str,
        request: AgentRunRequest,
        metadata: ExecutionMetadata,
    ) -> Result<ManagedTaskRun, TaskError> {
        let context = Context::new();
        let mut session = AgentSession::new(context.id, agent_id);
        session.workspace = request.workspace.clone();

        let mut task =
            AgentTask::with_workspace(agent_id, request.input.clone(), request.workspace.clone());
        task.context_id = context.id;
        task.agent_session_id = Some(session.id);

        self.contexts
            .create_run_setup(&context, &session, &task)
            .await?;
        if let Some(owner) = &metadata.runtime_owner {
            self.tasks.set_runtime_owner(task.id, owner).await?;
        }
        tracing::debug!(
            task_id = %task.id,
            context_id = %context.id,
            session_id = %session.id,
            agent = agent_id,
            "task persisted as submitted with context and session"
        );

        let adapter = match self.registry.get(agent_id) {
            Ok(adapter) => adapter,
            Err(err) => {
                let message = err.to_string();
                self.tasks.set_error(task.id, &message).await?;
                return Err(TaskError::AgentNotFound(agent_id.to_string()));
            }
        };

        // Isolated workspace for agents that require it. Failures leave a
        // Failed task instead of an orphaned run.
        let requirement = adapter.descriptor().workspace_requirement;
        let source_path = request
            .workspace
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let execution_workspace = match requirement {
            WorkspaceRequirement::IsolatedGit => {
                match self
                    .workspaces
                    .ensure_workspace(&session, &source_path)
                    .await
                {
                    Ok(workspace) => {
                        // Bind session + task to the isolated worktree.
                        let path = workspace.path.clone();
                        if let Err(err) = self.sessions.set_workspace(session.id, Some(&path)).await
                        {
                            self.tasks.set_error(task.id, &err.to_string()).await?;
                            return Err(err.into());
                        }
                        self.tasks.set_workspace(task.id, Some(&path)).await?;
                        Some(workspace)
                    }
                    Err(err) => {
                        let message = err.to_string();
                        self.tasks.set_error(task.id, &message).await?;
                        return Err(err.into());
                    }
                }
            }
            _ => None,
        };

        let run_request = self.build_request(&request, &task);
        let run_request = match &execution_workspace {
            Some(workspace) => {
                let mut request = run_request;
                request.workspace = Some(workspace.path.clone());
                request
            }
            None => run_request,
        };
        let handle = match adapter.start(run_request).await {
            Ok(handle) => handle,
            Err(err) => {
                let message = err.to_string();
                self.tasks.set_error(task.id, &message).await?;
                return Err(TaskError::AdapterStart(agent_id.to_string(), err));
            }
        };

        self.tasks.mark_started(task.id).await?;
        let run = self.wrap_run(task, session.id, execution_workspace, handle);
        Ok(run)
    }

    /// Resume a previous task: follows the task's context + agent session to
    /// the persisted native session id and continues it. Always creates a
    /// **new** task; the source task is never modified.
    pub async fn resume(
        &self,
        source_task_id: Uuid,
        request: AgentRunRequest,
    ) -> Result<ManagedTaskRun, TaskError> {
        self.resume_with_metadata(source_task_id, request, ExecutionMetadata::default())
            .await
    }

    /// Start a task inside an existing context for the given agent.
    ///
    /// Reuses the context's agent session (and its native session +
    /// workspace when available): the session's native session is resumed
    /// when one exists, otherwise the agent starts fresh inside the context.
    /// Creates a new task; the caller must hold the session lease.
    pub async fn start_in_context(
        &self,
        context_id: Uuid,
        agent_id: &str,
        request: AgentRunRequest,
    ) -> Result<ManagedTaskRun, TaskError> {
        self.start_in_context_with_metadata(
            context_id,
            agent_id,
            request,
            ExecutionMetadata::default(),
        )
        .await
    }

    /// [`Self::start_in_context`] with execution metadata.
    pub async fn start_in_context_with_metadata(
        &self,
        context_id: Uuid,
        agent_id: &str,
        request: AgentRunRequest,
        metadata: ExecutionMetadata,
    ) -> Result<ManagedTaskRun, TaskError> {
        self.start_in_context_lane_with_metadata(
            context_id,
            agent_id,
            agentmesh_core::DEFAULT_SESSION_LANE,
            request,
            metadata,
        )
        .await
    }

    /// [`Self::start_in_context`] with a specific session lane and execution metadata.
    pub async fn start_in_context_lane_with_metadata(
        &self,
        context_id: Uuid,
        agent_id: &str,
        session_lane: &str,
        request: AgentRunRequest,
        metadata: ExecutionMetadata,
    ) -> Result<ManagedTaskRun, TaskError> {
        self.ensure_context(context_id).await?;
        let session = self
            .sessions
            .get_by_context_agent_lane(context_id, agent_id, session_lane)
            .await?
            .ok_or(TaskError::SessionForAgentNotFound {
                context_id,
                agent_id: agent_id.to_string(),
            })?;

        // Workspace: reuse the session's workspace when bound; otherwise
        // provision an isolated worktree for a new agent joining the context
        // (Phase 11), deriving the source repository from an existing
        // workspace in the context.
        let execution_workspace = self.provision_context_workspace(&session, &request).await?;
        let workspace = execution_workspace
            .as_ref()
            .map(|workspace| workspace.path.clone());

        let adapter = self
            .registry
            .get(agent_id)
            .map_err(|_| TaskError::AgentNotFound(agent_id.to_string()))?;

        let mut task =
            AgentTask::with_workspace(agent_id, request.input.clone(), workspace.clone());
        task.context_id = context_id;
        task.agent_session_id = Some(session.id);

        self.contexts
            .create_task_for_context(&self.ensure_context(context_id).await?, &session, &task)
            .await?;
        if let Some(owner) = &metadata.runtime_owner {
            self.tasks.set_runtime_owner(task.id, owner).await?;
        }

        let mut run_request = self.build_request(&request, &task);
        run_request.workspace = workspace;

        // Continue the native session when one exists; otherwise start fresh
        // inside the context.
        let handle = match session.native_session_id.as_deref() {
            Some(native) => {
                tracing::debug!(
                    task_id = %task.id,
                    context_id = %context_id,
                    session_id = %session.id,
                    native_session = shortened(native),
                    "context continuation: resuming native session"
                );
                match adapter.resume(native, run_request).await {
                    Ok(handle) => handle,
                    Err(err) => {
                        let message = err.to_string();
                        self.tasks.set_error(task.id, &message).await?;
                        return Err(TaskError::AdapterStart(agent_id.to_string(), err));
                    }
                }
            }
            None => match adapter.start(run_request).await {
                Ok(handle) => handle,
                Err(err) => {
                    let message = err.to_string();
                    self.tasks.set_error(task.id, &message).await?;
                    return Err(TaskError::AdapterStart(agent_id.to_string(), err));
                }
            },
        };

        self.tasks.mark_started(task.id).await?;
        let run = self.wrap_run(task, session.id, execution_workspace, handle);
        Ok(run)
    }

    /// Resume with execution metadata (e.g. the owning daemon instance).
    pub async fn resume_with_metadata(
        &self,
        source_task_id: Uuid,
        request: AgentRunRequest,
        metadata: ExecutionMetadata,
    ) -> Result<ManagedTaskRun, TaskError> {
        let source = self
            .tasks
            .get(source_task_id)
            .await?
            .ok_or(TaskError::TaskNotFound(source_task_id))?;

        let session_id = source
            .agent_session_id
            .ok_or(TaskError::NativeSessionUnavailable(source_task_id))?;
        let session = self
            .sessions
            .get(session_id)
            .await?
            .ok_or(TaskError::AgentSessionNotFound(session_id))?;

        let native_session_id = session
            .native_session_id
            .as_deref()
            .ok_or(TaskError::NativeSessionUnavailable(source_task_id))?
            .to_string();

        // The session workspace wins over the current directory. For agents
        // with isolated workspaces this is the worktree path; verify it
        // through the workspace manager.
        let workspace = match &session.workspace {
            Some(path) => {
                if !path.exists() {
                    return Err(TaskError::WorkspaceUnavailable(path.display().to_string()));
                }
                Some(path.clone())
            }
            None => None,
        };
        let execution_workspace = match self.workspaces.workspace_for_session(session.id).await {
            Ok(workspace) => Some(workspace),
            Err(WorkspaceError::WorkspaceNotFound(_)) => None,
            Err(err) => return Err(err.into()),
        };

        let agent_id = session.agent_id.clone();
        let adapter = self
            .registry
            .get(&agent_id)
            .map_err(|_| TaskError::AgentNotFound(agent_id.clone()))?;

        let mut task =
            AgentTask::with_workspace(&agent_id, request.input.clone(), workspace.clone());
        task.context_id = session.context_id;
        task.agent_session_id = Some(session.id);

        self.contexts
            .create_task_for_context(
                &self.ensure_context(session.context_id).await?,
                &session,
                &task,
            )
            .await?;
        if let Some(owner) = &metadata.runtime_owner {
            self.tasks.set_runtime_owner(task.id, owner).await?;
        }
        tracing::debug!(
            task_id = %task.id,
            context_id = %task.context_id,
            session_id = %session.id,
            native_session = shortened(&native_session_id),
            "resume task persisted; continuing native session"
        );

        let mut run_request = self.build_request(&request, &task);
        run_request.workspace = workspace;

        let handle = match adapter.resume(&native_session_id, run_request).await {
            Ok(handle) => handle,
            Err(err) => {
                let message = err.to_string();
                self.tasks.set_error(task.id, &message).await?;
                return Err(TaskError::AdapterStart(agent_id.clone(), err));
            }
        };

        self.tasks.mark_started(task.id).await?;
        let run = self.wrap_run(task, session.id, execution_workspace, handle);
        Ok(run)
    }

    /// Resolve which agent session a resume would target, without starting
    /// anything. Used by the daemon to acquire the session lease *before*
    /// invoking `resume`.
    pub async fn resolve_resume_target(
        &self,
        source_task_id: Uuid,
    ) -> Result<(Uuid, Uuid), TaskError> {
        let source = self
            .tasks
            .get(source_task_id)
            .await?
            .ok_or(TaskError::TaskNotFound(source_task_id))?;
        let session_id = source
            .agent_session_id
            .ok_or(TaskError::NativeSessionUnavailable(source_task_id))?;
        Ok((source.context_id, session_id))
    }

    /// Resolve the agent session bound to a (context, agent) pair, without
    /// starting anything. Used by A2A contextId continuation.
    pub async fn resolve_context_session(
        &self,
        context_id: Uuid,
        agent_id: &str,
    ) -> Result<Uuid, TaskError> {
        self.resolve_context_session_in_lane(
            context_id,
            agent_id,
            agentmesh_core::DEFAULT_SESSION_LANE,
        )
        .await
    }

    /// Resolve the agent session bound to a (context, agent, lane) tuple.
    pub async fn resolve_context_session_in_lane(
        &self,
        context_id: Uuid,
        agent_id: &str,
        session_lane: &str,
    ) -> Result<Uuid, TaskError> {
        self.sessions
            .get_by_context_agent_lane(context_id, agent_id, session_lane)
            .await?
            .map(|session| session.id)
            .ok_or(TaskError::SessionForAgentNotFound {
                context_id,
                agent_id: agent_id.to_string(),
            })
    }

    /// Resolve the agent session for a (context, agent) pair, creating it
    /// when the agent joins the context for the first time.
    ///
    /// A workflow spans several agents inside one context; each agent gets
    /// its own session (Phase 10 invariant: `claude` -> `codex` creates a
    /// session per agent, while resuming `claude` reuses its session). The
    /// caller must hold the session lease before starting a task.
    pub async fn resolve_or_create_context_session(
        &self,
        context_id: Uuid,
        agent_id: &str,
    ) -> Result<Uuid, TaskError> {
        self.resolve_or_create_context_session_in_lane(
            context_id,
            agent_id,
            agentmesh_core::DEFAULT_SESSION_LANE,
        )
        .await
    }

    /// Resolve or create the agent session for a (context, agent, session_lane) tuple.
    pub async fn resolve_or_create_context_session_in_lane(
        &self,
        context_id: Uuid,
        agent_id: &str,
        session_lane: &str,
    ) -> Result<Uuid, TaskError> {
        self.ensure_context(context_id).await?;
        if let Some(session) = self
            .sessions
            .get_by_context_agent_lane(context_id, agent_id, session_lane)
            .await?
        {
            return Ok(session.id);
        }
        let session = AgentSession::with_lane(context_id, agent_id, session_lane);
        if let Err(create_err) = self.sessions.create(&session).await {
            // Concurrent creation for the same (context, agent, lane) is serialized
            // by the UNIQUE(context_id, agent_id, session_lane) constraint; re-fetch instead
            // of failing when a peer won the race.
            if let Some(session) = self
                .sessions
                .get_by_context_agent_lane(context_id, agent_id, session_lane)
                .await?
            {
                tracing::debug!(
                    context_id = %context_id,
                    agent = agent_id,
                    lane = session_lane,
                    "agent session created concurrently; reusing it"
                );
                return Ok(session.id);
            }
            return Err(create_err.into());
        }
        tracing::debug!(
            session_id = %session.id,
            context_id = %context_id,
            agent = agent_id,
            lane = session_lane,
            "created new agent session inside existing context and lane"
        );
        Ok(session.id)
    }

    /// Ensure a session running in a context has an isolated worktree.
    ///
    /// Reuses the session workspace when already bound. For a fresh session
    /// whose agent requires isolation, creates a worktree in the context's
    /// repository, deriving the source repository from another workspace
    /// already bound to the context (never another agent's dirty worktree).
    async fn provision_context_workspace(
        &self,
        session: &AgentSession,
        request: &AgentRunRequest,
    ) -> Result<Option<Workspace>, TaskError> {
        // Reuse an already-bound session workspace (verifies it still exists).
        match self.workspaces.workspace_for_session(session.id).await {
            Ok(workspace) => return Ok(Some(workspace)),
            Err(WorkspaceError::WorkspaceNotFound(_)) => {}
            Err(err) => return Err(err.into()),
        }

        // Only agents that require isolation get a worktree.
        let adapter = self
            .registry
            .get(&session.agent_id)
            .map_err(|_| TaskError::AgentNotFound(session.agent_id.clone()))?;
        if adapter.descriptor().workspace_requirement != WorkspaceRequirement::IsolatedGit {
            return Ok(None);
        }

        // Source repository: a workspace already bound to the context, else
        // the caller's requested workspace, else the current directory.
        let source_path = match self.context_workspace_root(session.context_id).await? {
            Some(root) => root,
            None => request
                .workspace
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(".")),
        };
        let workspace = self
            .workspaces
            .ensure_workspace(session, &source_path)
            .await?;
        self.sessions
            .set_workspace(session.id, Some(&workspace.path))
            .await?;
        Ok(Some(workspace))
    }

    /// The repository root of the first active workspace bound to any session
    /// in a context, if one exists. This is the *original* repository, not a
    /// sibling worktree.
    async fn context_workspace_root(
        &self,
        context_id: Uuid,
    ) -> Result<Option<std::path::PathBuf>, TaskError> {
        for session in self.sessions.list_by_context(context_id).await? {
            if let Ok(Some(row)) = self
                .workspaces
                .repository()
                .get_by_agent_session(session.id)
                .await
                && row.state == agentmesh_storage::WorkspaceState::Active
            {
                return Ok(Some(row.repository_root));
            }
        }
        Ok(None)
    }

    async fn ensure_context(&self, context_id: Uuid) -> Result<Context, TaskError> {
        self.contexts
            .get(context_id)
            .await?
            .ok_or(TaskError::ContextNotFound(context_id))
    }

    /// Build the adapter request with the manager-owned task/context ids.
    fn build_request(&self, request: &AgentRunRequest, task: &AgentTask) -> AgentRunRequest {
        AgentRunRequest {
            task_id: task.id,
            context_id: task.context_id,
            input: request.input.clone(),
            session_id: None,
            workspace: request.workspace.clone(),
        }
    }

    fn wrap_run(
        &self,
        task: AgentTask,
        agent_session_id: Uuid,
        execution_workspace: Option<Workspace>,
        handle: AgentRunHandle,
    ) -> ManagedTaskRun {
        let (tx, rx) = mpsc::channel(256);
        let run = ManagedTaskRun {
            task_id: task.id,
            context_id: task.context_id,
            agent_session_id: Some(agent_session_id),
            agent_id: task.agent_id.clone(),
            run_id: handle.run_id(),
            events: rx,
            manager: self.clone(),
        };
        self.spawn_forwarder(
            task.id,
            agent_session_id,
            task.agent_id.clone(),
            execution_workspace,
            handle,
            tx,
        );
        run
    }

    /// Spawn the event forwarder: persist state and the native session id on
    /// the way, forward events to the caller.
    fn spawn_forwarder(
        &self,
        task_id: Uuid,
        agent_session_id: Uuid,
        agent_id: String,
        execution_workspace: Option<Workspace>,
        mut handle: AgentRunHandle,
        tx: mpsc::Sender<AgentEvent>,
    ) {
        let tasks = self.tasks.clone();
        let artifacts = self.artifacts.clone();
        let sessions = self.sessions.clone();
        let registry = self.registry.clone();
        let workspaces = self.workspaces.clone();

        tokio::spawn(async move {
            let mut session_rx = handle.session_rx();

            // The session id may already be published before we subscribed:
            // persist it up front, then wait for changes.
            let initial = session_rx.borrow_and_update().clone();
            if let Some(native_session_id) = initial
                && !persist_session(
                    &sessions,
                    &registry,
                    &agent_id,
                    &handle,
                    agent_session_id,
                    task_id,
                    &tx,
                    &tasks,
                    &native_session_id,
                )
                .await
            {
                return;
            }

            loop {
                tokio::select! {
                    changed = session_rx.changed() => {
                        match changed {
                            Err(_) => {
                                // Watch channel closed; keep forwarding events.
                            }
                            Ok(()) => {
                                // Clone out of the borrow guard before any
                                // await so the future stays Send. The update
                                // also advances the version so `changed()`
                                // does not fire again immediately.
                                let current = session_rx.borrow_and_update().clone();
                                if let Some(native_session_id) = current
                                    && !persist_session(
                                        &sessions,
                                        &registry,
                                        &agent_id,
                                        &handle,
                                        agent_session_id,
                                        task_id,
                                        &tx,
                                        &tasks,
                                        &native_session_id,
                                    )
                                    .await
                                    {
                                        return;
                                    }
                            }
                        }
                    }
                    event = handle.next_event() => {
                        let Some(event) = event else { break };
                        match &event {
                            AgentEvent::Started => {
                                let _ = tasks.set_status(task_id, TaskStatus::Working).await;
                            }
                            AgentEvent::StatusChanged(status) => {
                                let _ = tasks.set_status(task_id, *status).await;
                            }
                            AgentEvent::ArtifactUpdated(artifact) => {
                                if let Err(err) = artifacts.insert(task_id, artifact).await {
                                    tracing::warn!(task_id = %task_id, error = %err, "failed to persist artifact");
                                }
                            }
                            AgentEvent::Completed => {
                                let _ = tasks.mark_completed(task_id).await;
                                // Generate the cumulative workspace diff
                                // artifact for isolated-workspace agents.
                                if let Some(workspace) = &execution_workspace
                                    && let Err(err) = persist_diff_artifact(
                                        &workspaces,
                                        workspace,
                                        task_id,
                                        &artifacts,
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        task_id = %task_id,
                                        error = %err,
                                        "failed to persist workspace diff artifact"
                                    );
                                }
                            }
                            AgentEvent::Failed(message) => {
                                let bounded = bound_error(message);
                                let _ = tasks.set_error(task_id, &bounded).await;
                            }
                            AgentEvent::Message(_) => {}
                        }
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Cancel a live run by its adapter run id.
    pub async fn cancel_run(&self, agent_id: &str, run_id: &Uuid) -> Result<(), TaskError> {
        let adapter = self
            .registry
            .get(agent_id)
            .map_err(|_| TaskError::AgentNotFound(agent_id.to_string()))?;
        adapter
            .cancel(&run_id.to_string())
            .await
            .map_err(|err| TaskError::AdapterStart(agent_id.to_string(), err))
    }

    /// Look up a persisted task (convenience for callers without a repository).
    pub async fn get_task(&self, task_id: Uuid) -> Result<Option<AgentTask>, TaskError> {
        Ok(self.tasks.get(task_id).await?)
    }

    /// List tasks with the given filter.
    pub async fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<AgentTask>, TaskError> {
        Ok(self.tasks.list(filter).await?)
    }

    /// Access the agent registry (for discovery).
    pub fn registry(&self) -> &Arc<AgentRegistry> {
        &self.registry
    }

    /// List artifacts of a task.
    pub async fn list_artifacts(&self, task_id: Uuid) -> Result<Vec<Artifact>, TaskError> {
        Ok(self.artifacts.list_by_task(task_id).await?)
    }
}

/// Persist a native session id; on failure fails the task and cancels the
/// agent. Returns `false` when the forwarder must stop.
#[allow(clippy::too_many_arguments)]
async fn persist_session(
    sessions: &AgentSessionRepository,
    registry: &Arc<AgentRegistry>,
    agent_id: &str,
    handle: &AgentRunHandle,
    agent_session_id: Uuid,
    task_id: Uuid,
    tx: &mpsc::Sender<AgentEvent>,
    tasks: &TaskRepository,
    native_session_id: &str,
) -> bool {
    if let Err(err) = sessions
        .set_native_session_id(agent_session_id, native_session_id)
        .await
    {
        // Persisting the session mapping is a hard requirement: without it
        // resume would silently break.
        tracing::error!(
            task_id = %task_id,
            session_id = %agent_session_id,
            error = %err,
            "failed to persist native session id"
        );
        let _ = tx
            .send(AgentEvent::Failed(format!(
                "failed to persist native session: {err}"
            )))
            .await;
        let _ = tasks
            .set_error(task_id, "failed to persist native session")
            .await;
        if let Ok(adapter) = registry.get(agent_id) {
            let _ = adapter.cancel(&handle.run_id().to_string()).await;
        }
        return false;
    }
    tracing::debug!(
        task_id = %task_id,
        native_session = shortened(native_session_id),
        "persisted native session id"
    );
    true
}

/// Generate and persist the cumulative workspace diff artifact (if any).
async fn persist_diff_artifact(
    workspaces: &WorkspaceManager,
    workspace: &Workspace,
    task_id: Uuid,
    artifacts: &ArtifactRepository,
) -> Result<(), agentmesh_storage::StorageError> {
    let diff = match workspaces.diff(workspace).await {
        Ok(diff) => diff,
        Err(err) => {
            tracing::warn!(error = %err, "workspace diff failed");
            return Ok(());
        }
    };
    if diff.is_empty() {
        return Ok(());
    }

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("scope".to_string(), "workspace".to_string());
    metadata.insert("base_revision".to_string(), workspace.base_revision.clone());
    metadata.insert(
        "changed_files".to_string(),
        diff.changed_files.len().to_string(),
    );
    if !diff.untracked_files.is_empty() {
        metadata.insert(
            "untracked_files".to_string(),
            diff.untracked_files
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    let mut artifact = Artifact::text("changes.patch", diff.patch);
    artifact.kind = ArtifactKind::Patch;
    artifact.mime_type = "text/x-diff".to_string();
    artifact.metadata = metadata;
    artifacts.insert(task_id, &artifact).await
}

/// Log-safe shortening of a native session id (not a credential, but noisy).
fn shortened(id: &str) -> String {
    if id.len() <= 12 {
        id.to_string()
    } else {
        format!("{}…", &id[..12])
    }
}

/// Bound the error message persisted to the database.
fn bound_error(message: &str) -> String {
    const MAX_ERROR: usize = 16 * 1024;
    if message.len() <= MAX_ERROR {
        message.to_string()
    } else {
        format!("{}… (truncated)", &message[..MAX_ERROR])
    }
}
