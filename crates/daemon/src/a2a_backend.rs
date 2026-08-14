//! Daemon-side implementation of the A2A backend: everything goes through
//! the existing TaskManager / LiveTaskRegistry / leases.

use std::path::PathBuf;
use std::sync::Arc;

use agentmesh_a2a::{A2ABackend, A2ABackendError, A2ARun, A2AStreamEvent};
use agentmesh_adapters::AgentRunRequest;
use agentmesh_core::{AgentMessage, TaskStatus};
use agentmesh_tasks::{ExecutionMetadata, TaskError};
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use uuid::Uuid;

use crate::server::{DaemonState, register_live_run};

/// A2A backend backed by a running daemon.
pub struct DaemonA2ABackend {
    state: Arc<DaemonState>,
}

impl DaemonA2ABackend {
    pub fn new(state: Arc<DaemonState>) -> Self {
        Self { state }
    }
}

fn backend_error(err: TaskError) -> A2ABackendError {
    match err {
        TaskError::AgentNotFound(_) => A2ABackendError::AgentNotFound(String::new()),
        TaskError::TaskNotFound(_) => A2ABackendError::TaskNotFound(Uuid::nil()),
        TaskError::SessionForAgentNotFound { .. } => A2ABackendError::SessionForAgentNotFound,
        TaskError::WorkspaceUnavailable(_) => A2ABackendError::Internal(err.to_string()),
        other => A2ABackendError::Internal(other.to_string()),
    }
}

/// Map a daemon stream event to its A2A form; task metadata is emitted by the
/// caller as the stream prefix, so it is skipped here.
fn map_a2a_event(event: crate::protocol::DaemonStreamEvent) -> Option<A2AStreamEvent> {
    match event {
        crate::protocol::DaemonStreamEvent::Agent { event } => Some(A2AStreamEvent::Agent(event)),
        crate::protocol::DaemonStreamEvent::TaskInfo { .. } => None,
        crate::protocol::DaemonStreamEvent::ReplayGap { oldest_available } => {
            Some(A2AStreamEvent::ReplayGap { oldest_available })
        }
    }
}

/// A lossless view of a live task's event history: replay buffered events
/// after `after` first (yielding a ReplayGap when the buffer no longer covers
/// that point), then the live tail. The replay buffer is the single source of
/// truth — the broadcast channel only wakes the forwarder — so there is no
/// snapshot window between a replay read and a subscription point in which a
/// pushed event could be lost. Sequence numbers deduplicate across reads.
fn live_event_stream(
    registry: crate::registry::LiveTaskRegistry,
    task_id: Uuid,
    after: u64,
) -> Pin<Box<dyn Stream<Item = A2AStreamEvent> + Send>> {
    let stream = async_stream::stream! {
        let Some(live) = registry.get(task_id).await else {
            tracing::warn!(%task_id, "a2a forwarder: live task not found");
            return;
        };
        let mut sent_after = after;
        let oldest = live.oldest_available().await;
        if after > 0 && oldest > after {
            yield A2AStreamEvent::ReplayGap { oldest_available: oldest };
        }
        let mut receiver = live.subscribe();
        loop {
            let events = live.replay_after(sent_after).await;
            if !events.is_empty() {
                tracing::debug!(%task_id, sent_after, replayed = events.len(), "a2a forwarder replay");
            }
            for (seq, event) in events {
                sent_after = seq;
                if let Some(event) = map_a2a_event(event) {
                    yield event;
                }
            }
            tokio::select! {
                _ = receiver.recv() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
            receiver = live.subscribe();
        }
    };
    Box::pin(stream)
}

impl DaemonA2ABackend {
    /// Shared run start path: acquire lease, run, register live, return A2ARun.
    async fn start_run(
        &self,
        agent_id: &str,
        _prompt: &str,
        workspace: Option<PathBuf>,
        start: impl std::future::Future<Output = Result<agentmesh_tasks::ManagedTaskRun, TaskError>>,
    ) -> Result<A2ARun, A2ABackendError> {
        let run = start.await.map_err(backend_error)?;
        let session_id = run.agent_session_id().unwrap_or_default();
        let lease = self
            .state
            .leases
            .acquire(session_id, run.task_id())
            .map_err(|_| A2ABackendError::SessionBusy)?;
        let response = register_live_run(&self.state, run, agent_id.to_string(), lease).await;
        let _ = workspace;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let info = A2AStreamEvent::TaskInfo {
            task_id: response.task_id,
            context_id: response.context_id,
            agent_session_id: Some(response.agent_session_id),
            agent_id: agent_id.to_string(),
        };
        // The A2ARun event stream is fed by the live task registry's replay.
        // register_live_run already streams into the daemon broadcast; here we
        // bridge that into the run channel for the A2A server.
        let task_id = response.task_id;
        let registry = self.state.registry.clone();
        let forwarder = tokio::spawn(async move {
            let _ = tx.send(info).await;
            let mut stream = live_event_stream(registry, task_id, 0);
            while let Some(event) = stream.next().await {
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        std::mem::drop(forwarder);
        Ok(A2ARun {
            task_id: response.task_id,
            context_id: response.context_id,
            agent_session_id: Some(response.agent_session_id),
            agent_id: agent_id.to_string(),
            events: rx,
        })
    }
}

#[async_trait]
impl A2ABackend for DaemonA2ABackend {
    async fn start(
        &self,
        agent_id: &str,
        prompt: &str,
        workspace: Option<PathBuf>,
    ) -> Result<A2ARun, A2ABackendError> {
        let mut request =
            AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), AgentMessage::user(prompt));
        request.workspace = workspace;
        let metadata = ExecutionMetadata {
            runtime_owner: Some(self.state.instance_id.to_string()),
        };
        let start = self
            .state
            .task_manager
            .start_with_metadata(agent_id, request, metadata);
        self.start_run(agent_id, prompt, None, start).await
    }

    async fn start_in_context(
        &self,
        context_id: Uuid,
        agent_id: &str,
        prompt: &str,
    ) -> Result<A2ARun, A2ABackendError> {
        self.start_in_context_with_lane(context_id, agent_id, prompt, None)
            .await
    }

    async fn start_in_context_with_lane(
        &self,
        context_id: Uuid,
        agent_id: &str,
        prompt: &str,
        session_lane: Option<&str>,
    ) -> Result<A2ARun, A2ABackendError> {
        let lane = session_lane.unwrap_or(agentmesh_core::DEFAULT_SESSION_LANE);
        // Session lease must be acquired before starting; resolve (or create,
        // when this agent joins the context for the first time in this lane) the session
        // first so a busy session is rejected without spawning anything.
        let session_id = self
            .state
            .task_manager
            .resolve_or_create_context_session_in_lane(context_id, agent_id, lane)
            .await
            .map_err(backend_error)?;
        let pending = Uuid::new_v4();
        let placeholder = self
            .state
            .leases
            .acquire(session_id, pending)
            .map_err(|_| A2ABackendError::SessionBusy)?;
        let request =
            AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), AgentMessage::user(prompt));
        let metadata = ExecutionMetadata {
            runtime_owner: Some(self.state.instance_id.to_string()),
        };
        let run = self
            .state
            .task_manager
            .start_in_context_lane_with_metadata(context_id, agent_id, lane, request, metadata)
            .await
            .map_err(backend_error)?;
        // Rebind the lease to the real task id.
        drop(placeholder);
        let task_id = run.task_id();
        let _ = self.state.leases.acquire(session_id, task_id);
        let agent_id_owned = agent_id.to_string();
        let lease = self
            .state
            .leases
            .acquire(session_id, task_id)
            .map_err(|_| A2ABackendError::SessionBusy)?;
        let response = register_live_run(&self.state, run, agent_id_owned.clone(), lease).await;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let info = A2AStreamEvent::TaskInfo {
            task_id: response.task_id,
            context_id: response.context_id,
            agent_session_id: Some(response.agent_session_id),
            agent_id: agent_id_owned,
        };
        let task_id = response.task_id;
        let registry = self.state.registry.clone();
        tokio::spawn(async move {
            let _ = tx.send(info).await;
            let mut stream = live_event_stream(registry, task_id, 0);
            while let Some(event) = stream.next().await {
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        Ok(A2ARun {
            task_id: response.task_id,
            context_id: response.context_id,
            agent_session_id: Some(response.agent_session_id),
            agent_id: agent_id.to_string(),
            events: rx,
        })
    }

    async fn get_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<(agentmesh_core::AgentTask, Vec<agentmesh_core::Artifact>)>, A2ABackendError>
    {
        let task = self
            .state
            .task_manager
            .get_task(task_id)
            .await
            .map_err(|err| A2ABackendError::Internal(err.to_string()))?;
        match task {
            Some(task) => {
                let artifacts = self
                    .state
                    .task_manager
                    .list_artifacts(task_id)
                    .await
                    .map_err(|err| A2ABackendError::Internal(err.to_string()))?;
                Ok(Some((task, artifacts)))
            }
            None => Ok(None),
        }
    }

    async fn list_tasks(
        &self,
        context_id: Option<Uuid>,
        status: Option<TaskStatus>,
        limit: usize,
    ) -> Result<Vec<(agentmesh_core::AgentTask, Vec<agentmesh_core::Artifact>)>, A2ABackendError>
    {
        let mut filter = agentmesh_storage::TaskFilter::default().limit(limit);
        if let Some(context_id) = context_id {
            filter = filter.context(context_id);
        }
        if let Some(status) = status {
            filter = filter.status(status);
        }
        let tasks = self
            .state
            .task_manager
            .list_tasks(&filter)
            .await
            .map_err(|err| A2ABackendError::Internal(err.to_string()))?;
        let mut out = Vec::new();
        for task in tasks {
            let artifacts = self
                .state
                .task_manager
                .list_artifacts(task.id)
                .await
                .map_err(|err| A2ABackendError::Internal(err.to_string()))?;
            out.push((task, artifacts));
        }
        Ok(out)
    }

    async fn cancel(&self, task_id: Uuid) -> Result<(), A2ABackendError> {
        tracing::debug!(?task_id, "backend cancel");
        if let Some(live) = self.state.registry.get(task_id).await {
            if live.status.read().await.is_terminal() {
                tracing::debug!(?task_id, "backend cancel: already terminal");
                return Ok(());
            }
            return live
                .cancel()
                .await
                .map_err(|err| A2ABackendError::Internal(err.to_string()));
        }
        tracing::debug!(?task_id, "backend cancel: not live");
        Err(A2ABackendError::TaskNotLive)
    }

    async fn subscribe(
        &self,
        task_id: Uuid,
        after: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = A2AStreamEvent> + Send>>, A2ABackendError> {
        let live = self
            .state
            .registry
            .get(task_id)
            .await
            .ok_or(A2ABackendError::TaskNotLive)?;
        if live.status.read().await.is_terminal() {
            return Err(A2ABackendError::Unsupported);
        }
        Ok(live_event_stream(
            self.state.registry.clone(),
            task_id,
            after,
        ))
    }
}
