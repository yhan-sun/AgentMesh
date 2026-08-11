//! Built-in mock agent used for development and tests without real CLIs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentSkill, Artifact, TaskStatus, WorkspaceRequirement,
};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::instrument;
use uuid::Uuid;

use crate::adapter::{AgentHealth, AgentRunHandle, AgentRunRequest, CodingAgentAdapter};
use crate::error::AgentError;

/// Cancellation flags of in-flight runs, keyed by run id.
type CancellationMap = Arc<Mutex<HashMap<Uuid, Arc<AtomicBool>>>>;

/// A fake agent that echoes the prompt and emits a text artifact.
///
/// Useful to exercise the full task pipeline (start -> stream -> complete)
/// without any external CLI installed.
#[derive(Default)]
pub struct MockAgentAdapter {
    cancellations: CancellationMap,
}

impl MockAgentAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    fn spawn_run(&self, request: AgentRunRequest) -> AgentRunHandle {
        let (tx, rx) = mpsc::channel(256);
        let run_id = Uuid::new_v4();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancellations
            .lock()
            .unwrap()
            .insert(run_id, cancel_flag.clone());

        let content = request.input.content.clone();
        let artifact_text = format!("Mock agent received: {content}");
        let cancellations = self.cancellations.clone();
        tokio::spawn(async move {
            let cancelled = || cancel_flag.load(Ordering::Relaxed);
            if tx.send(AgentEvent::Started).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            if cancelled() {
                let _ = tx
                    .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                    .await;
            } else if tx.send(AgentEvent::Message(content)).await.is_err() {
                return;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if cancelled() {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                        .await;
                } else {
                    let artifact = Artifact::text("summary.md", artifact_text);
                    if tx
                        .send(AgentEvent::ArtifactUpdated(artifact))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if cancelled() {
                        let _ = tx
                            .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                            .await;
                    } else {
                        let _ = tx.send(AgentEvent::Completed).await;
                    }
                }
            }
            cancellations.lock().unwrap().remove(&run_id);
        });

        AgentRunHandle::new(run_id, rx)
    }
}

#[async_trait]
impl CodingAgentAdapter for MockAgentAdapter {
    fn id(&self) -> &str {
        "mock"
    }

    fn name(&self) -> &str {
        "Mock Agent"
    }

    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: "mock".to_string(),
            name: "Mock Agent".to_string(),
            description: Some(
                "Built-in test agent that echoes prompts and emits a text artifact".to_string(),
            ),
            skills: vec![AgentSkill::new("mock", None)],
            endpoint: "agent://mock".to_string(),
            workspace_requirement: WorkspaceRequirement::None,
        }
    }

    #[instrument(skip_all, fields(agent_id = "mock"))]
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        Ok(AgentHealth::online(
            Some(env!("CARGO_PKG_VERSION").to_string()),
            None,
        ))
    }

    #[instrument(skip_all, fields(agent_id = "mock", task_id = %request.task_id))]
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        Ok(self.spawn_run(request))
    }

    #[instrument(skip_all, fields(agent_id = "mock", task_id = %request.task_id))]
    async fn resume(
        &self,
        native_session_id: &str,
        request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        tracing::debug!("resuming mock session {native_session_id}");
        Ok(self.spawn_run(request))
    }

    #[instrument(skip_all, fields(agent_id = "mock", run_id = %run_id))]
    async fn cancel(&self, run_id: &str) -> Result<(), AgentError> {
        let run_id = Uuid::parse_str(run_id)
            .map_err(|_| AgentError::InvalidRequest(format!("invalid run id `{run_id}`")))?;
        let flag = self
            .cancellations
            .lock()
            .unwrap()
            .get(&run_id)
            .cloned()
            .ok_or_else(|| AgentError::NotFound(run_id.to_string()))?;
        flag.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::HealthStatus;
    use agentmesh_core::AgentMessage;

    fn request(prompt: &str) -> AgentRunRequest {
        AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), AgentMessage::user(prompt))
    }

    async fn drain(handle: &mut AgentRunHandle) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        while let Some(event) = handle.next_event().await {
            let done = matches!(
                event,
                AgentEvent::Completed
                    | AgentEvent::Failed(_)
                    | AgentEvent::StatusChanged(TaskStatus::Cancelled)
            );
            events.push(event);
            if done {
                break;
            }
        }
        events
    }

    #[tokio::test]
    async fn health_check_reports_online() {
        let adapter = MockAgentAdapter::new();
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Online);
    }

    #[tokio::test]
    async fn run_streams_full_lifecycle() {
        let adapter = MockAgentAdapter::new();
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = tokio::time::timeout(Duration::from_secs(5), drain(&mut handle))
            .await
            .expect("run did not finish within 5s");

        assert_eq!(events.first(), Some(&AgentEvent::Started));
        assert!(events.contains(&AgentEvent::Message("hello".to_string())));
        assert!(events.iter().any(
            |event| matches!(event, AgentEvent::ArtifactUpdated(artifact) if artifact.name == "summary.md")
        ));
        assert_eq!(events.last(), Some(&AgentEvent::Completed));
    }

    #[tokio::test]
    async fn cancel_interrupts_run() {
        let adapter = MockAgentAdapter::new();
        let handle = adapter.start(request("hello")).await.expect("start");
        adapter
            .cancel(&handle.run_id().to_string())
            .await
            .expect("cancel");
        let mut handle = handle;
        let events = tokio::time::timeout(Duration::from_secs(5), drain(&mut handle))
            .await
            .expect("run did not finish within 5s");

        assert!(
            events
                .iter()
                .any(|event| matches!(event, AgentEvent::StatusChanged(TaskStatus::Cancelled))),
            "expected a cancellation event, got {events:?}"
        );
        assert!(!events.contains(&AgentEvent::Completed));
    }
}
