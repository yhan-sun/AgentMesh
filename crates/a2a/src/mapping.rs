//! AgentMesh domain ↔ A2A protocol mapping (no provider branches).

use std::path::Path;

use agentmesh_core::{AgentTask, Artifact, ArtifactKind, TaskStatus};
use serde_json::json;

use crate::types::{
    A2AArtifact, DataPart, File, FilePart, Message, Part, Role, Task, TaskState,
    TaskStatus as A2AStatus, TextPart,
};

/// Metadata key carrying the AgentMesh [`ArtifactKind`] over A2A (the A2A
/// spec has no artifact-kind field; metadata is the natural carrier).
pub const ARTIFACT_KIND_META_KEY: &str = "agentmesh_kind";

/// Map an AgentMesh task status to A2A state.
pub fn task_state(status: TaskStatus) -> TaskState {
    match status {
        TaskStatus::Submitted => TaskState::Submitted,
        TaskStatus::Working => TaskState::Working,
        TaskStatus::InputRequired => TaskState::InputRequired,
        TaskStatus::Completed => TaskState::Completed,
        TaskStatus::Failed => TaskState::Failed,
        TaskStatus::Cancelled => TaskState::Canceled,
    }
}

/// Build an A2A Task from an AgentMesh task and its artifacts.
pub fn to_task(task: &AgentTask, artifacts: &[Artifact]) -> Task {
    let status = A2AStatus {
        state: task_state(task.status),
        message: task.error.clone(),
        timestamp: task.completed_at.map(|t| t.to_rfc3339()),
    };
    Task {
        id: task.id,
        context_id: Some(task.context_id),
        state: task_state(task.status),
        messages: Some(vec![Message {
            role: Role::User,
            parts: vec![Part::Text(TextPart {
                text: task.input.content.clone(),
            })],
        }]),
        artifacts: Some(artifacts.iter().map(to_artifact).collect::<Vec<_>>()),
        status: Some(status),
        history: None,
        metadata: Some(json!({ "agent_id": task.agent_id })),
    }
}

/// Map an AgentMesh artifact to an A2A artifact.
///
/// File-backed artifacts are referenced by URI, never re-read. The artifact
/// kind is carried in metadata (see [`ARTIFACT_KIND_META_KEY`]) so consumers
/// such as the workflow handoff can filter by type.
pub fn to_artifact(artifact: &Artifact) -> A2AArtifact {
    let part = match artifact.kind {
        ArtifactKind::Text | ArtifactKind::Patch | ArtifactKind::Log | ArtifactKind::TestResult => {
            let text = artifact
                .content_as_str()
                .map(|s| s.to_string())
                .unwrap_or_default();
            Part::Text(TextPart { text })
        }
        ArtifactKind::Json => Part::Data(DataPart {
            data: serde_json::from_str(artifact.content_as_str().unwrap_or("{}"))
                .unwrap_or(serde_json::Value::Null),
        }),
        ArtifactKind::File => match &artifact.path {
            Some(path) => Part::File(FilePart {
                file: File {
                    name: artifact.name.clone(),
                    mime_type: Some(artifact.mime_type.clone()),
                    bytes: None,
                    uri: Some(path_uri(path)),
                },
            }),
            None => Part::Text(TextPart {
                text: artifact
                    .content_as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
            }),
        },
    };
    let mut metadata = artifact.metadata.clone();
    metadata.insert(
        ARTIFACT_KIND_META_KEY.to_string(),
        artifact.kind.key().to_string(),
    );
    A2AArtifact {
        name: artifact.name.clone(),
        parts: vec![part],
        metadata: Some(serde_json::to_value(&metadata).unwrap_or_else(|_| json!({}))),
    }
}

/// Local file URI for a path.
fn path_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}
