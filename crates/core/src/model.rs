//! Core domain types shared across AgentMesh crates.

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A capability an agent advertises (e.g. `code`, `architecture`, `review`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSkill {
    pub name: String,
    pub description: Option<String>,
}

impl AgentSkill {
    pub fn new(name: impl Into<String>, description: Option<String>) -> Self {
        Self {
            name: name.into(),
            description,
        }
    }
}

/// What kind of workspace an agent needs to run safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum WorkspaceRequirement {
    /// No workspace handling (e.g. the mock agent).
    #[default]
    None,
    /// Run in an existing directory (the caller's project).
    Existing,
    /// Requires an isolated Git worktree created by AgentMesh.
    IsolatedGit,
}

/// Static description of an agent, used for discovery and registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDescriptor {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub skills: Vec<AgentSkill>,
    /// Endpoint through which the agent is reached: `agent://claude` for a
    /// local process adapter, an `https://` URL for a remote A2A agent.
    pub endpoint: String,
    /// Workspace isolation the agent requires.
    #[serde(default)]
    pub workspace_requirement: WorkspaceRequirement,
}

/// Lifecycle status of an [`AgentTask`].
///
/// The string representation (`as_str`/`from_str`) is a persistence and
/// wire protocol: renaming the enum variants must not silently corrupt
/// stored databases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    /// Stable string form used when persisting to storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Submitted => "submitted",
            TaskStatus::Working => "working",
            TaskStatus::InputRequired => "input_required",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    /// Parse the stable string form; `None` for unknown values.
    ///
    /// Named `from_str` per the persistence protocol; the return type makes
    /// it deliberately distinct from `std::str::FromStr`.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "submitted" => Some(TaskStatus::Submitted),
            "working" => Some(TaskStatus::Working),
            "input_required" => Some(TaskStatus::InputRequired),
            "completed" => Some(TaskStatus::Completed),
            "failed" => Some(TaskStatus::Failed),
            "cancelled" => Some(TaskStatus::Cancelled),
            _ => None,
        }
    }

    /// Terminal states can never be left.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }

    /// Whether moving from `self` to `next` is a valid lifecycle transition.
    ///
    /// Same-state "transitions" are rejected; terminal states have no exits.
    pub fn can_transition_to(&self, next: TaskStatus) -> bool {
        use TaskStatus::*;
        if self.is_terminal() || *self == next {
            return false;
        }
        matches!(
            (self, next),
            (Submitted, Working | Failed | Cancelled)
                | (Working, InputRequired | Completed | Failed | Cancelled)
                | (InputRequired, Working | Failed | Cancelled)
        )
    }
}

/// Role of an [`AgentMessage`] within a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

/// A single message exchanged with an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessage {
    pub role: MessageRole,
    pub content: String,
}

impl AgentMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::User,
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::Assistant,
            content: content.into(),
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::System,
            content: content.into(),
        }
    }
}

/// Kind of an [`Artifact`] produced by an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ArtifactKind {
    Text,
    File,
    Patch,
    Json,
    Log,
    TestResult,
}

impl ArtifactKind {
    /// Stable snake_case key used on the wire (A2A artifact metadata) and in
    /// workflow handoffs.
    pub fn key(&self) -> &'static str {
        match self {
            ArtifactKind::Text => "text",
            ArtifactKind::File => "file",
            ArtifactKind::Patch => "patch",
            ArtifactKind::Json => "json",
            ArtifactKind::Log => "log",
            ArtifactKind::TestResult => "test_result",
        }
    }

    /// Parse a stable [`Self::key`]; `None` for unknown keys.
    pub fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "text" => ArtifactKind::Text,
            "file" => ArtifactKind::File,
            "patch" => ArtifactKind::Patch,
            "json" => ArtifactKind::Json,
            "log" => ArtifactKind::Log,
            "test_result" => ArtifactKind::TestResult,
            _ => return None,
        })
    }
}

/// An agent output that is more structured than a plain message:
/// files, patches, structured JSON, logs, test results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub id: Uuid,
    pub name: String,
    pub kind: ArtifactKind,
    pub mime_type: String,
    /// On-disk location when the artifact is persisted; `None` in-memory only.
    pub path: Option<PathBuf>,
    pub content: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

impl Artifact {
    pub fn text(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: ArtifactKind::Text,
            mime_type: "text/plain".to_string(),
            path: None,
            content: content.into().into_bytes(),
            metadata: HashMap::new(),
        }
    }

    /// Decode the artifact content as UTF-8, if possible.
    pub fn content_as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.content).ok()
    }
}

/// A unit of work executed by a single agent.
///
/// The task id is the stable identifier persisted by the storage layer;
/// adapter runs (run ids) are mapped to it by the orchestrator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTask {
    pub id: Uuid,
    pub context_id: Uuid,
    pub agent_id: String,
    pub status: TaskStatus,
    pub input: AgentMessage,
    pub artifacts: Vec<Artifact>,
    /// Task creation time (UTC). Persisted by the storage layer.
    pub created_at: DateTime<Utc>,
    /// When the task started executing (UTC), once the adapter spawns.
    pub started_at: Option<DateTime<Utc>>,
    /// When the task reached a terminal state (UTC).
    pub completed_at: Option<DateTime<Utc>>,
    /// Bounded, human-readable failure message for terminal `Failed` tasks.
    pub error: Option<String>,
    /// Working directory the task ran in, when one was requested.
    pub workspace: Option<PathBuf>,
    /// AgentMesh session this task belongs to, once sessions are tracked.
    pub agent_session_id: Option<Uuid>,
}

impl AgentTask {
    pub fn new(agent_id: impl Into<String>, input: AgentMessage) -> Self {
        Self::with_workspace(agent_id, input, None)
    }

    pub fn with_workspace(
        agent_id: impl Into<String>,
        input: AgentMessage,
        workspace: Option<PathBuf>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            context_id: Uuid::new_v4(),
            agent_id: agent_id.into(),
            status: TaskStatus::Submitted,
            input,
            artifacts: Vec::new(),
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            error: None,
            workspace,
            agent_session_id: None,
        }
    }

    pub fn add_artifact(&mut self, artifact: Artifact) {
        self.artifacts.push(artifact);
    }
}

pub const DEFAULT_SESSION_LANE: &str = "default";

fn default_session_lane() -> String {
    DEFAULT_SESSION_LANE.to_string()
}

/// A native agent session bound to a global context and session lane.
///
/// Sessions and contexts are deliberately separate: one global context may
/// span many agent sessions (e.g. `claude` -> `codex` -> resume `claude`).
/// Phase 23 generalizes this to (context_id, agent_id, session_lane).
/// The `native_session_id` belongs entirely to the concrete adapter
/// (Claude Code session id, Codex thread id, ...) and is stored per session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSession {
    pub id: Uuid,
    pub context_id: Uuid,
    /// Vendor-neutral agent identifier, e.g. `claude` or `codex`.
    pub agent_id: String,
    /// Logical lane for session isolation within a context (default: "default").
    #[serde(default = "default_session_lane")]
    pub session_lane: String,
    /// Identifier of the session inside the native agent, once the adapter
    /// reports one. May be `None` before the agent starts.
    pub native_session_id: Option<String>,
    /// Workspace the session is bound to (canonical path).
    pub workspace: Option<PathBuf>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AgentSession {
    pub fn new(context_id: Uuid, agent_id: impl Into<String>) -> Self {
        Self::with_lane(context_id, agent_id, DEFAULT_SESSION_LANE)
    }

    pub fn with_lane(
        context_id: Uuid,
        agent_id: impl Into<String>,
        session_lane: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            context_id,
            agent_id: agent_id.into(),
            session_lane: session_lane.into(),
            native_session_id: None,
            workspace: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// A global conversation context spanning one or more agents.
///
/// The context is a stable identity; sessions and tasks link back to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Context {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            created_at: now,
            updated_at: now,
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

/// High-level intent of a task, used by the router to pick an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskIntent {
    Architecture,
    Implementation,
    Debug,
    Review,
    UIUX,
    Testing,
    General,
}

impl TaskIntent {
    /// Stable snake_case key used in config (`[routing]`) and the CLI.
    pub fn key(&self) -> &'static str {
        match self {
            TaskIntent::Architecture => "architecture",
            TaskIntent::Implementation => "implementation",
            TaskIntent::Debug => "debug",
            TaskIntent::Review => "review",
            TaskIntent::Testing => "testing",
            TaskIntent::UIUX => "uiux",
            TaskIntent::General => "general",
        }
    }

    /// The skill this intent maps to (see Phase 9 routing table). The router
    /// filters agents by the skill their Agent Card declares, never by brand.
    pub fn skill(&self) -> &'static str {
        match self {
            TaskIntent::Architecture => "architecture",
            TaskIntent::Implementation => "code",
            TaskIntent::Debug => "debug",
            TaskIntent::Review => "review",
            TaskIntent::Testing => "testing",
            TaskIntent::UIUX => "ui",
            TaskIntent::General => "code",
        }
    }

    /// Parse a stable [`Self::key`]; `None` for unknown keys.
    pub fn from_key(value: &str) -> Option<Self> {
        Some(match value {
            "architecture" => TaskIntent::Architecture,
            "implementation" => TaskIntent::Implementation,
            "debug" => TaskIntent::Debug,
            "review" => TaskIntent::Review,
            "testing" => TaskIntent::Testing,
            "uiux" => TaskIntent::UIUX,
            "general" => TaskIntent::General,
            _ => return None,
        })
    }
}

/// Unified streaming event emitted by an agent adapter while a task runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentEvent {
    Started,
    StatusChanged(TaskStatus),
    Message(String),
    ArtifactUpdated(Artifact),
    Completed,
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    #[test]
    fn task_starts_submitted() {
        let task = AgentTask::new("mock", AgentMessage::user("hello"));
        assert_eq!(task.status, TaskStatus::Submitted);
        assert_eq!(task.agent_id, "mock");
        assert!(task.artifacts.is_empty());
        assert!(task.started_at.is_none());
        assert!(task.error.is_none());
    }

    #[test]
    fn artifact_text_round_trip() {
        let artifact = Artifact::text("note.md", "content");
        assert_eq!(artifact.kind, ArtifactKind::Text);
        assert_eq!(artifact.mime_type, "text/plain");
        assert_eq!(artifact.content_as_str(), Some("content"));
    }

    #[test]
    fn domain_types_serialize_to_json() {
        let descriptor = AgentDescriptor {
            id: "claude".into(),
            name: "Claude Code".into(),
            description: None,
            skills: vec![AgentSkill::new("code", None)],
            endpoint: "agent://claude".into(),
            workspace_requirement: WorkspaceRequirement::None,
        };
        let json = serde_json::to_string(&descriptor).unwrap();
        let back: AgentDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(back, descriptor);
    }

    #[test]
    fn session_and_context_are_separate_identities() {
        let ctx = Context::new();
        let claude_session = AgentSession::new(ctx.id, "claude");
        let codex_session = AgentSession::new(ctx.id, "codex");
        assert_ne!(claude_session.id, codex_session.id);
        assert_eq!(claude_session.context_id, ctx.id);
        assert_eq!(codex_session.context_id, ctx.id);
        assert_eq!(claude_session.agent_id, "claude");
        assert!(claude_session.native_session_id.is_none());
    }

    #[test]
    fn status_string_roundtrip() {
        for status in [
            TaskStatus::Submitted,
            TaskStatus::Working,
            TaskStatus::InputRequired,
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ] {
            assert_eq!(TaskStatus::from_str(status.as_str()), Some(status));
        }
        assert_eq!(TaskStatus::from_str("bogus"), None);
    }

    #[test]
    fn status_terminal_states() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
        assert!(!TaskStatus::Submitted.is_terminal());
        assert!(!TaskStatus::Working.is_terminal());
    }

    #[test]
    fn valid_transitions() {
        assert!(TaskStatus::Submitted.can_transition_to(TaskStatus::Working));
        assert!(TaskStatus::Submitted.can_transition_to(TaskStatus::Failed));
        assert!(TaskStatus::Submitted.can_transition_to(TaskStatus::Cancelled));
        assert!(TaskStatus::Working.can_transition_to(TaskStatus::InputRequired));
        assert!(TaskStatus::Working.can_transition_to(TaskStatus::Completed));
        assert!(TaskStatus::Working.can_transition_to(TaskStatus::Failed));
        assert!(TaskStatus::Working.can_transition_to(TaskStatus::Cancelled));
        assert!(TaskStatus::InputRequired.can_transition_to(TaskStatus::Working));
    }

    #[test]
    fn invalid_transitions() {
        assert!(!TaskStatus::Working.can_transition_to(TaskStatus::Working));
        assert!(!TaskStatus::Completed.can_transition_to(TaskStatus::Working));
        assert!(!TaskStatus::Failed.can_transition_to(TaskStatus::Completed));
        assert!(!TaskStatus::Cancelled.can_transition_to(TaskStatus::Completed));
        assert!(!TaskStatus::Completed.can_transition_to(TaskStatus::Failed));
        assert!(!TaskStatus::Working.can_transition_to(TaskStatus::Submitted));
    }

    #[test]
    fn intent_skills_follow_spec() {
        assert_eq!(TaskIntent::Architecture.skill(), "architecture");
        assert_eq!(TaskIntent::Implementation.skill(), "code");
        assert_eq!(TaskIntent::Debug.skill(), "debug");
        assert_eq!(TaskIntent::Review.skill(), "review");
        assert_eq!(TaskIntent::Testing.skill(), "testing");
        assert_eq!(TaskIntent::UIUX.skill(), "ui");
        assert_eq!(TaskIntent::General.skill(), "code");
    }

    #[test]
    fn intent_key_roundtrips() {
        for key in [
            "architecture",
            "implementation",
            "debug",
            "review",
            "testing",
            "uiux",
            "general",
        ] {
            let intent = TaskIntent::from_key(key).expect(key);
            assert_eq!(intent.key(), key);
        }
        assert_eq!(TaskIntent::from_key("bogus"), None);
    }

    #[test]
    fn artifact_kind_key_roundtrips() {
        for (key, kind) in [
            ("text", ArtifactKind::Text),
            ("file", ArtifactKind::File),
            ("patch", ArtifactKind::Patch),
            ("json", ArtifactKind::Json),
            ("log", ArtifactKind::Log),
            ("test_result", ArtifactKind::TestResult),
        ] {
            assert_eq!(kind.key(), key);
            assert_eq!(ArtifactKind::from_key(key), Some(kind));
        }
        assert_eq!(ArtifactKind::from_key("bogus"), None);
    }

    #[test]
    fn artifact_serializes_with_all_fields() {
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let artifact = Artifact {
            id: Uuid::new_v4(),
            name: "patch.diff".into(),
            kind: ArtifactKind::Patch,
            mime_type: "text/x-diff".into(),
            path: Some(PathBuf::from("/tmp/patch.diff")),
            content: b"diff --git".to_vec(),
            metadata,
        };
        let json = serde_json::to_string(&artifact).unwrap();
        let back: Artifact = serde_json::from_str(&json).unwrap();
        assert_eq!(back, artifact);
    }
}
