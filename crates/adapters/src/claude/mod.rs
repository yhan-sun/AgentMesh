//! Adapter for Claude Code (`claude`).
//!
//! Launch strategy (verified against Claude Code 2.1.227):
//!
//! ```text
//! claude -p --verbose --output-format stream-json <prompt>
//! ```
//!
//! `--output-format stream-json` requires `--print` and `--verbose`; the CLI
//! errors out otherwise. Output is newline-delimited JSON, parsed by
//! [`ClaudeParser`]. Sessions are resumed with `-r <session_id>`.

mod parser;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use agentmesh_core::{
    AgentConfig, AgentDescriptor, AgentEvent, AgentSkill, TaskStatus, WorkspaceRequirement,
};
use agentmesh_runtime::{Process, ProcessCancelHandle, ProcessSpec};
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tracing::instrument;
use uuid::Uuid;

use super::adapter::{AgentHealth, AgentRunHandle, AgentRunRequest, CodingAgentAdapter};
use super::error::AgentError;
use parser::ClaudeParser;

/// Max characters of stderr retained for failure messages.
const MAX_STDERR: usize = 4096;

/// Adapter that drives the Claude Code CLI over its stream-json protocol.
pub struct ClaudeAdapter {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
    runs: Arc<Mutex<HashMap<Uuid, ProcessCancelHandle>>>,
}

impl ClaudeAdapter {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            runs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build the adapter from a config section (`[agents.claude]`).
    pub fn from_config(config: &AgentConfig) -> Self {
        let mut adapter = Self::new(
            config
                .command
                .clone()
                .unwrap_or_else(|| "claude".to_string()),
        );
        adapter.args = config.args.clone();
        adapter.env = config.env.clone();
        adapter
    }

    fn build_spec(
        &self,
        prompt: &str,
        resume_session: Option<&str>,
        workspace: Option<&Path>,
    ) -> ProcessSpec {
        let mut spec = ProcessSpec::new(&self.command);
        if let Some(workspace) = workspace {
            spec = spec.cwd(workspace);
        }
        for arg in &self.args {
            spec = spec.arg(arg);
        }
        spec = spec
            .arg("-p")
            .arg("--verbose")
            .arg("--output-format")
            .arg("stream-json");
        if let Some(session_id) = resume_session {
            spec = spec.arg("-r").arg(session_id);
        }
        spec = spec.arg(prompt);
        for (key, value) in &self.env {
            tracing::debug!(
                agent_id = "claude",
                env_key = key,
                "setting environment variable"
            );
            spec = spec.env(key, value);
        }
        spec
    }

    async fn spawn_run(
        &self,
        request: AgentRunRequest,
        resume_session: Option<&str>,
        initial_session: Option<String>,
    ) -> Result<AgentRunHandle, AgentError> {
        let run_id = Uuid::new_v4();
        let spec = self.build_spec(
            &request.input.content,
            resume_session,
            request.workspace.as_deref(),
        );
        tracing::debug!(agent_id = "claude", run_id = %run_id, task_id = %request.task_id, "spawning claude process");

        let mut process = match Process::spawn(spec).await {
            Ok(process) => process,
            Err(err) => {
                return Err(match err {
                    agentmesh_runtime::RuntimeError::Spawn { program, message } => {
                        AgentError::CommandNotFound(format!(
                            "command '{program}' not found or not executable ({message})"
                        ))
                    }
                    other => AgentError::Agent(
                        "claude".to_string(),
                        format!("failed to spawn claude: {other}"),
                    ),
                });
            }
        };
        let cancel_handle = process.cancel_handle();
        self.runs.lock().unwrap().insert(run_id, cancel_handle);

        let (session_tx, session_rx) = watch::channel(initial_session);
        let (tx, rx) = mpsc::channel(256);
        let handle = AgentRunHandle::with_session_channel(run_id, rx, session_rx);
        let runs = self.runs.clone();
        tokio::spawn(async move {
            let mut parser = ClaudeParser::new();
            let mut stderr = String::new();
            let mut exit_code: Option<i32> = None;
            let mut sent_session: Option<String> = None;
            let _ = tx.send(AgentEvent::Started).await;

            while let Some(event) = process.next().await {
                match event {
                    agentmesh_runtime::ProcessEvent::Stdout(line) => {
                        for agent_event in parser.parse_line(&line) {
                            if let Some(session_id) = parser.session_id()
                                && sent_session.as_deref() != Some(session_id)
                            {
                                let _ = session_tx.send(Some(session_id.to_string()));
                                sent_session = Some(session_id.to_string());
                            }
                            let _ = tx.send(agent_event).await;
                        }
                    }
                    agentmesh_runtime::ProcessEvent::Stderr(line) => {
                        tracing::debug!(agent_id = "claude", run_id = %run_id, "claude stderr: {line}");
                        if stderr.len() < MAX_STDERR {
                            stderr.push_str(&line);
                            stderr.push('\n');
                        }
                    }
                    agentmesh_runtime::ProcessEvent::Exit(code) => {
                        exit_code = Some(code);
                    }
                }
            }

            match exit_code {
                Some(-1) => {
                    let _ = tx
                        .send(AgentEvent::StatusChanged(TaskStatus::Cancelled))
                        .await;
                }
                Some(code) if code != 0 => {
                    let message = summarize_stderr(&stderr, code);
                    let _ = tx.send(AgentEvent::Failed(message)).await;
                }
                _ => {
                    if !parser.saw_terminal() {
                        let _ = tx
                            .send(AgentEvent::Failed(
                                "claude exited without a result event".to_string(),
                            ))
                            .await;
                    }
                }
            }

            runs.lock().unwrap().remove(&run_id);
        });

        Ok(handle)
    }
}

fn summarize_stderr(stderr: &str, code: i32) -> String {
    let detail = stderr.trim();
    if detail.is_empty() {
        format!("claude exited with code {code}")
    } else {
        format!("claude exited with code {code}: {detail}")
    }
}

#[async_trait]
impl CodingAgentAdapter for ClaudeAdapter {
    fn id(&self) -> &str {
        "claude"
    }

    fn name(&self) -> &str {
        "Claude Code"
    }

    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: "claude".to_string(),
            name: "Claude Code".to_string(),
            description: Some(
                "Anthropic's coding agent, driven over its stream-json protocol".to_string(),
            ),
            skills: vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("debug", None),
                AgentSkill::new("review", None),
            ],
            endpoint: "agent://claude".to_string(),
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
        }
    }

    #[instrument(skip_all, fields(agent_id = "claude", command = %self.command))]
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        let spec = ProcessSpec::new(&self.command).arg("--version");
        let mut process = match Process::spawn(spec).await {
            Ok(process) => process,
            Err(_) => {
                return Ok(AgentHealth::offline(
                    Some(self.command.clone()),
                    "command not found or not executable",
                ));
            }
        };

        let mut version_stdout = String::new();
        let mut exit_code: Option<i32> = None;
        let probe = async {
            while let Some(event) = process.next().await {
                match event {
                    agentmesh_runtime::ProcessEvent::Stdout(line) => {
                        version_stdout.push_str(&line);
                    }
                    agentmesh_runtime::ProcessEvent::Stderr(line) => {
                        tracing::debug!(agent_id = "claude", "claude --version stderr: {line}");
                    }
                    agentmesh_runtime::ProcessEvent::Exit(code) => {
                        exit_code = Some(code);
                        break;
                    }
                }
            }
        };
        if tokio::time::timeout(Duration::from_secs(10), probe)
            .await
            .is_err()
        {
            return Ok(AgentHealth::offline(
                Some(self.command.clone()),
                "health check timed out",
            ));
        }

        if exit_code != Some(0) {
            return Ok(AgentHealth::offline(
                Some(self.command.clone()),
                format!(
                    "`{} --version` failed with exit code {exit_code:?}",
                    self.command
                ),
            ));
        }

        let version = version_stdout
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().next())
            .map(str::to_string);
        Ok(AgentHealth::online(version, Some(self.command.clone())))
    }

    #[instrument(skip_all, fields(agent_id = "claude", task_id = %request.task_id))]
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request, None, None).await
    }

    #[instrument(skip_all, fields(agent_id = "claude", native_session_id = %native_session_id, task_id = %request.task_id))]
    async fn resume(
        &self,
        native_session_id: &str,
        request: AgentRunRequest,
    ) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(
            request,
            Some(native_session_id),
            Some(native_session_id.to_string()),
        )
        .await
    }

    #[instrument(skip_all, fields(agent_id = "claude", run_id = %run_id))]
    async fn cancel(&self, run_id: &str) -> Result<(), AgentError> {
        let run_id = Uuid::parse_str(run_id)
            .map_err(|_| AgentError::InvalidRequest(format!("invalid run id `{run_id}`")))?;
        let handle = self
            .runs
            .lock()
            .unwrap()
            .get(&run_id)
            .cloned()
            .ok_or_else(|| AgentError::NotFound(run_id.to_string()))?;
        handle
            .cancel()
            .await
            .map_err(|err| AgentError::Agent("claude".to_string(), err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::HealthStatus;
    use agentmesh_core::AgentMessage;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentmesh-claude-test-{}-{tag}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_script(dir: &std::path::Path, name: &str, body: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, body).expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&path).expect("metadata").permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).expect("chmod");
        }
        path.to_string_lossy().to_string()
    }

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

    async fn drain_with_timeout(handle: &mut AgentRunHandle) -> Vec<AgentEvent> {
        tokio::time::timeout(Duration::from_secs(10), drain(handle))
            .await
            .expect("run did not finish within 10s")
    }

    #[tokio::test]
    async fn health_check_offline_when_command_missing() {
        let adapter = ClaudeAdapter::new("/nonexistent/agentmesh/claude");
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Offline);
        assert!(health.version.is_none());
    }

    #[tokio::test]
    async fn health_check_online_with_fake_command() {
        let dir = temp_dir("health");
        let script = write_script(
            &dir,
            "fake-claude",
            "#!/bin/sh\necho '2.1.227 (Claude Code)'\n",
        );
        let adapter = ClaudeAdapter::new(script);
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Online);
        assert_eq!(health.version.as_deref(), Some("2.1.227"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_streams_fake_claude_lifecycle() {
        let dir = temp_dir("run");
        let script = write_script(
            &dir,
            "fake-claude",
            r#"#!/bin/sh
echo '{"type":"system","subtype":"init","session_id":"sid-abc"}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"hello from fake"}]}}'
echo '{"type":"result","subtype":"success","session_id":"sid-abc"}'
"#,
        );
        let adapter = ClaudeAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;

        assert_eq!(events.first(), Some(&AgentEvent::Started));
        assert!(events.contains(&AgentEvent::Message("hello from fake".to_string())));
        assert_eq!(events.last(), Some(&AgentEvent::Completed));
        assert_eq!(handle.session_id().as_deref(), Some("sid-abc"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_passes_session_flag() {
        let dir = temp_dir("resume");
        let script = write_script(
            &dir,
            "fake-claude",
            r#"#!/bin/sh
if [ "$5" = "-r" ]; then
  echo '{"type":"result","subtype":"success","session_id":"sid-resume","result":"resumed ok"}'
else
  echo "expected -r flag, got: $@" >&2
  exit 9
fi
"#,
        );
        let adapter = ClaudeAdapter::new(script);
        let mut handle = adapter
            .resume("sid-resume", request("continue"))
            .await
            .expect("resume");
        let events = drain_with_timeout(&mut handle).await;
        assert!(events.contains(&AgentEvent::Completed));
        assert_eq!(handle.session_id().as_deref(), Some("sid-resume"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn nonzero_exit_emits_failed_with_stderr() {
        let dir = temp_dir("fail");
        let script = write_script(
            &dir,
            "fake-claude",
            "#!/bin/sh\necho 'boom happened' >&2\nexit 3\n",
        );
        let adapter = ClaudeAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, AgentEvent::Failed(msg) if msg.contains("boom happened") && msg.contains("3"))),
            "expected failed event with stderr, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancel_kills_running_run() {
        let dir = temp_dir("cancel");
        let script = write_script(
            &dir,
            "fake-claude",
            "#!/bin/sh\necho '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sid-c\"}'\nsleep 30\n",
        );
        let adapter = ClaudeAdapter::new(script);
        let handle = adapter.start(request("hello")).await.expect("start");
        adapter
            .cancel(&handle.run_id().to_string())
            .await
            .expect("cancel");
        let mut handle = handle;
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, AgentEvent::StatusChanged(TaskStatus::Cancelled))),
            "expected cancellation event, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancel_unknown_run_returns_not_found() {
        let adapter = ClaudeAdapter::new("/nonexistent/agentmesh/claude");
        let err = adapter.cancel("00000000-0000-0000-0000-000000000000").await;
        assert!(matches!(err, Err(AgentError::NotFound(_))));
    }

    #[tokio::test]
    async fn start_with_missing_command_returns_command_not_found() {
        let adapter = ClaudeAdapter::new("/nonexistent/agentmesh/claude");
        let err = adapter.start(request("hello")).await;
        assert!(
            matches!(err, Err(AgentError::CommandNotFound(ref msg)) if msg.contains("nonexistent")),
            "expected CommandNotFound, got {err:?}"
        );
    }
}
