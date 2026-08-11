//! Adapter for Codex (`codex`).
//!
//! Launch strategy (verified against codex-cli 0.147.0):
//!
//! ```text
//! codex exec --json -s <sandbox> <prompt>
//! codex exec resume --json <session_id> <prompt>
//! ```
//!
//! Output is newline-delimited JSON parsed by [`CodexParser`]. Codex's
//! `thread_id` maps to the AgentMesh native session id. The sandbox defaults
//! to `read-only`; `workspace-write` requires explicit configuration.
//! `danger-full-access` is never used by AgentMesh.
//!
//! Note: `codex exec resume` does not accept `-s/--sandbox`; a resumed
//! session keeps the sandbox of the original run.

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
use parser::CodexParser;

/// Max characters of stderr retained for failure messages.
const MAX_STDERR: usize = 8192;

/// Adapter that drives the Codex CLI over its `exec --json` protocol.
pub struct CodexAdapter {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
    sandbox: SandboxMode,
    runs: Arc<Mutex<HashMap<Uuid, ProcessCancelHandle>>>,
}

/// Sandbox policies AgentMesh is willing to pass to Codex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
}

impl SandboxMode {
    fn as_arg(self) -> &'static str {
        match self {
            SandboxMode::ReadOnly => "read-only",
            SandboxMode::WorkspaceWrite => "workspace-write",
        }
    }
}

impl CodexAdapter {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            sandbox: SandboxMode::ReadOnly,
            runs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build the adapter from a config section (`[agents.codex]`).
    ///
    /// Recognized options: `sandbox` (`read-only` default, `workspace-write`).
    /// `danger-full-access` is rejected.
    pub fn from_config(config: &AgentConfig) -> Result<Self, AgentError> {
        let mut adapter = Self::new(
            config
                .command
                .clone()
                .unwrap_or_else(|| "codex".to_string()),
        );
        adapter.args = config.args.clone();
        adapter.env = config.env.clone();
        adapter.sandbox = match config.options.get("sandbox").map(String::as_str) {
            None | Some("read-only") => SandboxMode::ReadOnly,
            Some("workspace-write") => SandboxMode::WorkspaceWrite,
            Some("danger-full-access") => return Err(AgentError::InvalidRequest(
                "sandbox 'danger-full-access' is not allowed; use 'read-only' or 'workspace-write'"
                    .to_string(),
            )),
            Some(other) => {
                return Err(AgentError::InvalidRequest(format!(
                    "invalid sandbox `{other}`; use 'read-only' or 'workspace-write'"
                )));
            }
        };
        Ok(adapter)
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
        spec = spec.arg("exec");
        match resume_session {
            Some(session_id) => {
                spec = spec.arg("resume").arg("--json").arg(session_id);
            }
            None => {
                spec = spec.arg("--json").arg("-s").arg(self.sandbox.as_arg());
            }
        }
        spec = spec.arg(prompt);
        for (key, value) in &self.env {
            tracing::debug!(
                agent_id = "codex",
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
        tracing::debug!(agent_id = "codex", run_id = %run_id, task_id = %request.task_id, sandbox = self.sandbox.as_arg(), "spawning codex process");

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
                        "codex".to_string(),
                        format!("failed to spawn codex: {other}"),
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
            let mut parser = CodexParser::new();
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
                        tracing::debug!(agent_id = "codex", run_id = %run_id, "codex stderr: {line}");
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
                        let message = parser
                            .last_error()
                            .map(str::to_string)
                            .unwrap_or_else(|| "codex exited without a terminal event".to_string());
                        let _ = tx.send(AgentEvent::Failed(message)).await;
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
        format!("codex exited with code {code}")
    } else if detail.contains("Not inside a trusted directory") {
        // Friendly enhancement on top of the exit-code signal only.
        "Codex requires a Git repository for this run (not inside a trusted directory).".to_string()
    } else {
        format!("codex exited with code {code}: {detail}")
    }
}

#[async_trait]
impl CodingAgentAdapter for CodexAdapter {
    fn id(&self) -> &str {
        "codex"
    }

    fn name(&self) -> &str {
        "Codex"
    }

    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: "codex".to_string(),
            name: "Codex".to_string(),
            description: Some(
                "OpenAI's coding agent, driven over its exec JSONL protocol".to_string(),
            ),
            skills: vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("debug", None),
                AgentSkill::new("review", None),
                AgentSkill::new("testing", None),
            ],
            endpoint: "agent://codex".to_string(),
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
        }
    }

    #[instrument(skip_all, fields(agent_id = "codex", command = %self.command))]
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        // 1. Binary present + version.
        let version = match run_simple_command(&self.command, &["--version"]).await {
            Some(stdout) => parse_version(&stdout),
            None => {
                return Ok(AgentHealth::offline(
                    Some(self.command.clone()),
                    "command not found or not executable",
                ));
            }
        };

        // 2. Authentication via the official CLI command (credentials are
        //    never read or logged).
        let authenticated = match run_simple_command(&self.command, &["login", "status"]).await {
            Some(stdout) => {
                tracing::debug!(agent_id = "codex", "codex login status succeeded");
                let _ = stdout;
                true
            }
            None => false,
        };

        if authenticated {
            Ok(AgentHealth::online(version, Some(self.command.clone())))
        } else {
            Ok(AgentHealth {
                status: super::adapter::HealthStatus::Offline,
                version,
                command: Some(self.command.clone()),
                message: Some("authentication required (run `codex login`)".to_string()),
                details: Some("codex is installed but not authenticated".to_string()),
            })
        }
    }

    #[instrument(skip_all, fields(agent_id = "codex", task_id = %request.task_id))]
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request, None, None).await
    }

    #[instrument(skip_all, fields(agent_id = "codex", native_session_id = %native_session_id, task_id = %request.task_id))]
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

    #[instrument(skip_all, fields(agent_id = "codex", run_id = %run_id))]
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
            .map_err(|err| AgentError::Agent("codex".to_string(), err.to_string()))
    }
}

/// Run a simple `<command> <args>` probe; returns captured stdout on success
/// (exit code 0), `None` on spawn failure, non-zero exit or timeout.
async fn run_simple_command(command: &str, args: &[&str]) -> Option<String> {
    let mut spec = ProcessSpec::new(command);
    for arg in args {
        spec = spec.arg(*arg);
    }
    let mut process = Process::spawn(spec).await.ok()?;

    let mut stdout = String::new();
    let mut exit_code: Option<i32> = None;
    let probe = async {
        while let Some(event) = process.next().await {
            match event {
                agentmesh_runtime::ProcessEvent::Stdout(line) => stdout.push_str(&line),
                agentmesh_runtime::ProcessEvent::Stderr(line) => {
                    tracing::debug!(command = command, "probe stderr: {line}");
                }
                agentmesh_runtime::ProcessEvent::Exit(code) => {
                    exit_code = Some(code);
                    break;
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), probe)
        .await
        .ok()?;
    (exit_code == Some(0)).then_some(stdout)
}

/// Extract a version-like token from CLI output, e.g. `codex-cli 0.147.0`.
fn parse_version(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .next()?
        .split_whitespace()
        .find(|token| token.chars().any(|c| c.is_ascii_digit()))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmesh_core::AgentMessage;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("agentmesh-codex-test-{}-{tag}", std::process::id()));
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

    const THREAD_STARTED: &str =
        r#"{"type":"thread.started","thread_id":"019fef17-7ec9-76a0-855f-87bb9d399bfd"}"#;
    const TURN_STARTED: &str = r#"{"type":"turn.started"}"#;
    const AGENT_MESSAGE: &str =
        r#"{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"hello"}}"#;
    const TURN_COMPLETED: &str = r#"{"type":"turn.completed","usage":{"input_tokens":1}}"#;

    #[tokio::test]
    async fn health_check_offline_when_command_missing() {
        let adapter = CodexAdapter::new("/nonexistent/agentmesh/codex");
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, super::super::adapter::HealthStatus::Offline);
        assert!(health.version.is_none());
    }

    #[tokio::test]
    async fn health_check_online_with_fake_command() {
        let dir = temp_dir("health");
        let script = write_script(
            &dir,
            "fake-codex",
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex-cli 0.147.0'; fi\nif [ \"$1\" = \"login\" ]; then exit 0; fi\n",
        );
        let adapter = CodexAdapter::new(script);
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, super::super::adapter::HealthStatus::Online);
        assert_eq!(health.version.as_deref(), Some("0.147.0"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn health_check_reports_unauthenticated() {
        let dir = temp_dir("auth");
        let script = write_script(
            &dir,
            "fake-codex",
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex-cli 0.147.0'; fi\nif [ \"$1\" = \"login\" ]; then exit 1; fi\n",
        );
        let adapter = CodexAdapter::new(script);
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, super::super::adapter::HealthStatus::Offline);
        assert_eq!(health.version.as_deref(), Some("0.147.0"));
        assert!(
            health
                .message
                .as_deref()
                .unwrap_or("")
                .contains("authentication")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_streams_full_lifecycle() {
        let dir = temp_dir("run");
        let script = write_script(
            &dir,
            "fake-codex",
            &format!(
                "#!/bin/sh\necho '{THREAD_STARTED}'\necho '{TURN_STARTED}'\necho '{AGENT_MESSAGE}'\necho '{TURN_COMPLETED}'\n"
            ),
        );
        let adapter = CodexAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;

        assert_eq!(events.first(), Some(&AgentEvent::Started));
        assert!(events.contains(&AgentEvent::Message("hello".to_string())));
        assert_eq!(events.last(), Some(&AgentEvent::Completed));
        assert_eq!(
            handle.session_id().as_deref(),
            Some("019fef17-7ec9-76a0-855f-87bb9d399bfd")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_passes_thread_id() {
        let dir = temp_dir("resume");
        let script = write_script(
            &dir,
            "fake-codex",
            r#"#!/bin/sh
if [ "$1" = "exec" ] && [ "$2" = "resume" ] && [ "$3" = "--json" ] && [ "$4" = "019fef17-7ec9-76a0-855f-87bb9d399bfd" ]; then
  echo '{"type":"thread.started","thread_id":"019fef17-7ec9-76a0-855f-87bb9d399bfd"}'
  echo '{"type":"item.completed","item":{"id":"i0","type":"agent_message","text":"resumed ok"}}'
  echo '{"type":"turn.completed"}'
else
  echo "unexpected args: $@" >&2
  exit 9
fi
"#,
        );
        let adapter = CodexAdapter::new(script);
        let mut handle = adapter
            .resume("019fef17-7ec9-76a0-855f-87bb9d399bfd", request("continue"))
            .await
            .expect("resume");
        let events = drain_with_timeout(&mut handle).await;
        assert!(events.contains(&AgentEvent::Message("resumed ok".to_string())));
        assert!(events.contains(&AgentEvent::Completed));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn start_passes_sandbox_flag() {
        let dir = temp_dir("sandbox");
        let script = write_script(
            &dir,
            "fake-codex",
            r#"#!/bin/sh
if [ "$1" = "exec" ] && [ "$2" = "--json" ] && [ "$3" = "-s" ] && [ "$4" = "read-only" ]; then
  echo '{"type":"thread.started","thread_id":"t1"}'
  echo '{"type":"turn.completed"}'
else
  echo "unexpected args: $@" >&2
  exit 9
fi
"#,
        );
        let adapter = CodexAdapter::new(script);
        let mut handle = adapter.start(request("hi")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(events.contains(&AgentEvent::Completed));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn workspace_sets_process_cwd() {
        let dir = temp_dir("cwd");
        let ws = temp_dir("cwd-ws");
        let script = write_script(
            &dir,
            "fake-codex",
            r#"#!/bin/sh
p=$(pwd)
printf '{"type":"item.completed","item":{"id":"i0","type":"agent_message","text":"%s"}}' "$p"
echo
echo '{"type":"turn.completed"}'
"#,
        );
        let adapter = CodexAdapter::new(script);
        let mut request = request("hi");
        let ws = ws.canonicalize().expect("canonicalize workspace");
        request.workspace = Some(ws.clone());
        let mut handle = adapter.start(request).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                AgentEvent::Message(text) if *text == ws.to_string_lossy()
            )),
            "expected message with workspace path, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&ws);
    }

    #[tokio::test]
    async fn nonzero_exit_emits_failed() {
        let dir = temp_dir("fail");
        let script = write_script(
            &dir,
            "fake-codex",
            "#!/bin/sh\necho 'boom happened' >&2\nexit 3\n",
        );
        let adapter = CodexAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(|event| matches!(event, AgentEvent::Failed(msg) if msg.contains("boom happened") && msg.contains("3"))),
            "expected failed event with stderr, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn git_repo_requirement_produces_friendly_error() {
        let dir = temp_dir("git");
        let script = write_script(
            &dir,
            "fake-codex",
            "#!/bin/sh\necho 'Not inside a trusted directory and --skip-git-repo-check was not specified.' >&2\nexit 1\n",
        );
        let adapter = CodexAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(
                |event| matches!(event, AgentEvent::Failed(msg) if msg.contains("Git repository"))
            ),
            "expected friendly git repo error, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn malformed_stream_does_not_crash() {
        let dir = temp_dir("malformed");
        let script = write_script(
            &dir,
            "fake-codex",
            "#!/bin/sh\necho 'not json at all'\necho '{\"type\":\"turn.completed\"}'\n",
        );
        let adapter = CodexAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(events.contains(&AgentEvent::Completed));
        assert!(!events.iter().any(|e| matches!(e, AgentEvent::Message(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancel_kills_running_run() {
        let dir = temp_dir("cancel");
        let script = write_script(
            &dir,
            "fake-codex",
            "#!/bin/sh\necho '{\"type\":\"thread.started\",\"thread_id\":\"t-c\"}'\nsleep 30\n",
        );
        let adapter = CodexAdapter::new(script);
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
        let adapter = CodexAdapter::new("/nonexistent/agentmesh/codex");
        let err = adapter.cancel("00000000-0000-0000-0000-000000000000").await;
        assert!(matches!(err, Err(AgentError::NotFound(_))));
    }

    #[tokio::test]
    async fn start_with_missing_command_returns_command_not_found() {
        let adapter = CodexAdapter::new("/nonexistent/agentmesh/codex");
        let err = adapter.start(request("hello")).await;
        assert!(
            matches!(err, Err(AgentError::CommandNotFound(ref msg)) if msg.contains("nonexistent")),
            "expected CommandNotFound, got {err:?}"
        );
    }

    #[test]
    fn from_config_rejects_danger_full_access() {
        let mut config = AgentConfig::default();
        config
            .options
            .insert("sandbox".to_string(), "danger-full-access".to_string());
        assert!(matches!(
            CodexAdapter::from_config(&config),
            Err(AgentError::InvalidRequest(_))
        ));
    }

    #[test]
    fn from_config_defaults_to_read_only() {
        let adapter = CodexAdapter::from_config(&AgentConfig::default()).expect("config");
        assert_eq!(adapter.sandbox, SandboxMode::ReadOnly);
    }

    #[test]
    fn from_config_accepts_workspace_write() {
        let mut config = AgentConfig::default();
        config
            .options
            .insert("sandbox".to_string(), "workspace-write".to_string());
        let adapter = CodexAdapter::from_config(&config).expect("config");
        assert_eq!(adapter.sandbox, SandboxMode::WorkspaceWrite);
    }
}
