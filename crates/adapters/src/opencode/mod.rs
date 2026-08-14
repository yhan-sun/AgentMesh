//! Adapter for OpenCode (`opencode`).
//!
//! Launch strategy (verified against opencode 1.18.16):
//!
//! ```text
//! opencode run --format json <prompt>
//! opencode run --format json -s <session_id> <prompt>
//! ```
//!
//! Output is newline-delimited JSON parsed by [`OpenCodeParser`]. OpenCode's
//! `sessionID` maps to the AgentMesh native session id. Permission handling
//! follows OpenCode's own policy: `--auto` is never passed unless the user
//! config requests it explicitly.

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
use parser::OpenCodeParser;

/// Max characters of stderr retained for failure messages.
const MAX_STDERR: usize = 8192;

/// Adapter that drives the OpenCode CLI over its `run --format json` protocol.
pub struct OpenCodeAdapter {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
    auto: bool,
    runs: Arc<Mutex<HashMap<Uuid, ProcessCancelHandle>>>,
}

impl OpenCodeAdapter {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            auto: false,
            runs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build the adapter from a config section (`[agents.opencode]`).
    ///
    /// `options.auto = "true"` passes OpenCode's `--auto` (auto-approve
    /// permissions not explicitly denied). AgentMesh never defaults to it.
    pub fn from_config(config: &AgentConfig) -> Result<Self, AgentError> {
        let mut adapter = Self::new(
            config
                .command
                .clone()
                .unwrap_or_else(|| "opencode".to_string()),
        );
        adapter.args = config.args.clone();
        adapter.env = config.env.clone();
        adapter.auto = match config.options.get("auto").map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            Some(other) => {
                return Err(AgentError::InvalidRequest(format!(
                    "invalid option `auto` value `{other}`; use 'true' or 'false'"
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
        spec = spec.arg("run").arg("--format").arg("json");
        if let Some(session_id) = resume_session {
            spec = spec.arg("-s").arg(session_id);
        }
        if self.auto {
            spec = spec.arg("--auto");
        }
        spec = spec.arg(prompt);
        for (key, value) in &self.env {
            tracing::debug!(
                agent_id = "opencode",
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
        tracing::debug!(agent_id = "opencode", run_id = %run_id, task_id = %request.task_id, "spawning opencode process");

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
                        "opencode".to_string(),
                        format!("failed to spawn opencode: {other}"),
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
            let mut parser = OpenCodeParser::new();
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
                        tracing::debug!(agent_id = "opencode", run_id = %run_id, "opencode stderr: {line}");
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
                Some(code) if code != 0 && !parser.saw_terminal() => {
                    let message = summarize_stderr(&stderr, code);
                    let _ = tx.send(AgentEvent::Failed(message)).await;
                }
                _ => {
                    if !parser.saw_terminal() {
                        let message =
                            parser.last_error().map(str::to_string).unwrap_or_else(|| {
                                "opencode exited without a terminal event".to_string()
                            });
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
        format!("opencode exited with code {code}")
    } else {
        format!("opencode exited with code {code}: {detail}")
    }
}

#[async_trait]
impl CodingAgentAdapter for OpenCodeAdapter {
    fn id(&self) -> &str {
        "opencode"
    }

    fn name(&self) -> &str {
        "OpenCode"
    }

    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: "opencode".to_string(),
            name: "OpenCode".to_string(),
            description: Some(
                "OpenCode coding agent, driven over its `run --format json` protocol".to_string(),
            ),
            skills: vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("debug", None),
                AgentSkill::new("review", None),
                AgentSkill::new("testing", None),
            ],
            endpoint: "agent://opencode".to_string(),
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
        }
    }

    #[instrument(skip_all, fields(agent_id = "opencode", command = %self.command))]
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        // Binary present + version via the official CLI. Credentials are
        // never read from files; opencode has no separate auth-status probe
        // that reports reliably, so found/version is the health signal.
        let version = match run_simple_command(&self.command, &["--version"]).await {
            Some(stdout) => parse_version(&stdout),
            None => {
                return Ok(AgentHealth::offline(
                    Some(self.command.clone()),
                    "command not found or not executable",
                ));
            }
        };
        let Some(version) = version else {
            return Ok(AgentHealth::offline(
                Some(self.command.clone()),
                "`opencode --version` produced no version output",
            ));
        };
        Ok(AgentHealth::online(
            Some(version),
            Some(self.command.clone()),
        ))
    }

    #[instrument(skip_all, fields(agent_id = "opencode", task_id = %request.task_id))]
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request, None, None).await
    }

    #[instrument(skip_all, fields(agent_id = "opencode", native_session_id = %native_session_id, task_id = %request.task_id))]
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

    #[instrument(skip_all, fields(agent_id = "opencode", run_id = %run_id))]
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
            .map_err(|err| AgentError::Agent("opencode".to_string(), err.to_string()))
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

/// Extract a version-like token from CLI output, e.g. `1.18.16`.
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
    use crate::adapter::HealthStatus;
    use agentmesh_core::AgentMessage;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "agentmesh-opencode-test-{}-{tag}",
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

    const STEP_START: &str = r#"{"type":"step_start","timestamp":1,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"p1","messageID":"m1","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"step-start"}}"#;
    const TEXT: &str = r#"{"type":"text","timestamp":2,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"p2","messageID":"m1","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"text","text":"hello"}}"#;
    const STEP_FINISH: &str = r#"{"type":"step_finish","timestamp":3,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"p3","reason":"stop","messageID":"m1","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"step-finish"}}"#;

    #[tokio::test]
    async fn health_check_offline_when_command_missing() {
        let adapter = OpenCodeAdapter::new("/nonexistent/agentmesh/opencode");
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Offline);
        assert!(health.version.is_none());
    }

    #[tokio::test]
    async fn health_check_online_with_fake_command() {
        let dir = temp_dir("health");
        let script = write_script(&dir, "fake-opencode", "#!/bin/sh\necho '1.18.16'\n");
        let adapter = OpenCodeAdapter::new(script);
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Online);
        assert_eq!(health.version.as_deref(), Some("1.18.16"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_streams_full_lifecycle() {
        let dir = temp_dir("run");
        let script = write_script(
            &dir,
            "fake-opencode",
            &format!("#!/bin/sh\necho '{STEP_START}'\necho '{TEXT}'\necho '{STEP_FINISH}'\n"),
        );
        let adapter = OpenCodeAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;

        assert_eq!(events.first(), Some(&AgentEvent::Started));
        assert!(events.contains(&AgentEvent::Message("hello".to_string())));
        assert_eq!(events.last(), Some(&AgentEvent::Completed));
        assert_eq!(
            handle.session_id().as_deref(),
            Some("ses_00c1578aeffeI57OZaxthCr4R4")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_passes_session_flag() {
        let dir = temp_dir("resume");
        let script = write_script(
            &dir,
            "fake-opencode",
            r#"#!/bin/sh
if [ "$1" = "run" ] && [ "$2" = "--format" ] && [ "$3" = "json" ] && [ "$4" = "-s" ] && [ "$5" = "ses_00c1578aeffeI57OZaxthCr4R4" ]; then
  echo '{"type":"text","timestamp":2,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"p2","messageID":"m2","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"text","text":"resumed ok"}}'
  echo '{"type":"step_finish","timestamp":3,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"p3","reason":"stop","messageID":"m2","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"step-finish"}}'
else
  echo "unexpected args: $@" >&2
  exit 9
fi
"#,
        );
        let adapter = OpenCodeAdapter::new(script);
        let mut handle = adapter
            .resume("ses_00c1578aeffeI57OZaxthCr4R4", request("continue"))
            .await
            .expect("resume");
        let events = drain_with_timeout(&mut handle).await;
        assert!(events.contains(&AgentEvent::Message("resumed ok".to_string())));
        assert!(events.contains(&AgentEvent::Completed));
        assert_eq!(
            handle.session_id().as_deref(),
            Some("ses_00c1578aeffeI57OZaxthCr4R4")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn workspace_sets_process_cwd() {
        let dir = temp_dir("cwd");
        let ws = temp_dir("cwd-ws");
        let script = write_script(
            &dir,
            "fake-opencode",
            r#"#!/bin/sh
p=$(pwd)
printf '{"type":"text","sessionID":"ses_1","part":{"type":"text","text":"%s"}}' "$p"
echo
echo '{"type":"step_finish","sessionID":"ses_1","part":{"type":"step-finish","reason":"stop"}}'
"#,
        );
        let adapter = OpenCodeAdapter::new(script);
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
    async fn stderr_exit_emits_failed() {
        let dir = temp_dir("fail");
        let script = write_script(
            &dir,
            "fake-opencode",
            "#!/bin/sh\necho 'boom happened' >&2\nexit 3\n",
        );
        let adapter = OpenCodeAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(|event| matches!(event, AgentEvent::Failed(msg) if msg.contains("boom happened") && msg.contains("3"))),
            "expected failed event with stderr, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn error_event_with_exit_1_fails_once() {
        let dir = temp_dir("err");
        let error_line = r#"{"type":"error","timestamp":1,"sessionID":"ses_e","error":{"name":"UnknownError","data":{"message":"Unexpected server error. Check server logs for details.","ref":"err_a96a05eb"}}}"#;
        let script = write_script(
            &dir,
            "fake-opencode",
            &format!("#!/bin/sh\necho '{error_line}'\nexit 1\n"),
        );
        let adapter = OpenCodeAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        let fails = events
            .iter()
            .filter(|e| matches!(e, AgentEvent::Failed(_)))
            .count();
        assert_eq!(
            fails, 1,
            "expected exactly one failed event, got {events:?}"
        );
        assert!(events.iter().any(
            |event| matches!(event, AgentEvent::Failed(msg) if msg.contains("Unexpected server error"))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn no_terminal_event_exits_with_failed() {
        let dir = temp_dir("noterm");
        let script = write_script(
            &dir,
            "fake-opencode",
            "#!/bin/sh\necho 'some random output'\nexit 0\n",
        );
        let adapter = OpenCodeAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(
                |e| matches!(e, AgentEvent::Failed(msg) if msg.contains("without a terminal event"))
            ),
            "expected failed event, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancel_kills_running_run() {
        let dir = temp_dir("cancel");
        let script = write_script(
            &dir,
            "fake-opencode",
            "#!/bin/sh\necho '{\"type\":\"step_start\",\"sessionID\":\"ses_c\",\"part\":{\"type\":\"step-start\"}}'\nsleep 30\n",
        );
        let adapter = OpenCodeAdapter::new(script);
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
        let adapter = OpenCodeAdapter::new("/nonexistent/agentmesh/opencode");
        let err = adapter.cancel("00000000-0000-0000-0000-000000000000").await;
        assert!(matches!(err, Err(AgentError::NotFound(_))));
    }

    #[tokio::test]
    async fn start_with_missing_command_returns_command_not_found() {
        let adapter = OpenCodeAdapter::new("/nonexistent/agentmesh/opencode");
        let err = adapter.start(request("hello")).await;
        assert!(
            matches!(err, Err(AgentError::CommandNotFound(ref msg)) if msg.contains("nonexistent")),
            "expected CommandNotFound, got {err:?}"
        );
    }

    #[test]
    fn from_config_defaults_no_auto() {
        let adapter = OpenCodeAdapter::from_config(&AgentConfig::default()).expect("config");
        assert!(!adapter.auto);
    }

    #[test]
    fn from_config_enables_auto_only_explicitly() {
        let mut config = AgentConfig::default();
        config
            .options
            .insert("auto".to_string(), "true".to_string());
        let adapter = OpenCodeAdapter::from_config(&config).expect("config");
        assert!(adapter.auto);
    }

    #[test]
    fn from_config_rejects_invalid_auto() {
        let mut config = AgentConfig::default();
        config
            .options
            .insert("auto".to_string(), "maybe".to_string());
        assert!(matches!(
            OpenCodeAdapter::from_config(&config),
            Err(AgentError::InvalidRequest(_))
        ));
    }
}
