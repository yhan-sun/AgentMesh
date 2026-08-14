//! Adapter for Antigravity CLI (`agy`).
//!
//! Launch strategy (verified against agy 1.1.12):
//!
//! ```text
//! agy -p --output-format stream-json <prompt>
//! agy -p --output-format stream-json --conversation <conversation_id> <prompt>
//! ```
//!
//! Output is newline-delimited JSON parsed by [`AntigravityParser`]; the
//! conversation id maps to the AgentMesh native session id. Permission
//! handling follows Antigravity's own policy: `--dangerously-skip-permissions`
//! is never passed unless the user config requests it explicitly.

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
use parser::AntigravityParser;

/// Max characters of stderr retained for failure messages.
const MAX_STDERR: usize = 8192;

/// Adapter that drives the Antigravity CLI over its print-mode stream-json
/// protocol.
pub struct AntigravityAdapter {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
    skip_permissions: bool,
    runs: Arc<Mutex<HashMap<Uuid, ProcessCancelHandle>>>,
}

impl AntigravityAdapter {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: HashMap::new(),
            skip_permissions: false,
            runs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build the adapter from a config section (`[agents.antigravity]`).
    ///
    /// `options.skip_permissions = "true"` passes
    /// `--dangerously-skip-permissions`. AgentMesh never defaults to it.
    pub fn from_config(config: &AgentConfig) -> Result<Self, AgentError> {
        let mut adapter = Self::new(config.command.clone().unwrap_or_else(|| "agy".to_string()));
        adapter.args = config.args.clone();
        adapter.env = config.env.clone();
        adapter.skip_permissions = match config.options.get("skip_permissions").map(String::as_str)
        {
            None | Some("false") => false,
            Some("true") => true,
            Some(other) => {
                return Err(AgentError::InvalidRequest(format!(
                    "invalid option `skip_permissions` value `{other}`; use 'true' or 'false'"
                )));
            }
        };
        Ok(adapter)
    }

    fn build_spec(
        &self,
        prompt: &str,
        resume_conversation: Option<&str>,
        workspace: Option<&Path>,
    ) -> ProcessSpec {
        let mut spec = ProcessSpec::new(&self.command);
        if let Some(workspace) = workspace {
            spec = spec.cwd(workspace);
        }
        for arg in &self.args {
            spec = spec.arg(arg);
        }
        spec = spec.arg("-p").arg("--output-format").arg("stream-json");
        if let Some(conversation_id) = resume_conversation {
            spec = spec.arg("--conversation").arg(conversation_id);
        }
        if self.skip_permissions {
            spec = spec.arg("--dangerously-skip-permissions");
        }
        spec = spec.arg(prompt);
        for (key, value) in &self.env {
            tracing::debug!(
                agent_id = "antigravity",
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
        resume_conversation: Option<&str>,
        initial_conversation: Option<String>,
    ) -> Result<AgentRunHandle, AgentError> {
        let run_id = Uuid::new_v4();
        let spec = self.build_spec(
            &request.input.content,
            resume_conversation,
            request.workspace.as_deref(),
        );
        tracing::debug!(agent_id = "antigravity", run_id = %run_id, task_id = %request.task_id, "spawning agy process");

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
                        "antigravity".to_string(),
                        format!("failed to spawn agy: {other}"),
                    ),
                });
            }
        };
        let cancel_handle = process.cancel_handle();
        self.runs.lock().unwrap().insert(run_id, cancel_handle);

        let (session_tx, session_rx) = watch::channel(initial_conversation);
        let (tx, rx) = mpsc::channel(256);
        let handle = AgentRunHandle::with_session_channel(run_id, rx, session_rx);
        let runs = self.runs.clone();
        tokio::spawn(async move {
            let mut parser = AntigravityParser::new();
            let mut stderr = String::new();
            let mut exit_code: Option<i32> = None;
            let mut sent_conversation: Option<String> = None;
            let _ = tx.send(AgentEvent::Started).await;

            while let Some(event) = process.next().await {
                match event {
                    agentmesh_runtime::ProcessEvent::Stdout(line) => {
                        for agent_event in parser.parse_line(&line) {
                            if let Some(conversation_id) = parser.conversation_id()
                                && sent_conversation.as_deref() != Some(conversation_id)
                            {
                                let _ = session_tx.send(Some(conversation_id.to_string()));
                                sent_conversation = Some(conversation_id.to_string());
                            }
                            let _ = tx.send(agent_event).await;
                        }
                    }
                    agentmesh_runtime::ProcessEvent::Stderr(line) => {
                        tracing::debug!(agent_id = "antigravity", run_id = %run_id, "agy stderr: {line}");
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
                        let _ = tx
                            .send(AgentEvent::Failed(
                                "antigravity exited without a result event".to_string(),
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
        format!("antigravity exited with code {code}")
    } else {
        format!("antigravity exited with code {code}: {detail}")
    }
}

#[async_trait]
impl CodingAgentAdapter for AntigravityAdapter {
    fn id(&self) -> &str {
        "antigravity"
    }

    fn name(&self) -> &str {
        "Antigravity"
    }

    fn descriptor(&self) -> AgentDescriptor {
        AgentDescriptor {
            id: "antigravity".to_string(),
            name: "Antigravity".to_string(),
            description: Some(
                "Google's Antigravity coding agent, driven over its print-mode stream-json protocol"
                    .to_string(),
            ),
            skills: vec![
                AgentSkill::new("code", None),
                AgentSkill::new("architecture", None),
                AgentSkill::new("debug", None),
                AgentSkill::new("review", None),
                AgentSkill::new("testing", None),
                AgentSkill::new("ui", None),
            ],
            endpoint: "agent://antigravity".to_string(),
            workspace_requirement: WorkspaceRequirement::IsolatedGit,
        }
    }

    #[instrument(skip_all, fields(agent_id = "antigravity", command = %self.command))]
    async fn health_check(&self) -> Result<AgentHealth, AgentError> {
        // Binary present + version via the official CLI. Credentials are
        // never read from keyring/token files: no separate reliable
        // auth-status command exists for agy, so found/version is the
        // health signal. Runs that hit an auth error surface it themselves.
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
                "`agy --version` produced no version output",
            ));
        };
        Ok(AgentHealth::online(
            Some(version),
            Some(self.command.clone()),
        ))
    }

    #[instrument(skip_all, fields(agent_id = "antigravity", task_id = %request.task_id))]
    async fn start(&self, request: AgentRunRequest) -> Result<AgentRunHandle, AgentError> {
        self.spawn_run(request, None, None).await
    }

    #[instrument(skip_all, fields(agent_id = "antigravity", native_session_id = %native_session_id, task_id = %request.task_id))]
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

    #[instrument(skip_all, fields(agent_id = "antigravity", run_id = %run_id))]
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
            .map_err(|err| AgentError::Agent("antigravity".to_string(), err.to_string()))
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

/// Extract a version-like token from CLI output, e.g. `1.1.12`.
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
            "agentmesh-antigravity-test-{}-{tag}",
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

    const INIT: &str = r#"{"event":"init","conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","init":{"cwd":"/tmp","permission_mode":"request-review"}}"#;
    const STEP_DONE: &str = r#"{"event":"step_update","step_update":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"hello"}}"#;
    const RESULT_SUCCESS: &str = r#"{"event":"result","result":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","status":"SUCCESS","response":"hello","duration_seconds":2.0,"num_turns":1}}"#;

    #[tokio::test]
    async fn health_check_offline_when_command_missing() {
        let adapter = AntigravityAdapter::new("/nonexistent/agentmesh/agy");
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Offline);
        assert!(health.version.is_none());
    }

    #[tokio::test]
    async fn health_check_online_with_fake_command() {
        let dir = temp_dir("health");
        let script = write_script(&dir, "fake-agy", "#!/bin/sh\necho '1.1.12'\n");
        let adapter = AntigravityAdapter::new(script);
        let health = adapter.health_check().await.expect("health check");
        assert_eq!(health.status, HealthStatus::Online);
        assert_eq!(health.version.as_deref(), Some("1.1.12"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_streams_full_lifecycle() {
        let dir = temp_dir("run");
        let script = write_script(
            &dir,
            "fake-agy",
            &format!("#!/bin/sh\necho '{INIT}'\necho '{STEP_DONE}'\necho '{RESULT_SUCCESS}'\n"),
        );
        let adapter = AntigravityAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;

        assert_eq!(events.first(), Some(&AgentEvent::Started));
        assert!(events.contains(&AgentEvent::Message("hello".to_string())));
        assert_eq!(events.last(), Some(&AgentEvent::Completed));
        assert_eq!(
            handle.session_id().as_deref(),
            Some("c3b66b04-872b-4fbe-a3a4-058a026ef20a")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resume_passes_conversation_flag() {
        let dir = temp_dir("resume");
        let script = write_script(
            &dir,
            "fake-agy",
            r#"#!/bin/sh
if [ "$1" = "-p" ] && [ "$2" = "--output-format" ] && [ "$3" = "stream-json" ] && [ "$4" = "--conversation" ] && [ "$5" = "c3b66b04-872b-4fbe-a3a4-058a026ef20a" ]; then
  echo '{"event":"init","conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","init":{"cwd":"/tmp","permission_mode":"request-review"}}'
  echo '{"event":"step_update","step_update":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","step_index":0,"state":"DONE","step_type":"agent_response","text_delta":"resumed ok"}}'
  echo '{"event":"result","result":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","status":"SUCCESS","response":"resumed ok","num_turns":2}}'
else
  echo "unexpected args: $@" >&2
  exit 9
fi
"#,
        );
        let adapter = AntigravityAdapter::new(script);
        let mut handle = adapter
            .resume("c3b66b04-872b-4fbe-a3a4-058a026ef20a", request("continue"))
            .await
            .expect("resume");
        let events = drain_with_timeout(&mut handle).await;
        assert!(events.contains(&AgentEvent::Message("resumed ok".to_string())));
        assert!(events.contains(&AgentEvent::Completed));
        assert_eq!(
            handle.session_id().as_deref(),
            Some("c3b66b04-872b-4fbe-a3a4-058a026ef20a")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn workspace_sets_process_cwd() {
        let dir = temp_dir("cwd");
        let ws = temp_dir("cwd-ws");
        let script = write_script(
            &dir,
            "fake-agy",
            r#"#!/bin/sh
p=$(pwd)
printf '{"event":"step_update","step_update":{"conversation_id":"c1","step_index":0,"state":"DONE","step_type":"agent_response","text_delta":"%s"}}' "$p"
echo
echo '{"event":"result","result":{"conversation_id":"c1","status":"SUCCESS","response":"x"}}'
"#,
        );
        let adapter = AntigravityAdapter::new(script);
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
    async fn error_result_emits_failed() {
        let dir = temp_dir("fail");
        let script = write_script(
            &dir,
            "fake-agy",
            "#!/bin/sh\necho '{\"event\":\"result\",\"result\":{\"conversation_id\":\"\",\"status\":\"ERROR\",\"response\":\"\",\"error\":\"authentication failed or timed out\"}}'\nexit 1\n",
        );
        let adapter = AntigravityAdapter::new(script);
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
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::Failed(msg) if msg.contains("authentication failed or timed out")
        )));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn nonzero_exit_without_stream_emits_failed() {
        let dir = temp_dir("fail2");
        let script = write_script(
            &dir,
            "fake-agy",
            "#!/bin/sh\necho 'boom happened' >&2\nexit 3\n",
        );
        let adapter = AntigravityAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(|event| matches!(event, AgentEvent::Failed(msg) if msg.contains("boom happened") && msg.contains("3"))),
            "expected failed event with stderr, got {events:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn no_result_event_exits_with_failed() {
        let dir = temp_dir("noterm");
        let script = write_script(
            &dir,
            "fake-agy",
            "#!/bin/sh\necho '{\"event\":\"init\",\"conversation_id\":\"c1\",\"init\":{\"cwd\":\"/tmp\"}}'\nexit 0\n",
        );
        let adapter = AntigravityAdapter::new(script);
        let mut handle = adapter.start(request("hello")).await.expect("start");
        let events = drain_with_timeout(&mut handle).await;
        assert!(
            events.iter().any(
                |e| matches!(e, AgentEvent::Failed(msg) if msg.contains("without a result event"))
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
            "fake-agy",
            "#!/bin/sh\necho '{\"event\":\"init\",\"conversation_id\":\"c-cancel\",\"init\":{\"cwd\":\"/tmp\"}}'\nsleep 30\n",
        );
        let adapter = AntigravityAdapter::new(script);
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
        let adapter = AntigravityAdapter::new("/nonexistent/agentmesh/agy");
        let err = adapter.cancel("00000000-0000-0000-0000-000000000000").await;
        assert!(matches!(err, Err(AgentError::NotFound(_))));
    }

    #[tokio::test]
    async fn start_with_missing_command_returns_command_not_found() {
        let adapter = AntigravityAdapter::new("/nonexistent/agentmesh/agy");
        let err = adapter.start(request("hello")).await;
        assert!(
            matches!(err, Err(AgentError::CommandNotFound(ref msg)) if msg.contains("nonexistent")),
            "expected CommandNotFound, got {err:?}"
        );
    }

    #[test]
    fn from_config_defaults_no_skip_permissions() {
        let adapter = AntigravityAdapter::from_config(&AgentConfig::default()).expect("config");
        assert!(!adapter.skip_permissions);
    }

    #[test]
    fn from_config_uses_custom_command() {
        let config = AgentConfig {
            command: Some("/custom/agy".to_string()),
            ..Default::default()
        };
        let adapter = AntigravityAdapter::from_config(&config).expect("config");
        assert_eq!(adapter.command, "/custom/agy");
    }

    #[test]
    fn from_config_enables_skip_permissions_only_explicitly() {
        let mut config = AgentConfig::default();
        config
            .options
            .insert("skip_permissions".to_string(), "true".to_string());
        let adapter = AntigravityAdapter::from_config(&config).expect("config");
        assert!(adapter.skip_permissions);
    }

    #[test]
    fn from_config_rejects_invalid_skip_permissions() {
        let mut config = AgentConfig::default();
        config
            .options
            .insert("skip_permissions".to_string(), "always".to_string());
        assert!(matches!(
            AntigravityAdapter::from_config(&config),
            Err(AgentError::InvalidRequest(_))
        ));
    }
}
