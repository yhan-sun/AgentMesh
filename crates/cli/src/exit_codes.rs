//! AgentMesh stable exit codes and structured error handling for 1.0.

use std::fmt;

/// Stable exit codes for the AgentMesh CLI (1.0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    /// Command completed successfully.
    Success = 0,
    /// Invalid arguments or configuration error.
    InvalidArgumentsOrConfig = 2,
    /// Requested agent is offline, not installed, or unavailable.
    AgentUnavailable = 3,
    /// Workflow or task execution ended in Failed status.
    WorkflowOrTaskFailed = 4,
    /// Workflow or task was cancelled.
    Cancelled = 5,
    /// Structural or policy budget violation (e.g. node cap, evaluator limit).
    PolicyViolation = 6,
    /// Workspace or git error (dirty repository, patch conflict, missing worktree).
    WorkspaceOrGitError = 7,
    /// Daemon or runtime error (daemon unavailable, lock failure, database error).
    DaemonOrRuntimeError = 8,
    /// Protocol error (A2A JSON-RPC failure, SSE stream decode error).
    ProtocolError = 9,
    /// Provenance hash-chain tamper detection or deterministic replay mismatch.
    IntegrityOrReplayFailure = 10,
}

impl ExitCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Structured CLI error with actionable user message, hint, and optional technical error chain.
#[derive(Debug)]
pub struct CliError {
    pub code: ExitCode,
    pub message: String,
    pub hint: Option<String>,
    pub technical: Option<String>,
}

impl CliError {
    pub fn new(code: ExitCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: None,
            technical: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_technical(mut self, technical: impl Into<String>) -> Self {
        self.technical = Some(technical.into());
        self
    }

    pub fn invalid_args(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::InvalidArgumentsOrConfig, msg)
    }

    pub fn agent_unavailable(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::AgentUnavailable, msg)
            .with_hint("run `agentmesh doctor` to check agent binary installations")
    }

    pub fn workflow_failed(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::WorkflowOrTaskFailed, msg)
    }

    pub fn cancelled(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::Cancelled, msg)
    }

    pub fn policy_violation(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::PolicyViolation, msg).with_hint(
            "check `[planner.policy]` or `[evaluation]` limits in `.agentmesh/config.toml`",
        )
    }

    pub fn workspace_error(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::WorkspaceOrGitError, msg)
    }

    pub fn daemon_error(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::DaemonOrRuntimeError, msg)
            .with_hint("verify daemon is running with `agentmesh doctor`")
    }

    pub fn protocol_error(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::ProtocolError, msg)
    }

    pub fn integrity_failure(msg: impl Into<String>) -> Self {
        Self::new(ExitCode::IntegrityOrReplayFailure, msg)
            .with_hint("run `agentmesh workflow audit <id>` to inspect cryptographic hash-chain")
    }

    /// Formats the error string for output.
    /// In normal mode: concise, clear, and actionable.
    /// In verbose mode: includes the technical error chain.
    pub fn render(&self, verbose: bool) -> String {
        let mut out = format!("error: {}", self.message);
        if verbose && let Some(tech) = &self.technical {
            out.push_str(&format!("\ndetails: {tech}"));
        }
        if let Some(hint) = &self.hint {
            out.push_str(&format!("\nhint: {hint}"));
        }
        out
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.render(false))
    }
}

impl std::error::Error for CliError {}

impl From<anyhow::Error> for CliError {
    fn from(err: anyhow::Error) -> Self {
        let msg = err.to_string();
        let technical = format!("{err:?}");
        let lower = msg.to_ascii_lowercase();

        if lower.contains("no capable agent")
            || lower.contains("agent offline")
            || lower.contains("agent not found")
        {
            Self::agent_unavailable(msg).with_technical(technical)
        } else if lower.contains("policy")
            || lower.contains("budget")
            || lower.contains("exceeded")
            || lower.contains("quorum")
        {
            Self::policy_violation(msg).with_technical(technical)
        } else if lower.contains("cancelled") {
            Self::cancelled(msg).with_technical(technical)
        } else if lower.contains("git")
            || lower.contains("workspace")
            || lower.contains("dirty")
            || lower.contains("conflict")
        {
            Self::workspace_error(msg).with_technical(technical)
        } else if lower.contains("daemon")
            || lower.contains("connection refused")
            || lower.contains("socket")
        {
            Self::daemon_error(msg).with_technical(technical)
        } else if lower.contains("tamper")
            || lower.contains("mismatch")
            || lower.contains("replay failed")
            || lower.contains("integrity")
        {
            Self::integrity_failure(msg).with_technical(technical)
        } else if lower.contains("invalid")
            || lower.contains("missing")
            || lower.contains("parse")
            || lower.contains("config")
        {
            Self::invalid_args(msg).with_technical(technical)
        } else {
            Self::new(ExitCode::DaemonOrRuntimeError, msg).with_technical(technical)
        }
    }
}
