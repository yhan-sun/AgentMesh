//! Thin, safe wrapper around the system `git` CLI.
//!
//! Program/args are always separate (no shell strings); cwd is explicit.

use std::path::Path;
use std::process::Stdio;

use tokio::process::Command;

use crate::error::WorkspaceError;

/// Result of a git invocation.
pub struct GitOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl GitOutput {
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }
}

/// Run `git <args>` in `cwd`, capturing stdout/stderr.
pub async fn git(cwd: &Path, args: &[&str]) -> Result<GitOutput, WorkspaceError> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = command
        .output()
        .await
        .map_err(|err| WorkspaceError::NotAGitRepository(err.to_string()))?;
    Ok(GitOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

/// Run `git <args>` and require success, mapping failures to a Git error.
pub async fn git_ok(cwd: &Path, args: &[&str]) -> Result<String, WorkspaceError> {
    let output = git(cwd, args).await?;
    if output.success() {
        Ok(output.stdout)
    } else {
        Err(WorkspaceError::GitCommand {
            stderr: output.stderr.trim().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;

    fn init_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        StdCommand::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .status()
            .expect("git init");
        StdCommand::new("git")
            .args(["config", "user.name", "AgentMesh Test"])
            .current_dir(dir.path())
            .status()
            .expect("config user");
        StdCommand::new("git")
            .args(["config", "user.email", "agentmesh@example.invalid"])
            .current_dir(dir.path())
            .status()
            .expect("config email");
        dir
    }

    #[tokio::test]
    async fn rev_parse_show_toplevel() {
        let dir = init_repo();
        let output = git(dir.path(), &["rev-parse", "--show-toplevel"])
            .await
            .expect("git");
        assert!(output.success());
        assert!(!output.stdout.trim().is_empty());
    }

    #[tokio::test]
    async fn failing_command_reports_stderr() {
        let dir = init_repo();
        // init_repo has no commits, so HEAD cannot be resolved.
        let err = git_ok(dir.path(), &["rev-parse", "HEAD"]).await;
        assert!(matches!(err, Err(WorkspaceError::GitCommand { .. })));
    }

    #[tokio::test]
    async fn git_unavailable_reports_not_a_repository() {
        let dir = tempfile::tempdir().expect("tempdir");
        let output = git(dir.path(), &["status"]).await.expect("git runs");
        // Outside a repo `git status` fails but the binary exists.
        assert!(!output.success());
    }
}
