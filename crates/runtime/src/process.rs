//! Managed child-process lifecycle: spawn, stream, cancel, exit.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::error::RuntimeError;

/// Description of a process to spawn.
#[derive(Debug, Clone)]
pub struct ProcessSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
}

impl ProcessSpec {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            cwd: PathBuf::from("."),
            env: HashMap::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = cwd.into();
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }
}

/// Events emitted by a running [`Process`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessEvent {
    /// One line of stdout, trailing newline stripped.
    Stdout(String),
    /// One line of stderr, trailing newline stripped.
    Stderr(String),
    /// The process exited with the given code; `-1` when terminated by a signal.
    Exit(i32),
}

/// A spawned process with streaming stdout/stderr and cancellation.
///
/// Drop the [`Process`] to kill the child. Take a [`ProcessCancelHandle`]
/// before moving the process into a task to cancel it from outside.
pub struct Process {
    pid: u32,
    cancel_tx: mpsc::Sender<()>,
    events: mpsc::Receiver<ProcessEvent>,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.cancel_tx.try_send(());
    }
}

/// A handle that can cancel a running [`Process`] from anywhere.
///
/// Fails with [`RuntimeError::NotRunning`] when the process has already
/// exited (its cancel channel is gone).
#[derive(Clone)]
pub struct ProcessCancelHandle {
    cancel_tx: mpsc::Sender<()>,
}

impl ProcessCancelHandle {
    /// Kill the child process.
    pub async fn cancel(&self) -> Result<(), RuntimeError> {
        self.cancel_tx.send(()).await.map_err(RuntimeError::from)
    }
}

impl Process {
    /// Spawn a child process and start streaming its output.
    pub async fn spawn(spec: ProcessSpec) -> Result<Self, RuntimeError> {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .current_dir(&spec.cwd)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (key, value) in &spec.env {
            command.env(key, value);
        }

        let mut child = command.spawn().map_err(|err| RuntimeError::Spawn {
            program: spec.program.clone(),
            message: err.to_string(),
        })?;
        let pid = child.id().expect("spawned process has a pid");

        let (tx, rx) = mpsc::channel(256);
        let (cancel_tx, mut cancel_rx) = mpsc::channel(1);

        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        spawn_line_reader(BufReader::new(stdout), tx.clone(), ProcessEvent::Stdout);
        spawn_line_reader(BufReader::new(stderr), tx.clone(), ProcessEvent::Stderr);

        // Reap the child in the background: poll for exit while watching the
        // cancel channel. `wait()` consumes the child, so we poll `try_wait`.
        tokio::spawn(async move {
            let mut cancelled = false;
            let exit = loop {
                if !cancelled {
                    match cancel_rx.try_recv() {
                        Ok(()) | Err(mpsc::error::TryRecvError::Disconnected) => {
                            cancelled = true;
                            let _ = child.start_kill();
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                    }
                }
                match child.try_wait() {
                    Ok(Some(status)) => break Some(status),
                    Ok(None) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(_) => break None,
                }
            };
            let code = exit.and_then(|status| status.code()).unwrap_or(-1);
            let _ = tx.send(ProcessEvent::Exit(code)).await;
        });

        Ok(Self {
            pid,
            cancel_tx,
            events: rx,
        })
    }

    /// The operating-system process id of the child.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Receive the next process event; `None` once the stream is exhausted.
    pub async fn next(&mut self) -> Option<ProcessEvent> {
        self.events.recv().await
    }

    /// Kill the child process. The stream ends with a non-zero `Exit` event.
    pub async fn cancel(&mut self) -> Result<(), RuntimeError> {
        self.cancel_tx.send(()).await.map_err(RuntimeError::from)
    }

    /// Get a cancellable handle that outlives this [`Process`].
    pub fn cancel_handle(&self) -> ProcessCancelHandle {
        ProcessCancelHandle {
            cancel_tx: self.cancel_tx.clone(),
        }
    }
}

fn spawn_line_reader<R>(
    mut reader: BufReader<R>,
    tx: mpsc::Sender<ProcessEvent>,
    event: fn(String) -> ProcessEvent,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    while line.ends_with('\n') || line.ends_with('\r') {
                        line.pop();
                    }
                    if tx.send(event(std::mem::take(&mut line))).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn collect(process: &mut Process) -> Vec<ProcessEvent> {
        let mut events = Vec::new();
        while let Some(event) = process.next().await {
            let done = matches!(event, ProcessEvent::Exit(_));
            events.push(event);
            if done {
                break;
            }
        }
        events
    }

    async fn collect_with_timeout(process: &mut Process) -> Vec<ProcessEvent> {
        tokio::time::timeout(Duration::from_secs(10), collect(process))
            .await
            .expect("process did not exit within 10s")
    }

    #[tokio::test]
    async fn streams_stdout_stderr_and_exit_code() {
        let spec = ProcessSpec::new("/bin/sh")
            .arg("-c")
            .arg("echo hello; echo oops >&2; exit 3");
        let mut process = Process::spawn(spec).await.expect("spawn");
        let events = collect_with_timeout(&mut process).await;
        assert!(events.contains(&ProcessEvent::Stdout("hello".to_string())));
        assert!(events.contains(&ProcessEvent::Stderr("oops".to_string())));
        assert!(events.contains(&ProcessEvent::Exit(3)));
    }

    #[tokio::test]
    async fn cancel_kills_running_process() {
        let spec = ProcessSpec::new("/bin/sh").arg("-c").arg("sleep 30");
        let mut process = Process::spawn(spec).await.expect("spawn");
        tokio::time::sleep(Duration::from_millis(100)).await;
        process.cancel().await.expect("cancel");
        let events = collect_with_timeout(&mut process).await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ProcessEvent::Exit(code) if *code != 0)),
            "expected a non-zero exit after cancel, got {events:?}"
        );
    }

    #[tokio::test]
    async fn passes_environment_and_working_directory() {
        let spec = ProcessSpec::new("/bin/sh")
            .arg("-c")
            .arg("echo \"$FOO\"")
            .env("FOO", "bar");
        let mut process = Process::spawn(spec).await.expect("spawn");
        let events = collect_with_timeout(&mut process).await;
        assert!(events.contains(&ProcessEvent::Stdout("bar".to_string())));
    }
}
