//! Live integration test against the real Antigravity CLI.
//!
//! Requires: `agy` on PATH, authenticated with Google, and a working network
//! connection. Skipped by default; run with:
//!
//! ```text
//! cargo test -p agentmesh-adapters --test antigravity_live -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::time::Duration;

use agentmesh_adapters::{AgentRunHandle, AgentRunRequest, AntigravityAdapter, CodingAgentAdapter};
use agentmesh_core::{AgentEvent, AgentMessage, TaskStatus};
use uuid::Uuid;

fn git_repo(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "agentmesh-antigravity-live-{}-{tag}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create dir");
    let status = StdCommand::new("git")
        .arg("init")
        .arg("-q")
        .current_dir(&dir)
        .status()
        .expect("run git init");
    assert!(status.success(), "git init failed");
    dir
}

fn request(prompt: &str, workspace: &std::path::Path) -> AgentRunRequest {
    let mut request =
        AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), AgentMessage::user(prompt));
    request.workspace = Some(workspace.to_path_buf());
    request
}

async fn drain_to_terminal(handle: &mut AgentRunHandle) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    while let Some(event) = handle.next_event().await {
        let done = matches!(
            event,
            AgentEvent::Completed | AgentEvent::Failed(_) | AgentEvent::StatusChanged(_)
        );
        events.push(event);
        if done {
            break;
        }
    }
    events
}

#[tokio::test]
#[ignore = "requires real Antigravity CLI, Google authentication and network"]
async fn start_captures_conversation_and_resume_keeps_memory() {
    let ws = git_repo("mem");
    let adapter = AntigravityAdapter::new("agy");

    // Sanity: binary available, otherwise skip silently (CI machines).
    let health = adapter.health_check().await.expect("health check");
    if !matches!(health.status, agentmesh_adapters::HealthStatus::Online) {
        eprintln!("skipping: agy not online: {health:?}");
        return;
    }

    let mut handle = adapter
        .start(request(
            "Remember the token: AGENTMESH-AGY-5937. Reply with exactly: stored",
            &ws,
        ))
        .await
        .expect("start");
    let events = drain_to_terminal(&mut handle).await;
    assert!(
        events.contains(&AgentEvent::Completed),
        "first run did not complete: {events:?}"
    );
    let native_session_id = handle.session_id().expect("conversation id captured");
    eprintln!("conversation id: {native_session_id}");

    let mut handle = adapter
        .resume(
            &native_session_id,
            request("What token did I ask you to remember?", &ws),
        )
        .await
        .expect("resume");
    let events = drain_to_terminal(&mut handle).await;
    eprintln!("resume events: {events:?}");
    assert!(
        events.contains(&AgentEvent::Completed),
        "resume failed: {events:?}"
    );
    assert!(
        events.iter().any(
            |event| matches!(event, AgentEvent::Message(text) if text.contains("AGENTMESH-AGY-5937"))
        ),
        "resume did not preserve the remembered token: {events:?}"
    );

    tokio::time::timeout(Duration::from_secs(1), async {})
        .await
        .expect("tick");
    let _ = std::fs::remove_dir_all(&ws);
}

#[tokio::test]
#[ignore = "requires real Antigravity CLI, Google authentication and network"]
async fn workspace_write_landing_file_in_isolated_worktree() {
    let ws = git_repo("write");
    let adapter = AntigravityAdapter::new("agy");
    let health = adapter.health_check().await.expect("health check");
    if !matches!(health.status, agentmesh_adapters::HealthStatus::Online) {
        eprintln!("skipping: agy not online: {health:?}");
        return;
    }

    let mut handle = adapter
        .start(request(
            "Create a file named agentmesh_probe.txt containing the text ANTIGRAVITY-WRITE-PROBE",
            &ws,
        ))
        .await
        .expect("start");
    let events = drain_to_terminal(&mut handle).await;
    assert!(
        events.contains(&AgentEvent::Completed),
        "write run did not complete: {events:?}"
    );
    assert!(
        ws.join("agentmesh_probe.txt").exists(),
        "antigravity did not write the file in the workspace"
    );

    tokio::time::timeout(Duration::from_secs(1), async {})
        .await
        .expect("tick");
    let _ = std::fs::remove_dir_all(&ws);
}

#[tokio::test]
#[ignore = "requires real Antigravity CLI, Google authentication and network"]
async fn cancel_kills_long_running_run() {
    let ws = git_repo("cancel");
    let adapter = AntigravityAdapter::new("agy");
    let health = adapter.health_check().await.expect("health check");
    if !matches!(health.status, agentmesh_adapters::HealthStatus::Online) {
        eprintln!("skipping: agy not online: {health:?}");
        return;
    }

    let handle = adapter
        .start(request(
            "Write an extremely long and detailed history of computing without stopping.",
            &ws,
        ))
        .await
        .expect("start");
    adapter
        .cancel(&handle.run_id().to_string())
        .await
        .expect("cancel");
    let mut handle = handle;
    let events = drain_to_terminal(&mut handle).await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::StatusChanged(TaskStatus::Cancelled))),
        "expected cancellation event, got {events:?}"
    );
    eprintln!("cancel events: {events:?}");

    tokio::time::timeout(Duration::from_secs(1), async {})
        .await
        .expect("tick");
    let _ = std::fs::remove_dir_all(&ws);
}
