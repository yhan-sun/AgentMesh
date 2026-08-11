//! Live integration test against the real Codex CLI.
//!
//! Requires: `codex` on PATH, authenticated (`codex login`), and a working
//! network connection. Skipped by default; run with:
//!
//! ```text
//! cargo test -p agentmesh-adapters --test codex_live -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::time::Duration;

use agentmesh_adapters::{AgentRunRequest, CodexAdapter, CodingAgentAdapter};
use agentmesh_core::{AgentEvent, AgentMessage};
use uuid::Uuid;

fn git_repo(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("agentmesh-codex-live-{}-{tag}", std::process::id()));
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

async fn run_to_terminal(
    adapter: &CodexAdapter,
    request: AgentRunRequest,
) -> (Vec<AgentEvent>, Option<String>) {
    let mut handle = adapter.start(request).await.expect("start");
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
    let session_id = handle.session_id();
    (events, session_id)
}

#[tokio::test]
#[ignore = "requires real Codex CLI, authentication and network"]
async fn start_captures_thread_id_and_resume_keeps_memory() {
    let ws = git_repo("mem");
    let adapter = CodexAdapter::new("codex");

    // Sanity: binary + auth available, otherwise skip silently (CI machines).
    let health = adapter.health_check().await.expect("health check");
    if !matches!(health.status, agentmesh_adapters::HealthStatus::Online) {
        eprintln!("skipping: codex not online: {health:?}");
        return;
    }

    let (events, session_id) = run_to_terminal(
        &adapter,
        request(
            "Remember the token: AGENTMESH-7319. Reply with exactly: stored",
            &ws,
        ),
    )
    .await;
    assert!(
        events.contains(&AgentEvent::Completed),
        "first run did not complete: {events:?}"
    );
    let native_session_id = session_id.expect("native session id captured");
    eprintln!("native session id: {native_session_id}");

    // Resume the same thread and check the memory persisted.
    let mut handle = adapter
        .resume(
            &native_session_id,
            request("What token did I ask you to remember?", &ws),
        )
        .await
        .expect("resume");
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
    eprintln!("resume events: {events:?}");
    assert!(
        events.contains(&AgentEvent::Completed),
        "resume failed: {events:?}"
    );
    assert!(
        events.iter().any(
            |event| matches!(event, AgentEvent::Message(text) if text.contains("AGENTMESH-7319"))
        ),
        "resume did not preserve the remembered token: {events:?}"
    );

    tokio::time::timeout(Duration::from_secs(1), async {})
        .await
        .expect("tick");
    let _ = std::fs::remove_dir_all(&ws);
}
