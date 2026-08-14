//! Live end-to-end tests for Phase 9 routing and delegation.
//!
//! Exercises the real `agentmesh` binary end to end: daemon → AgentDirectory →
//! RuleRouter → A2A client → A2A server → existing daemon runtime. Requires
//! the real Claude Code and/or Codex CLIs installed and authenticated. Tests
//! skip individual assertions when an agent is not online. Skipped by default:
//!
//! ```text
//! cargo test -p agentmesh-cli --test live -- --ignored --nocapture
//! ```

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_agentmesh");

fn git_repo(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("agentmesh-live-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");
    let git = |args: &[&str]| {
        let status = StdCommand::new("git")
            .args(args)
            .current_dir(&dir)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "AgentMesh Test"]);
    git(&["config", "user.email", "agentmesh@example.invalid"]);
    std::fs::write(dir.join("README.md"), "# live test repo\n").expect("write");
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "initial"]);
    dir
}

fn run_in(dir: &Path, args: &[&str]) -> Output {
    StdCommand::new(BIN)
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run agentmesh")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn start_daemon(dir: &Path) {
    let out = run_in(dir, &["daemon", "start"]);
    assert!(out.status.success(), "daemon start failed:\n{}", text(&out));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let out = run_in(dir, &["daemon", "status"]);
        if String::from_utf8_lossy(&out.stdout).contains("status:   running") {
            return;
        }
        if Instant::now() > deadline {
            panic!("daemon did not start:\n{}", text(&out));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

fn stop_daemon(dir: &Path) {
    let _ = run_in(dir, &["daemon", "stop", "--force"]);
}

/// Agents with a live A2A listener, from `agentmesh a2a agents`.
fn available_agents(dir: &Path) -> Vec<String> {
    let out = run_in(dir, &["a2a", "agents"]);
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .skip(1)
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .collect()
}

#[test]
#[ignore = "requires real daemon and real Claude/Codex CLIs"]
fn live_route_matches_config() {
    let repo = git_repo("route");
    start_daemon(&repo);
    let agents = available_agents(&repo);
    let routing = agentmesh_core::AgentMeshConfig::load().routing_config();

    if agents.iter().any(|a| a == "claude") {
        let out = run_in(&repo, &["route", "architecture"]);
        assert!(
            out.status.success(),
            "route architecture failed:\n{}",
            text(&out)
        );
        let text = text(&out);
        assert!(text.contains("Intent: architecture"), "{text}");
        let chosen = text
            .lines()
            .find_map(|l| l.strip_prefix("Agent:  "))
            .expect("agent line")
            .trim()
            .to_string();
        assert!(
            routing.architecture.contains(&chosen),
            "chosen `{chosen}` must be a preferred config agent {:?}: {text}",
            routing.architecture
        );
        assert!(
            agents.contains(&chosen),
            "chosen agent must have an A2A listener: {agents:?}"
        );
    } else {
        eprintln!("skipping architecture assertion: claude not online");
    }

    if agents.iter().any(|a| a == "codex") {
        let out = run_in(&repo, &["route", "testing"]);
        assert!(
            out.status.success(),
            "route testing failed:\n{}",
            text(&out)
        );
        let text = text(&out);
        assert!(text.contains("Intent: testing"), "{text}");
        let chosen = text
            .lines()
            .find_map(|l| l.strip_prefix("Agent:  "))
            .expect("agent line")
            .trim()
            .to_string();
        // `testing` prefers codex, and claude never declares the testing skill.
        assert_eq!(chosen, "codex", "{text}");
    } else {
        eprintln!("skipping testing assertion: codex not online");
    }

    stop_daemon(&repo);
}

#[test]
#[ignore = "requires real daemon and real Claude/Codex CLIs"]
fn live_delegate_routed_and_explicit() {
    let repo = git_repo("delegate");
    start_daemon(&repo);
    let agents = available_agents(&repo);

    if agents.iter().any(|a| a == "claude") {
        let out = run_in(
            &repo,
            &[
                "delegate",
                "--intent",
                "architecture",
                "Design an authentication module",
            ],
        );
        let out_text = text(&out);
        assert!(
            out.status.success(),
            "delegate architecture failed:\n{out_text}"
        );
        assert!(out_text.contains("task completed"), "{out_text}");
        assert!(out_text.contains("Task:    "), "{out_text}");

        let out = run_in(
            &repo,
            &[
                "delegate",
                "--agent",
                "claude",
                "Reply exactly: CLAUDE-A2A-CLIENT",
            ],
        );
        let out_text = text(&out);
        assert!(
            out.status.success(),
            "delegate --agent claude failed:\n{out_text}"
        );
        assert!(
            out_text.contains("CLAUDE-A2A-CLIENT"),
            "claude reply missing:\n{out_text}"
        );
    } else {
        eprintln!("skipping claude delegation: claude not online");
    }

    if agents.iter().any(|a| a == "codex") {
        let out = run_in(
            &repo,
            &[
                "delegate",
                "--intent",
                "testing",
                "Review the test coverage",
            ],
        );
        let out_text = text(&out);
        assert!(out.status.success(), "delegate testing failed:\n{out_text}");
        assert!(out_text.contains("task completed"), "{out_text}");

        let out = run_in(
            &repo,
            &[
                "delegate",
                "--agent",
                "codex",
                "Reply exactly: CODEX-A2A-CLIENT",
            ],
        );
        let out_text = text(&out);
        assert!(
            out.status.success(),
            "delegate --agent codex failed:\n{out_text}"
        );
        assert!(
            out_text.contains("CODEX-A2A-CLIENT"),
            "codex reply missing:\n{out_text}"
        );
    } else {
        eprintln!("skipping codex delegation: codex not online");
    }

    stop_daemon(&repo);
}

#[test]
#[ignore = "requires real daemon and a real agent CLI"]
fn live_cancel_through_a2a() {
    let repo = git_repo("cancel");
    start_daemon(&repo);
    let agents = available_agents(&repo);
    let agent = agents
        .iter()
        .find(|a| a.as_str() == "claude" || a.as_str() == "codex")
        .cloned();
    let Some(agent) = agent else {
        eprintln!("skipping cancel: no real agent online");
        stop_daemon(&repo);
        return;
    };

    let mut child = StdCommand::new(BIN)
        .args([
            "delegate",
            "--agent",
            &agent,
            "Write an extremely long and detailed history of computing, going on for many paragraphs without stopping. Be exhaustive and verbose.",
        ])
        .current_dir(&repo)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn delegate");

    let stdout = child.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if tx.send(line.clone()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Wait for the task to start streaming.
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut started = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(line) => {
                print!("{line}");
                if line.contains("task started") {
                    started = true;
                    break;
                }
                if line.contains("delegation failed") {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    assert!(started, "delegate did not start a task within 120s");

    // Interrupt like Ctrl+C; the CLI cancels through A2A CancelTask.
    let _ = StdCommand::new("kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status();
    let status = child.wait().expect("wait for child");
    reader.join().expect("reader thread");

    let mut saw_cancel = false;
    for line in rx.iter() {
        print!("{line}");
        if line.contains("status: cancelled") || line.contains("cancelled") {
            saw_cancel = true;
        }
    }
    assert!(
        saw_cancel,
        "delegate did not report cancellation (exit {status:?})"
    );
    stop_daemon(&repo);
}
