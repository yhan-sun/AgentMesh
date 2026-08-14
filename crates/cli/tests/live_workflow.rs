//! Live end-to-end workflow tests: real Claude/Codex/… agents through the
//! real daemon, driven entirely by `agentmesh workflow`.
//!
//! These are skip-tolerant: an offline agent (or a Codex 503) must never make
//! the *architecture* of the workflow change, so the assertions check the
//! workflow structure (step headers, terminal state) rather than demanding
//! that every step completes. Skipped by default:
//!
//! ```text
//! cargo test -p agentmesh-cli --test live_workflow -- --ignored --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Output};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_agentmesh");

fn git_repo(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("agentmesh-live-wf-{}-{tag}", std::process::id()));
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
    std::fs::write(dir.join("README.md"), "# live workflow repo\n").expect("write");
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

/// The workflow must print all three step headers and reach a terminal state.
#[test]
#[ignore = "requires real daemon and real agent CLIs"]
fn live_workflow_runs_three_sequential_steps() {
    let repo = git_repo("steps");
    start_daemon(&repo);
    let agents = available_agents(&repo);

    if !agents.iter().any(|a| a == "claude" || a == "codex") {
        eprintln!("skipping workflow: no agent online ({agents:?})");
        stop_daemon(&repo);
        return;
    }

    let out = run_in(
        &repo,
        &[
            "workflow",
            "--preset",
            "architect-implement-review",
            "Design an authentication module and implement it",
        ],
    );
    let output = text(&out);
    let terminal = output.contains("Workflow completed")
        || output.contains("Workflow failed")
        || output.contains("Workflow cancelled");
    assert!(
        terminal,
        "workflow must reach a terminal state (Codex 503s are tolerated):\n{output}"
    );
    // The workflow architecture must not change: three step headers, in order.
    assert!(output.contains("[1] Architect →"), "{output}");
    assert!(output.contains("[2] Implementer →"), "{output}");
    assert!(output.contains("[3] Reviewer →"), "{output}");
    // If the reviewer requested changes, the fix loop must be structurally
    // coherent (5-step plan, final review header and verdict printed).
    if output.contains("[4] Fixer →") {
        assert!(output.contains("[5] Final Review →"), "{output}");
        assert!(output.contains("Final review:"), "{output}");
    }
    // Every step must have routed through an A2A agent listener.
    let step_headers = ["[1] Architect", "[2] Implementer", "[3] Reviewer"]
        .iter()
        .filter(|h| output.contains(**h))
        .count();
    assert_eq!(step_headers, 3, "step headers present: {output}");

    stop_daemon(&repo);
}

/// A workflow must not depend on a specific brand combination: with routing
/// pinned to one agent, every step runs on that agent.
#[test]
#[ignore = "requires real daemon and real Claude CLI"]
fn live_workflow_single_agent_claude_runs_all_steps() {
    let repo = git_repo("single-agent");
    start_daemon(&repo);
    let agents = available_agents(&repo);

    if !agents.iter().any(|a| a == "claude") {
        eprintln!("skipping single-agent workflow: claude not online ({agents:?})");
        stop_daemon(&repo);
        return;
    }

    // Pin every intent to claude via the project routing config.
    std::fs::create_dir_all(repo.join(".agentmesh")).expect("config dir");
    std::fs::write(
        repo.join(".agentmesh").join("config.toml"),
        "[routing]\narchitecture = [\"claude\"]\nimplementation = [\"claude\"]\nreview = [\"claude\"]\ndebug = [\"claude\"]\ntesting = [\"claude\"]\nuiux = [\"claude\"]\ngeneral = [\"claude\"]\n",
    )
    .expect("config");

    let out = run_in(&repo, &["workflow", "Add a fibonacci helper and review it"]);
    let output = text(&out);
    let terminal = output.contains("Workflow completed")
        || output.contains("Workflow failed")
        || output.contains("Workflow cancelled");
    assert!(terminal, "workflow must reach a terminal state:\n{output}");
    assert!(output.contains("[1] Architect → claude"), "{output}");
    assert!(output.contains("[2] Implementer → claude"), "{output}");
    assert!(output.contains("[3] Reviewer → claude"), "{output}");

    stop_daemon(&repo);
}

/// The `--max-review-rounds` flag must be accepted and still reach a terminal
/// state (with `0` the fix loop is disabled entirely).
#[test]
#[ignore = "requires real daemon and real agent CLIs"]
fn live_workflow_accepts_max_review_rounds_flag() {
    let repo = git_repo("max-rounds");
    start_daemon(&repo);
    let agents = available_agents(&repo);

    if !agents.iter().any(|a| a == "claude" || a == "codex") {
        eprintln!("skipping max-rounds workflow: no agent online ({agents:?})");
        stop_daemon(&repo);
        return;
    }

    let out = run_in(
        &repo,
        &[
            "workflow",
            "--max-review-rounds",
            "0",
            "Design a small utility and review it",
        ],
    );
    let output = text(&out);
    let terminal = output.contains("Workflow completed")
        || output.contains("Workflow failed")
        || output.contains("Workflow cancelled");
    assert!(terminal, "workflow must reach a terminal state:\n{output}");
    assert!(output.contains("[1] Architect →"), "{output}");
    assert!(output.contains("[2] Implementer →"), "{output}");
    assert!(output.contains("[3] Reviewer →"), "{output}");

    stop_daemon(&repo);
}
