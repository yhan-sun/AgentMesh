# AgentMesh

> **A2A Runtime and Orchestrator for Heterogeneous Coding Agents**

[English](README_en.md) | [中文](README.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust: 1.80+](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![Status: 1.0 Release Candidate](https://img.shields.io/badge/Status-1.0%20RC-brightgreen.svg)]()

AgentMesh unifies diverse AI coding agents (**Claude Code**, **Codex**, **OpenCode**, **Antigravity**, and **Mock**) into a single, deterministic orchestration runtime. It provides isolated Git worktree execution, parallel DAG scheduling, consensus review loops, Best-of-N blind evaluation competitions, and cryptographic provenance ledgers with verifiable decision replay.

---

## Why AgentMesh?

| Capability | Unmanaged Agent CLIs | AgentMesh 1.0 |
| :--- | :--- | :--- |
| **Execution Safety** | Modifies working directory in-place | **Isolated Git worktrees** per agent task; working copy never touched directly |
| **Multi-Agent Flow** | Manual copy-pasting between terminals | **Persistent DAG workflows** with parallel dispatch and policy budgets |
| **Code Review & Quality** | Trust single LLM output | **Multi-evaluator consensus** with automatic fix loops (up to 3 rounds) |
| **Exploration / Competitions** | Single candidate only | **Best-of-N blind competitions** with deterministic `SelectionGate` |
| **Apply Controls** | Ad-hoc unversioned changes | **Safe Apply** (`--check`, `--yes`) with full snapshot validation and atomic rollout |
| **Auditability & Compliance** | Ephemeral console logs | **Immutable SHA-256 provenance ledger** with deterministic decision replay |
| **Security & Secrets** | Secrets saved in history files | **Zero secret persistence**; strict credential and reasoning redaction |

---

## Quick Start (under 10 minutes)

### 1. Build and Install

```bash
git clone https://github.com/yhan-sun/AgentMesh.git
cd AgentMesh
cargo build --release
cp target/release/agentmesh ~/.cargo/bin/
```

### 2. Initialize and Verify Environment

In your Git repository root:

```bash
# 1. Initialize project configuration
agentmesh init

# 2. Check daemon, SQLite database, Git worktree capabilities, and agent CLIs
agentmesh doctor
```

```text
AgentMesh Doctor

Runtime
  ✓ Git (git version 2.45.0)
  ✓ SQLite (database connected, 16 migrations applied)
  ✓ Daemon (stopped, auto-starts on demand)

Agents
  ✓ Claude Code    (claude 1.2.0, ready)
  ✓ Codex          (codex 0.8.4, ready)
  ✓ OpenCode       (opencode 0.5.1, ready)
  ✓ Antigravity    (agy 2.1.0, ready)

Workspace
  ✓ Repository (/path/to/my-repo)
  ✓ Clean source (HEAD at a1b2c3d)
  ✓ Configuration (.agentmesh/config.toml)

Result:
  4 agents ready, 0 warning(s)
```

---

## 3 Core Demos

### Demo 1: Single Agent Run with Isolated Worktree & Safe Apply

Run an agent on an isolated Git worktree branch without risking your current working tree:

```bash
# Run Claude Code in an isolated worktree
agentmesh run claude "Add exponential backoff retry to src/client.rs"

# Inspect the workspace diff
agentmesh diff <TASK_ID>

# Review dry-run patch preview
agentmesh apply <TASK_ID> --check

# Safely merge changes into your working branch
agentmesh apply <TASK_ID> --yes
```

### Demo 2: Parallel DAG Workflow with Consensus Review & Fix Loop

Coordinate multiple specialized agents in a persistent directed acyclic graph:

```bash
# Run a full plan-build-review workflow
agentmesh workflow --preset full "Refactor user authentication to support OAuth2 PKCE"
```

1. **Architecture Phase**: Claude designs the OAuth2 contract.
2. **Parallel Implementation**: Codex and OpenCode implement backend and client components concurrently in dedicated worktrees.
3. **Consensus Review**: Antigravity and Claude independently inspect the diffs.
4. **Fix Loop**: If reviews detect issues, AgentMesh automatically routes actionable feedback to the implementing agent (up to 3 rounds).
5. **Safe Apply**: Inspect the final consolidated patch and apply cleanly.

```bash
# Check status, attach to live stream, or inspect audit trail
agentmesh workflow show <WORKFLOW_ID>
agentmesh workflow attach <WORKFLOW_ID>
```

### Demo 3: Best-of-N Competition with Blind Evaluation & Deterministic Winner

Run competing implementations in parallel session lanes and select the provably superior candidate:

```bash
# Start a Best-of-N competition workflow
agentmesh workflow --preset best-of-n "Optimize the JSON parser throughput by 2x"
```

1. **Candidate Generation**: Candidate A (Claude) and Candidate B (Codex) generate independent solutions in isolated lanes (`lane_candidate_a`, `lane_candidate_b`).
2. **Blind Evaluation**: Independent evaluators (OpenCode and Antigravity) assess anonymized diffs on correctness, performance, and issues.
3. **Deterministic SelectionGate**: The candidate with higher approval consensus and fewer issues wins deterministically.
4. **Winner-only Safe Apply**: Only the winning candidate branch is eligible for merge; losing branches are archived.
5. **Replay & Audit**:

```bash
# Deterministically replay decision logic from provenance ledger
agentmesh workflow replay <WORKFLOW_ID> --verify

# Export tamper-evident audit ledger
agentmesh workflow export <WORKFLOW_ID> --output audit.json
```

---

## CLI Reference

### Core Commands

| Command | Description |
| :--- | :--- |
| `agentmesh init [--force]` | Initialize `.agentmesh/config.toml` in current repository |
| `agentmesh doctor [--json]` | Diagnose system runtime, database, Git, and agent availability |
| `agentmesh config validate [--json]` | Validate syntax and semantic policy bounds of configuration |
| `agentmesh agents [--json]` | List registered agents and health status |
| `agentmesh run <agent> <prompt>` | Run an ad-hoc prompt on a specific agent |
| `agentmesh tasks [--status <s>] [--limit <n>]` | List tasks with optional status and limit filters |
| `agentmesh task <task_id> [--json]` | View detailed task status, sessions, and artifacts |
| `agentmesh diff <task_id>` | View unified Git diff generated by a task |
| `agentmesh apply <task_id> [--check] [--yes]` | Dry-run check or apply task changes to working repository |
| `agentmesh cancel <task_id>` | Cancel an active task and terminate child process tree |
| `agentmesh resume <task_id> <prompt>` | Resume an existing task session with follow-up input |

### Workflow & DAG Commands

| Command | Description |
| :--- | :--- |
| `agentmesh workflow "goal" [--preset <p>]` | Start a workflow (`standard`, `full`, `quick`, `best-of-n`) |
| `agentmesh workflows [--json]` | List all workflow runs |
| `agentmesh workflow show <id> [--json]` | Show graph status, nodes, edges, and dependencies |
| `agentmesh workflow attach <id>` | Attach to live SSE event stream of running workflow |
| `agentmesh workflow cancel <id>` | Gracefully cancel all running nodes in DAG |
| `agentmesh workflow resume <id>` | Resume workflow after interruption or crash |
| `agentmesh workflow replan <id> "reason"` | Propose runtime graph modification via Planner agent |
| `agentmesh workflow recover <id>` | Generate recovery child workflow for failed steps |
| `agentmesh workflow audit <id> [--ndjson]` | Inspect chronological immutable decision ledger |
| `agentmesh workflow replay <id> [--verify]` | Deterministically replay decisions from stored provenance |
| `agentmesh workflow export <id> -o file` | Export verifiable provenance ledger (JSON / NDJSON) |

### Exit Codes

AgentMesh adheres to strict, predictable process exit codes:

| Code | Status | Meaning |
| :---: | :--- | :--- |
| `0` | **Success** | Command completed successfully |
| `2` | **InvalidArgs / Config** | Missing arguments, syntax error, or invalid `.agentmesh/config.toml` |
| `3` | **AgentUnavailable** | Requested agent binary not found in `PATH` or adapter offline |
| `4` | **Task / Workflow Failed** | Execution failed, tests failed, or candidate unapproved |
| `5` | **Cancelled** | Operation was cancelled by user or signal |
| `6` | **PolicyViolation** | DAG budget exceeded (e.g. `max_nodes > 100`, illegal intents) |
| `7` | **Workspace / Git Error** | Dirty working tree, merge conflict, or worktree creation failure |
| `8` | **Daemon / Runtime Error**| Daemon communication error or database failure |
| `9` | **ProtocolError** | A2A framing or JSON-RPC protocol violation |
| `10`| **IntegrityFailure** | Cryptographic hash mismatch or provenance tampering detected |

---

## Architecture Overview

```text
┌────────────────────────────────────────────────────────┐
│                      AgentMesh CLI                     │
└───────────────────────────┬────────────────────────────┘
                            │ Unix Domain Socket / HTTP (Bearer Token)
┌───────────────────────────▼────────────────────────────┐
│                    AgentMesh Daemon                    │
│   ┌────────────────────────────────────────────────┐   │
│   │             Workflow / DAG Scheduler           │   │
│   │    (Parallel Nodes, SelectionGate, Recovery)   │   │
│   └───────────────────────┬────────────────────────┘   │
│                           │ A2A Protocol (JSON-RPC)    │
│   ┌───────────────────────▼────────────────────────┐   │
│   │               Per-Agent A2A Servers            │   │
│   └───────────────────────┬────────────────────────┘   │
│                           │ Process Adapter            │
│   ┌───────────────────────▼────────────────────────┐   │
│   │  Claude Code  │  Codex  │  OpenCode  │  AGY    │   │
│   └────────────────────────────────────────────────┘   │
│                           │ Isolated Worktrees         │
│   ┌───────────────────────▼────────────────────────┐   │
│   │  .agentmesh/workspaces/<task-id> (Git Branches)│   │
│   └────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────┘
```

For full architectural invariants, state machines, and identity models, see [`docs/architecture.md`](docs/architecture.md).

---

## Configuration (`.agentmesh/config.toml`)

Project configuration is stored in `.agentmesh/config.toml` in your repository root:

```toml
[agents.claude]
enabled = true
command = "claude"

[agents.codex]
enabled = true
command = "codex"

[agents.opencode]
enabled = true
command = "opencode"

[agents.antigravity]
enabled = true
command = "agy"

[routing]
architecture = ["claude", "codex", "opencode", "antigravity"]
implementation = ["codex", "opencode", "claude", "antigravity"]
review = ["claude", "codex", "opencode", "antigravity"]
testing = ["codex", "opencode", "claude", "antigravity"]

[evaluation]
default_evaluators = 3
default_quorum = 2
strategy = "majority"

[competition]
default_candidates = 2
max_candidates = 3
```

Validate your configuration anytime with:
```bash
agentmesh config validate
```

---

## Security & Git Safety Guarantees

1. **Zero Secret Persistence**: AgentMesh never stores API keys, keyring secrets, authentication headers, or vendor tokens in SQLite or logs.
2. **Deterministic Redaction**: Reasoning text and sensitive metadata are systematically redacted in exported audit logs.
3. **No Automatic Commits or Pushes**: AgentMesh never executes `git push` or commits directly to your working branch without explicit `--yes` confirmation.
4. **Isolated Worktrees**: Every agent execution occurs on a private branch (`agentmesh/<task-id>`) in a detached worktree directory.
5. **Cryptographic Provenance**: Every state transition and evaluation decision is recorded with SHA-256 hash chaining (`payload_hash`, `previous_hash`, `event_hash`).

---

## License

AgentMesh is open source software released under the [MIT License](LICENSE).
