# AgentMesh Changelog

All notable changes to AgentMesh are documented in this file.

---

## [1.0.0] - 2026-08-14

### Summary
AgentMesh 1.0 is the first production-ready release of the A2A runtime and orchestrator for heterogeneous coding agents.

### Core Features

#### Multi-Agent Runtime & A2A Server
- **Heterogeneous Adapters**: Native support for Claude Code, Codex, OpenCode, Antigravity, and Mock adapters.
- **A2A Protocol**: Full A2A Agent Card discovery, JSON-RPC 2.0 streaming, and bidirectional communication.
- **Sole Daemon Runtime**: Background daemon managing process execution, session leases, and atomic state transitions.

#### Git Worktree & Workspace Isolation
- **Isolated Worktrees**: Every task executes in a dedicated Git worktree branch (`agentmesh/<task-id>`).
- **Safe Apply**: Two-phase patch application with `--check` dry-run validation and `--yes` atomic merge.
- **Workspace Lifecycle**: Worktree archival, pruning, and dirty repository detection.

#### Workflow & Parallel DAG Scheduling
- **Directed Acyclic Graphs**: Persistent DAG execution with parallel node dispatch.
- **Policy & Budgets**: Structural DAG limits on node count, concurrency, intents, and roles.
- **Runtime Replan**: Dynamic in-flight DAG modification via Planner agents with delta validation.
- **Recovery Workflows**: Automated recovery child workflows for failed steps with workspace continuity.

#### Consensus Review & Best-of-N Competitions
- **Consensus Review Loop**: Multi-evaluator independent review panel with automated fix loops (up to 3 rounds).
- **Best-of-N Competition**: Parallel candidate implementation in isolated session lanes (`lane_id`).
- **Blind Evaluation Panel**: Anonymized diff review by independent evaluator agents.
- **Deterministic SelectionGate**: Pure-function winner selection based on approval consensus and issue ranking.
- **Winner-Only Apply**: Enforced merge restrictions ensuring only verified winning candidate branches are applied.

#### Provenance, Audit & Deterministic Replay
- **Cryptographic Provenance Ledger**: Immutable SHA-256 event chaining (`payload_hash`, `previous_hash`, `event_hash`).
- **Deterministic Decision Replay**: Offline replay of scheduler decisions and competition selection without calling LLMs or Git.
- **Tamper-evident Export**: JSON / NDJSON export with automatic credential and reasoning redaction.

#### Developer Experience & CLI UX
- **Structured CLI**: Full support for `--json` output format across all commands.
- **Predictable Exit Codes**: Standardized exit codes (`0`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`, `10`).
- **`agentmesh init`**: Minimal `.agentmesh/config.toml` project initialization with overwrite guards.
- **`agentmesh doctor`**: Comprehensive system diagnostic for Git, SQLite, Daemon, and agent binaries.
- **`agentmesh config validate`**: Detailed syntax and semantic policy bounds validation.
