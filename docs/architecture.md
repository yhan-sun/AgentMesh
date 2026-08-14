# AgentMesh 1.0 Architecture & Invariants

This document details the core architectural layers, execution flow, identity relationships, and long-term invariants of AgentMesh.

---

## 1. System Execution Flow

AgentMesh adopts a star topology where the Daemon acts as the single authoritative runtime orchestrator. Agents never communicate directly peer-to-peer; all interactions, streaming, state mutations, and evaluations pass through the Daemon.

```text
┌────────────────────────────────────────────────────────┐
│                      AgentMesh CLI                     │
│  (Human command execution, streaming renderer, DTOs)   │
└───────────────────────────┬────────────────────────────┘
                            │ Unix Domain Socket / HTTP (Bearer Token)
┌───────────────────────────▼────────────────────────────┐
│                    AgentMesh Daemon                    │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │             Workflow & DAG Scheduler             │  │
│  │   - Dependency resolution & parallel dispatch    │  │
│  │   - Session lane isolation                       │  │
│  │   - Blind evaluation panel & SelectionGate       │  │
│  │   - Consensus review & Fix Loop                  │  │
│  │   - Policy enforcement & Structural budgets      │  │
│  │   - Runtime Replan & Recovery child workflows    │  │
│  │   - Cryptographic SHA-256 Provenance Ledger      │  │
│  └────────────────────────┬─────────────────────────┘  │
│                           │ A2A Client (JSON-RPC)      │
│  ┌────────────────────────▼─────────────────────────┐  │
│  │               Per-Agent A2A Server               │  │
│  │   - Agent Card metadata endpoint                 │  │
│  │   - Task lifecycle endpoint                      │  │
│  │   - JSON-RPC 2.0 streaming backend              │  │
│  └────────────────────────┬─────────────────────────┘  │
│                           │ Adapter Trait              │
│  ┌────────────────────────▼─────────────────────────┐  │
│  │                Process Adapter                   │  │
│  │   (Claude Code / Codex / OpenCode / Antigravity) │  │
│  │   - Process tree spawn, monitoring & signal kill │  │
│  │   - Stream-JSON output parser                    │  │
│  │   - Native session ID tracking                   │  │
│  └────────────────────────┬─────────────────────────┘  │
│                           │ Subprocess execution       │
│  ┌────────────────────────▼─────────────────────────┐  │
│  │               Vendor Agent CLI                   │  │
│  └────────────────────────┬─────────────────────────┘  │
│                           │ Filesystem isolation       │
│  ┌────────────────────────▼─────────────────────────┐  │
│  │               Git Isolated Worktree              │  │
│  │    .agentmesh/workspaces/<task-id> (branch)      │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

---

## 2. Core Entities: Identity Distinction

In AgentMesh, **Context**, **AgentSession**, **Task**, and **Workspace** represent strictly decoupled operational concerns:

```text
┌────────────────────────────────────────────────────────────────────────┐
│ Context (UUID)                                                         │
│ High-level conversation thread spanning multiple turns / tasks        │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Task (UUID)                                                      │  │
│  │ Single executable unit: prompt, status, artifacts, timestamps     │  │
│  │                                                                  │  │
│  │  ┌───────────────────────────────┐ ┌───────────────────────────┐ │  │
│  │  │ AgentSession (UUID)           │ │ Workspace (UUID)          │ │  │
│  │  │ - Agent ID & Lane (UUID)      │ │ - Isolated Git Worktree   │ │  │
│  │  │ - Heartbeat Lease             │ │ - Branch: agentmesh/<id>  │ │  │
│  │  │ - Native Session Token        │ │ - Target repository path  │ │  │
│  │  └───────────────────────────────┘ └───────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### `Context`
- **What it is**: The high-level thread or project milestone context.
- **Role**: Groups related tasks across sequential user prompts or workflow executions. Multiple tasks can share a `context_id`.

### `Task`
- **What it is**: An atomic unit of agent execution with input prompt, output artifacts, start/completion timestamps, and status (`submitted`, `working`, `input_required`, `completed`, `failed`, `cancelled`).
- **Role**: The foundational billing and state tracking unit.

### `AgentSession`
- **What it is**: A runtime session binding an agent adapter process to a specific task and lane.
- **Role**: Tracks active process leases (`lease_expires_at`), session lanes (`lane_id`), and vendor-specific resumption keys (`native_session_id`, e.g. Claude session ID or Codex thread ID).
- **Isolation**: When competing candidates run in parallel, each receives a distinct `lane_id` and isolated `AgentSession` so native vendor histories never cross-contaminate.

### `Workspace`
- **What it is**: An isolated filesystem directory backed by a dedicated Git worktree branch (`agentmesh/<task-id>`).
- **Role**: Allows agents to modify, compile, test, and generate artifacts without altering the developer's working directory.

---

## 3. Core Architectural Invariants

### Invariant 1: Sole Daemon Runtime Ownership
- Only one active Daemon instance may execute tasks and manage SQLite state in a given scope (`User` or `Project`).
- Every live task is protected by a heartbeat lease (`SessionLeaseManager`). If a daemon crashes, leases expire safely and the recovery reconciler restores interrupted tasks without data loss.

### Invariant 2: State Machine Terminal Reachability
Every operation, task, and workflow is guaranteed to terminate in one of four terminal states:
- `Completed`: All steps/nodes completed successfully with approval.
- `Failed`: Step failed execution, tests failed, or no acceptable candidate was approved.
- `Cancelled`: Clean cancellation signal propagated to process tree; worktrees preserved for inspection.
- `Interrupted`: Daemon terminated unexpectedly; cleanly resumable via `agentmesh workflow resume <id>`.

### Invariant 3: Zero Secret Persistence & Strict Redaction
- SQLite databases, provenance event ledgers, and log files never store credentials, API keys, keyring tokens, or environment secrets.
- Reasoning text and prompt secrets are systematically redacted during `agentmesh workflow audit` and `agentmesh workflow export`.

### Invariant 4: Deterministic Decision Replay
- DAG scheduling, competition candidate selection (`SelectionGate`), and consensus evaluation outcomes are deterministic pure functions of their input artifacts and scores.
- Replaying a workflow via `agentmesh workflow replay <id> --verify` reconstructs identical decision trees without re-executing LLM prompts or re-applying Git patches.

### Invariant 5: Winner-Only Safe Apply
- In Best-of-N competition workflows, only the verified winner candidate branch can be merged via `ApplyManager`.
- Losing candidate branches are cleanly archived or pruned and cannot overwrite project source code.
- Apply operations always verify base revisions and provide dry-run `--check` before altering the working tree.
