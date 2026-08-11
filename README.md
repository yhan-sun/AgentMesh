# AgentMesh

A2A runtime and orchestrator for coding agents.

Unify different coding agents (Claude Code, Codex, OpenCode, ...) behind a
single adapter interface, then discover, invoke, stream, schedule and compose
them from one orchestrator. Star topology — every agent is managed by the
orchestrator, no agent-to-agent mesh.

```text
Coding Agent
    ↓
Adapter
    ↓
A2A Agent Server
    ↓
AgentMesh Orchestrator
    ↓
Task / Context / Workspace / Artifact
```

## Status

V0.1 (in development):

- Cargo workspace skeleton
- Core domain model (task, context, session, artifact, events)
- Config system (project `.agentmesh/config.toml` > user `~/.config/agentmesh/config.toml` > defaults)
- Unified `CodingAgentAdapter` trait
- `mock` agent (works without any external CLI)
- `claude` agent: Claude Code via `claude -p --verbose --output-format stream-json`
- `codex` agent: Codex via `codex exec --json`, default sandbox `read-only`
- TaskManager + SQLite persistence (`tasks` / `task <id>` commands)
- Process runtime (spawn / stream / cancel / exit / env / cwd)
- A2A Agent Card protocol types
- CLI: `agentmesh agents`, `agentmesh run <agent> <prompt>`, `agentmesh doctor`

## Quick start

```bash
cargo build
agentmesh agents
agentmesh doctor
agentmesh run mock "hello"
agentmesh run claude "Reply with exactly: hello"
```

Expected output:

```text
NAME      STATUS    SKILLS
mock      online    mock
claude    online    code, architecture, debug, review

[claude] task started
[claude] hello
[claude] task completed

Task: 0b1f7a7c-...
```

## Configuration

`.agentmesh/config.toml` (project) and `~/.config/agentmesh/config.toml`
(user) are layered over the built-in defaults. Example:

```toml
[agents.claude]
enabled = true
command = "claude"

[agents.claude.env]
ANTHROPIC_API_KEY = "sk-..."
```

Agents not installed are reported as `offline`; AgentMesh itself keeps working.
Codex defaults to its `read-only` sandbox; enable `workspace-write` explicitly:

```toml
[agents.codex]
enabled = true
command = "codex"

[agents.codex.options]
sandbox = "workspace-write"
```

## Local state

AgentMesh stores task metadata and prompts locally in SQLite.

Project databases live under:

```text
<git-root>/.agentmesh/agentmesh.db
```

When no Git repository is detected, the database falls back to the user data
directory (`~/.local/share/agentmesh/agentmesh.db`). No agent credentials are
stored by AgentMesh.

## Session persistence

AgentMesh persists the mapping between its contexts and native coding-agent
sessions locally. This allows a task to be resumed after AgentMesh exits:

```bash
agentmesh run claude "Remember 123"

agentmesh resume <task-id> "What did I ask you to remember?"
```

A resume continues the same agent session (same context) in a **new** task.
Agent credentials are never stored by AgentMesh.

## Workspace isolation

Coding-agent sessions run in isolated Git worktrees:

```text
~/.local/share/agentmesh/workspaces/<repo-key>/<agent-session-id>/
```

A workspace belongs to an **AgentSession**, not an individual Task. Resumed
tasks continue in the same worktree, so agent work accumulates across
resumes. AgentMesh never automatically merges or commits agent changes.

```bash
agentmesh run codex "refactor auth"

agentmesh workspace <task-id>   # path, branch, base revision, changed files
agentmesh diff <task-id>        # cumulative git patch since the base revision
```

Fresh runs require a clean source repository (commit or stash first); the
`changes.patch` artifact records the workspace diff on completion. Add
`.agentmesh/` to your `.gitignore` so AgentMesh's local database does not
make the repository look dirty.

## Layout

```text
crates/
├── core/       domain model shared by all crates
├── a2a/        A2A protocol types (Agent Card, ...)
├── adapters/   CodingAgentAdapter trait, mock agent, registry
├── runtime/    managed child-process lifecycle
└── cli/        agentmesh CLI (clap)
```

## Roadmap

- P0: Claude + Codex adapters, task lifecycle + SQLite, context/session,
  workspace isolation via `git worktree`, streaming
- P1: A2A agent server, agent discovery, rule router
- P2: OpenCode, Antigravity adapters
- P3: multi-agent workflows, desktop UI (React + Tauri)
