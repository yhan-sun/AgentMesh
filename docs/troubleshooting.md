# AgentMesh Troubleshooting Guide

This guide covers common errors, diagnostic steps, and solutions when working with AgentMesh.

---

## 1. Quick Diagnostics

Always run `agentmesh doctor` as your first troubleshooting step:

```bash
agentmesh doctor
```

For machine-readable diagnostics:
```bash
agentmesh doctor --json
```

---

## 2. Common Issues & Solutions

### Issue 1: `Daemon connection error / Daemon is not running` (Exit Code 8)

**Symptom**: `agentmesh` commands report `cannot connect to daemon` or `connection refused`.

**Cause**: The daemon background process may have been terminated or a stale socket/metadata file exists.

**Resolution**:
1. Check daemon status:
   ```bash
   agentmesh daemon status
   ```
2. Stop any stale daemon processes:
   ```bash
   agentmesh daemon stop --force
   ```
3. Start the daemon manually or let CLI start it on demand:
   ```bash
   agentmesh daemon start
   ```

---

### Issue 2: `Git working tree has uncommitted modifications` (Exit Code 7)

**Symptom**: `agentmesh workflow` or `agentmesh apply` fails with `dirty source repository`.

**Cause**: Safe apply requires a clean working tree to ensure patches can be applied without conflicts or overwriting uncommitted work.

**Resolution**:
1. Commit or stash your changes:
   ```bash
   git stash
   ```
2. Re-run your apply or workflow command.

---

### Issue 3: `Agent binary not found in PATH` (Exit Code 3)

**Symptom**: `agentmesh doctor` flags an agent as `unavailable` or `agentmesh run <agent>` fails.

**Cause**: The agent executable (e.g. `claude`, `codex`, `opencode`, `agy`) is not installed or not in your shell's `PATH`.

**Resolution**:
1. Verify the binary is executable in your terminal:
   ```bash
   which claude
   which codex
   which opencode
   which agy
   ```
2. If installed in a non-standard path, update your `.agentmesh/config.toml`:
   ```toml
   [agents.claude]
   enabled = true
   command = "/opt/homebrew/bin/claude"
   ```
3. Validate the updated configuration:
   ```bash
   agentmesh config validate
   ```

---

### Issue 4: `Policy Violation: Graph exceeds node budget` (Exit Code 6)

**Symptom**: `agentmesh workflow replan` or `agentmesh plan` fails with `max_nodes limit exceeded`.

**Cause**: The planner generated a graph that violates the project safety budget in `.agentmesh/config.toml`.

**Resolution**:
1. Check your structural policy settings in `.agentmesh/config.toml`:
   ```toml
   [planner.policy]
   max_nodes = 50
   max_parallel = 4
   ```
2. Request a more focused replan with fewer sub-tasks.

---

### Issue 5: `Best-of-N: No acceptable candidate approved` (Exit Code 4)

**Symptom**: A Best-of-N competition workflow terminates in `Failed` status with `no candidate met quorum or approval threshold`.

**Cause**: All competing candidates produced issues flagged by the blind evaluation panel.

**Resolution**:
1. Inspect the evaluation reviews:
   ```bash
   agentmesh workflow evaluations <WORKFLOW_ID>
   ```
2. Inspect individual candidate diffs:
   ```bash
   agentmesh competition show <GROUP_ID>
   ```
3. Re-run with more detailed requirements or adjusted evaluator quorum in `.agentmesh/config.toml`.

---

## 3. Exit Code Cheat Sheet

| Exit Code | Name | Actionable Recovery |
| :---: | :--- | :--- |
| **`0`** | `Success` | None; operation succeeded. |
| **`2`** | `InvalidArgumentsOrConfig` | Check CLI flags with `--help` or run `agentmesh config validate`. |
| **`3`** | `AgentUnavailable` | Run `agentmesh doctor` and install missing agent CLI. |
| **`4`** | `WorkflowOrTaskFailed` | Inspect failure logs with `agentmesh task <id>` or `agentmesh workflow show <id>`. |
| **`5`** | `Cancelled` | Task/workflow was aborted cleanly. |
| **`6`** | `PolicyViolation` | Adjust DAG limits in `.agentmesh/config.toml`. |
| **`7`** | `WorkspaceOrGitError` | Clean working tree (`git stash`) or check permissions. |
| **`8`** | `DaemonOrRuntimeError` | Run `agentmesh daemon stop --force && agentmesh daemon start`. |
| **`9`** | `ProtocolError` | Verify agent adapter version compatibility. |
| **`10`**| `IntegrityOrReplayFailure`| Provenance hash verification failed; check for manual database edits. |
