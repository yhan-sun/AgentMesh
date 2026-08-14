# AgentMesh 1.0 Release Checklist

This document defines the strict quality gates required before tagging an AgentMesh 1.0 Release Candidate.

---

## 1. Quality Gates Matrix

| Verification Area | Command | Required Result | Status |
| :--- | :--- | :--- | :---: |
| **Workspace Compilation** | `cargo check --workspace` | Exit `0`, zero compiler errors | PASS |
| **Code Formatting** | `cargo fmt --all -- --check` | Exit `0`, formatted to standard | PASS |
| **Clippy Linter** | `cargo clippy --all-targets -- -D warnings` | Exit `0`, zero warnings | PASS |
| **Workspace Test Suite** | `cargo test --workspace` | Exit `0`, all unit & integration tests pass | PASS |
| **20-Round Stress Suite**| `./scripts/stress-core.sh 20` | Exit `0`, 20/20 rounds with zero flakes | PASS |
| **Migration Integrity** | `cargo test -p agentmesh-storage --test migrations` | Exit `0`, full 0001–0016 chain preserved | PASS |
| **CLI UX & Exit Codes** | `cargo test --test cli_1_0` | Exit `0`, init, doctor, config validation pass | PASS |

---

## 2. End-to-End Lifecycle Verification

Run the following end-to-end flow in a clean temporary Git repository:

1. **Project Initialization**:
   ```bash
   mkdir /tmp/agentmesh-test && cd /tmp/agentmesh-test
   git init && git config user.email "test@agentmesh.dev" && git config user.name "Test"
   echo "# Test" > README.md && git add . && git commit -m "initial commit"
   agentmesh init
   ```
2. **Environment Diagnostics**:
   ```bash
   agentmesh doctor
   agentmesh config validate
   ```
3. **Task & Isolated Worktree Verification**:
   ```bash
   agentmesh run mock "Generate test artifact"
   agentmesh diff <TASK_ID>
   agentmesh apply <TASK_ID> --check
   agentmesh apply <TASK_ID> --yes
   ```
4. **Audit Ledger & Deterministic Replay**:
   ```bash
   agentmesh workflow audit <WORKFLOW_ID>
   agentmesh workflow replay <WORKFLOW_ID> --verify
   agentmesh workflow export <WORKFLOW_ID> --output audit.json
   ```

---

## 3. Sign-off Criteria

- [x] All 16 database migrations execute cleanly and idempotently on both empty and existing databases.
- [x] Zero process orphans after cancellation (`kill child process tree`).
- [x] Zero unredacted credentials or reasoning tokens in exported logs.
- [x] Winner-only apply enforced on Best-of-N competition workflows.
- [x] 100% stable exit codes (`0`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`, `10`).
- [x] Documentation complete (`README.md`, `docs/architecture.md`, `docs/troubleshooting.md`, `CHANGELOG.md`, `BACKLOG.md`).

**Release Sign-off Status**: `AgentMesh 1.0 Release Candidate: READY`
