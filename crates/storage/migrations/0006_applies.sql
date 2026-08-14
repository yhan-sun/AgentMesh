-- Apply persistence (Phase 13): one row per ApplyManager run.
--
-- A successful apply of a workspace's result is recorded here so a repeated
-- `agentmesh apply ... --yes` is rejected as AlreadyApplied while `--check`
-- stays available. Statuses are stable snake_case strings:
-- `planned`, `applying`, `completed`, `failed`.

CREATE TABLE applies (
    id TEXT PRIMARY KEY,

    -- Either the source task or the source workflow; exactly one is set.
    task_id TEXT,
    workflow_id TEXT,

    -- The workspace whose result was applied.
    workspace_id TEXT NOT NULL,

    -- Where the changes were written (the user's source repository).
    source_repository TEXT NOT NULL,

    -- The workspace base revision the source was required to match.
    base_revision TEXT NOT NULL,

    status TEXT NOT NULL,

    -- Bounded failure description for `failed` applies.
    error TEXT,

    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX idx_applies_workspace_status ON applies(workspace_id, status);
CREATE INDEX idx_applies_task_id ON applies(task_id);
CREATE INDEX idx_applies_workflow_id ON applies(workflow_id);
