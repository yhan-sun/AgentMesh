-- Workflow persistence (Phase 12): daemon-owned workflows with crash resume.
--
-- One row per workflow; step rows are appended as the (dynamic) plan grows.
-- Statuses are stable snake_case strings matching the orchestrator's
-- as_str/from_str wire format. Runtime ownership mirrors tasks
-- (0004_daemon_runtime.sql).

CREATE TABLE workflows (
    id TEXT PRIMARY KEY,

    preset TEXT NOT NULL,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,

    -- The single context shared by all steps; NULL until the first step
    -- reports one.
    context_id TEXT,

    -- WorkflowOptions serialized as JSON.
    options_json TEXT NOT NULL,

    -- Number of fix rounds already scheduled.
    review_rounds INTEGER NOT NULL DEFAULT 0,

    -- Owning daemon instance id (mirrors tasks.runtime_owner).
    runtime_owner TEXT,
    runtime_heartbeat_at TEXT,

    error TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE workflow_steps (
    id TEXT PRIMARY KEY,

    workflow_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,

    role TEXT NOT NULL,
    intent TEXT NOT NULL,
    status TEXT NOT NULL,

    agent_id TEXT,
    task_id TEXT,

    -- Which fix round this step belongs to (0 for the base plan).
    review_round INTEGER NOT NULL DEFAULT 0,

    -- The step's final agent message (handoff summary).
    summary TEXT,

    -- Serialized PersistedStepResult (summary, review result, ids).
    result_json TEXT,

    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    error TEXT,

    FOREIGN KEY(workflow_id)
        REFERENCES workflows(id)
        ON DELETE CASCADE,

    UNIQUE(workflow_id, ordinal)
);

CREATE INDEX idx_workflow_steps_workflow ON workflow_steps(workflow_id);
CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_workflows_runtime_owner ON workflows(runtime_owner);
