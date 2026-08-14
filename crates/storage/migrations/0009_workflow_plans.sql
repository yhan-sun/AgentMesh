-- AI Planner workflow plans (Phase 17): a generated plan that has NOT yet
-- been executed is stored here; once executed it links to the persisted
-- workflow. SQL stays out of orchestrator/CLI — only repositories touch it.

CREATE TABLE workflow_plans (
    id TEXT PRIMARY KEY,

    -- The original natural-language goal the planner was given.
    goal TEXT NOT NULL,

    -- generating | ready | invalid | failed | executed
    status TEXT NOT NULL,

    -- The agent that acted as the planner (A2A) and its task id.
    planner_agent_id TEXT,
    planner_task_id TEXT,

    -- The validated plan JSON once generation succeeds (NULL otherwise).
    plan_json TEXT,

    -- Why the plan is `invalid` or `failed` (parse/validation error).
    validation_error TEXT,

    -- The persisted workflow created by `plan execute`; NULL until executed.
    workflow_id TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    executed_at TEXT
);

CREATE INDEX idx_workflow_plans_created ON workflow_plans(created_at DESC);
CREATE INDEX idx_workflow_plans_workflow ON workflow_plans(workflow_id);
