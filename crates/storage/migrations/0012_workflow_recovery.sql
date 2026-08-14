-- Failure recovery + bounded self-healing (Phase 20).
--
-- * workflows.parent_workflow_id / recovery_of_node_id / recovery_attempt:
--   a recovery runs as a NEW child workflow, never by reopening the failed
--   parent. The failed parent stays Failed; its immutable history is preserved.
-- * workflow_recoveries: one row per failure-recovery proposal, from the
--   Failure Analyzer (an ordinary A2A agent) through validation to the atomic
--   execute claim and the child workflow it created.

ALTER TABLE workflows ADD COLUMN parent_workflow_id TEXT;
ALTER TABLE workflows ADD COLUMN recovery_of_node_id TEXT;
ALTER TABLE workflows ADD COLUMN recovery_attempt INTEGER NOT NULL DEFAULT 0;

CREATE TABLE workflow_recoveries (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    failed_node_id TEXT NOT NULL,
    status TEXT NOT NULL,

    -- The agent that acted as the Failure Analyzer (A2A) and its task id.
    planner_agent_id TEXT,
    planner_task_id TEXT,

    -- The validated recovery WorkflowPlan once generation succeeds.
    plan_json TEXT,

    -- Why the proposal is `invalid` or `failed` (parse/validation error).
    validation_error TEXT,

    -- The child workflow created by `recovery execute`; NULL until executed.
    recovery_workflow_id TEXT,

    -- Which attempt this recovery is for (1 = first recovery of the parent).
    attempt INTEGER NOT NULL,

    created_at TEXT NOT NULL,
    executed_at TEXT
);

CREATE INDEX idx_workflow_recoveries_workflow ON workflow_recoveries(workflow_id);
