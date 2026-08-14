-- Runtime replanning + safe DAG mutation (Phase 19).
--
-- * workflows.graph_revision: bumped on every successful replan apply, so a
--   proposal's `base_graph_revision` gates whether it can still apply (a stale
--   proposal whose base no longer matches is rejected, not silently applied).
-- * workflow_steps.objective: a node's untrusted planner objective is now
--   persisted on the node row itself, so a replanned DAG (which no longer
--   matches the original plan) survives a crash and resumes with the same
--   objectives. Legacy rows keep NULL.
-- * workflow_replans: one row per user-triggered replan proposal, from the
--   A2A planner through validation to the atomic apply.

ALTER TABLE workflows ADD COLUMN graph_revision INTEGER NOT NULL DEFAULT 1;

ALTER TABLE workflow_steps ADD COLUMN objective TEXT;

CREATE TABLE workflow_replans (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    status TEXT NOT NULL,

    -- The agent that acted as the replan planner (A2A) and its task id.
    planner_agent_id TEXT,
    planner_task_id TEXT,

    -- The validated WorkflowPlanDelta once generation succeeds.
    delta_json TEXT,

    -- Why the proposal is `invalid` or `failed` (parse/validation error).
    validation_error TEXT,

    -- The workflow's graph_revision the delta was generated against; apply is
    -- only allowed while the workflow is still on this revision.
    base_graph_revision INTEGER NOT NULL,

    -- The workflow's graph_revision after a successful apply (audit).
    applied_graph_revision INTEGER,

    created_at TEXT NOT NULL,
    applied_at TEXT
);

CREATE INDEX idx_workflow_replans_workflow ON workflow_replans(workflow_id);
