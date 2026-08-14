-- Multi-agent evaluation + consensus gate (Phase 21).
--
-- * evaluation_groups: one row per parallel evaluation of an implementation
--   snapshot, holding the deterministic consensus strategy/quorum and the
--   final ConsensusResult. The snapshot hash is recorded before evaluators
--   start and re-verified before consensus.
-- * evaluation_members: one row per evaluator, with its agent, task and the
--   structured verdict (result_json). Members never carry artifact content.

CREATE TABLE evaluation_groups (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    -- The task whose result is evaluated (the implementation task).
    source_task_id TEXT,

    -- majority | unanimous (deterministic local code, never the planner).
    strategy TEXT NOT NULL,
    -- Minimum valid evaluator results needed for a consensus.
    quorum INTEGER NOT NULL,

    -- pending | running | completed | failed | cancelled
    status TEXT NOT NULL,
    -- Serialized ConsensusResult once the gate has run (audit).
    consensus TEXT,

    -- Workspace snapshot hash the evaluators saw; re-verified before consensus.
    snapshot_hash TEXT,

    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE evaluation_members (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    -- The DAG node id of this evaluator (for mapping back to the graph).
    node_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    task_id TEXT,
    status TEXT NOT NULL,
    -- Serialized EvaluationResult (structured verdict), never artifact content.
    result_json TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX idx_evaluation_members_group ON evaluation_members(group_id);
CREATE INDEX idx_evaluation_groups_workflow ON evaluation_groups(workflow_id);
