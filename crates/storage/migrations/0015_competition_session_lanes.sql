-- 0015_competition_session_lanes.sql
-- Session lanes and competition storage (Phase 23)

-- Rebuild agent_sessions to add session_lane with UNIQUE(context_id, agent_id, session_lane).
CREATE TABLE agent_sessions_new (
    id TEXT PRIMARY KEY,
    context_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    session_lane TEXT NOT NULL DEFAULT 'default',
    native_session_id TEXT,
    workspace TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(context_id)
        REFERENCES contexts(id)
        ON DELETE CASCADE,
    UNIQUE(context_id, agent_id, session_lane)
);

INSERT INTO agent_sessions_new (id, context_id, agent_id, session_lane, native_session_id, workspace, created_at, updated_at)
SELECT id, context_id, agent_id, 'default', native_session_id, workspace, created_at, updated_at
FROM agent_sessions;

DROP TABLE agent_sessions;
ALTER TABLE agent_sessions_new RENAME TO agent_sessions;

CREATE INDEX idx_agent_sessions_context_id ON agent_sessions(context_id);
CREATE INDEX idx_agent_sessions_native_session_id ON agent_sessions(native_session_id);
CREATE INDEX idx_agent_sessions_lane ON agent_sessions(context_id, agent_id, session_lane);

-- Competition groups (Best-of-N): one row per competition group in a workflow.
CREATE TABLE competition_groups (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    source_workspace TEXT,
    base_revision TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    winner_candidate_id TEXT,
    winner_task_id TEXT,
    winner_workspace_id TEXT,
    winner_snapshot_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(workflow_id)
        REFERENCES workflows(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_competition_groups_workflow ON competition_groups(workflow_id);

-- Competition candidates: one row per implementation candidate in a group.
CREATE TABLE competition_candidates (
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    session_lane TEXT NOT NULL,
    task_id TEXT,
    workspace_id TEXT,
    snapshot_hash TEXT,
    evaluation_group_id TEXT,
    status TEXT NOT NULL,
    summary TEXT,
    patch_path TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(group_id)
        REFERENCES competition_groups(id)
        ON DELETE CASCADE,
    UNIQUE(group_id, candidate_id)
);

CREATE INDEX idx_competition_candidates_group ON competition_candidates(group_id);
CREATE INDEX idx_competition_candidates_task ON competition_candidates(task_id);
