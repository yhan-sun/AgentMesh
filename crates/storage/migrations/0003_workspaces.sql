-- Workspaces: isolated Git worktrees bound to agent sessions.
--
-- One agent session has exactly one workspace. Links to agent_sessions are
-- validated at the application layer (SQLite cannot add FK constraints via
-- ALTER TABLE).

CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,

    agent_session_id TEXT NOT NULL UNIQUE,

    repository_root TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,

    branch TEXT NOT NULL,
    base_revision TEXT NOT NULL,

    state TEXT NOT NULL,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_workspaces_state ON workspaces(state);
