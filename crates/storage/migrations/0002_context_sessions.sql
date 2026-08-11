-- Contexts and agent sessions: persistent session mapping.
--
-- Tasks gain optional context/session links. Old tasks (Phase 4) keep
-- NULL links and remain fully readable. SQLite cannot add foreign key
-- constraints via ALTER TABLE, so the links are validated at the
-- application layer.

CREATE TABLE contexts (
    id TEXT PRIMARY KEY,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE agent_sessions (
    id TEXT PRIMARY KEY,

    context_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,

    native_session_id TEXT,

    workspace TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    FOREIGN KEY(context_id)
        REFERENCES contexts(id)
        ON DELETE CASCADE,

    UNIQUE(context_id, agent_id)
);

CREATE INDEX idx_agent_sessions_context_id ON agent_sessions(context_id);
CREATE INDEX idx_agent_sessions_native_session_id ON agent_sessions(native_session_id);

ALTER TABLE tasks ADD COLUMN context_id TEXT;
ALTER TABLE tasks ADD COLUMN agent_session_id TEXT;

CREATE INDEX idx_tasks_context_id ON tasks(context_id);
CREATE INDEX idx_tasks_agent_session_id ON tasks(agent_session_id);
