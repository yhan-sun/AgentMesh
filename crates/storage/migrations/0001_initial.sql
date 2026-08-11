-- Initial AgentMesh state schema: tasks and artifacts.
-- Agent credentials are never stored here.

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    status TEXT NOT NULL,
    prompt TEXT NOT NULL,

    workspace TEXT,

    error TEXT,

    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT
);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,

    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    mime_type TEXT,

    path TEXT,
    content TEXT,
    metadata TEXT,

    created_at TEXT NOT NULL,

    FOREIGN KEY(task_id)
        REFERENCES tasks(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_artifacts_task_id ON artifacts(task_id);
CREATE INDEX idx_tasks_created_at ON tasks(created_at DESC);
