CREATE TABLE task_events (
    id          UUID PRIMARY KEY,
    task_id     UUID          NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    type        VARCHAR(50)   NOT NULL,
    attempt     INT           NOT NULL,
    message     TEXT,
    error       TEXT,
    stack_trace TEXT,
    created_at  TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_task_events_task_id_created_at
    ON task_events (task_id, created_at);

