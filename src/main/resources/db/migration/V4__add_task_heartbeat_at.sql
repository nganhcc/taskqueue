ALTER TABLE tasks ADD COLUMN heartbeat_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_tasks_running_heartbeat_at
ON tasks(status, heartbeat_at)
WHERE status = 'RUNNING';