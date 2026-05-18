--Them truong priority cho task
ALTER TABLE tasks
ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_tasks_queue_status_priority
ON tasks(queue, status, priority DESC, created_at);
