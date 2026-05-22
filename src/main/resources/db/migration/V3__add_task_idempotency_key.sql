ALTER TABLE tasks ADD COLUMN idempotency_key VARCHAR(200);

CREATE UNIQUE INDEX ux_tasks_queue_idempotency_key
ON tasks(queue, idempotency_key)
WHERE idempotency_key IS NOT NULL;