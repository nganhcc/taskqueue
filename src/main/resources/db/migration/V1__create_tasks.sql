CREATE TABLE tasks (
    id            UUID PRIMARY KEY,
    queue         VARCHAR(100)  NOT NULL,
    fn            VARCHAR(200)  NOT NULL,
    payload       TEXT          NOT NULL,
    status        VARCHAR(20)   NOT NULL,
    attempt       INT           NOT NULL DEFAULT 0,
    max_retries   INT           NOT NULL DEFAULT 3,
    run_at        TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ   NOT NULL,
    result        TEXT,
    error         TEXT,
    stack_trace   TEXT
);

-- Cach PostGres build Indexing:
-- 1. Build the Index (B-tree) -> O(N log N) - loop qua N rows trong table, va insert O(log N)
-- 2. Lookup and search O(log N) - balanced tree
-- 3. Insert O(log N) - Phai lap qua toi da h= log N layers de hoan doi vi tri lam nen Balanced tree

-- One-time cost: Building the index is expensive, but you usually do it once (or rarely).
-- Ongoing benefit: After it's built, all your SELECT queries that use the index become dramatically faster.
-- Maintenance cost: Every time you INSERT, UPDATE, or DELETE a row, PostgreSQL must also update the index → this adds a small O(log n) overhead per write.

-- Fast lookup by status (dashboard, reaper)
CREATE INDEX idx_tasks_status
    ON tasks (status);

-- Fast lookup by queue + status (worker polling history)
CREATE INDEX idx_tasks_queue_status
    ON tasks (queue, status);

-- Partial index for pending tasks with a future run_at (scheduler)
CREATE INDEX idx_tasks_run_at_pending
    ON tasks (run_at)
    WHERE status = 'PENDING';

-- Fast lookup of running tasks older than N minutes (reaper)
CREATE INDEX idx_tasks_started_at_running
    ON tasks (started_at)
    WHERE status = 'RUNNING';