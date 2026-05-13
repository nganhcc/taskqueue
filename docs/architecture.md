# Architecture

## Overview

`taskqueue` is a distributed task queue built on Java 21 + Redis + PostgreSQL. Clients submit work via a REST API; background workers pick it up, execute it, and store results. Failed tasks are retried with exponential backoff and eventually moved to a dead letter queue if all retries are exhausted.

The system is designed around three guarantees:

- **At-least-once delivery** — a task is never silently lost, even if a worker crashes mid-execution
- **Ordered retry with backoff** — failed tasks are re-queued at increasing intervals with jitter to prevent thundering herd
- **Inspectable failures** — dead tasks preserve their full error context and can be replayed without redeployment

---

## Component map

```
┌─────────────────────────────────────────────────────────────────┐
│  Clients                                                        │
│  POST /tasks  ─────────────────────────────────────────────┐   │
│  GET  /tasks/{id}  ◄──────────────────────────────────┐    │   │
└──────────────────────────────────────────────────────────────── ┘
                                                         │    │
                                              ResultStore│    │EnqueueController
                                              (Postgres) │    │
                                                         │    ▼
┌──────────────────────────────────────────────────────────────────┐
│  Broker layer (Redis)                                            │
│                                                                  │
│   taskqueue:default  ──────────────────► WorkerPool (default)   │
│   taskqueue:high_priority  ────────────► WorkerPool (high)      │
│                                               │  │               │
│   taskqueue:delayed (sorted set) ──► Scheduler│  │               │
│                                               │  │               │
│   taskqueue:dlq  ◄────────────── RetryEngine ◄──┘               │
│                                       │                          │
│   taskqueue:{q}:processing ◄──────────┘  (in-flight set)        │
│                          ▲                                       │
│                       Reaper (crash recovery)                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Data model

Every unit of work is a `Task`. It is the only entity that flows through the entire system.

| Field | Type | Description |
|---|---|---|
| `id` | UUID | Immutable, assigned at enqueue time |
| `queue` | String | Which queue this task belongs to (`default`, `high_priority`, etc.) |
| `fn` | String | Handler name — maps to a registered `TaskHandler` |
| `payload` | JSON | Arguments passed to the handler |
| `status` | Enum | `PENDING → RUNNING → DONE / FAILED / DEAD` |
| `attempt` | int | Current attempt number, starts at 0 |
| `max_retries` | int | Max attempts before moving to DLQ |
| `run_at` | Instant | Earliest time the task should execute (used for delayed tasks) |
| `started_at` | Instant | When a worker last picked this task up |
| `created_at` | Instant | When the task was first enqueued |
| `result` | String | JSON result from the handler on success |
| `error` | String | Exception message on failure |
| `stack_trace` | String | Full stacktrace on failure, preserved in DLQ |

### Task lifecycle

```
                    ┌─────────────────────────────────────┐
  POST /tasks       │                                     │
       │            ▼                                     │
       └──► PENDING ──► [worker picks up] ──► RUNNING     │
                                                 │        │
                                    success ─────┤        │
                                                 ▼        │
                                               DONE       │
                                                          │
                                    failure ──► attempt++ │
                                                 │        │
                              attempt < max ─────┤        │
                                                 ▼        │
                                            PENDING (delayed, re-queued)
                                                          │
                              attempt == max ─────────────┘
                                                 │
                                                 ▼
                                               DEAD (moved to DLQ)
```

---

## Broker

The broker is the only component that talks directly to Redis. Everything else goes through it.

### Queue implementation

Each named queue is a Redis **list**. Producers call `LPUSH`, workers call `RPOPLPUSH` (right-to-left = FIFO).

```
LPUSH taskqueue:default  <task_json>     # enqueue
RPOPLPUSH taskqueue:default taskqueue:default:processing  # worker pops atomically
```

`RPOPLPUSH` is the key operation for at-least-once delivery — see [Crash recovery](#crash-recovery) below.

### Delayed tasks

Tasks with a future `run_at` go into a Redis **sorted set** scored by Unix timestamp:

```
ZADD taskqueue:delayed <unix_ms> <task_json>
```

The Scheduler wakes every second and moves ready tasks into their target queue:

```
ZRANGEBYSCORE taskqueue:delayed 0 <now_ms>   # find ready tasks
ZREM + LPUSH (in a Lua script)               # atomic move to queue
```

The Lua script makes the pop-and-push atomic — without it, a crash between `ZREM` and `LPUSH` would silently drop a task.

---

## Worker pool

Each queue has its own `WorkerPool` — a fixed set of workers polling that queue concurrently. Pool sizes are configured per queue in `application.yml`.

### Worker loop

```
while running:
    task = broker.poll(queue, timeout=5s)   // RPOPLPUSH — blocks up to 5s
    if task == null: continue               // timeout, nothing in queue

    task.status = RUNNING
    task.startedAt = now()
    repo.save(task)

    try:
        handler = registry.find(task.fn)
        result = handler.handle(task)
        task.status = DONE
        task.result = result
    catch Exception e:
        retryEngine.handleFailure(task, e)
    finally:
        repo.save(task)
        broker.acknowledge(task)            // remove from :processing set
```

### Java 21 virtual threads

Each worker runs in a virtual thread via `Executors.newVirtualThreadPerTaskExecutor()`. The JVM multiplexes thousands of virtual threads onto a small number of OS threads — blocking Redis polls don't waste OS thread resources.

This means pool sizes can be set much higher than with traditional thread pools before hitting OS limits.

---

## Crash recovery

This is the core mechanism that makes delivery reliable.

### The problem

If a worker picks up a task and crashes before finishing, the task disappears from the queue but never completes. Standard `RPOP` has no recovery path.

### The solution: processing set + reaper

`RPOPLPUSH` atomically moves the task from the queue into a separate `taskqueue:{queue}:processing` set in one Redis operation. Two outcomes:

- **Worker succeeds or retries normally** → worker calls `LREM` to remove the task from `:processing`
- **Worker crashes** → task remains in `:processing` forever

The `Reaper` runs every 30 seconds and finds tasks stuck in `:processing` for longer than the stuck threshold (default 5 minutes):

```java
@Scheduled(fixedDelay = 30_000)
void reclaimStaleTasks() {
    Instant cutoff = Instant.now().minus(stuckThreshold);
    // find all tasks in :processing where startedAt < cutoff
    // hand each one to retryEngine.handleFailure(task, new WorkerCrashException())
}
```

From the retry engine's perspective, a crashed worker looks identical to a handler exception — the task goes through normal retry/DLQ logic.

---

## Retry engine

Called whenever a handler throws an exception or a worker crash is detected.

### Backoff formula

```java
long delay = baseDelayMs * (1L << attempt)           // exponential: 1s, 2s, 4s, 8s...
           + ThreadLocalRandom.current().nextLong(0, 1000);  // jitter: 0–1000ms
delay = Math.min(delay, MAX_DELAY_MS);               // cap at 60s
```

The jitter prevents **thundering herd**: if 500 tasks all fail simultaneously and retry at exactly the same interval, they all hammer the downstream service again at once. Random jitter spreads them out.

### Decision logic

```
attempt++
if attempt < max_retries:
    task.status = PENDING
    task.run_at = now + delay
    broker.enqueueDelayed(task)    // ZADD into sorted set
else:
    task.status = DEAD
    broker.sendToDLQ(task)         // LPUSH into taskqueue:dlq
```

### Retry configuration

Each queue has its own retry policy in `application.yml`:

```yaml
taskqueue:
  queues:
    default:
      max_retries: 3
      base_delay_ms: 1000
    high_priority:
      max_retries: 5
      base_delay_ms: 500
```

---

## Dead letter queue

The DLQ is a Redis list (`taskqueue:dlq`). Tasks land here when all retries are exhausted.

Every dead task retains:
- Full error message and stack trace from the last failure
- Attempt count and history
- Original payload — exactly as submitted

### Replay

`POST /dlq/{taskId}/replay` resets the task to `attempt=0`, `status=PENDING`, and pushes it back into its original queue. It goes through the full worker → retry cycle again from scratch.

This is intentional: replay is for after you've fixed the bug. The task runs exactly as if it were freshly enqueued.

---

## Scheduler

Responsible for moving delayed tasks into the live queue when their `run_at` time arrives.

```java
@Scheduled(fixedDelay = 1_000)
void tick() {
    if (!acquireLock()) return;        // only one instance runs at a time
    try {
        broker.flushReadyDelayedTasks();   // ZRANGEBYSCORE + atomic move via Lua
    } finally {
        releaseLock();
    }
}
```

### Distributed lock

When running multiple app instances (e.g. for high availability), both Schedulers would otherwise pop the same delayed tasks simultaneously — causing duplicate executions.

The lock uses Redis `SETNX` with a 5-second TTL:

```
SETNX taskqueue:scheduler:lock <instance_id>  EX 5
```

Only the instance that wins the `SETNX` runs the tick. The TTL ensures the lock is released even if the winning instance crashes mid-tick.

---

## Result store

Task results and status are persisted to PostgreSQL via Spring Data JPA. Redis holds the live queue state; Postgres holds the durable record.

This separation means:
- Redis can be flushed without losing task history
- You can query task history with SQL (filter by status, queue, time range)
- The dashboard reads from Postgres, not Redis, so it doesn't add load to the broker

### Schema

```sql
CREATE TABLE tasks (
    id            UUID PRIMARY KEY,
    queue         VARCHAR(100) NOT NULL,
    fn            VARCHAR(200) NOT NULL,
    payload       TEXT NOT NULL,
    status        VARCHAR(20) NOT NULL,
    attempt       INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 3,
    run_at        TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL,
    result        TEXT,
    error         TEXT,
    stack_trace   TEXT
);

CREATE INDEX idx_tasks_status  ON tasks(status);
CREATE INDEX idx_tasks_queue   ON tasks(queue, status);
CREATE INDEX idx_tasks_run_at  ON tasks(run_at) WHERE status = 'PENDING';
```

---

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/tasks` | Enqueue a task. Returns `task_id` immediately. |
| `GET` | `/tasks/{id}` | Get task status and result. |
| `GET` | `/tasks?queue=X&status=Y` | List tasks with filters. |
| `GET` | `/dlq` | List all dead tasks. |
| `POST` | `/dlq/{id}/replay` | Re-enqueue a dead task from scratch. |
| `DELETE` | `/dlq/{id}` | Permanently discard a dead task. |
| `GET` | `/metrics/queues` | Queue depths and throughput (for dashboard). |
| `GET` | `/actuator/prometheus` | Prometheus metrics endpoint. |

### Enqueue request

```json
POST /tasks
{
  "queue": "default",
  "fn": "send_email",
  "payload": { "to": "user@example.com", "subject": "Hello" },
  "run_at": "2026-05-13T12:00:00Z",   // optional — omit for immediate
  "max_retries": 3                     // optional — uses queue default
}
```

### Task response

```json
{
  "id": "9f3a1b2c-...",
  "queue": "default",
  "fn": "send_email",
  "status": "DONE",
  "attempt": 1,
  "createdAt": "2026-05-13T10:00:00Z",
  "startedAt": "2026-05-13T10:00:01Z",
  "result": "{ \"messageId\": \"msg-882\" }",
  "error": null
}
```

---

## Observability

### Metrics (Micrometer → Prometheus)

| Metric | Type | Tags |
|---|---|---|
| `taskqueue.depth` | Gauge | `queue` |
| `taskqueue.dlq.size` | Gauge | — |
| `taskqueue.tasks.completed` | Counter | `queue`, `fn` |
| `taskqueue.tasks.failed` | Counter | `queue`, `fn` |
| `taskqueue.task.duration` | Timer | `queue`, `fn` |
| `taskqueue.workers.active` | Gauge | `queue` |

### Structured logging

Every log line inside a worker includes the task ID via MDC:

```
2026-05-13T10:00:01Z INFO  Worker [taskId=9f3a1b2c queue=default fn=send_email attempt=1] executing handler
2026-05-13T10:00:02Z INFO  Worker [taskId=9f3a1b2c queue=default fn=send_email attempt=1] completed in 834ms
```

This makes it trivial to trace a single task's journey across log lines without a tracing system.

---

## Failure modes and mitigations

| Failure | What happens | Mitigation |
|---|---|---|
| Worker crashes mid-task | Task stuck in `:processing` | Reaper rescues after 5 min |
| Redis goes down | Workers stop polling; app still accepts enqueues (buffered) | Redis restart restores queue state from AOF/RDB |
| Postgres goes down | Results not written; workers continue executing | Tasks re-execute on restart (at-least-once); idempotent handlers recommended |
| Scheduler crashes mid-tick | Lock TTL expires in 5s; next instance acquires lock | No duplicate execution |
| All retries exhausted | Task moves to DLQ | Operator inspects and replays after fix |
| Thundering herd on retry | All failed tasks retry simultaneously | Jitter in backoff formula spreads retries |
| Two Schedulers running | Both try to move delayed tasks | SETNX lock ensures only one wins |

---

## Design decisions

**Why Redis for the queue, not Postgres?**
Redis `BRPOP`/`RPOPLPUSH` are O(1) and designed for this exact pattern. Polling a Postgres table with `SELECT FOR UPDATE SKIP LOCKED` works but adds write amplification and requires tuning. Redis gives lower latency and simpler queue semantics. Postgres is used for what it's good at: durable storage and queryable history.

**Why not Kafka?**
Kafka is the right choice at high scale (millions of tasks/sec, multiple consumer groups, replay from offset). For a portfolio project and most real workloads under 10k tasks/sec, Redis is simpler to operate and reason about. The architecture is designed so the broker interface (`RedisBroker`) can be swapped for a Kafka implementation without touching worker or retry logic.

**Why Java 21 virtual threads over a reactive framework?**
Reactive code (WebFlux, Project Reactor) is harder to read and debug. Virtual threads give the same throughput for I/O-bound workloads with blocking-style code that's straightforward to follow. This is the explicit recommendation from the Java team for most server workloads.

**Why at-least-once instead of exactly-once?**
Exactly-once delivery requires distributed transactions across Redis and Postgres, which adds significant complexity and latency. At-least-once with idempotent handlers is simpler, more performant, and sufficient for most task types. Handlers that must not run twice (e.g. charge a credit card) should implement idempotency using the task ID.