# Architecture

This document describes the architecture implemented in this repository today.
Where reliability gaps remain, those gaps are called out explicitly so future
work can improve the right parts.

## Overview

`task-queue` is a Spring Boot backend for creating and executing background
tasks. PostgreSQL stores durable task state. Redis stores live queue state.
Workers poll Redis, execute registered Java handlers, and update PostgreSQL with
status, result, heartbeat, and failure details.

The system aims for:

- Durable task records in PostgreSQL
- Priority-aware live queues in Redis
- Delayed execution through a Redis sorted set
- Retry with capped exponential backoff and jitter
- DLQ inspection and replay
- Idempotent task creation
- Operational queue controls
- Heartbeat-based stuck task recovery
- Retention cleanup for old terminal tasks

Delivery should be treated as at-least-once. Handlers must be idempotent.

## Component Map

```text
Clients
  |
  | HTTP
  v
api controllers
  |  TaskController / QueueController / DlqController / MetricsController
  v
services
  |  TaskService / QueueControllerService / TaskFailureService
  v
PostgreSQL <------------------------- workers ----------------------+
  | tasks table                         |                           |
  |                                     v                           |
  +------------------------------ RedisBroker ----------------------+
                                        |
                                        v
Redis keys:
  taskqueue:{queue}             ready queue sorted set
  taskqueue:{queue}:processing  in-flight task list
  taskqueue:delayed             delayed task sorted set
  taskqueue:dlq                 dead letter queue list
  taskqueue:scheduler:lock      delayed scheduler lock
  taskqueue:queues:paused       paused queue names set
```

Scheduled components:

- `WorkerBootstrap` submits virtual-thread workers for each configured queue.
- `DelayedTaskScheduler` promotes ready delayed tasks while holding a Redis lock.
- `StuckTaskReaper` finds old `RUNNING` DB tasks by `heartbeatAt` and routes
  them through retry/DLQ handling.
- `TaskRetentionCleaner` removes old terminal task rows.

## Package Responsibilities

```text
api/        HTTP endpoints, API DTO conversion, exception mapping
broker/     Redis operations, Redis key names, task JSON serialization
config/     queue properties and Redis template configuration
exception/  API/domain exceptions
model/      JPA task entity and status enum
reaper/     recovery of stale RUNNING tasks
retention/  scheduled cleanup of old terminal tasks
retry/      retry decision and backoff calculation
scheduler/  delayed task promotion
service/    task creation, lookup, cancel, retry, heartbeat, and queue operations
store/      Spring Data JPA repositories
worker/     task handlers, handler lookup, worker loop, worker scheduling
```

## Task Model

Every unit of work is a `Task` row in PostgreSQL.

| Field | Purpose |
| --- | --- |
| `id` | Task UUID |
| `queue` | Queue name, such as `default` or `high_priority` |
| `fn` | Handler name matched against `TaskHandler.fn()` |
| `payload` | JSON payload stored as text |
| `status` | `PENDING`, `RUNNING`, `DONE`, `FAILED`, or `DEAD` |
| `attempt` | Number of failed execution attempts |
| `maxRetries` | Retry limit for this task |
| `priority` | Higher values run earlier within a queue |
| `idempotencyKey` | Optional key for safe duplicate enqueue requests |
| `heartbeatAt` | Last heartbeat timestamp for a running task |
| `runAt` | Earliest execution time for delayed/retry tasks |
| `startedAt` | Last time a worker started this task |
| `createdAt` | Creation timestamp |
| `result` | Handler result JSON string |
| `error` | Last failure/cancel/purge message |
| `stackTrace` | Failure stack trace captured by `Worker` |

Main lifecycle:

```text
PENDING -> RUNNING -> DONE
PENDING -> RUNNING -> PENDING   retry scheduled with future runAt
PENDING -> RUNNING -> DEAD      retries exhausted and copied to DLQ
PENDING/RUNNING -> FAILED       cancel, manual purge, or administrative failure
DEAD -> PENDING                 DLQ replay
FAILED -> PENDING               manual task retry
```

## Enqueue Flow

`POST /tasks` calls `TaskService.enqueue`.

1. Validate the request body, queue, handler name, payload, retry count, and
   queue configuration.
2. Default a missing/blank queue to `default`.
3. Normalize optional `idempotencyKey`.
4. Default `maxRetries` from the queue configuration.
5. Default `priority` to `0`.
6. Serialize payload with Jackson 3 `JsonMapper`.
7. If `queue + idempotencyKey` exists, compare request shape and return the
   existing task when it matches.
8. If an existing idempotency key conflicts with different task details, return
   HTTP 409.
9. Save a `PENDING` task in PostgreSQL.
10. If `runAt` is absent or due, enqueue to the live Redis queue.
11. If `runAt` is in the future, enqueue to `taskqueue:delayed`.

Unknown queues are rejected before a DB row is created.

## Redis Broker

`RedisBroker` is the only application component that should directly manipulate
queue keys.

### Ready Queues

Each live queue is a Redis sorted set:

```text
taskqueue:{queue}
```

Members are serialized task JSON. Scores are calculated so higher priority
comes first, and tasks with the same priority preserve older-first order by
`createdAt`.

Polling uses a Redis Lua script to atomically move one task from the ready
sorted set into the processing list and return its serialized JSON.

Ready entries can be removed by task id for cancellation.

### Processing Queues

Each queue has an in-flight Redis list:

```text
taskqueue:{queue}:processing
```

When a worker finishes, `RedisBroker.acknowledge` removes the originally polled
JSON from this list. Stale entries can later be removed by task id.

### Delayed Queue

Delayed and retry tasks are stored in:

```text
taskqueue:delayed
```

This is a sorted set scored by `runAt.toEpochMilli()`. The scheduler calls a Lua
script that atomically finds and removes one due delayed entry. Java then loads
the DB task, verifies it still exists and is still `PENDING`, clears `runAt`,
saves it, and re-enqueues it to the live priority queue.

Delayed entries can be removed by task id for cancellation.

### Dead Letter Queue

The DLQ is a Redis list:

```text
taskqueue:dlq
```

When retries are exhausted, the worker marks the DB task `DEAD` and pushes task
JSON to this list. `/dlq` reads from Redis, and replay removes the Redis entry,
loads the DB task, resets it to `PENDING`, clears failure and heartbeat fields,
and re-enqueues it. Replay keeps the existing `attempt` count by default, or
resets it to `0` with `resetAttempts=true`.

### Scheduler Lock

The delayed scheduler lock uses:

```text
taskqueue:scheduler:lock
```

`RedisBroker.acquireSchedulerLock` uses Redis `SET NX` with a TTL. Lock release
uses a Lua script that deletes the key only if the stored token matches the
caller token. This prevents a slow scheduler instance from deleting another
instance's newer lock.

### Paused Queue State

Paused queues are stored in a Redis set:

```text
taskqueue:queues:paused
```

`QueueControllerService` delegates pause/resume/isPaused/list operations to
`RedisBroker`, so pause state survives application restarts.

## Worker Runtime

`WorkerBootstrap` is scheduled by:

```yaml
taskqueue.worker.poll-interval-ms
```

On each tick it:

1. Iterates configured queues from `QueueProperties`.
2. Skips queues paused by `QueueControllerService`.
3. Submits `queue.concurrency` calls to `Worker.runOnce(queue)`.
4. Runs those calls on `Executors.newVirtualThreadPerTaskExecutor()`.
5. Uses an `AtomicBoolean` guard to avoid overlapping submit batches.

`Worker.runOnce`:

1. Polls a task from Redis.
2. Loads the authoritative DB task by id.
3. If the DB task is not `PENDING`, acknowledges the Redis processing entry and
   exits.
4. Marks the DB task `RUNNING` and sets `startedAt` and `heartbeatAt`.
5. Looks up a handler by `fn`.
6. Executes `TaskHandler.handle(task)` through a timed future.
7. On success, reloads DB state, skips if no longer `RUNNING`, marks `DONE`,
   clears `heartbeatAt`, and stores result.
8. On failure or timeout, reloads DB state, skips if no longer `RUNNING`, and
   delegates to `TaskFailureService`.
9. Acknowledges the originally polled processing entry in `finally`.

Reloading before final saves prevents cancel/purge state from being overwritten
by a handler that finishes after an operator action.

## Retry And Failure

`TaskFailureService` centralizes worker failure, timeout, interruption, and
stuck-task handling.

`RetryPolicy.shouldRetry(task)` returns true while:

```text
task.attempt < task.maxRetries
```

Delay calculation:

```text
cappedDelay = min(baseDelayMs * 2^attempt, maxDelayMs)
actualDelay = random value between cappedDelay * (1 - jitterPercent) and cappedDelay
```

When retrying, failure handling sets:

- `attempt = attempt + 1`
- `status = PENDING`
- `runAt = now + actualDelay`
- `startedAt = null`
- `heartbeatAt = null`
- `error` and `stackTrace`

Then it saves the DB task and enqueues it into `taskqueue:delayed`.

When retries are exhausted, failure handling marks the task `DEAD`, clears
runtime timestamps, saves it, and pushes it to DLQ.

## Scheduler

`DelayedTaskScheduler` is scheduled by:

```yaml
taskqueue.scheduler.poll-interval-ms
```

It first tries to acquire `taskqueue:scheduler:lock` with TTL configured by:

```yaml
taskqueue.scheduler.lock-ttl-ms
```

If the lock is unavailable, the scheduler does nothing for that tick. If the
lock is acquired, it promotes ready delayed tasks until no due delayed task is
returned. The lock is released in `finally`.

## Stuck Task Reaper

`StuckTaskReaper` is scheduled by:

```yaml
taskqueue.reaper.poll-interval-ms
```

It uses:

```yaml
taskqueue.reaper.stuck-threshold-minutes
```

The reaper finds `RUNNING` tasks with old `heartbeatAt`, removes matching
processing Redis entries by task id, and routes each task through
`TaskFailureService`. Stale tasks therefore increment attempts, retry with
backoff when attempts remain, or move to DLQ when retries are exhausted.

## Queue Controls

`QueueController` exposes operational endpoints:

```http
GET /queues
POST /queues/{queue}/pause
POST /queues/{queue}/resume
POST /queues/{queue}/drain
POST /queues/{queue}/run-once
POST /queues/{queue}/run-once?force=true
POST /queues/{queue}/purge
POST /queues/delayed/purge
POST /queues/dlq/purge
```

Pause state is stored in Redis under `taskqueue:queues:paused`.

Drain pauses a queue without deleting ready work or marking DB tasks failed.

Normal `run-once` rejects paused queues. `run-once?force=true` is a manual
override that invokes `Worker.runOnce(queue)` even when paused.

Purging a named queue deletes ready and processing Redis keys and marks matching
`PENDING` or `RUNNING` PostgreSQL tasks as `FAILED` with error `Queue purged`.

Purging delayed deletes `taskqueue:delayed` and marks pending delayed DB tasks
as `FAILED` with error `Delayed queue purged`.

Purging DLQ deletes `taskqueue:dlq` and marks `DEAD` DB tasks as `FAILED` with
error `DLQ purged`.

## Metrics

Implemented metrics endpoints are JSON API endpoints, not custom Micrometer
meters:

```http
GET /metrics/tasks
GET /metrics/queues
```

Task metrics count PostgreSQL tasks by status. Queue metrics report Redis ready
and processing depths per configured queue, paused state, drained state, plus
delayed and DLQ depths.

A queue is drained when:

```text
paused == true && readyDepth == 0 && processingDepth == 0
```

Spring Actuator is configured to expose `health`, `info`, `metrics`, and
`prometheus`.

## Retention Cleanup

`TaskRetentionCleaner` is scheduled by:

```yaml
taskqueue.retention.poll-interval-ms
```

It skips cleanup when:

```yaml
taskqueue.retention.enabled: false
```

It deletes terminal task rows older than their configured windows:

- `DONE` uses `done-retention-days`
- `FAILED` uses `failed-retention-days`
- `DEAD` uses `dead-retention-days`

It never deletes `PENDING` or `RUNNING` tasks.

## API Reference

```http
POST /tasks
GET /tasks
GET /tasks?queue={queue}
GET /tasks?status={status}
GET /tasks?queue={queue}&status={status}
GET /tasks/{id}
POST /tasks/{id}/cancel
POST /tasks/{id}/retry
POST /tasks/{id}/heartbeat

GET /dlq
POST /dlq/{id}/replay
POST /dlq/{id}/replay?resetAttempts=true

GET /queues
POST /queues/{queue}/pause
POST /queues/{queue}/resume
POST /queues/{queue}/drain
POST /queues/{queue}/run-once
POST /queues/{queue}/run-once?force=true
POST /queues/{queue}/purge
POST /queues/delayed/purge
POST /queues/dlq/purge

GET /metrics/tasks
GET /metrics/queues
```

Example enqueue request:

```json
{
  "queue": "default",
  "fn": "log_message",
  "payload": {
    "message": "hello"
  },
  "runAt": null,
  "maxRetries": 3,
  "priority": 5,
  "idempotencyKey": "optional-client-key"
}
```

## Database Schema

The schema is managed by Flyway migrations:

```text
V1__create_tasks.sql
V2__add_task_priority.sql
V3__add_task_idempotency_key.sql
V4__add_task_heartbeat_at.sql
```

`tasks` has indexes for status, queue/status, pending `run_at`, running
`started_at`, queue/status/priority/created order, queue/idempotency key, and
running `heartbeat_at`.

JPA runs with:

```yaml
spring.jpa.hibernate.ddl-auto: validate
```

Any schema change needs a new migration and matching entity update.

## Configuration

Queue configuration lives under `taskqueue`:

```yaml
taskqueue:
  retention:
    enabled: true
    poll-interval-ms: 3600000
    done-retention-days: 7
    failed-retention-days: 30
    dead-retention-days: 30
  queues:
    high_priority:
      concurrency: 10
      max-retries: 5
      base-delay-ms: 500
      max-delay-ms: 10000
      jitter-percent: 0.5
    default:
      concurrency: 4
      max-retries: 3
      base-delay-ms: 1000
      max-delay-ms: 60000
      jitter-percent: 0.5
  worker:
    poll-interval-ms: 1000
    handler-timeout-ms: 30000
  scheduler:
    poll-interval-ms: 1000
    lock-ttl-ms: 10000
  reaper:
    stuck-threshold-minutes: 5
    poll-interval-ms: 30000
```

`QueueProperties` binds this to Java camelCase fields.

## Known Reliability Gaps

These are the main architecture improvements still open:

1. Consider a more transactional delayed promotion strategy across Redis and DB
   if stronger guarantees are needed.
2. Remove `TaskType` or turn it into a real enum/value object.
3. Add richer task execution history if debugging needs attempt-by-attempt
   events instead of only the latest error/result on the task row.

## Design Decisions

PostgreSQL stores the durable record because tasks need queryable status,
history, result, error state, heartbeat, and idempotency metadata. Redis stores
live broker state because it is a good fit for fast priority queues, delayed
sets, short-lived operational lists, distributed locks, and operational flags.

The worker model uses Java 21 virtual threads instead of a reactive stack. This
keeps handler and broker code straightforward while still allowing many blocking
poll/execute operations.

The architecture does not promise exactly-once execution. Idempotency belongs in
task handlers or downstream systems, usually keyed by `Task.id` or the
caller-provided `idempotencyKey`.
