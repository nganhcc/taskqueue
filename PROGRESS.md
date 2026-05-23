# Task Queue Progress

Use this file as handoff context for a new chat session.

## Project Goal

Build a Java 21 Spring Boot task queue backed by PostgreSQL and Redis.

Current design:

- PostgreSQL stores durable task state, payloads, results, errors, attempts,
  heartbeat timestamps, and idempotency keys.
- Redis stores live queue state, delayed tasks, processing tasks, DLQ entries,
  scheduler locks, and paused queue state.
- REST APIs create, inspect, cancel, retry, heartbeat, replay, and operate
  queues.
- Workers poll Redis, execute registered handlers, update PostgreSQL, retry
  failures, and move exhausted tasks to DLQ.

Important package:

```text
com.nganhcc.task_queue
```

Current branch checkpoint:

```text
feature/main-queue-stabilization
3efe456 Finish main task queue features
```

## Stack

- Java 21
- Spring Boot 4.0.6
- Spring Web MVC
- Spring Data JPA
- Spring Data Redis
- Flyway
- PostgreSQL 16 via Docker Compose
- Redis 7 via Docker Compose
- Jackson 3 / `tools.jackson.*` types from Spring Boot 4

## Implemented Features

### Configuration

Main config:

```text
src/main/resources/application.yaml
```

Implemented:

- PostgreSQL datasource
- Redis/Lettuce connection config
- Flyway migrations
- Actuator endpoint exposure
- `taskqueue.queues` config for `default` and `high_priority`
- worker polling interval and handler timeout
- scheduler polling interval and distributed lock TTL
- reaper polling interval and stuck threshold
- retention cleanup config

`QueueProperties` is bound with:

```java
@ConfigurationProperties(prefix = "taskqueue")
```

Queue config currently supports:

- `concurrency`
- `maxRetries`
- `baseDelayMs`
- `maxDelayMs`
- `jitterPercent`

Worker config supports:

- `pollIntervalMs`
- `handlerTimeoutMs`

Scheduler config supports:

- `pollIntervalMs`
- `lockTtlMs`

Reaper config supports:

- `stuckThresholdMinutes`
- `pollIntervalMs`

Retention config supports:

- `enabled`
- `pollIntervalMs`
- `doneRetentionDays`
- `failedRetentionDays`
- `deadRetentionDays`

### Database

Migrations:

```text
src/main/resources/db/migration/V1__create_tasks.sql
src/main/resources/db/migration/V2__add_task_priority.sql
src/main/resources/db/migration/V3__add_task_idempotency_key.sql
src/main/resources/db/migration/V4__add_task_heartbeat_at.sql
```

`Task` fields currently include:

- `id`
- `queue`
- `fn`
- `payload`
- `status`
- `attempt`
- `maxRetries`
- `priority`
- `idempotencyKey`
- `heartbeatAt`
- `runAt`
- `startedAt`
- `createdAt`
- `result`
- `error`
- `stackTrace`

`TaskStatus` values:

```text
PENDING
RUNNING
DONE
FAILED
DEAD
```

`TaskRepository` supports lookup by status, queue, queue/status,
queue/status-in, pending delayed tasks, stale heartbeat tasks, status counts,
idempotency lookup, and terminal-task retention deletes.

### Task API

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/api/TaskController.java
src/main/java/com/nganhcc/task_queue/service/TaskService.java
```

Endpoints:

```http
POST /tasks
GET /tasks
GET /tasks?queue=default
GET /tasks?status=PENDING
GET /tasks?queue=default&status=PENDING
GET /tasks/{id}
POST /tasks/{id}/cancel
POST /tasks/{id}/retry
POST /tasks/{id}/heartbeat
```

Create task behavior:

- validates request body, `fn`, payload, queue, retry count, and queue config
- defaults blank/missing queue to `default`
- defaults `maxRetries` from queue config
- defaults `priority` to `0`
- stores payload as JSON text
- saves task as `PENDING`
- enqueues immediate tasks to Redis
- enqueues future `runAt` tasks to delayed Redis sorted set
- supports optional `idempotencyKey`
- returns the existing task for repeated matching `queue + idempotencyKey`
- returns HTTP 409 when the same idempotency key is reused with different task
  details

Cancel behavior:

- allows cancelling non-terminal tasks except `DONE` and `DEAD`
- marks task `FAILED`
- sets error to `Task cancelled`
- clears `runAt`, `startedAt`, and `heartbeatAt`
- removes matching ready, delayed, and running processing Redis entries
- worker reloads DB state before saving success/failure so cancellation is not
  overwritten

Manual retry behavior:

- only allows `FAILED` tasks
- resets status to `PENDING`
- clears `runAt`, `startedAt`, `heartbeatAt`, `error`, and `stackTrace`
- re-enqueues task to Redis

Heartbeat behavior:

- only allows `RUNNING` tasks
- updates `heartbeatAt` to now
- returns the updated task

### Error Handling

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/api/ApiExceptionHandler.java
```

Mappings:

- `BadRequestException` -> HTTP 400
- unreadable JSON -> HTTP 400
- `TaskNotFoundException` -> HTTP 404
- `ConflictException` -> HTTP 409

### Redis Broker

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/broker/RedisBroker.java
src/main/java/com/nganhcc/task_queue/broker/RedisKeys.java
src/main/java/com/nganhcc/task_queue/broker/TaskSerializer.java
```

Redis keys:

```text
taskqueue:{queue}
taskqueue:{queue}:processing
taskqueue:delayed
taskqueue:dlq
taskqueue:scheduler:lock
taskqueue:queues:paused
```

Current storage:

- live queue: Redis sorted set ordered by priority score
- processing queue: Redis list
- delayed queue: Redis sorted set scored by `runAt.toEpochMilli()`
- DLQ: Redis list
- paused queues: Redis set
- scheduler lock: Redis string with TTL and token

Broker cleanup helpers can remove ready, delayed, processing, and DLQ entries
by task identity.

### Worker Runtime

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/worker/
```

Main classes:

- `TaskHandler`
- `HandlerRegistry`
- `LoggingTaskHandler`
- `Worker`
- `WorkerBootstrap`

Current worker flow:

```text
Worker.runOnce(queue)
  -> RedisBroker.poll(queue)
  -> if no task, return
  -> load DB task by id
  -> if DB task is not PENDING, acknowledge Redis processing entry and return
  -> mark RUNNING, set startedAt and heartbeatAt
  -> find handler by task.fn
  -> execute handler with timeout
  -> on success: reload DB, skip if no longer RUNNING, mark DONE, clear heartbeatAt
  -> on failure/timeout: reload DB, skip if no longer RUNNING
      -> TaskFailureService increments attempt and records error/stackTrace
      -> retry if attempts remain
      -> otherwise mark DEAD and send to DLQ
  -> acknowledge processing entry in finally
```

`WorkerBootstrap`:

- scheduled by `taskqueue.worker.poll-interval-ms`
- loops through configured queues
- skips queues paused in Redis
- submits up to `queue.concurrency` work items per tick
- uses Java 21 virtual threads
- has an `AtomicBoolean` guard to avoid overlapping submit batches
- shuts down its executor with `@PreDestroy`

### Retry

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/retry/RetryPolicy.java
src/main/java/com/nganhcc/task_queue/service/TaskFailureService.java
```

Current retry behavior:

- `shouldRetry(task)` returns `task.getAttempt() < task.getMaxRetries()`
- delay uses exponential backoff based on queue `baseDelayMs`
- delay is capped by queue `maxDelayMs`
- jitter reduces the capped delay by up to `jitterPercent`
- failed retryable tasks are set back to `PENDING`
- `runAt` is set to now + delay
- task is saved and put into `taskqueue:delayed`

### Delayed Scheduler

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/scheduler/DelayedTaskScheduler.java
```

Behavior:

- scheduled by `taskqueue.scheduler.poll-interval-ms`
- acquires `taskqueue:scheduler:lock` using configured `lock-ttl-ms`
- exits early when another app instance holds the lock
- polls ready delayed Redis tasks with score <= now through Lua
- loads matching DB task
- skips missing or non-`PENDING` tasks
- clears `runAt`
- saves DB task
- enqueues saved task back to the live priority queue
- releases scheduler lock in `finally`

### DLQ

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/api/DlqController.java
```

Endpoints:

```http
GET /dlq
POST /dlq/{id}/replay
POST /dlq/{id}/replay?resetAttempts=true
```

Behavior:

- exhausted tasks become `DEAD`
- worker pushes exhausted task JSON to `taskqueue:dlq`
- `/dlq` lists current DLQ entries
- replay removes the DLQ entry, loads the DB task, sets it to `PENDING`, clears
  failure and heartbeat fields, saves, and re-enqueues
- replay keeps `attempt` unchanged by default
- replay with `resetAttempts=true` resets `attempt` to `0`

### Stuck Task Reaper

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/reaper/StuckTaskReaper.java
```

Behavior:

- scheduled by `taskqueue.reaper.poll-interval-ms`
- uses `taskqueue.reaper.stuck-threshold-minutes`
- finds `RUNNING` tasks with old `heartbeatAt`
- removes matching stale processing Redis entry by task id
- routes stale tasks through `TaskFailureService`
- retries with backoff/jitter or sends exhausted tasks to DLQ

### Queue Operations API

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/api/QueueController.java
src/main/java/com/nganhcc/task_queue/service/QueueControllerService.java
```

Endpoints:

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

Behavior:

- pause/resume state is persisted in Redis set `taskqueue:queues:paused`
- drain pauses the queue without deleting ready work or failing DB tasks
- normal `run-once` rejects paused queues
- `run-once?force=true` manually invokes `Worker.runOnce(queue)` even when
  paused
- queue purge deletes live and processing Redis keys and marks matching
  `PENDING`/`RUNNING` DB tasks as `FAILED` with error `Queue purged`
- delayed purge deletes `taskqueue:delayed` and marks pending delayed DB tasks
  as `FAILED` with error `Delayed queue purged`
- DLQ purge deletes `taskqueue:dlq` and marks `DEAD` DB tasks as `FAILED` with
  error `DLQ purged`

### Metrics

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/api/MetricsController.java
```

Endpoints:

```http
GET /metrics/tasks
GET /metrics/queues
```

Task metrics:

- counts DB tasks by status using `countByStatus`

Queue metrics:

- live ready depth per queue
- processing depth per queue
- paused state per queue
- drained state per queue
- delayed depth
- DLQ depth

### Retention Cleanup

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/retention/TaskRetentionCleaner.java
```

Behavior:

- scheduled by `taskqueue.retention.poll-interval-ms`
- skips when `taskqueue.retention.enabled=false`
- deletes old terminal tasks only: `DONE`, `FAILED`, and `DEAD`
- uses separate retention windows for done, failed, and dead tasks

### Tests

Current focused tests include:

```text
src/test/java/com/nganhcc/task_queue/api/DlqControllerTest.java
src/test/java/com/nganhcc/task_queue/api/QueueControllerTest.java
src/test/java/com/nganhcc/task_queue/api/TaskControllerTest.java
src/test/java/com/nganhcc/task_queue/broker/RedisBrokerTest.java
src/test/java/com/nganhcc/task_queue/scheduler/DelayedTaskSchedulerTest.java
src/test/java/com/nganhcc/task_queue/service/QueueControllerServiceTest.java
src/test/java/com/nganhcc/task_queue/service/TaskServiceTest.java
src/test/java/com/nganhcc/task_queue/worker/WorkerTest.java
```

Also present:

```text
src/test/java/com/nganhcc/task_queue/SmokeTest.java
src/test/java/com/nganhcc/task_queue/TaskQueueApplicationTests.java
```

Normal verification:

```bash
./mvnw test
```

Smoke verification, requires Docker services:

```bash
docker compose up -d
./mvnw test -Dtaskqueue.smoke=true
```

Latest verification:

```text
Tests run: 52, Failures: 0, Errors: 0, Skipped: 3
```

## Manual API Examples

Start dependencies and app:

```bash
docker compose up -d
./mvnw spring-boot:run
```

Create immediate task:

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "fn": "log_message",
    "payload": {
      "message": "hello"
    },
    "priority": 5,
    "idempotencyKey": "manual-log-message-1"
  }'
```

Create delayed task:

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "fn": "log_message",
    "payload": {
      "message": "later"
    },
    "runAt": "2030-01-01T00:00:00Z",
    "priority": 1
  }'
```

Task operations:

```bash
curl -i http://localhost:8080/tasks/{id}
curl -i -X POST http://localhost:8080/tasks/{id}/heartbeat
curl -i -X POST http://localhost:8080/tasks/{id}/cancel
curl -i -X POST http://localhost:8080/tasks/{id}/retry
```

Queue operations:

```bash
curl -i http://localhost:8080/queues
curl -i -X POST http://localhost:8080/queues/default/pause
curl -i -X POST http://localhost:8080/queues/default/resume
curl -i -X POST http://localhost:8080/queues/default/drain
curl -i -X POST http://localhost:8080/queues/default/run-once
curl -i -X POST 'http://localhost:8080/queues/default/run-once?force=true'
curl -i -X POST http://localhost:8080/queues/default/purge
curl -i -X POST http://localhost:8080/queues/delayed/purge
curl -i -X POST http://localhost:8080/queues/dlq/purge
```

DLQ:

```bash
curl -i http://localhost:8080/dlq
curl -i -X POST http://localhost:8080/dlq/{id}/replay
curl -i -X POST 'http://localhost:8080/dlq/{id}/replay?resetAttempts=true'
```

Metrics:

```bash
curl -i http://localhost:8080/metrics/tasks
curl -i http://localhost:8080/metrics/queues
```

Redis inspection:

```bash
docker exec -it taskqueue-redis redis-cli
ZRANGE taskqueue:default 0 -1 WITHSCORES
LRANGE taskqueue:default:processing 0 -1
ZRANGE taskqueue:delayed 0 -1 WITHSCORES
LRANGE taskqueue:dlq 0 -1
SMEMBERS taskqueue:queues:paused
GET taskqueue:scheduler:lock
```

## Useful Commands

```bash
./mvnw -q -DskipTests compile
./mvnw test
./mvnw test -Dtaskqueue.smoke=true
docker compose up -d
docker compose ps
```

## Known Limitations / Next Improvements

1. Delayed promotion could be made stronger across Redis and PostgreSQL.
   - Redis delayed removal is atomic, but DB validation and live enqueue are
     separate operations.

2. `TaskType` currently exists but has no behavior.
   - Remove it or turn it into a real enum/value object when handler typing
     needs it.

3. There is no dashboard/frontend yet.
   - Current operation is API-first.
