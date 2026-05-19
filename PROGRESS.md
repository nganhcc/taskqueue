# Task Queue Progress

Use this file as handoff context for a new chat session.

## Project Goal

Build a Java 21 Spring Boot task queue backed by PostgreSQL and Redis.

Current design:

- PostgreSQL stores durable task state.
- Redis stores live queue state, delayed tasks, processing tasks, and DLQ entries.
- REST APIs create, inspect, cancel, retry, replay, and operate queues.
- Workers poll Redis, execute registered handlers, update PostgreSQL, retry failures, and move exhausted tasks to DLQ.

Important package:

```text
com.nganhcc.task_queue
```

Current branch:

```text
feature-task-queue-backend
```

Remote branch:

```text
origin/feature-task-queue-backend
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
- worker polling interval
- scheduler polling interval
- reaper polling interval and stuck threshold

`QueueProperties` is bound with:

```java
@ConfigurationProperties(prefix = "taskqueue")
```

Queue config currently supports:

- `concurrency`
- `maxRetries`
- `baseDelayMs`

### Database

Migrations:

```text
src/main/resources/db/migration/V1__create_tasks.sql
src/main/resources/db/migration/V2__add_task_priority.sql
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

`TaskRepository` supports lookup by status, queue, queue/status, queue/status-in, delayed run time, stale started time, and status counts.

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
```

Create task behavior:

- validates request body, `fn`, payload, queue, and retry count
- defaults blank/missing queue to `default`
- defaults `maxRetries` from queue config
- defaults `priority` to `0`
- stores payload as JSON text
- saves task as `PENDING`
- enqueues immediate tasks to Redis
- enqueues future `runAt` tasks to delayed Redis sorted set

Cancel behavior:

- allows cancelling non-terminal tasks
- marks task `FAILED`
- sets error to `Task cancelled`
- worker skips non-`PENDING` tasks if stale Redis entries are later polled

Manual retry behavior:

- only allows `FAILED` tasks
- resets status to `PENDING`
- clears `runAt`, `startedAt`, `error`, and `stackTrace`
- re-enqueues task to Redis

### Error Handling

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/api/ApiExceptionHandler.java
```

Mappings:

- `BadRequestException` -> HTTP 400
- unreadable JSON -> HTTP 400
- `TaskNotFoundException` -> HTTP 404

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
```

Current storage:

- live queue: Redis sorted set ordered by priority score
- processing queue: Redis list
- delayed queue: Redis sorted set scored by `runAt.toEpochMilli()`
- DLQ: Redis list

Priority behavior:

- higher `Task.priority` should run first
- same priority is ordered by `createdAt`
- live poll uses a Redis Lua script to atomically move one task from the ready sorted set into the processing list

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
  -> mark RUNNING and set startedAt
  -> find handler by task.fn
  -> execute handler
  -> on success: mark DONE, store result, acknowledge
  -> on failure: increment attempt, store error
      -> retry if attempts remain
      -> otherwise mark DEAD and send to DLQ
```

`WorkerBootstrap`:

- scheduled by `taskqueue.worker.poll-interval-ms`
- loops through configured queues
- skips paused queues
- submits up to `queue.concurrency` work items per tick
- uses Java 21 virtual threads
- has an `AtomicBoolean` guard to avoid overlapping submit batches
- shuts down its executor with `@PreDestroy`

### Retry

Implemented in:

```text
src/main/java/com/nganhcc/task_queue/retry/RetryPolicy.java
```

Current retry behavior:

- `shouldRetry(task)` returns `task.getAttempt() < task.getMaxRetries()`
- delay uses exponential backoff based on queue `baseDelayMs`
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
- finds delayed Redis tasks with score <= now
- removes them from delayed Redis set
- loads matching DB task
- skips missing or non-`PENDING` tasks
- clears `runAt`
- saves DB task
- enqueues saved task back to the live priority queue

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
- replay removes the DLQ entry, loads the DB task, sets it to `PENDING`, clears failure fields, saves, and re-enqueues
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
- finds `RUNNING` tasks with old `startedAt`
- removes matching stale processing Redis entry by task id
- marks DB task `PENDING`
- clears `startedAt`
- saves and re-enqueues task

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
POST /queues/{queue}/run-once
POST /queues/{queue}/purge
POST /queues/delayed/purge
POST /queues/dlq/purge
```

Behavior:

- pause/resume state is in memory
- `run-once` manually invokes `Worker.runOnce(queue)`
- queue purge deletes live and processing Redis keys and marks matching `PENDING`/`RUNNING` DB tasks as `FAILED` with error `Queue purged`
- delayed purge deletes `taskqueue:delayed`
- DLQ purge deletes `taskqueue:dlq`

Important limitation:

- pause state is not persisted. Restarting the app clears paused queues.

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
- delayed depth
- DLQ depth

### Tests

Current focused tests:

```text
src/test/java/com/nganhcc/task_queue/api/TaskControllerTest.java
src/test/java/com/nganhcc/task_queue/broker/RedisBrokerTest.java
src/test/java/com/nganhcc/task_queue/service/TaskServiceTest.java
src/test/java/com/nganhcc/task_queue/worker/WorkerTest.java
```

Also present:

```text
src/test/java/com/nganhcc/task_queue/SmokeTest.java
src/test/java/com/nganhcc/task_queue/TaskQueueApplicationTests.java
```

Targeted verification command used recently:

```bash
./mvnw test -Dtest=WorkerTest,TaskServiceTest,RedisBrokerTest,TaskControllerTest
```

Result on 2026-05-18:

```text
21 tests, 0 failures
```

Full test suite may require PostgreSQL and Redis running locally because smoke tests touch Docker-published ports.

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
    "priority": 5
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

List tasks:

```bash
curl -i http://localhost:8080/tasks
curl -i 'http://localhost:8080/tasks?queue=default&status=PENDING'
```

Get task:

```bash
curl -i http://localhost:8080/tasks/{id}
```

Cancel or retry:

```bash
curl -i -X POST http://localhost:8080/tasks/{id}/cancel
curl -i -X POST http://localhost:8080/tasks/{id}/retry
```

Queue operations:

```bash
curl -i http://localhost:8080/queues
curl -i -X POST http://localhost:8080/queues/default/pause
curl -i -X POST http://localhost:8080/queues/default/resume
curl -i -X POST http://localhost:8080/queues/default/run-once
curl -i -X POST http://localhost:8080/queues/default/purge
```

DLQ:

```bash
curl -i http://localhost:8080/dlq
curl -i -X POST http://localhost:8080/dlq/{id}/replay
curl -i -X POST http://localhost:8080/queues/dlq/purge
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
```

## Useful Commands

```bash
./mvnw -q -DskipTests compile
./mvnw test -Dtest=WorkerTest,TaskServiceTest,RedisBrokerTest,TaskControllerTest
./mvnw test
docker compose up -d
docker compose ps
```

## Current Git State

Clean feature branch was pushed:

```text
feature-task-queue-backend
```

Commits on the branch:

```text
feat: add task lifecycle API and Redis broker
feat: add worker runtime with retry scheduling
feat: add operational queue and DLQ APIs
test: cover task service broker and worker flows
docs: add project progress handoff
```

PR URL:

```text
https://github.com/nganhcc/taskqueue/pull/new/feature-task-queue-backend
```

## Known Limitations / Next Improvements

1. Queue pause state is in memory.
   - Restarting the app clears paused queues.
   - Persist pause state in Redis if operational persistence is needed.

2. Purging delayed and DLQ queues only clears Redis.
   - Per-queue purge updates DB tasks, but delayed/DLQ purge does not update matching DB rows.

3. Tests need broader coverage.
   - Add focused tests for `QueueController`, `MetricsController`, `DelayedTaskScheduler`, and `StuckTaskReaper`.
   - Update smoke/full integration tests as the API stabilizes.

4. `TaskType` currently exists but has no behavior.
   - Remove it or turn it into a real enum/value object when handler typing needs it.
