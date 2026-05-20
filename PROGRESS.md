# Task Queue Progress

Use this file as handoff context for a new chat session.

## Project Goal

Build a Java 21 Spring Boot task queue backed by PostgreSQL and Redis.

Current design:

- PostgreSQL stores durable task state.
- Redis stores live queue state, delayed tasks, processing tasks, DLQ entries,
  scheduler locks, and paused queue state.
- REST APIs create, inspect, cancel, retry, replay, and operate queues.
- Workers poll Redis, execute registered handlers, update PostgreSQL, retry
  failures, and move exhausted tasks to DLQ.

Important package:

```text
com.nganhcc.task_queue
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

`QueueProperties` is bound with:

```java
@ConfigurationProperties(prefix = "taskqueue")
```

Queue config currently supports:

- `concurrency`
- `maxRetries`
- `baseDelayMs`

Worker config supports:

- `pollIntervalMs`
- `handlerTimeoutMs`

Scheduler config supports:

- `pollIntervalMs`
- `lockTtlMs`

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

`TaskRepository` supports lookup by status, queue, queue/status,
queue/status-in, pending delayed tasks, stale running tasks, and status counts.

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

- allows cancelling non-terminal tasks except `DONE` and `DEAD`
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
taskqueue:queues:paused
```

Current storage:

- live queue: Redis sorted set ordered by priority score
- processing queue: Redis list
- delayed queue: Redis sorted set scored by `runAt.toEpochMilli()`
- DLQ: Redis list
- paused queues: Redis set
- scheduler lock: Redis string with TTL and token

Priority behavior:

- higher `Task.priority` should run first
- same priority is ordered by `createdAt`
- live poll uses a Redis Lua script to atomically move one task from the ready
  sorted set into the processing list

Delayed behavior:

- due delayed poll uses a Redis Lua script to atomically find and remove one due
  delayed task JSON
- scheduler validates against PostgreSQL before live enqueue

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
  -> execute handler with timeout
  -> on success: mark DONE, store result, acknowledge
  -> on failure/timeout: increment attempt, store error and stackTrace
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
  failure fields, saves, and re-enqueues
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

Current limitation:

- stale tasks are directly re-enqueued and do not increment `attempt` or route
  through retry/DLQ exhaustion logic

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

- pause/resume state is persisted in Redis set `taskqueue:queues:paused`
- `run-once` manually invokes `Worker.runOnce(queue)`
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
- delayed depth
- DLQ depth

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

Focused verification command:

```bash
./mvnw test -Dtest=DlqControllerTest,QueueControllerTest,DelayedTaskSchedulerTest,WorkerTest,TaskServiceTest,QueueControllerServiceTest,RedisBrokerTest,TaskControllerTest
```

Full test suite may require PostgreSQL and Redis running locally because smoke
tests touch Docker-published ports.

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

Queue operations:

```bash
curl -i http://localhost:8080/queues
curl -i -X POST http://localhost:8080/queues/default/pause
curl -i -X POST http://localhost:8080/queues/default/resume
curl -i -X POST http://localhost:8080/queues/default/run-once
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
./mvnw test -Dtest=DlqControllerTest,QueueControllerTest,DelayedTaskSchedulerTest,WorkerTest,TaskServiceTest,QueueControllerServiceTest,RedisBrokerTest,TaskControllerTest
./mvnw test
docker compose up -d
docker compose ps
```

## Known Limitations / Next Improvements

1. Stuck reaper should use retry/DLQ policy.
   - Current behavior directly re-enqueues stale `RUNNING` tasks.
   - Better behavior would increment `attempt`, apply backoff, and send to DLQ
     when retries are exhausted.

2. Retry backoff could use jitter and a max cap.
   - Current exponential delay is deterministic and unbounded.

3. Delayed promotion could be made stronger across Redis and PostgreSQL.
   - Redis delayed removal is atomic, but DB validation and live enqueue are
     separate operations.

4. `TaskType` currently exists but has no behavior.
   - Remove it or turn it into a real enum/value object when handler typing
     needs it.
