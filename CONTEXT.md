# Agent Context

This file is the working brief for coding agents. Keep it accurate when the
codebase changes.

## Project Snapshot

`task-queue` is a Java 21 Spring Boot 4.0.6 backend for a Redis/PostgreSQL task
queue.

Current implementation is a feature-complete backend foundation:

- REST APIs for tasks, queues, DLQ, and metrics
- PostgreSQL durable task storage through Spring Data JPA and Flyway
- Redis live queue state through `RedisTemplate<String, String>`
- Immediate queues with priority ordering
- Delayed task scheduling and promotion
- Atomic Redis Lua polling for ready and delayed queues
- Distributed Redis lock around delayed scheduling
- Worker polling and handler execution on Java virtual threads
- Handler timeout with shared retry/DLQ failure handling
- Retry with exponential backoff, max delay cap, and jitter
- Dead letter queue replay with optional attempt reset
- Stuck task reaper based on `heartbeatAt`
- Task heartbeat endpoint
- Persistent task execution history with `GET /tasks/{id}/events`
- Paginated task and task-event listing with validated sorting
- Redis-backed persistent queue pause state
- Queue pause, resume, drain, run-once, force run-once, and purge operations
- Queue, delayed, and DLQ purge operations that update DB task state
- Queue metrics with paused and drained state
- Idempotent task creation with `idempotencyKey` and HTTP 409 conflict handling
- Scheduled retention cleanup for old terminal tasks
- Focused controller, service, broker, scheduler, and worker tests

There is no React dashboard in this repository yet.

## Stack

- Java 21
- Spring Boot 4.0.6
- Spring Web MVC
- Spring Data JPA
- Spring Data Redis
- Flyway
- PostgreSQL 16
- Redis 7
- Maven
- Jackson 3 APIs under `tools.jackson.*`

Use package `com.nganhcc.task_queue`. The Maven artifact is hyphenated, but
Java package names are not.

## Source Layout

```text
src/main/java/com/nganhcc/task_queue/
  TaskQueueApplication.java
  api/              REST controllers and response/error DTOs
  broker/           Redis key, serialization, and queue operations
  config/           application properties and Redis template config
  exception/        domain/API exceptions
  model/            JPA task entity and status enum
  reaper/           stuck RUNNING task recovery
  retention/        terminal task cleanup
  retry/            retry decision and backoff calculation
  scheduler/        delayed task promotion
  service/          task and queue orchestration
  store/            Spring Data repositories
  worker/           handlers, registry, worker loop, worker bootstrap

src/main/resources/
  application.yaml
  db/migration/

src/test/java/com/nganhcc/task_queue/
```

Only create packages when they contain real code. Do not add empty placeholder
folders.

## Runtime Model

PostgreSQL is the durable source of truth for task state and history. Redis is
the live broker for ready work, processing work, delayed work, DLQ entries,
scheduler locks, and paused queue state.

High-level flow:

```text
POST /tasks
  -> TaskService validates, handles idempotency, and stores a PENDING task
  -> RedisBroker enqueues now, or puts future runAt tasks into taskqueue:delayed
  -> WorkerBootstrap schedules per-queue virtual-thread worker submissions
  -> Worker polls Redis, loads the DB task, marks RUNNING, sets heartbeatAt
  -> handler executes with timeout
  -> success marks DONE
  -> failure routes through TaskFailureService for retry or DLQ
```

Handlers implement:

```java
public interface TaskHandler {
    String fn();
    String handle(Task task);
}
```

The built-in handler is `log_message`, implemented by `LoggingTaskHandler`.

## Public API

Implemented endpoints:

```http
POST /tasks
GET /tasks
GET /tasks?queue=default
GET /tasks?status=PENDING
GET /tasks?queue=default&status=PENDING
GET /tasks/{id}
GET /tasks/{id}/events
POST /tasks/{id}/cancel
POST /tasks/{id}/retry
POST /tasks/{id}/heartbeat

GET /queues
POST /queues/{queue}/pause
POST /queues/{queue}/resume
POST /queues/{queue}/drain
POST /queues/{queue}/run-once
POST /queues/{queue}/run-once?force=true
POST /queues/{queue}/purge
POST /queues/delayed/purge
POST /queues/dlq/purge

GET /dlq
POST /dlq/{id}/replay
POST /dlq/{id}/replay?resetAttempts=true

GET /metrics/tasks
GET /metrics/queues
```

`GET /tasks` and `GET /tasks/{id}/events` return paginated response objects
with `items`, `page`, `size`, `totalElements`, `totalPages`, `first`, and
`last`. Page defaults to `0`, size defaults to `50`, and max size is `200`.

## Configuration

Main config is `src/main/resources/application.yaml`.

Queue config is bound by `QueueProperties` with:

```java
@ConfigurationProperties(prefix = "taskqueue")
```

Use the `taskqueue` prefix consistently. YAML uses kebab-case fields such as
`max-retries`, `base-delay-ms`, `max-delay-ms`, `jitter-percent`,
`handler-timeout-ms`, `poll-interval-ms`, and `lock-ttl-ms`; Java uses
camelCase.

Configured queues currently include:

- `high_priority`
- `default`

Each queue supports:

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

## Database Rules

Flyway migrations live in `src/main/resources/db/migration/`.

Rules for agents:

- Never edit an existing applied migration to change behavior.
- Add new migrations as `V{N}__description.sql`.
- Keep JPA mappings aligned with migrations because `ddl-auto` is `validate`.
- Preserve the existing `tasks` table naming style.

Current migrations:

- `V1__create_tasks.sql`
- `V2__add_task_priority.sql`
- `V3__add_task_idempotency_key.sql`
- `V4__add_task_heartbeat_at.sql`
- `V5__create_task_events.sql`

Current `TaskStatus` values:

```text
PENDING
RUNNING
DONE
FAILED
DEAD
```

Important `Task` fields include:

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

## Redis Conventions

`RedisConfig` provides a string-only `RedisTemplate<String, String>`.

Task objects are serialized explicitly as JSON strings through
`TaskSerializer`. Do not use Java object serialization.

Current Redis keys:

```text
taskqueue:{queue}             # ready queue sorted set
taskqueue:{queue}:processing  # in-flight task list
taskqueue:delayed             # delayed task sorted set
taskqueue:dlq                 # dead letter queue list
taskqueue:scheduler:lock      # delayed scheduler distributed lock
taskqueue:queues:paused       # Redis set of paused queue names
```

Ready queues are sorted sets, not lists. `RedisBroker.enqueue` scores tasks so
higher `priority` runs first, and older tasks of the same priority run first.

`RedisBroker.poll` uses a Lua script to atomically move one task from the ready
sorted set into the processing list. `RedisBroker.pollReadyDelayed` uses a Lua
script to atomically remove one due task from the delayed sorted set.

Scheduler lock release uses a Lua token check so one app instance cannot delete
another instance's lock.

## Current Limitations

Keep these in mind when extending the system:

- Delayed promotion atomically removes from `taskqueue:delayed`, but DB lookup
  and live queue enqueue are still separate operations.
- `TaskType` currently exists as a placeholder with no behavior.
- Handlers must be idempotent because delivery is at-least-once in design and
  duplicates are possible.
- There is no dashboard/frontend in this repository.

## Coding Guidance

- Read the relevant package and focused tests before changing behavior.
- Prefer local patterns over introducing new abstractions.
- Keep controllers thin; put orchestration in services, broker, scheduler, or
  worker classes.
- Validate API inputs in `TaskService` or controller-adjacent exception
  handling.
- Use `BadRequestException` for 400-style validation errors.
- Use `ConflictException` for 409 conflicts such as idempotency key mismatches.
- Preserve JSON payloads as JSON text in PostgreSQL and JSON nodes in API DTOs.
- When acknowledging a processing item, use the Redis payload that was polled,
  not a later mutated DB copy.
- Do not add dashboard or frontend code unless explicitly requested.
- If you add or change behavior, add focused tests in the same feature branch.

## Useful Commands

```bash
# Start local dependencies
docker compose up -d

# Run the app
./mvnw spring-boot:run

# Run normal tests; smoke tests are skipped unless enabled
./mvnw test

# Run smoke tests, requires PostgreSQL and Redis from docker-compose
./mvnw test -Dtaskqueue.smoke=true

# Run focused backend tests
./mvnw test -Dtest=DlqControllerTest,QueueControllerTest,DelayedTaskSchedulerTest,WorkerTest,TaskServiceTest,QueueControllerServiceTest,RedisBrokerTest,TaskControllerTest
```

Local services from `docker-compose.yml`:

```text
PostgreSQL: localhost:5432/taskqueue, user taskqueue, password taskqueue
Redis:      localhost:6379
```
