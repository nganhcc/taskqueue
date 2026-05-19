# Agent Context

This file is the working brief for coding agents. Keep it accurate when the
codebase changes.

## Project Snapshot

`task-queue` is a Java 21 Spring Boot 4.0.6 backend for a Redis/PostgreSQL
task queue.

Current implementation is a working backend foundation:

- REST APIs for tasks, queues, DLQ, and basic metrics
- PostgreSQL durable task storage through Spring Data JPA and Flyway
- Redis live queue state through `RedisTemplate<String, String>`
- Immediate queues with priority ordering
- Delayed task scheduling
- Worker polling and handler execution on Java virtual threads
- Retry with exponential backoff
- Dead letter queue replay
- Stuck task reaper
- Focused controller, service, broker, and worker tests

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
the live broker for ready work, processing work, delayed work, and DLQ entries.

High-level flow:

```text
POST /tasks
  -> TaskService validates and stores a PENDING Task in PostgreSQL
  -> RedisBroker enqueues now, or puts future runAt tasks into taskqueue:delayed
  -> WorkerBootstrap schedules per-queue virtual-thread worker submissions
  -> Worker polls Redis, loads the DB task, marks RUNNING, executes handler
  -> success marks DONE
  -> failure either schedules retry or marks DEAD and writes to DLQ
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
POST /tasks/{id}/cancel
POST /tasks/{id}/retry

GET /queues
POST /queues/{queue}/pause
POST /queues/{queue}/resume
POST /queues/{queue}/run-once
POST /queues/{queue}/purge
POST /queues/delayed/purge
POST /queues/dlq/purge

GET /dlq
POST /dlq/{id}/replay

GET /metrics/tasks
GET /metrics/queues
```

## Configuration

Main config is `src/main/resources/application.yaml`.

Queue config is bound by `QueueProperties` with:

```java
@ConfigurationProperties(prefix = "taskqueue")
```

Use the `taskqueue` prefix consistently. YAML uses kebab-case fields such as
`max-retries`, `base-delay-ms`, and `poll-interval-ms`; Java uses camelCase.

Configured queues currently include:

- `high_priority`
- `default`

Each queue supports:

- `concurrency`
- `maxRetries`
- `baseDelayMs`

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

Current `TaskStatus` values:

```text
PENDING
RUNNING
DONE
FAILED
DEAD
```

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
taskqueue:scheduler:lock      # key helper exists, lock is not implemented yet
```

Ready queues are sorted sets, not lists. `RedisBroker.enqueue` scores tasks so
higher `priority` runs first, and older tasks of the same priority run first.

## Known Limitations

Keep these in mind when extending the system:

- `RedisBroker.poll` uses `ZPOPMIN` and then `LPUSH` as two Redis commands. A
  crash between them can remove a task from the ready queue before it reaches
  the processing list. A Lua script should make this atomic.
- Delayed promotion removes from `taskqueue:delayed` and then enqueues to the
  live queue as separate operations.
- `RedisKeys.schedulerLock()` exists, but `DelayedTaskScheduler` does not use a
  distributed lock yet.
- Queue pause state is in memory only and is lost on restart.
- DLQ replay keeps the existing `attempt` count. Decide intentionally before
  changing this behavior.
- The worker stores the latest error message, but does not currently populate
  full stack traces.
- Handlers must be idempotent because delivery is at-least-once in design and
  duplicates are possible.

## Coding Guidance

- Read the relevant package and focused tests before changing behavior.
- Prefer local patterns over introducing new abstractions.
- Keep controllers thin; put orchestration in services or broker/worker classes.
- Validate API inputs in `TaskService` or controller-adjacent exception handling.
- Use `BadRequestException` for 400-style domain validation errors.
- Preserve JSON payloads as JSON text in PostgreSQL and JSON nodes in API DTOs.
- When acknowledging a processing item, use the Redis payload that was polled,
  not a later mutated DB copy.
- Do not add dashboard or frontend code unless explicitly requested.

## Useful Commands

```bash
# Start local dependencies
docker compose up -d

# Run the app
./mvnw spring-boot:run

# Run all tests
./mvnw test

# Run focused backend tests
./mvnw test -Dtest=WorkerTest,TaskServiceTest,RedisBrokerTest,TaskControllerTest

# Run smoke tests, requires PostgreSQL and Redis from docker-compose
./mvnw test -Dtest=SmokeTest
```

Local services from `docker-compose.yml`:

```text
PostgreSQL: localhost:5432/taskqueue, user taskqueue, password taskqueue
Redis:      localhost:6379
```
