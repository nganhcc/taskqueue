# task-queue

Java 21 Spring Boot project for a Redis/PostgreSQL-backed task queue.

This repository is currently an early backend foundation, not the full queue system described in `docs/architecture.md`. Treat the architecture doc as the product direction, and treat the source tree as the source of truth for what is implemented today.

## Current implementation

Implemented now:

- Spring Boot 4.0.6 application entry point with scheduling enabled
- Redis connection configuration via `RedisTemplate<String, String>`
- PostgreSQL datasource, JPA validation, and Flyway migrations
- Queue configuration properties class
- Docker Compose services for PostgreSQL 16 and Redis 7
- Smoke tests for Redis, PostgreSQL, and the `tasks` table

Not implemented yet:

- REST controllers for enqueue/status/DLQ/metrics
- Task entity, repository, status enum, or handler model
- Redis broker, scheduler, worker pool, retry engine, reaper, or DLQ service
- React dashboard

Do not reference missing packages or files as if they already exist.

## Project structure

```text
src/main/java/com/nganhcc/task_queue/
  TaskQueueApplication.java
  config/
    QueueProperties.java
    RedisConfig.java

src/main/resources/
  application.yaml
  db/migration/
    V1__create_tasks.sql

src/test/java/com/nganhcc/task_queue/
  TaskQueueApplicationTests.java
  SmokeTest.java

docs/
  architecture.md

docker-compose.yml
pom.xml
```

Use package `com.nganhcc.task_queue`. The hyphenated artifact name is `task-queue`, but Java package names must not use hyphens.

## Intended package structure

Build the backend in layers around the task lifecycle: enqueue, persist, push to Redis, execute, retry, and inspect failures.

Use this structure as the project grows:

```text
src/main/java/com/nganhcc/task_queue/
  TaskQueueApplication.java

  config/
    QueueProperties.java
    RedisConfig.java

  model/
    Task.java
    TaskStatus.java

  store/
    TaskRepository.java

  api/
    TaskController.java
    DlqController.java
    MetricsController.java
    dto/
      EnqueueTaskRequest.java
      TaskResponse.java

  service/
    TaskService.java
    DlqService.java

  broker/
    RedisBroker.java
    RedisKeys.java
    TaskSerializer.java

  worker/
    TaskHandler.java
    TaskType.java
    HandlerRegistry.java
    Worker.java
    WorkerPool.java
    WorkerBootstrap.java

  retry/
    RetryEngine.java
    RetryPolicy.java

  scheduler/
    DelayedTaskScheduler.java

  reaper/
    StuckTaskReaper.java

  exception/
    TaskNotFoundException.java
    NoHandlerFoundException.java
```

Only create a package when there is real code for it. Do not add empty folders or placeholder classes just to match this list.

## Suggested implementation order

1. Fix configuration consistency:
   - `application.yaml` uses `taskqueue`
   - `QueueProperties` currently uses `@ConfigurationProperties(prefix = "task_queue")`
   - Prefer `taskqueue` everywhere.
2. Add the domain model:
   - `Task`
   - `TaskStatus`
   - `TaskRepository`
3. Add the API and service layer:
   - `POST /tasks`
   - `GET /tasks/{id}`
   - `TaskService` should save the task in PostgreSQL and push it to Redis.
4. Add the Redis broker:
   - immediate queue push/pop
   - processing queue
   - delayed queue
   - DLQ helpers
5. Add the worker layer:
   - `TaskHandler`
   - `TaskType`
   - handler discovery
   - worker loop
   - virtual-thread worker pool
6. Add retry and DLQ behavior:
   - retry decision logic
   - exponential backoff
   - delayed requeue
   - final move to DLQ
7. Add scheduler and reaper:
   - scheduler moves ready delayed tasks into live queues
   - reaper recovers tasks stuck in processing after worker crashes
8. Add dashboard only after the backend API is stable.

## Common commands

```bash
# Start local dependencies
docker compose up -d

# Run the application
./mvnw spring-boot:run

# Run tests
./mvnw test

# Run one test class
./mvnw test -Dtest=SmokeTest

# Build
./mvnw clean package
```

`SmokeTest` requires PostgreSQL and Redis to be running on the local ports from `docker-compose.yml`.

## Runtime dependencies

`docker-compose.yml` starts:

- PostgreSQL on `localhost:5432`
  - database: `taskqueue`
  - username: `taskqueue`
  - password: `taskqueue`
- Redis on `localhost:6379`
  - append-only persistence enabled

## Configuration

Main config lives in `src/main/resources/application.yaml`.

Important sections:

- `spring.datasource` for PostgreSQL
- `spring.data.redis` for Redis/Lettuce
- `spring.flyway` for schema migrations
- `management.endpoints.web.exposure` for Actuator endpoints
- `taskqueue` for planned queue, scheduler, and reaper settings

Before adding code that depends on queue properties, verify the property prefix. The YAML currently uses `taskqueue`, while `QueueProperties` is annotated with `@ConfigurationProperties(prefix = "task_queue")`.

## Database

Flyway migrations live in `src/main/resources/db/migration/`.

Rules:

- Never edit an existing migration after it has been applied.
- Add new migrations using `V{N}__description.sql`.
- Keep schema names consistent with the existing `tasks` table.
- JPA is configured with `ddl-auto: validate`, so Hibernate should validate the schema instead of creating or changing it.

The first migration creates `tasks` with indexes for status, queue/status, pending `run_at`, and running `started_at`.

## Redis conventions

`RedisConfig` provides a `RedisTemplate<String, String>` with string serializers for keys, values, hash keys, and hash values.

When implementing queue storage, serialize task payloads explicitly to JSON strings before writing to Redis. Do not rely on Java object serialization.

Planned keys from the architecture:

```text
taskqueue:{queue_name}             # main queue list
taskqueue:{queue_name}:processing  # in-flight tasks
taskqueue:delayed                  # delayed task sorted set
taskqueue:dlq                      # dead letter queue
taskqueue:scheduler:lock           # scheduler distributed lock
```

## Coding conventions

- Use Java 21 features where they keep code simple.
- Follow Spring Boot conventions and keep beans in `com.nganhcc.task_queue` so component scanning finds them.
- Use explicit constructors/getters/setters unless the project adds Lombok later.
- Use SLF4J logging, not `System.out.println`.
- Keep configuration names aligned between Java property classes and `application.yaml`.
- Keep comments short and useful. The codebase currently has some learning comments; new production code should be clearer and more concise.
- Prefer focused tests around each new component as it is added.

## Architecture direction

`docs/architecture.md` describes the intended design:

- REST API accepts tasks.
- Redis stores live queue state.
- PostgreSQL stores durable task records and results.
- Workers execute task handlers with at-least-once delivery.
- Failed tasks retry with backoff and eventually move to a DLQ.
- Scheduler moves delayed tasks when `run_at` arrives.
- Reaper recovers tasks stuck in processing after worker crashes.

When implementing that design, add the missing layers incrementally and keep the package structure under `com.nganhcc.task_queue`, for example:

```text
api/
broker/
model/
queue/
retry/
scheduler/
service/
store/
worker/
```

Only create these packages when there is real code for them.

## Testing approach

Current tests:

- `TaskQueueApplicationTests` checks Spring context startup.
- `SmokeTest` verifies Redis, PostgreSQL, and the `tasks` table.

As the queue implementation grows:

- Use unit tests for retry, scheduling, and worker decision logic.
- Use Spring integration tests for full Redis/PostgreSQL flows.
- Avoid fixed `Thread.sleep` in async tests; prefer polling/assertion utilities when added.

## Gotchas

- `docs/architecture.md` is ahead of the code. Check the source tree before following examples from the doc.
- `application.yaml` uses `taskqueue`, but `QueueProperties` currently uses prefix `task_queue`.
- The logging package in `application.yaml` is `com.nganhcc.taskqueue`, while the Java package is `com.nganhcc.task_queue`.
- The reaper property is misspelled internally as `stuckThresoldMinutes` in `QueueProperties`.
- Spring Boot tests that load the full context may fail if local Docker dependencies are not running.
