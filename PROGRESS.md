# Task Queue Progress

Use this file as handoff context for a new chat session.

## Project Goal

Build a Java 21 Spring Boot task queue backed by PostgreSQL and Redis.

High-level design:

- PostgreSQL stores durable task records.
- Redis stores live queue state.
- API creates and reads tasks.
- Later workers will poll Redis, execute handlers, update PostgreSQL, retry failures, and move exhausted tasks to a DLQ.

## Current Stack

- Java 21
- Spring Boot 4.0.6
- Spring Web MVC
- Spring Data JPA
- Spring Data Redis
- Flyway
- PostgreSQL 16 via Docker Compose
- Redis 7 via Docker Compose
- Jackson 3 / `tools.jackson.*` types from Spring Boot 4

Important package:

```text
com.nganhcc.task_queue
```

## What Has Been Implemented

### Project Documentation

- Rewrote `CLAUDE.md` to match the current project instead of a larger future architecture.
- Added intended package structure and implementation order.
- Documented that the architecture file is the target direction, while source files are the current truth.

### Configuration

- `QueueProperties` is bound to `taskqueue`.
- `application.yaml` contains:
  - PostgreSQL datasource config
  - Redis config
  - queue configs for `default` and `high_priority`
  - scheduler/reaper config placeholders
- `RedisConfig` defines `RedisTemplate<String, String>` with string serializers.

### Database Model

Implemented:

```text
src/main/java/com/nganhcc/task_queue/model/Task.java
src/main/java/com/nganhcc/task_queue/model/TaskStatus.java
src/main/java/com/nganhcc/task_queue/store/TaskRepository.java
```

`Task` maps to the `tasks` table from:

```text
src/main/resources/db/migration/V1__create_tasks.sql
```

Current task fields include:

- `id`
- `queue`
- `fn`
- `payload`
- `status`
- `attempt`
- `maxRetries`
- `runAt`
- `startedAt`
- `createdAt`
- `result`
- `error`
- `stackTrace`

`TaskRepository` extends `JpaRepository<Task, UUID>` and also has query methods by status, queue/status, runAt, and startedAt.

### API Layer

Implemented:

```text
src/main/java/com/nganhcc/task_queue/api/TaskController.java
src/main/java/com/nganhcc/task_queue/api/ApiExceptionHandler.java
src/main/java/com/nganhcc/task_queue/api/dto/EnqueueTaskRequest.java
src/main/java/com/nganhcc/task_queue/api/dto/TaskResponse.java
src/main/java/com/nganhcc/task_queue/api/dto/ErrorResponse.java
src/main/java/com/nganhcc/task_queue/exception/BadRequestException.java
src/main/java/com/nganhcc/task_queue/exception/TaskNotFoundException.java
```

Implemented endpoints:

```http
POST /tasks
GET /tasks/{id}
```

`POST /tasks`:

- validates request
- defaults queue to `default`
- validates queue exists in `QueueProperties`
- defaults `maxRetries` from queue config
- stores payload as JSON text in PostgreSQL
- saves task as `PENDING`
- pushes immediate tasks to Redis
- pushes future `runAt` tasks to Redis delayed sorted set

`GET /tasks/{id}`:

- loads task from PostgreSQL
- returns `TaskResponse`
- throws `TaskNotFoundException` if missing
- `ApiExceptionHandler` maps that to HTTP 404

Validation/error handling:

- `BadRequestException` maps to HTTP 400
- unreadable JSON maps to HTTP 400
- `TaskNotFoundException` maps to HTTP 404

### Redis Broker

Implemented:

```text
src/main/java/com/nganhcc/task_queue/broker/RedisBroker.java
src/main/java/com/nganhcc/task_queue/broker/RedisKeys.java
src/main/java/com/nganhcc/task_queue/broker/TaskSerializer.java
```

Current Redis keys:

```text
taskqueue:{queue}
taskqueue:{queue}:processing
taskqueue:delayed
taskqueue:dlq
taskqueue:scheduler:lock
```

Current broker methods:

- `enqueue(Task task)`
  - serializes task
  - `LPUSH` into `taskqueue:{queue}`
- `poll(String queueName)`
  - atomically moves from `taskqueue:{queue}` to `taskqueue:{queue}:processing`
  - uses `rightPopAndLeftPush`
  - deserializes task JSON
  - returns `null` when no task exists
- `acknowledge(Task task)`
  - serializes task
  - removes one matching value from `taskqueue:{queue}:processing`
- `enqueueDelayed(Task task)`
  - requires `runAt`
  - serializes task
  - adds to `taskqueue:delayed` sorted set with score `runAt.toEpochMilli()`

### Tests

Current test files:

```text
src/test/java/com/nganhcc/task_queue/SmokeTest.java
src/test/java/com/nganhcc/task_queue/TaskQueueApplicationTests.java
src/test/java/com/nganhcc/task_queue/api/TaskControllerTest.java
src/test/java/com/nganhcc/task_queue/broker/RedisBrokerTest.java
src/test/java/com/nganhcc/task_queue/service/TaskServiceTest.java
```

Tests added/updated for:

- API controller creation and bad request behavior
- service validation/defaulting
- Redis broker enqueue
- Redis broker poll
- Redis broker empty poll
- Redis broker acknowledge

Full `./mvnw test` requires local PostgreSQL and Redis to be running and may need permission to connect to local Docker-published ports.

## Important Concepts Learned

- `model` contains app data structures like `Task`.
- `store` contains persistence access like `TaskRepository`.
- `TaskRepository` is an interface because Spring Data JPA creates the implementation at runtime.
- `findById(...)` returns `Optional<Task>`, not `Task`, because the row may not exist.
- `BadRequestException` is the signal; `ApiExceptionHandler` translates it into an HTTP response.
- `@RestControllerAdvice` registers global exception handlers in Spring MVC.
- `@Component`, `@Service`, `@RestController`, and `@RestControllerAdvice` register Spring beans.
- A bean is an object Spring creates, manages, wires, and injects.
- `RedisKeys.queue(...)` is static, so no `RedisKeys` object is needed.
- Spring Boot 4 uses Jackson 3 types such as `tools.jackson.databind.JsonNode` and `tools.jackson.databind.json.JsonMapper`.

## Current Manual API Examples

Start services and app:

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
    "fn": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Hello"
    }
  }'
```

Create delayed task:

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "fn": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Delayed"
    },
    "runAt": "2030-01-01T00:00:00Z"
  }'
```

Get task:

```bash
curl -i http://localhost:8080/tasks/{id}
```

Check immediate Redis queue:

```bash
docker exec -it taskqueue-redis redis-cli
LRANGE taskqueue:default 0 -1
```

Check delayed Redis queue:

```redis
ZRANGE taskqueue:delayed 0 -1 WITHSCORES
```

## Current Issues / Cleanup Needed

These should be fixed before building workers.

1. In `TaskService.enqueue(...)`, Redis enqueue uses `task` instead of `saved`.

Current logic uses:

```java
redisBroker.enqueue(task);
redisBroker.enqueueDelayed(task);
```

Prefer:

```java
redisBroker.enqueue(saved);
redisBroker.enqueueDelayed(saved);
```

Reason: `save(...)` is the final persisted object and should be the object pushed to Redis.

2. In `TaskService.enqueue(...)`, branch condition checks `task.getRunAt()`.

Prefer checking `saved.getRunAt()` after save for consistency.

3. In `RedisBroker.enqueueDelayed(...)`, error message says:

```text
Delayed task must have getRunAt()
```

Prefer:

```text
Delayed task must have runAt
```

4. In `RedisBroker`, TODO comments for immediate pop and acknowledge remain even though methods exist.

Update or remove those TODOs to avoid confusion.

5. `TaskSerializer.deserialize(...)` may need verification with a real Redis round trip.

If Jackson cannot deserialize `Task` because the no-args constructor is `protected`, make that constructor public or add a Jackson-friendly constructor.

6. `acknowledge(Task task)` removes by exact serialized JSON.

This works only if the task JSON in processing exactly matches the task passed to `acknowledge`. Later, once workers update `status`, `startedAt`, or attempts, this may fail. A future design may store task IDs in Redis lists and keep full task state in PostgreSQL.

## Recommended Next Steps

1. Clean up the current Redis broker feature:
   - use `saved` in `TaskService`
   - update delayed error message
   - clean completed TODO comments
   - add tests for `enqueueDelayed`
   - add tests for `TaskService` immediate vs delayed routing

2. Then implement worker foundation:
   - `TaskHandler`
   - `TaskType`
   - `HandlerRegistry`
   - a sample handler for learning
   - `Worker` that polls Redis and marks task `RUNNING`

3. After worker foundation:
   - mark success as `DONE`
   - store result
   - acknowledge processing queue entry

4. After success path:
   - retry engine
   - delayed retry
   - DLQ after max retries

5. After retry/DLQ:
   - scheduler to flush `taskqueue:delayed`
   - reaper to recover stuck processing tasks

## Useful Commands

```bash
./mvnw -q -DskipTests compile
./mvnw test -Dtest=RedisBrokerTest
./mvnw test -Dtest=TaskServiceTest,RedisBrokerTest
./mvnw test
```

Docker:

```bash
docker compose up -d
docker compose ps
docker exec -it taskqueue-redis redis-cli
```

## Current Mental Model

Current create flow:

```text
POST /tasks
  -> TaskController.enqueue
  -> TaskService.enqueue
  -> validate/default request
  -> save Task in PostgreSQL
  -> if runAt is future: RedisBroker.enqueueDelayed
  -> else: RedisBroker.enqueue
  -> return TaskResponse
```

Current read flow:

```text
GET /tasks/{id}
  -> TaskController.getTask
  -> TaskService.getTask
  -> TaskRepository.findById
  -> return TaskResponse or 404
```

Current Redis flow:

```text
enqueue:
  taskqueue:{queue} <- task JSON

poll:
  taskqueue:{queue} -> taskqueue:{queue}:processing

acknowledge:
  remove task JSON from taskqueue:{queue}:processing

enqueueDelayed:
  taskqueue:delayed sorted set <- task JSON scored by runAt epoch millis
```
