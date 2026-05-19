# Task Queue

A Java 21 Spring Boot task queue backed by PostgreSQL and Redis.

The application exposes REST APIs for creating and inspecting tasks, stores durable task state in PostgreSQL, uses Redis for live queue state, and runs background workers that execute registered task handlers with retry and DLQ support.

## Features

- Enqueue immediate and delayed tasks
- Store task state, payload, result, and error details in PostgreSQL
- Use Redis for ready queues, processing queues, delayed tasks, and the dead letter queue
- Per-queue concurrency and retry settings
- Priority ordering for ready tasks
- Worker execution with Java 21 virtual threads
- Exponential retry backoff
- Stuck task reaper for tasks left in `RUNNING`
- Queue pause, resume, run-once, and purge operations
- Task, queue, and DLQ inspection APIs

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

## Quick Start

Start PostgreSQL and Redis:

```bash
docker compose up -d
```

Run the application:

```bash
./mvnw spring-boot:run
```

The API runs on:

```text
http://localhost:8080
```

Run tests:

```bash
./mvnw test
```

Some tests, including smoke tests, expect PostgreSQL and Redis to be available on the ports from `docker-compose.yml`.

## Local Services

`docker-compose.yml` starts:

| Service | URL | Credentials |
| --- | --- | --- |
| PostgreSQL | `localhost:5432/taskqueue` | `taskqueue` / `taskqueue` |
| Redis | `localhost:6379` | none |

## Configuration

Main configuration lives in:

```text
src/main/resources/application.yaml
```

Queue settings are under `taskqueue`:

```yaml
taskqueue:
  queues:
    high_priority:
      concurrency: 10
      max-retries: 5
      base-delay-ms: 500
    default:
      concurrency: 4
      max-retries: 3
      base-delay-ms: 1000
  worker:
    poll-interval-ms: 1000
  scheduler:
    poll-interval-ms: 1000
  reaper:
    stuck-threshold-minutes: 5
    poll-interval-ms: 30000
```

## API Examples

### Create an immediate task

The built-in sample handler is `log_message`.

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

### Create a delayed task

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "fn": "log_message",
    "payload": {
      "message": "run later"
    },
    "runAt": "2030-01-01T00:00:00Z",
    "priority": 1
  }'
```

### List and inspect tasks

```bash
curl -i http://localhost:8080/tasks
curl -i 'http://localhost:8080/tasks?queue=default'
curl -i 'http://localhost:8080/tasks?status=PENDING'
curl -i 'http://localhost:8080/tasks?queue=default&status=PENDING'
curl -i http://localhost:8080/tasks/{id}
```

### Cancel or retry a task

```bash
curl -i -X POST http://localhost:8080/tasks/{id}/cancel
curl -i -X POST http://localhost:8080/tasks/{id}/retry
```

### Queue operations

```bash
curl -i http://localhost:8080/queues
curl -i -X POST http://localhost:8080/queues/default/pause
curl -i -X POST http://localhost:8080/queues/default/resume
curl -i -X POST http://localhost:8080/queues/default/run-once
curl -i -X POST http://localhost:8080/queues/default/purge
curl -i -X POST http://localhost:8080/queues/delayed/purge
curl -i -X POST http://localhost:8080/queues/dlq/purge
```

### Dead letter queue

```bash
curl -i http://localhost:8080/dlq
curl -i -X POST http://localhost:8080/dlq/{id}/replay
curl -i -X POST 'http://localhost:8080/dlq/{id}/replay?resetAttempts=true'
```

### Metrics

```bash
curl -i http://localhost:8080/metrics/tasks
curl -i http://localhost:8080/metrics/queues
```

## Task Model

Tasks commonly move through these statuses:

```text
PENDING -> RUNNING -> DONE
RUNNING -> PENDING  # retry scheduled
RUNNING -> DEAD     # retries exhausted
PENDING/RUNNING -> FAILED  # cancelled or purged
```

Important fields:

| Field | Description |
| --- | --- |
| `id` | Task UUID |
| `queue` | Queue name, such as `default` or `high_priority` |
| `fn` | Handler name |
| `payload` | JSON payload stored as text in PostgreSQL |
| `status` | Current task status |
| `attempt` | Number of failed attempts so far |
| `maxRetries` | Retry limit for the task |
| `priority` | Higher priority tasks run first |
| `runAt` | Earliest execution time for delayed tasks |
| `startedAt` | Last time a worker started the task |
| `result` | Handler result JSON string |
| `error` | Last error message |

## Workers and Handlers

Handlers implement `TaskHandler`:

```java
public interface TaskHandler {
    String fn();
    String handle(Task task);
}
```

The handler `fn()` value is matched against the task request `fn`.

The current built-in handler is:

```text
log_message
```

It returns:

```json
{"ok":true}
```

## Redis Keys

The broker currently uses these Redis keys:

```text
taskqueue:{queue}             # ready queue sorted set
taskqueue:{queue}:processing  # in-flight task list
taskqueue:delayed             # delayed task sorted set
taskqueue:dlq                 # dead letter queue list
taskqueue:scheduler:lock      # scheduler lock key
```

Useful inspection commands:

```bash
docker exec -it taskqueue-redis redis-cli
ZRANGE taskqueue:default 0 -1 WITHSCORES
LRANGE taskqueue:default:processing 0 -1
ZRANGE taskqueue:delayed 0 -1 WITHSCORES
LRANGE taskqueue:dlq 0 -1
```

## Database Migrations

Flyway migrations live in:

```text
src/main/resources/db/migration
```

Current migrations:

```text
V1__create_tasks.sql
V2__add_task_priority.sql
```

Hibernate is configured with `ddl-auto: validate`, so schema changes should be made through Flyway migrations.

## Project Structure

```text
src/main/java/com/nganhcc/task_queue/
  api/         REST controllers and DTOs
  broker/      Redis queue access
  config/      Spring configuration properties and Redis config
  exception/   API/domain exceptions
  model/       Task entity and status enum
  reaper/      Stuck task recovery
  retry/       Retry policy
  scheduler/   Delayed task scheduler
  service/     Task and queue application services
  store/       JPA repository
  worker/      Handler registry, worker, and bootstrap
```

## Known Limitations

- Queue pause state is in memory only and is cleared on application restart.
- DLQ replay keeps the existing attempt count by default. Use `resetAttempts=true` to reset it during replay.
- `TaskType` is currently a placeholder.

## More Documentation

- [Architecture](docs/architecture.md)
- [Progress notes](PROGRESS.md)
