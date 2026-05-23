# Task Queue

A Java 21 Spring Boot task queue backed by PostgreSQL and Redis.

The application exposes REST APIs for creating and inspecting tasks, stores
durable task state in PostgreSQL, uses Redis for live queue state, and runs
background workers that execute registered task handlers with retry, DLQ,
heartbeat, idempotency, and queue-operation support.

## Features

- Enqueue immediate and delayed tasks
- Store task state, payload, result, error, stack trace, heartbeat, and
  idempotency key in PostgreSQL
- Use Redis for ready queues, processing queues, delayed tasks, DLQ entries,
  scheduler locks, and paused queue state
- Priority ordering for ready tasks
- Atomic Redis Lua polling for ready queues and delayed tasks
- Distributed Redis lock for delayed scheduling
- Worker execution with Java 21 virtual threads
- Handler timeout with retry/DLQ behavior
- Retry with exponential backoff, max delay cap, and jitter
- Shared failure handling for worker failures and stuck task recovery
- Stuck task reaper based on `heartbeatAt`
- Task heartbeat endpoint for long-running work
- Persistent task execution history with `GET /tasks/{id}/events`
- Idempotent task creation with `idempotencyKey` and HTTP 409 conflict handling
- Stronger task cancellation that cleans ready, delayed, and processing Redis entries
- Dead letter queue inspection and replay with optional attempt reset
- Queue pause, resume, drain, run-once, force run-once, and purge operations
- Queue pause state persists in Redis across app restarts
- Queue metrics include paused and drained state
- Scheduled retention cleanup for old terminal tasks
- Normal test suite does not require Docker services; smoke tests are opt-in

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
- Jackson 3 / `tools.jackson.*`

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

Run normal tests:

```bash
./mvnw test
```

Run smoke tests against Docker-published PostgreSQL and Redis:

```bash
./mvnw test -Dtaskqueue.smoke=true
```

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
    "priority": 5,
    "idempotencyKey": "demo-log-message-1"
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
curl -i http://localhost:8080/tasks/{id}/events
```

### Operate a task

```bash
curl -i -X POST http://localhost:8080/tasks/{id}/cancel
curl -i -X POST http://localhost:8080/tasks/{id}/retry
curl -i -X POST http://localhost:8080/tasks/{id}/heartbeat
```

### Queue operations

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
PENDING -> RUNNING -> PENDING   retry scheduled
PENDING -> RUNNING -> DEAD      retries exhausted
PENDING/RUNNING -> FAILED       cancel, purge, or administrative failure
DEAD -> PENDING                 DLQ replay
FAILED -> PENDING               manual retry
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
| `idempotencyKey` | Optional client key for safe duplicate enqueue requests |
| `heartbeatAt` | Last heartbeat timestamp for a running task |
| `runAt` | Earliest execution time for delayed/retry tasks |
| `startedAt` | Last time a worker started the task |
| `createdAt` | Creation timestamp |
| `result` | Handler result JSON string |
| `error` | Last error message |
| `stackTrace` | Last failure stack trace |

## Workers and Handlers

Handlers implement `TaskHandler`:

```java
public interface TaskHandler {
    String fn();
    String handle(Task task);
}
```

The current built-in handler is:

```text
log_message
```

It returns:

```json
{"ok":true}
```

Delivery is at-least-once. Handlers should be idempotent, usually keyed by
`Task.id` or by the caller-provided `idempotencyKey`.

## Redis Keys

The broker currently uses these Redis keys:

```text
taskqueue:{queue}             # ready queue sorted set
taskqueue:{queue}:processing  # in-flight task list
taskqueue:delayed             # delayed task sorted set
taskqueue:dlq                 # dead letter queue list
taskqueue:scheduler:lock      # delayed scheduler lock
taskqueue:queues:paused       # paused queue names set
```

Useful inspection commands:

```bash
docker exec -it taskqueue-redis redis-cli
ZRANGE taskqueue:default 0 -1 WITHSCORES
LRANGE taskqueue:default:processing 0 -1
ZRANGE taskqueue:delayed 0 -1 WITHSCORES
LRANGE taskqueue:dlq 0 -1
SMEMBERS taskqueue:queues:paused
GET taskqueue:scheduler:lock
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
V3__add_task_idempotency_key.sql
V4__add_task_heartbeat_at.sql
V5__create_task_events.sql
```

Hibernate is configured with `ddl-auto: validate`, so schema changes should be
made through Flyway migrations.

## Project Structure

```text
src/main/java/com/nganhcc/task_queue/
  api/         REST controllers and DTOs
  broker/      Redis queue access
  config/      Spring configuration properties and Redis config
  exception/   API/domain exceptions
  model/       Task entity and status enum
  reaper/      Stuck task recovery
  retention/   Terminal task cleanup
  retry/       Retry policy
  scheduler/   Delayed task scheduler
  service/     Task and queue application services
  store/       JPA repository
  worker/      Handler registry, worker, and bootstrap
```

## Known Limitations

- Delayed promotion is reconciled from PostgreSQL if a Redis delayed entry is
  removed before live enqueue completes, but Redis and DB writes are still not a
  single transaction.
- `TaskType` is currently a placeholder.
- The project has no frontend dashboard.

## More Documentation

- [Architecture](docs/architecture.md)
- [Progress notes](PROGRESS.md)
