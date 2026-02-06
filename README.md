# monitor-core

A high-performance event ingestion service that receives monitoring events via HTTP and writes them to ClickHouse in batches.

## Architecture

```
go services
  ↓ (batched NDJSON over HTTP)
monitor-core
  ↓ (batched inserts)
ClickHouse
```

## Features

- **HTTP ingestion endpoint**: `POST /v1/events` accepts NDJSON (newline-delimited JSON)
- **Gzip support**: Automatically handles gzip-compressed request bodies
- **Streaming parser**: Processes events line-by-line without loading entire body into memory
- **Batched writes**: Collects events and writes to ClickHouse in configurable batches
- **Non-blocking ingestion**: HTTP handler enqueues events and returns immediately
- **Simple API key authentication**: Via `X-Api-Key` header

## Quick Start

### 1. Start local ClickHouse

```bash
dev up
```

Or manually:

```bash
docker-compose -f docker-compose.dev.yml up -d
```

### 2. Run schema migrations

```bash
dev migrate
```

Or manually:

```bash
clickhouse-client < migrations/001_schema.sql
```

### 3. Configure environment (optional)

```bash
export API_KEY="your-secret-key"
```

All other defaults work with `dev up`.

### 4. Run the service

```bash
dev run
```

Or build and run:

```bash
go build -o bin/monitor-core .
./bin/monitor-core
```

### Docker (Production)

```bash
docker-compose up -d
```

## API

### Health Check

```bash
curl http://localhost:8080/health
```

Response:

```json
{ "status": "ok", "enqueued": 0, "dropped": 0, "pending": 0 }
```

### Ingest Events

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/x-ndjson" \
  -H "X-Api-Key: your-secret-key" \
  -d '{"timestamp":"2026-02-06T23:01:02.123Z","service":"users","job_id":"job_x","request_id":"req_y","trace_id":"trc_z","name":"user.created","data":{"user_id":42}}
{"timestamp":"2026-02-06T23:01:02.456Z","service":"users","job_id":"job_x","request_id":"req_y","trace_id":"trc_z","name":"db.query","data":{"table":"users"}}'
```

Response:

```json
{ "accepted": 2 }
```

### Event Format

Each event must be a JSON object on its own line with these fields:

| Field        | Type             | Required | Description                                   |
| ------------ | ---------------- | -------- | --------------------------------------------- |
| `timestamp`  | string (RFC3339) | Yes      | When the event occurred                       |
| `service`    | string           | Yes      | Service name that generated the event         |
| `name`       | string           | Yes      | Event type/name                               |
| `env`        | string           | No       | Environment (e.g., production, staging)       |
| `job_id`     | string           | No       | Groups related requests within a service      |
| `request_id` | string           | No       | Unique identifier per incoming request        |
| `trace_id`   | string           | No       | Spans across services for distributed tracing |
| `level`      | string           | No       | Log level (info, warn, error, debug)          |
| `data`       | object           | No       | Additional event data                         |

## Configuration

| Environment Variable  | Default          | Description                                   |
| --------------------- | ---------------- | --------------------------------------------- |
| `HTTP_PORT`           | `8080`           | HTTP server port                              |
| `CLICKHOUSE_ADDR`     | `localhost:9000` | ClickHouse server address                     |
| `CLICKHOUSE_DATABASE` | `monitor`        | ClickHouse database name                      |
| `CLICKHOUSE_USERNAME` | `default`        | ClickHouse username                           |
| `CLICKHOUSE_PASSWORD` | ``               | ClickHouse password                           |
| `API_KEY`             | ``               | API key for authentication (empty = disabled) |
| `BATCH_SIZE`          | `1000`           | Number of events per batch insert             |
| `FLUSH_INTERVAL`      | `5s`             | Max time to wait before flushing batch        |
| `QUEUE_SIZE`          | `100000`         | Max events in memory queue                    |

## Limits

- **Request body size**: 10 MB maximum
- **ClickHouse connection retry**: 10 attempts with linear backoff (1s, 2s, ... 10s)

## Development

Use the `dev` CLI for common tasks:

```bash
dev help                  # List available commands
dev up                    # Start local ClickHouse
dev migrate               # Run schema migrations
dev run                   # Run the application
dev check                 # Format, vet, and test
dev down                  # Stop local ClickHouse
```

## Project Structure

```
monitor-core/
  main.go                     # Entry point with routes
  Devfile.yaml                # Dev CLI commands
  Dockerfile                  # Multi-stage production build
  docker-compose.yml          # Production stack
  docker-compose.dev.yml      # Local development with ClickHouse
  db/
    clickhouse.go             # ClickHouse connection and batch writer
  env/
    env.go                    # Environment configuration
  middleware/
    auth.go                   # API key authentication middleware
  routes/
    events.go                 # Event ingestion handler
  services/
    queue.go                  # Buffered event queue
    batcher.go                # Batch collection and flushing
  structs/
    event.go                  # Event struct and validation
  migrations/
    001_schema.sql            # ClickHouse schema
  .github/workflows/
    build-and-deploy.yml      # CI/CD pipeline
```

## Querying Events

```sql
SELECT * FROM monitor.events LIMIT 10;

-- Find events by trace
SELECT * FROM monitor.events WHERE trace_id = 'trc_z';

-- Find events by service and time range
SELECT * FROM monitor.events
WHERE service = 'users'
  AND timestamp >= '2026-02-06 00:00:00';
```
