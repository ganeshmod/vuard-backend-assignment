# Food Delivery Order Processing Service

Backend engineering assignment for **Vaurd** — a Go service that consumes a continuous stream of food-order events, materializes current order state in PostgreSQL, and exposes a List Orders HTTP API.

## Architecture (short)

```
┌─────────────┐     Redis Streams      ┌──────────────────┐     PostgreSQL
│  Generator  │ ─────────────────────► │  Order Service   │ ────────────────►
│  (Go)       │   order-events         │  consumer + API  │   orders table
└─────────────┘                        └──────────────────┘
                                                │
                                                ▼
                                         GET /orders
```

Transport: **Redis Streams** (consumer group). Persistence: **PostgreSQL**. API: **HTTP/JSON**.

See [DESIGN.md](DESIGN.md) for rationale and edge-case decisions, and [API.md](API.md) for the full API contract.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose v2+
- Optional (local non-Docker runs): Go 1.22+, PostgreSQL 16, Redis 7

## Quick start (recommended)

Bring up Postgres, Redis, the backend, and the event generator:

```bash
docker compose up --build
```

What starts:

| Service     | Port  | Role                                      |
|-------------|-------|-------------------------------------------|
| `postgres`  | 5432  | Order state store                         |
| `redis`     | 6379  | Event stream (`order-events`)             |
| `server`    | 8081  | Event consumer + List Orders API          |
| `generator` | —     | Emits ~5 events/sec (configurable)        |

Verify health:

```bash
curl http://localhost:8081/healthz
```

List orders (after a few seconds of events):

```bash
curl "http://localhost:8081/orders?limit=10"
```

Filter / sort examples:

```bash
curl "http://localhost:8081/orders?status=Preparing"
curl "http://localhost:8081/orders?sort=updated_at_asc&limit=20&offset=0"
```

Stop everything:

```bash
docker compose down
```

Reset data volumes:

```bash
docker compose down -v
```

## Running components individually

### Backend only (with infra)

```bash
docker compose up --build postgres redis server
```

### Generator with a custom rate

Events per second via `-rate` (also works inside Compose by overriding `command`):

```bash
docker compose run --rm generator -rate 10 -redis redis:6379 -stream order-events
```

Or against a local Redis:

```bash
go run ./cmd/generator -rate 5 -redis localhost:6379
```

### Local Go development

```bash
docker compose up -d postgres redis
export DATABASE_URL='postgres://orders:orders@localhost:5432/orders?sslmode=disable'
export REDIS_ADDR=localhost:6379
go mod tidy
go run ./cmd/server
# in another terminal
go run ./cmd/generator -rate 5
```

## Configuration

| Variable           | Default                                                      | Description                |
|--------------------|--------------------------------------------------------------|----------------------------|
| `HTTP_ADDR`        | `:8080`                                                      | HTTP listen address        |
| `DATABASE_URL`     | `postgres://orders:orders@localhost:5432/orders?sslmode=disable` | Postgres DSN          |
| `REDIS_ADDR`       | `localhost:6379`                                             | Redis host:port            |
| `STREAM_NAME`      | `order-events`                                               | Redis stream key           |
| `CONSUMER_GROUP`   | `order-processors`                                           | Redis consumer group       |
| `CONSUMER_NAME`    | `worker-1`                                                   | Consumer instance name     |
| `PENDING_TTL`      | `5m`                                                         | TTL for buffered OO-order updates |

Generator flags: `-rate`, `-redis`, `-stream`, `-create-weight`, `-status-weight`, `-items-weight`.

## Project layout

```
cmd/server/          HTTP API + stream consumer entrypoint
cmd/generator/       Continuous order-event generator
internal/api/        List Orders HTTP handlers
internal/config/     Environment configuration
internal/models/     Order + event types, status FSM
internal/processor/  Event application, idempotency, OO-order buffer
internal/store/      PostgreSQL persistence
internal/stream/     Redis Streams publisher/consumer
```

## Tests

```bash
go test ./...
```

## Deliverables checklist

- [x] Order event generator (continuous, configurable rate)
- [x] Go backend consuming the stream
- [x] PostgreSQL current-state persistence
- [x] List Orders API (+ pagination, status filter, sort)
- [x] `docker-compose.yml`
- [x] `README.md`, `API.md`, `DESIGN.md`
