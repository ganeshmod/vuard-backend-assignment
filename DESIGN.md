# Design Document

## 1. Goal

Build a small event-driven order-monitoring backend: a continuous generator emits food-order events; a Go service consumes them, keeps each order’s **current state** durable, and serves a List Orders API that always reflects the latest applied state.

## 2. Architecture

```
Generator ──XADD──► Redis Stream (order-events)
                         │
                    XREADGROUP
                         │
                         ▼
              Order Service (Go)
                 │         │
           Processor     HTTP API
                 │         │
                 ▼         ▼
              PostgreSQL   GET /orders
```

| Component | Choice | Why |
|-----------|--------|-----|
| Transport | **Redis Streams** | Fits a continuous append-only event stream; consumer groups give at-least-once delivery and horizontal scale; trivial in `docker-compose`; lower ops cost than Kafka for this assignment size |
| Database | **PostgreSQL** | Strong consistency for concurrent updates, transactional idempotency keys, rich filtering/sorting for the List API |
| API | **HTTP/JSON** | Simple to demo with `curl`; pagination/filter/sort are natural query params |
| Generator language | **Go** | Shares models/publisher with the service; one toolchain |

### Why not the alternatives?

- **HTTP push generator → service**: simpler, but couples producer to consumer availability and loses durable buffering when the service restarts.
- **Kafka**: excellent at scale; heavier local setup than needed for a reviewable assignment.
- **In-process Go channels**: fine for a single process demo; does not exercise durable transport or independent scaling of generator vs service.
- **SQLite / Redis-only store**: workable, but Postgres better matches concurrent writers + filtered list queries.

## 3. Data model

### Orders (current state)

| Column | Type | Notes |
|--------|------|-------|
| `order_id` | TEXT PK | Canonical ID |
| `customer_id` | TEXT | From create |
| `restaurant_id` | TEXT | From create |
| `items` | JSONB | Latest item list |
| `status` | TEXT | FSM value |
| `created_at` | TIMESTAMPTZ | Create event time |
| `updated_at` | TIMESTAMPTZ | Last applied event time |
| `version` | BIGINT | Optimistic concurrency |

### Processed events

`processed_events(event_id PK)` — makes processing **idempotent**. A duplicate `eventId` is detected inside the same transaction as the state write and skipped.

### Pending updates

`pending_updates` — buffers status/items updates that arrive **before** their `order.create`. Drained (oldest `event_ts` first) when the create is applied. Rows older than `PENDING_TTL` are expired.

## 4. Event envelope

```json
{
  "eventId": "uuid",
  "type": "order.create | order.update.status | order.update.items",
  "timestamp": "RFC3339",
  "payload": { }
}
```

- `eventId` → idempotency key  
- `timestamp` → stale-event detection and “latest” ordering  
- `type` + `payload` → business mutation  

### Order ID ownership

The assignment says the service generates `orderId`. In this design:

1. If `order.create` omits `orderId`, the service mints a UUID.
2. The generator **pre-assigns** `orderId` in the create payload so it can emit updates that reference real IDs without polling the API. The backend accepts a provided ID and remains the system of record for persistence and validation.

This keeps update events “referencing orders that actually exist in the stream” (assignment §4.1) while preserving service ownership of identity rules.

## 5. Event flow

1. Generator `XADD`s JSON envelope to Redis Stream `order-events`.
2. Service consumer group reads with `XREADGROUP`.
3. Processor starts a DB transaction:
   - Insert `event_id` into `processed_events` (conflict → duplicate skip).
   - Apply mutation under a per-order shard lock + row `FOR UPDATE`.
   - Commit; then `XACK` the Redis message.
4. List API reads only from Postgres (materialized current state) — never from the stream.

Failed processing leaves the message unacked so it can be retried (at-least-once). Idempotency prevents double application.

## 6. Design considerations (§5)

### Out-of-order events

**Decision:** Buffer updates for unknown `orderId` in `pending_updates`; apply them in timestamp order when create arrives. Expire orphans after `PENDING_TTL` (default 5m).

**Trade-off:** Slightly more storage/complexity vs dropping updates. Dropping would lose data under normal reordering/lag; creating “ghost” orders from updates alone would pollute the List API with incomplete records.

### Concurrency

**Decision:** Two layers:

1. **In-process shard mutexes** keyed by `orderId` — serialize handlers for the same order within one process.
2. **Postgres `SELECT … FOR UPDATE` + `version` column** — correct under multi-instance consumers.

**Trade-off:** Shard locks reduce DB contention for the common single-instance review setup; optimistic versioning remains the source of truth across replicas.

### Duplicate / replayed events

**Decision:** `processed_events` unique `event_id`, inserted in the same transaction as the state change. Replays are no-ops.

**Trade-off:** Unbounded growth of the table over long runs — acceptable here; production would partition/TTL old keys once the stream retention window has passed.

### Status transitions

**Decision:** Enforce a finite state machine:

```
Received  → Preparing | Cancelled
Preparing → Complete  | Cancelled
Complete  → (terminal)
Cancelled → (terminal)
```

Invalid transitions are **logged and skipped** (event still marked processed so poison messages do not retry forever). Item updates are rejected on terminal orders.

**Trade-off:** Stricter than “any status overwrites.” Matches real kitchen workflows and makes List API status meaningful.

### Throughput

**Decision:** Redis Streams consumer groups + batched `XREADGROUP` (count 16) + connection pooling (pgx pool). Per-order sharding avoids global locks.

**How it scales:** Add more service replicas with distinct `CONSUMER_NAME`s in the same group. Bottlenecks shift to Postgres writes — mitigate with batching or partitioning by `order_id` hash if volume grows further. Stream maxlen / trimming can be added for retention control.

### “Latest” guarantees

**Decision:**

1. State is only exposed via Postgres after a successful commit.
2. Events with `timestamp < orders.updated_at` are treated as **stale** and skipped.
3. List API sorts/filters on `updated_at` from the DB.

**Meaning of “latest”:** Latest **successfully applied** event by event timestamp, not “whatever Redis delivered last.” This prefers producer time over broker arrival time, which matches the envelope contract. Clock skew across producers is a residual risk; a production system might use Lamport/vector clocks or broker-assigned IDs as a secondary key.

## 7. API design

`GET /orders` returns current state with optional:

- `status` filter  
- `sort=updated_at_desc|updated_at_asc`  
- `limit` / `offset` pagination + `total` count  

Documented fully in [API.md](API.md).

## 8. Operational notes

- Schema migrates automatically on process start (`CREATE TABLE IF NOT EXISTS`).
- Graceful shutdown: stop accepting HTTP, cancel consumer context, drain with timeout.
- Health check: `GET /healthz` pings Postgres (used by Compose).

## 9. What was deliberately kept simple

- No auth / multi-tenancy  
- No dead-letter queue UI (invalid transitions are logged + acked via idempotency)  
- No exactly-once end-to-end claim — at-least-once + idempotent handlers is the pragmatic model  
- Single stream, single consumer group for the demo  

These can be layered on without changing the core materialization approach.
