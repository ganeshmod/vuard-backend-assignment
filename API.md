# API Specification

Base URL (local Compose): `http://localhost:8081`

All responses are JSON. Timestamps are RFC 3339 / ISO-8601 in UTC.

---

## `GET /healthz`

Liveness / readiness probe. Checks PostgreSQL connectivity.

### Response `200 OK`

```json
{ "status": "ok" }
```

### Response `503 Service Unavailable`

```json
{ "status": "unhealthy", "error": "..." }
```

---

## `GET /orders`

Alias: `GET /api/v1/orders`

Returns the **latest materialized state** of orders as currently stored in the database after event processing.

### Query parameters

| Name     | Type    | Default            | Description |
|----------|---------|--------------------|-------------|
| `limit`  | integer | `50` (max `200`)   | Page size |
| `offset` | integer | `0`                | Number of rows to skip |
| `status` | string  | _(none)_           | Filter: `Received` \| `Preparing` \| `Complete` \| `Cancelled` |
| `sort`   | string  | `updated_at_desc`  | `updated_at_desc` or `updated_at_asc` |

Also accepted: `updatedAtDesc`, `updatedAtAsc`.

### Response `200 OK`

```json
{
  "orders": [
    {
      "orderId": "6f1c2a8e-9b3d-4e1a-8c7f-2d4e5a6b7c8d",
      "customerId": "cust-alice",
      "restaurantId": "rest-spice-route",
      "items": [
        { "itemId": "item-burger", "qty": 2 },
        { "itemId": "item-fries", "qty": 1 }
      ],
      "status": "Preparing",
      "createdAt": "2026-07-18T12:00:01.123Z",
      "updatedAt": "2026-07-18T12:00:08.456Z"
    }
  ],
  "total": 42,
  "limit": 10,
  "offset": 0
}
```

| Field          | Type     | Notes |
|----------------|----------|-------|
| `orders`       | array    | Current state of each order |
| `orders[].orderId` | string | Stable order identifier |
| `orders[].customerId` | string | From `order.create` |
| `orders[].restaurantId` | string | From `order.create` |
| `orders[].items` | array | Latest item list `{ itemId, qty }` |
| `orders[].status` | string | `Received` \| `Preparing` \| `Complete` \| `Cancelled` |
| `orders[].createdAt` | string | Create event timestamp |
| `orders[].updatedAt` | string | Last applied event timestamp |
| `total`        | integer  | Total matching rows (before pagination) |
| `limit`        | integer  | Page size used |
| `offset`       | integer  | Offset used |

### Error responses

| Status | Body | When |
|--------|------|------|
| `400`  | `{ "error": "..." }` | Invalid `limit`, `offset`, `status`, or `sort` |
| `500`  | `{ "error": "failed to list orders" }` | Unexpected store failure |

### Examples

```bash
# First page
curl "http://localhost:8081/orders?limit=10&offset=0"

# Only preparing orders, oldest updates first
curl "http://localhost:8081/orders?status=Preparing&sort=updated_at_asc"

# Completed orders
curl "http://localhost:8081/orders?status=Complete&limit=50"
```

---

## Event envelope (ingest, not HTTP)

Events are published to Redis Stream `order-events` (field `payload` = JSON).

```json
{
  "eventId": "uuid",
  "type": "order.create | order.update.status | order.update.items",
  "timestamp": "2026-07-18T12:00:01.123Z",
  "payload": { }
}
```

### `order.create` payload

```json
{
  "orderId": "optional-but-used-by-generator-for-correlation",
  "customerId": "cust-alice",
  "restaurantId": "rest-spice-route",
  "items": [{ "itemId": "item-burger", "qty": 1 }]
}
```

If `orderId` is omitted, the service generates one (UUID). New orders start in status `Received`.

### `order.update.status` payload

```json
{
  "orderId": "6f1c2a8e-9b3d-4e1a-8c7f-2d4e5a6b7c8d",
  "status": "Preparing"
}
```

### `order.update.items` payload

```json
{
  "orderId": "6f1c2a8e-9b3d-4e1a-8c7f-2d4e5a6b7c8d",
  "items": [
    { "itemId": "item-pizza", "qty": 2 }
  ]
}
```
