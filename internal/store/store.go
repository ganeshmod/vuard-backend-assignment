package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vaurd/order-service/internal/models"
)

var (
	ErrNotFound      = errors.New("order not found")
	ErrConflict      = errors.New("optimistic lock conflict")
	ErrDuplicateEvent = errors.New("duplicate event")
)

// Store persists orders and processed-event markers.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a Store and runs schema migrations.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL,
    restaurant_id TEXT NOT NULL,
    items         JSONB NOT NULL DEFAULT '[]',
    status        TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    version       BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at DESC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id    TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pending_updates (
    id         BIGSERIAL PRIMARY KEY,
    order_id   TEXT NOT NULL,
    event_id   TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    payload    JSONB NOT NULL,
    event_ts   TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pending_order ON pending_updates (order_id);
CREATE INDEX IF NOT EXISTS idx_pending_created ON pending_updates (created_at);
`
	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// MarkEventProcessed inserts the event id. Returns ErrDuplicateEvent if already seen.
func (s *Store) MarkEventProcessed(ctx context.Context, tx pgx.Tx, eventID string) error {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDuplicateEvent
	}
	return nil
}

// Begin starts a transaction.
func (s *Store) Begin(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// GetOrder returns the current order or ErrNotFound.
func (s *Store) GetOrder(ctx context.Context, tx pgx.Tx, orderID string) (*models.Order, error) {
	var (
		o     models.Order
		items []byte
	)
	err := tx.QueryRow(ctx, `
		SELECT order_id, customer_id, restaurant_id, items, status, created_at, updated_at, version
		FROM orders WHERE order_id = $1
		FOR UPDATE`, orderID,
	).Scan(&o.OrderID, &o.CustomerID, &o.RestaurantID, &items, &o.Status, &o.CreatedAt, &o.UpdatedAt, &o.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return nil, fmt.Errorf("unmarshal items: %w", err)
	}
	return &o, nil
}

// InsertOrder creates a new order row.
func (s *Store) InsertOrder(ctx context.Context, tx pgx.Tx, o *models.Order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (order_id, customer_id, restaurant_id, items, status, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		o.OrderID, o.CustomerID, o.RestaurantID, items, o.Status, o.CreatedAt, o.UpdatedAt, o.Version,
	)
	return err
}

// UpdateOrder writes a full order row with optimistic locking on version.
func (s *Store) UpdateOrder(ctx context.Context, tx pgx.Tx, o *models.Order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return err
	}
	newVersion := o.Version + 1
	tag, err := tx.Exec(ctx, `
		UPDATE orders
		SET customer_id = $2, restaurant_id = $3, items = $4, status = $5,
		    updated_at = $6, version = $7
		WHERE order_id = $1 AND version = $8`,
		o.OrderID, o.CustomerID, o.RestaurantID, items, o.Status, o.UpdatedAt, newVersion, o.Version,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	o.Version = newVersion
	return nil
}

// PendingUpdate is a buffered update waiting for its order.create.
type PendingUpdate struct {
	OrderID   string
	EventID   string
	EventType string
	Payload   json.RawMessage
	EventTS   time.Time
}

// BufferPending stores an out-of-order update until the create arrives.
func (s *Store) BufferPending(ctx context.Context, tx pgx.Tx, p PendingUpdate) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO pending_updates (order_id, event_id, event_type, payload, event_ts)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`,
		p.OrderID, p.EventID, p.EventType, p.Payload, p.EventTS,
	)
	return err
}

// DrainPending returns and deletes buffered updates for an order, oldest first.
func (s *Store) DrainPending(ctx context.Context, tx pgx.Tx, orderID string) ([]PendingUpdate, error) {
	rows, err := tx.Query(ctx, `
		SELECT order_id, event_id, event_type, payload, event_ts
		FROM pending_updates
		WHERE order_id = $1
		ORDER BY event_ts ASC
		FOR UPDATE`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingUpdate
	for rows.Next() {
		var p PendingUpdate
		if err := rows.Scan(&p.OrderID, &p.EventID, &p.EventType, &p.Payload, &p.EventTS); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		_, err = tx.Exec(ctx, `DELETE FROM pending_updates WHERE order_id = $1`, orderID)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ExpirePending deletes buffered updates older than ttl.
func (s *Store) ExpirePending(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM pending_updates
		WHERE created_at < NOW() - ($1 * INTERVAL '1 second')`,
		ttl.Seconds(),
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ListFilter controls ListOrders queries.
type ListFilter struct {
	Status    *models.OrderStatus
	SortAsc   bool
	Limit     int
	Offset    int
}

// ListOrders returns the latest order states matching the filter.
func (s *Store) ListOrders(ctx context.Context, f ListFilter) ([]models.Order, int, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 200 {
		f.Limit = 200
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	orderDir := "DESC"
	if f.SortAsc {
		orderDir = "ASC"
	}

	args := []any{}
	where := "WHERE 1=1"
	if f.Status != nil {
		args = append(args, string(*f.Status))
		where += fmt.Sprintf(" AND status = $%d", len(args))
	}

	countQ := "SELECT COUNT(*) FROM orders " + where
	var total int
	if err := s.pool.QueryRow(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, f.Limit, f.Offset)
	listQ := fmt.Sprintf(`
		SELECT order_id, customer_id, restaurant_id, items, status, created_at, updated_at, version
		FROM orders %s
		ORDER BY updated_at %s
		LIMIT $%d OFFSET $%d`, where, orderDir, len(args)-1, len(args))

	rows, err := s.pool.Query(ctx, listQ, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var orders []models.Order
	for rows.Next() {
		var (
			o     models.Order
			items []byte
		)
		if err := rows.Scan(&o.OrderID, &o.CustomerID, &o.RestaurantID, &items, &o.Status, &o.CreatedAt, &o.UpdatedAt, &o.Version); err != nil {
			return nil, 0, err
		}
		if err := json.Unmarshal(items, &o.Items); err != nil {
			return nil, 0, err
		}
		orders = append(orders, o)
	}
	if orders == nil {
		orders = []models.Order{}
	}
	return orders, total, rows.Err()
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
