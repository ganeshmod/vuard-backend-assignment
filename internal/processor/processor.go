package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/vaurd/order-service/internal/models"
	"github.com/vaurd/order-service/internal/store"
)

// Processor applies events to order state with concurrency control,
// idempotency, out-of-order buffering, and status transition rules.
type Processor struct {
	store *store.Store
	log   *slog.Logger

	// Per-order serialization via hashed shard mutexes.
	shards    []sync.Mutex
	shardMask int
}

const defaultShards = 64

// New creates a Processor.
func New(st *store.Store, log *slog.Logger) *Processor {
	return &Processor{
		store:     st,
		log:       log,
		shards:    make([]sync.Mutex, defaultShards),
		shardMask: defaultShards - 1,
	}
}

func (p *Processor) lockKey(key string) func() {
	h := fnv32(key)
	idx := int(h) & p.shardMask
	p.shards[idx].Lock()
	return p.shards[idx].Unlock
}

func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ProcessResult describes how an event was handled.
type ProcessResult struct {
	Skipped bool
	Reason  string
	OrderID string
}

// Process applies a single event envelope.
func (p *Processor) Process(ctx context.Context, env models.EventEnvelope) (ProcessResult, error) {
	if env.EventID == "" {
		return ProcessResult{}, fmt.Errorf("missing eventId")
	}
	if env.Type == "" {
		return ProcessResult{}, fmt.Errorf("missing event type")
	}
	if env.Timestamp.IsZero() {
		env.Timestamp = time.Now().UTC()
	}

	switch env.Type {
	case models.EventTypeOrderCreate:
		return p.processCreate(ctx, env)
	case models.EventTypeOrderUpdateStatus:
		return p.processUpdateStatus(ctx, env)
	case models.EventTypeOrderUpdateItems:
		return p.processUpdateItems(ctx, env)
	default:
		return ProcessResult{}, fmt.Errorf("unknown event type: %s", env.Type)
	}
}

func (p *Processor) processCreate(ctx context.Context, env models.EventEnvelope) (ProcessResult, error) {
	var payload models.OrderCreatePayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return ProcessResult{}, fmt.Errorf("decode create payload: %w", err)
	}
	if payload.CustomerID == "" || payload.RestaurantID == "" {
		return ProcessResult{}, fmt.Errorf("customerId and restaurantId are required")
	}
	if err := validateItems(payload.Items); err != nil {
		return ProcessResult{}, err
	}

	orderID := uuid.NewString()

	// Generator may include orderId so updates can reference the same order
	// before create is processed. The service still "owns" ID generation at
	// the generator boundary; the backend accepts a pre-assigned id when present.
	type createWithID struct {
		models.OrderCreatePayload
		OrderID string `json:"orderId"`
	}
	var withID createWithID
	if err := json.Unmarshal(env.Payload, &withID); err == nil && withID.OrderID != "" {
		orderID = withID.OrderID
	}

	unlock := p.lockKey(orderID)
	defer unlock()

	tx, err := p.store.Begin(ctx)
	if err != nil {
		return ProcessResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := p.store.MarkEventProcessed(ctx, tx, env.EventID); err != nil {
		if errors.Is(err, store.ErrDuplicateEvent) {
			return ProcessResult{Skipped: true, Reason: "duplicate event", OrderID: orderID}, nil
		}
		return ProcessResult{}, err
	}

	existing, err := p.store.GetOrder(ctx, tx, orderID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return ProcessResult{}, err
	}
	if existing != nil {
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Skipped: true, Reason: "order already exists", OrderID: orderID}, nil
	}

	now := env.Timestamp.UTC()
	order := &models.Order{
		OrderID:      orderID,
		CustomerID:   payload.CustomerID,
		RestaurantID: payload.RestaurantID,
		Items:        payload.Items,
		Status:       models.StatusReceived,
		CreatedAt:    now,
		UpdatedAt:    now,
		Version:      1,
	}
	if err := p.store.InsertOrder(ctx, tx, order); err != nil {
		return ProcessResult{}, err
	}

	pending, err := p.store.DrainPending(ctx, tx, orderID)
	if err != nil {
		return ProcessResult{}, err
	}
	for _, pu := range pending {
		if err := p.applyPending(ctx, tx, order, pu); err != nil {
			p.log.Warn("pending update rejected",
				"orderId", orderID,
				"eventId", pu.EventID,
				"err", err,
			)
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ProcessResult{}, err
	}
	p.log.Info("order created", "orderId", orderID, "eventId", env.EventID)
	return ProcessResult{OrderID: orderID}, nil
}

func (p *Processor) processUpdateStatus(ctx context.Context, env models.EventEnvelope) (ProcessResult, error) {
	var payload models.OrderUpdateStatusPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return ProcessResult{}, fmt.Errorf("decode status payload: %w", err)
	}
	if payload.OrderID == "" {
		return ProcessResult{}, fmt.Errorf("orderId is required")
	}
	if !models.ValidStatuses[payload.Status] {
		return ProcessResult{}, fmt.Errorf("invalid status: %s", payload.Status)
	}

	unlock := p.lockKey(payload.OrderID)
	defer unlock()

	tx, err := p.store.Begin(ctx)
	if err != nil {
		return ProcessResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := p.store.MarkEventProcessed(ctx, tx, env.EventID); err != nil {
		if errors.Is(err, store.ErrDuplicateEvent) {
			return ProcessResult{Skipped: true, Reason: "duplicate event", OrderID: payload.OrderID}, nil
		}
		return ProcessResult{}, err
	}

	order, err := p.store.GetOrder(ctx, tx, payload.OrderID)
	if errors.Is(err, store.ErrNotFound) {
		if err := p.store.BufferPending(ctx, tx, store.PendingUpdate{
			OrderID:   payload.OrderID,
			EventID:   env.EventID,
			EventType: env.Type,
			Payload:   env.Payload,
			EventTS:   env.Timestamp.UTC(),
		}); err != nil {
			return ProcessResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		p.log.Info("buffered status update for unknown order",
			"orderId", payload.OrderID, "eventId", env.EventID)
		return ProcessResult{Skipped: true, Reason: "buffered pending create", OrderID: payload.OrderID}, nil
	}
	if err != nil {
		return ProcessResult{}, err
	}

	if env.Timestamp.Before(order.UpdatedAt) {
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Skipped: true, Reason: "stale event", OrderID: payload.OrderID}, nil
	}

	if err := applyStatus(order, payload.Status, env.Timestamp.UTC()); err != nil {
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		p.log.Warn("invalid status transition",
			"orderId", payload.OrderID,
			"from", order.Status,
			"to", payload.Status,
			"eventId", env.EventID,
		)
		return ProcessResult{Skipped: true, Reason: err.Error(), OrderID: payload.OrderID}, nil
	}

	if err := p.store.UpdateOrder(ctx, tx, order); err != nil {
		return ProcessResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProcessResult{}, err
	}
	p.log.Info("order status updated",
		"orderId", payload.OrderID, "status", payload.Status, "eventId", env.EventID)
	return ProcessResult{OrderID: payload.OrderID}, nil
}

func (p *Processor) processUpdateItems(ctx context.Context, env models.EventEnvelope) (ProcessResult, error) {
	var payload models.OrderUpdateItemsPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return ProcessResult{}, fmt.Errorf("decode items payload: %w", err)
	}
	if payload.OrderID == "" {
		return ProcessResult{}, fmt.Errorf("orderId is required")
	}
	if err := validateItems(payload.Items); err != nil {
		return ProcessResult{}, err
	}

	unlock := p.lockKey(payload.OrderID)
	defer unlock()

	tx, err := p.store.Begin(ctx)
	if err != nil {
		return ProcessResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := p.store.MarkEventProcessed(ctx, tx, env.EventID); err != nil {
		if errors.Is(err, store.ErrDuplicateEvent) {
			return ProcessResult{Skipped: true, Reason: "duplicate event", OrderID: payload.OrderID}, nil
		}
		return ProcessResult{}, err
	}

	order, err := p.store.GetOrder(ctx, tx, payload.OrderID)
	if errors.Is(err, store.ErrNotFound) {
		if err := p.store.BufferPending(ctx, tx, store.PendingUpdate{
			OrderID:   payload.OrderID,
			EventID:   env.EventID,
			EventType: env.Type,
			Payload:   env.Payload,
			EventTS:   env.Timestamp.UTC(),
		}); err != nil {
			return ProcessResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		p.log.Info("buffered items update for unknown order",
			"orderId", payload.OrderID, "eventId", env.EventID)
		return ProcessResult{Skipped: true, Reason: "buffered pending create", OrderID: payload.OrderID}, nil
	}
	if err != nil {
		return ProcessResult{}, err
	}

	if order.Status == models.StatusComplete || order.Status == models.StatusCancelled {
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Skipped: true, Reason: "order is terminal", OrderID: payload.OrderID}, nil
	}

	if env.Timestamp.Before(order.UpdatedAt) {
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Skipped: true, Reason: "stale event", OrderID: payload.OrderID}, nil
	}

	order.Items = payload.Items
	order.UpdatedAt = env.Timestamp.UTC()
	if err := p.store.UpdateOrder(ctx, tx, order); err != nil {
		return ProcessResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProcessResult{}, err
	}
	p.log.Info("order items updated", "orderId", payload.OrderID, "eventId", env.EventID)
	return ProcessResult{OrderID: payload.OrderID}, nil
}

func (p *Processor) applyPending(ctx context.Context, tx pgx.Tx, order *models.Order, pu store.PendingUpdate) error {
	switch pu.EventType {
	case models.EventTypeOrderUpdateStatus:
		var payload models.OrderUpdateStatusPayload
		if err := json.Unmarshal(pu.Payload, &payload); err != nil {
			return err
		}
		if pu.EventTS.Before(order.UpdatedAt) {
			return nil
		}
		if err := applyStatus(order, payload.Status, pu.EventTS); err != nil {
			return err
		}
		return p.store.UpdateOrder(ctx, tx, order)

	case models.EventTypeOrderUpdateItems:
		var payload models.OrderUpdateItemsPayload
		if err := json.Unmarshal(pu.Payload, &payload); err != nil {
			return err
		}
		if order.Status == models.StatusComplete || order.Status == models.StatusCancelled {
			return fmt.Errorf("order is terminal")
		}
		if pu.EventTS.Before(order.UpdatedAt) {
			return nil
		}
		if err := validateItems(payload.Items); err != nil {
			return err
		}
		order.Items = payload.Items
		order.UpdatedAt = pu.EventTS
		return p.store.UpdateOrder(ctx, tx, order)

	default:
		return fmt.Errorf("unsupported pending type %s", pu.EventType)
	}
}

func applyStatus(order *models.Order, next models.OrderStatus, ts time.Time) error {
	if !models.CanTransition(order.Status, next) {
		return fmt.Errorf("invalid transition %s -> %s", order.Status, next)
	}
	if order.Status == next {
		return nil
	}
	order.Status = next
	order.UpdatedAt = ts
	return nil
}

func validateItems(items []models.Item) error {
	if len(items) == 0 {
		return fmt.Errorf("items must be non-empty")
	}
	for _, it := range items {
		if it.ItemID == "" || it.Qty <= 0 {
			return fmt.Errorf("each item needs itemId and qty > 0")
		}
	}
	return nil
}
