package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/vaurd/order-service/internal/models"
	"github.com/vaurd/order-service/internal/stream"
)

var (
	customers = []string{
		"cust-alice", "cust-bob", "cust-carol", "cust-dave", "cust-eve",
		"cust-frank", "cust-grace", "cust-heidi",
	}
	restaurants = []string{
		"rest-spice-route", "rest-noodle-bar", "rest-burger-lab",
		"rest-green-bowl", "rest-pizza-oven",
	}
	menuItems = []string{
		"item-burger", "item-fries", "item-soda", "item-salad",
		"item-pizza", "item-pasta", "item-sushi", "item-taco",
		"item-soup", "item-dessert",
	}
	statuses = []models.OrderStatus{
		models.StatusReceived,
		models.StatusPreparing,
		models.StatusComplete,
		models.StatusCancelled,
	}
)

func main() {
	rate := flag.Float64("rate", 5, "events per second")
	redisAddr := flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "redis address")
	streamName := flag.String("stream", envOr("STREAM_NAME", "order-events"), "redis stream name")
	createWeight := flag.Int("create-weight", 40, "relative weight for order.create events")
	statusWeight := flag.Int("status-weight", 35, "relative weight for order.update.status events")
	itemsWeight := flag.Int("items-weight", 25, "relative weight for order.update.items events")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	pub := stream.NewPublisher(rdb, *streamName)
	gen := &generator{
		pub:          pub,
		log:          log,
		createWeight: *createWeight,
		statusWeight: *statusWeight,
		itemsWeight:  *itemsWeight,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	interval := time.Duration(float64(time.Second) / *rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}

	log.Info("order generator started",
		"rate", *rate,
		"interval", interval.String(),
		"stream", *streamName,
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("generator shutting down", "knownOrders", gen.orderCount())
			return
		case <-ticker.C:
			if err := gen.emit(ctx); err != nil {
				log.Error("emit failed", "err", err)
			}
		}
	}
}

type generator struct {
	pub          *stream.Publisher
	log          *slog.Logger
	createWeight int
	statusWeight int
	itemsWeight  int
	rng          *rand.Rand

	mu     sync.Mutex
	orders []trackedOrder
}

type trackedOrder struct {
	ID     string
	Status models.OrderStatus
}

func (g *generator) orderCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.orders)
}

func (g *generator) emit(ctx context.Context) error {
	total := g.createWeight + g.statusWeight + g.itemsWeight
	if total <= 0 {
		total = 1
	}
	roll := g.rng.Intn(total)

	g.mu.Lock()
	hasOrders := len(g.orders) > 0
	g.mu.Unlock()

	// Force creates until we have at least one order to update.
	if !hasOrders || roll < g.createWeight {
		return g.emitCreate(ctx)
	}
	roll -= g.createWeight
	if roll < g.statusWeight {
		return g.emitStatusUpdate(ctx)
	}
	return g.emitItemsUpdate(ctx)
}

func (g *generator) emitCreate(ctx context.Context) error {
	orderID := uuid.NewString()
	items := g.randomItems()
	payload := map[string]any{
		"orderId":      orderID, // pre-assigned so updates can correlate
		"customerId":   pick(g.rng, customers),
		"restaurantId": pick(g.rng, restaurants),
		"items":        items,
	}
	raw, _ := json.Marshal(payload)
	env := models.EventEnvelope{
		EventID:   uuid.NewString(),
		Type:      models.EventTypeOrderCreate,
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}
	id, err := g.pub.Publish(ctx, env)
	if err != nil {
		return err
	}

	g.mu.Lock()
	g.orders = append(g.orders, trackedOrder{ID: orderID, Status: models.StatusReceived})
	g.mu.Unlock()

	g.log.Info("emitted", "type", env.Type, "orderId", orderID, "eventId", env.EventID, "redisId", id)
	return nil
}

func (g *generator) emitStatusUpdate(ctx context.Context) error {
	g.mu.Lock()
	if len(g.orders) == 0 {
		g.mu.Unlock()
		return g.emitCreate(ctx)
	}
	idx := g.rng.Intn(len(g.orders))
	order := g.orders[idx]
	next := g.nextStatus(order.Status)
	if next != order.Status {
		g.orders[idx].Status = next
	}
	orderID := order.ID
	g.mu.Unlock()

	payload, _ := json.Marshal(models.OrderUpdateStatusPayload{
		OrderID: orderID,
		Status:  next,
	})
	env := models.EventEnvelope{
		EventID:   uuid.NewString(),
		Type:      models.EventTypeOrderUpdateStatus,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	id, err := g.pub.Publish(ctx, env)
	if err != nil {
		return err
	}
	g.log.Info("emitted", "type", env.Type, "orderId", orderID, "status", next, "eventId", env.EventID, "redisId", id)
	return nil
}

func (g *generator) emitItemsUpdate(ctx context.Context) error {
	g.mu.Lock()
	if len(g.orders) == 0 {
		g.mu.Unlock()
		return g.emitCreate(ctx)
	}
	// Prefer non-terminal orders for item changes.
	var candidates []int
	for i, o := range g.orders {
		if o.Status != models.StatusComplete && o.Status != models.StatusCancelled {
			candidates = append(candidates, i)
		}
	}
	var orderID string
	if len(candidates) == 0 {
		orderID = g.orders[g.rng.Intn(len(g.orders))].ID
	} else {
		orderID = g.orders[candidates[g.rng.Intn(len(candidates))]].ID
	}
	g.mu.Unlock()

	payload, _ := json.Marshal(models.OrderUpdateItemsPayload{
		OrderID: orderID,
		Items:   g.randomItems(),
	})
	env := models.EventEnvelope{
		EventID:   uuid.NewString(),
		Type:      models.EventTypeOrderUpdateItems,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	id, err := g.pub.Publish(ctx, env)
	if err != nil {
		return err
	}
	g.log.Info("emitted", "type", env.Type, "orderId", orderID, "eventId", env.EventID, "redisId", id)
	return nil
}

func (g *generator) nextStatus(current models.OrderStatus) models.OrderStatus {
	switch current {
	case models.StatusReceived:
		if g.rng.Float64() < 0.15 {
			return models.StatusCancelled
		}
		return models.StatusPreparing
	case models.StatusPreparing:
		if g.rng.Float64() < 0.1 {
			return models.StatusCancelled
		}
		return models.StatusComplete
	default:
		// Occasionally emit an invalid transition so the service can demonstrate rejection.
		if g.rng.Float64() < 0.05 {
			return pickStatus(g.rng)
		}
		return current
	}
}

func (g *generator) randomItems() []models.Item {
	n := 1 + g.rng.Intn(4)
	items := make([]models.Item, n)
	for i := 0; i < n; i++ {
		items[i] = models.Item{
			ItemID: pick(g.rng, menuItems),
			Qty:    1 + g.rng.Intn(3),
		}
	}
	return items
}

func pick[T any](rng *rand.Rand, xs []T) T {
	return xs[rng.Intn(len(xs))]
}

func pickStatus(rng *rand.Rand) models.OrderStatus {
	return statuses[rng.Intn(len(statuses))]
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
