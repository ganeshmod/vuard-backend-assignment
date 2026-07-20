package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vaurd/order-service/internal/models"
	"github.com/vaurd/order-service/internal/processor"
)

// Consumer reads order events from a Redis Stream consumer group.
type Consumer struct {
	rdb       *redis.Client
	stream    string
	group     string
	consumer  string
	block     time.Duration
	processor *processor.Processor
	log       *slog.Logger
}

// NewConsumer creates a stream consumer and ensures the consumer group exists.
func NewConsumer(
	ctx context.Context,
	rdb *redis.Client,
	stream, group, consumerName string,
	block time.Duration,
	proc *processor.Processor,
	log *slog.Logger,
) (*Consumer, error) {
	c := &Consumer{
		rdb:       rdb,
		stream:    stream,
		group:     group,
		consumer:  consumerName,
		block:     block,
		processor: proc,
		log:       log,
	}
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}
	return c, nil
}

// Run blocks until ctx is cancelled, continuously consuming events.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("stream consumer started",
		"stream", c.stream,
		"group", c.group,
		"consumer", c.consumer,
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    16,
			Block:    c.block,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.log.Error("xreadgroup failed", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				if err := c.handleMessage(ctx, msg); err != nil {
					c.log.Error("failed to process message",
						"id", msg.ID,
						"err", err,
					)
					// Leave unacked for retry via XAUTOCLAIM / reclaim on restart.
					continue
				}
				if err := c.rdb.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
					c.log.Error("xack failed", "id", msg.ID, "err", err)
				}
			}
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg redis.XMessage) error {
	raw, ok := msg.Values["payload"].(string)
	if !ok {
		// Also accept byte slices from some redis clients.
		if b, ok := msg.Values["payload"].([]byte); ok {
			raw = string(b)
		} else {
			return fmt.Errorf("message %s missing payload field", msg.ID)
		}
	}

	var env models.EventEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	_, err := c.processor.Process(ctx, env)
	return err
}

// Publisher publishes events onto the Redis Stream.
type Publisher struct {
	rdb    *redis.Client
	stream string
}

// NewPublisher creates a stream publisher.
func NewPublisher(rdb *redis.Client, stream string) *Publisher {
	return &Publisher{rdb: rdb, stream: stream}
}

// Publish writes an event envelope to the stream. Returns the Redis message id.
func (p *Publisher) Publish(ctx context.Context, env models.EventEnvelope) (string, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	id, err := p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]any{"payload": string(body)},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}
	return id, nil
}
