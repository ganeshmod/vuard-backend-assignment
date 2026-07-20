package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vaurd/order-service/internal/api"
	"github.com/vaurd/order-service/internal/config"
	"github.com/vaurd/order-service/internal/processor"
	"github.com/vaurd/order-service/internal/store"
	"github.com/vaurd/order-service/internal/stream"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting order service",
		"http", cfg.HTTPAddr,
		"stream", cfg.StreamName,
	)

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database init failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	proc := processor.New(st, log)
	consumer, err := stream.NewConsumer(
		ctx, rdb,
		cfg.StreamName, cfg.ConsumerGroup, cfg.ConsumerName,
		cfg.ReadBlockTimeout,
		proc, log,
	)
	if err != nil {
		log.Error("stream consumer init failed", "err", err)
		os.Exit(1)
	}

	// Periodically expire orphaned pending updates.
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := st.ExpirePending(ctx, cfg.PendingTTL)
				if err != nil {
					log.Warn("expire pending failed", "err", err)
					continue
				}
				if n > 0 {
					log.Info("expired pending updates", "count", n)
				}
			}
		}
	}()

	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("consumer stopped", "err", err)
			stop()
		}
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
