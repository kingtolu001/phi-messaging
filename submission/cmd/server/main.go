package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"phi-messaging/internal/server"
	"phi-messaging/internal/storage"
)

func run() error {
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		return errors.New("invalid database configuration")
	}
	defer db.Close()
	db.SetMaxOpenConns(32)
	// Recovery polls arrive in bursts. Retain the bounded pool between bursts
	// instead of repeatedly creating PostgreSQL backends for the same devices.
	db.SetMaxIdleConns(32)
	opts, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		return errors.New("invalid Redis configuration")
	}
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second
	opts.MaxRetries = -1
	opts.ContextTimeoutEnabled = true
	cache := redis.NewClient(opts)
	defer cache.Close()
	raw, err := os.ReadFile(os.Getenv("TOKENS_FILE"))
	if err != nil {
		return errors.New("cannot read mock identity configuration")
	}
	var tokens map[string]string
	if json.Unmarshal(raw, &tokens) != nil {
		return errors.New("invalid mock identity configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err = db.PingContext(ctx); err == nil {
		err = cache.Ping(ctx).Err()
	}
	cancel()
	if err != nil {
		return errors.New("startup dependencies unavailable")
	}
	instance, _ := os.Hostname()
	app, err := server.New(storage.New(db), cache, server.Config{InstanceID: instance, Tokens: tokens, Origins: strings.Split(os.Getenv("ALLOWED_ORIGINS"), ","), Prefix: "phi:"})
	if err != nil {
		return err
	}
	address := os.Getenv("LISTEN_ADDR")
	if address == "" {
		address = ":8080"
	}
	httpServer := &http.Server{Addr: address, Handler: app, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopped)
	result := make(chan error, 1)
	go func() { result <- httpServer.ListenAndServe() }()
	select {
	case <-stopped:
	case err = <-result:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	appErr := app.Close(shutdown)
	httpErr := httpServer.Shutdown(shutdown)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if appErr != nil {
		return appErr
	}
	return httpErr
}
func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
