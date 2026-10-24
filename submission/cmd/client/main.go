package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"phi-messaging/internal/client"
	"phi-messaging/internal/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	address := os.Getenv("APP_URL")
	if address == "" {
		address = "http://localhost:8080"
	}
	c := client.Client{URL: address, Token: os.Getenv("APP_TOKEN"), OnMessage: func(m storage.Message) error { return json.NewEncoder(os.Stdout).Encode(m) }}
	if err := c.Run(ctx); err != nil && ctx.Err() == nil {
		log.Print(err)
		os.Exit(1)
	}
}
