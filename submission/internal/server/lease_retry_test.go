package server

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"phi-messaging/internal/realtime"
)

func TestTransientLeaseRenewalFailureKeepsValidConnection(t *testing.T) {
	e := setup(t)
	app, node := node(t, e, e.config)
	c := conversation(t, e)
	reader := connect(t, node.URL, e.tokens[1], map[string]string{c.ID: "0"})
	next(t, reader, "read")
	next(t, reader, "read")
	ctx := context.Background()
	key := e.config.Prefix + "leases:" + e.users[1]
	// A real Redis script failure is repaired before the old lease deadline.
	if err := e.redis.Set(ctx, key, "injected wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return app.diagnostics.leaseError.Load() > 0 })
	app.mu.Lock()
	active := len(app.sessions)
	app.mu.Unlock()
	if active != 1 {
		t.Fatal("one failed renewal discarded a still-valid connection")
	}
	if err := e.redis.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	leases := realtime.Leases{Redis: e.redis, Prefix: e.config.Prefix, TTL: e.config.LeaseTTL}
	eventually(t, time.Second, func() bool { p, err := leases.Get(ctx, e.users[1]); return err == nil && p.Online })
	request(t, node.URL, e.tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "lease recovered"}, 201, nil)
	if message := next(t, reader, "message"); message.Sequence != 1 {
		t.Fatal(message)
	}
}

func TestFailedRenewalsCannotExtendOriginalLeaseDeadline(t *testing.T) {
	e := setup(t)
	cache := redis.NewClient(e.redis.Options())
	app, err := New(e.store, cache, e.config)
	if err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(app)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		app.Close(ctx)
		node.Close()
		cache.Close()
	})
	c := conversation(t, e)
	reader := connect(t, node.URL, e.tokens[1], map[string]string{c.ID: "0"})
	next(t, reader, "read")
	next(t, reader, "read")
	cache.Close()
	eventually(t, time.Second, func() bool { return app.diagnostics.leaseError.Load() > 0 })
	app.mu.Lock()
	active := len(app.sessions)
	app.mu.Unlock()
	if active != 1 {
		t.Fatal("connection closed before its valid lease expired")
	}
	eventually(t, 2*time.Second, func() bool { app.mu.Lock(); defer app.mu.Unlock(); return len(app.sessions) == 0 })
	leases := realtime.Leases{Redis: e.redis, Prefix: e.config.Prefix, TTL: e.config.LeaseTTL}
	eventually(t, time.Second, func() bool { p, err := leases.Get(context.Background(), e.users[1]); return err == nil && !p.Online })
}
