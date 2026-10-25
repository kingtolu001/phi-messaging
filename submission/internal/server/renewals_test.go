package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"phi-messaging/internal/realtime"
)

func TestBatchedRenewalsPreserveDeviceLeasesAndErrors(t *testing.T) {
	e := setup(t)
	app, _ := node(t, e, e.config)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const devices = 64
	var wg sync.WaitGroup
	errors := make(chan error, devices)
	for i := 0; i < devices; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errors <- app.renewLease(ctx, e.users[0], time.Unix(int64(i), 0).String())
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	count, err := e.redis.ZCard(ctx, e.config.Prefix+"leases:"+e.users[0]).Result()
	if err != nil || count != devices {
		t.Fatal("batch merged or lost device leases", count, err)
	}
	// Redis script eviction must not break a subsequent batch.
	if err := e.redis.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := app.renewLease(ctx, e.users[0], "after-script-flush"); err != nil {
		t.Fatal(err)
	}
	closed := *e.redis.Options()
	brokenClient := redis.NewClient(&closed)
	brokenClient.Close()
	broken := realtime.Leases{Redis: brokenClient, Prefix: e.config.Prefix, TTL: e.config.LeaseTTL}
	results := broken.TouchBatch(ctx, []realtime.Renewal{{User: e.users[0], Connection: "failed-one"}, {User: e.users[1], Connection: "failed-two"}})
	if len(results) != 2 || results[0] == nil || results[1] == nil {
		t.Fatal("batch failure reported success", results)
	}
}

func TestRenewalQueueCancellationIsBounded(t *testing.T) {
	s := &Server{renewals: make(chan renewalRequest, 1)}
	s.renewals <- renewalRequest{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.renewLease(ctx, "user", "device"); err != context.Canceled {
		t.Fatal("full renewal queue ignored caller cancellation", err)
	}
}
