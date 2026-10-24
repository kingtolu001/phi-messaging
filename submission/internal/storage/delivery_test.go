package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestOutboxPublisherConnectionCrash(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "direct")
	send(t, s, u[0], c.ID)
	ctx := context.Background()
	publications := 0
	ok, err := s.PublishNext(ctx, func(context.Context, string) error {
		publications++
		var pid int
		// Terminate the transaction's backend after publication, before marking.
		if e := s.db.QueryRow(`SELECT pid FROM pg_locks WHERE relation='outbox'::regclass AND mode='RowShareLock' AND granted AND pid<>pg_backend_pid() LIMIT 1`).Scan(&pid); e != nil {
			return e
		}
		_, e := s.db.Exec("SELECT pg_terminate_backend($1)", pid)
		return e
	})
	if err == nil || ok {
		t.Fatal("crashed publisher incorrectly marked success", ok, err)
	}
	ok, err = s.PublishNext(ctx, func(context.Context, string) error { publications++; return nil })
	if err != nil || !ok || publications != 2 {
		t.Fatal("claim did not recover with repeated publication", ok, err, publications)
	}
}

func TestOutboxRetryAndConcurrentClaims(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "direct")
	send(t, s, u[0], c.ID)
	ctx := context.Background()
	// Redis accepted the hint, then the callback lost its result. The row must
	// remain pending, so a second attempt may publish the same hint again.
	calls := 0
	if _, err := s.PublishNext(ctx, func(context.Context, string) error { calls++; return errors.New("unknown publish outcome") }); err == nil {
		t.Fatal("expected failure")
	}
	if ok, err := s.PublishNext(ctx, func(_ context.Context, id string) error {
		calls++
		if id != c.ID {
			t.Error("wrong hint")
		}
		return nil
	}); err != nil || !ok || calls != 2 {
		t.Fatalf("retry ok=%v calls=%d err=%v", ok, calls, err)
	}
	for i := 0; i < 20; i++ {
		send(t, s, u[0], c.ID)
	}
	var published atomic.Int32
	concurrent(t, 8, func(int) error {
		for {
			ok, err := s.PublishNext(ctx, func(context.Context, string) error { published.Add(1); return nil })
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
	})
	if published.Load() != 20 {
		t.Fatalf("claims=%d", published.Load())
	}
	var pending int
	if err := s.db.QueryRow("SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&pending); err != nil || pending != 0 {
		t.Fatal(pending, err)
	}
}
