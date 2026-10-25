package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"phi-messaging/internal/client"
	"phi-messaging/internal/storage"
)

func TestClientUsesSharedPostgresRepairWithoutRedisHints(t *testing.T) {
	e := setup(t)
	app, _ := node(t, e, e.config)
	c := conversation(t, e)
	var historyReads atomic.Int32
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/messages") {
			historyReads.Add(1)
		}
		app.ServeHTTP(w, r)
	}))
	defer front.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	received := make(chan int64, 1)
	consumer := client.Client{URL: front.URL, Token: e.tokens[1], Subscriptions: []string{c.ID}, RecoveryInterval: 20 * time.Millisecond, OnMessage: func(m storage.Message) error { received <- m.Sequence; return nil }}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("client did not stop")
		}
	}()
	eventually(t, time.Second, func() bool { return historyReads.Load() == 1 })
	if _, err := e.db.Exec(`CREATE FUNCTION lose_hint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.published_at=now(); RETURN NEW; END $$; CREATE TRIGGER lose_hint BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION lose_hint()`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, _, err := e.store.Send(ctx, e.users[0], c.ID, uuid.NewString(), "periodic shared repair"); err != nil {
		t.Fatal(err)
	}
	select {
	case sequence := <-received:
		if sequence != 1 {
			t.Fatal(sequence)
		}
	case <-ctx.Done():
		t.Fatal("shared PostgreSQL sweep did not recover missed notification")
	}
	if historyReads.Load() != 1 {
		t.Fatal("healthy subscribed client repeated per-session history reads", historyReads.Load())
	}
}
