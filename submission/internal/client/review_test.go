package client

import (
	"context"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"phi-messaging/internal/storage"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRejectedResumeIsPermanent(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := websocket.Upgrader{}
		conn, e := u.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		attempts.Add(1)
		conn.WriteJSON(map[string]string{"type": "ready"})
		var resume any
		conn.ReadJSON(&resume)
		conn.WriteJSON(map[string]string{"type": "error", "code": "invalid_input"})
	}))
	defer srv.Close()
	c := Client{URL: strings.TrimRight(srv.URL, "/"), Token: "test-token", Subscriptions: []string{"known"}, OnMessage: func(storage.Message) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.Run(ctx)
	if err == nil || ctx.Err() != nil || attempts.Load() != 1 {
		t.Fatalf("bad resume retried: attempts=%d err=%v ctx=%v", attempts.Load(), err, ctx.Err())
	}
}

func TestRetryJitterStillVariesAtCap(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		d := retryDelay(10 * time.Second)
		if d < 5*time.Second || d >= 10*time.Second {
			t.Fatal(d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatal("capped backoff lost jitter")
	}
}

func TestSlowFailedHandshakeDoesNotResetBackoff(t *testing.T) {
	// No accepted resume means no healthy session, regardless of dial duration.
	c := Client{}
	if c.stableSession() {
		t.Fatal("failed handshakes count as healthy uptime")
	}
	c.connectedAt = time.Now().Add(-time.Second)
	if c.stableSession() {
		t.Fatal("short session resets backoff")
	}
	c.connectedAt = time.Now().Add(-11 * time.Second)
	if !c.stableSession() {
		t.Fatal("healthy uptime does not reset backoff")
	}
}
func TestRejectUnsolicitedMessages(t *testing.T) {
	c := Client{Cursors: map[string]int64{"known": 0}, OnMessage: func(storage.Message) error { t.Fatal("unsolicited callback"); return nil }}
	if c.Accept(storage.Message{ConversationID: "other", Sequence: 1}) == nil {
		t.Fatal("accepted unsolicited conversation")
	}
	if c.Accept(storage.Message{ConversationID: "known", Sequence: 0}) == nil {
		t.Fatal("accepted invalid sequence")
	}
}
