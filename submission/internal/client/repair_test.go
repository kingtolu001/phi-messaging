package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

func TestConfiguredPeriodicRepairRecoversMissingSocketEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reads, deliveries atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/ws" {
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			conn.WriteJSON(map[string]string{"type": "ready"})
			var resume any
			conn.ReadJSON(&resume)
			conn.WriteJSON(map[string]string{"type": "resumed"})
			// Deliberately omit the live event. The durable endpoint is authoritative.
			conn.ReadMessage()
			return
		}
		if reads.Add(1) == 1 {
			w.Write([]byte("[]"))
			return
		}
		json.NewEncoder(w).Encode([]storage.Message{{ConversationID: "conversation", Sequence: 1}})
	}))
	defer srv.Close()
	c := Client{URL: srv.URL, Token: "local-repair-token", Subscriptions: []string{"conversation"}, RecoveryInterval: 60 * time.Millisecond,
		OnMessage: func(storage.Message) error { deliveries.Add(1); cancel(); return nil }}
	c.Run(ctx)
	if deliveries.Load() != 1 || reads.Load() < 2 || c.Cursors["conversation"] != 1 {
		t.Fatal("missed socket event was not durably repaired", deliveries.Load(), reads.Load(), c.Cursors)
	}
}
