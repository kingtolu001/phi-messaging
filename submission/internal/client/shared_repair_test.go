package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

func TestSharedDurableRepairSkipsIdlePollsButRepairsGaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reads, sockets atomic.Int32
	var gapSent, unexpectedPoll atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/ws" {
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			sockets.Add(1)
			conn.WriteJSON(map[string]string{"type": "ready"})
			var resume any
			conn.ReadJSON(&resume)
			conn.WriteJSON(map[string]any{"type": "resumed", "durable_reconciliation": true})
			time.Sleep(200 * time.Millisecond)
			gapSent.Store(true)
			conn.WriteJSON(map[string]string{"type": "message", "conversation_id": "conversation", "sequence": "3"})
			conn.ReadMessage()
			return
		}
		n := reads.Add(1)
		if !gapSent.Load() {
			if n > 1 {
				unexpectedPoll.Store(true)
			}
			w.Write([]byte("[]"))
			return
		}
		json.NewEncoder(w).Encode([]storage.Message{{ConversationID: "conversation", Sequence: 1}, {ConversationID: "conversation", Sequence: 2}, {ConversationID: "conversation", Sequence: 3}})
	}))
	defer srv.Close()
	var received []int64
	c := Client{URL: srv.URL, Token: "local-token", Subscriptions: []string{"conversation"}, RecoveryInterval: 20 * time.Millisecond, OnMessage: func(m storage.Message) error {
		received = append(received, m.Sequence)
		if m.Sequence == 3 {
			cancel()
		}
		return nil
	}}
	c.Run(ctx)
	if unexpectedPoll.Load() || reads.Load() != 2 || sockets.Load() != 1 || !reflect.DeepEqual(received, []int64{1, 2, 3}) {
		t.Fatal("shared repair lost gap recovery or repeated idle HTTP reads", unexpectedPoll.Load(), reads.Load(), sockets.Load(), received)
	}
}
