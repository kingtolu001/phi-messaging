package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

func TestSmallBuffersStreamLargeFramesAndIgnoreReceiptBurst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	body := strings.Repeat("🙂", 4000)
	var received atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/ws" {
			time.Sleep(50 * time.Millisecond)
			w.Write([]byte("[]"))
			return
		}
		conn, err := (&websocket.Upgrader{WriteBufferSize: 1024}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(map[string]string{"type": "ready"})
		var resume any
		conn.ReadJSON(&resume)
		conn.WriteJSON(map[string]string{"type": "resumed"})
		for i := 0; i < 200; i++ {
			conn.WriteJSON(map[string]string{"type": "read", "conversation_id": "conversation"})
		}
		raw, _ := json.Marshal(storage.Message{ConversationID: "conversation", Sequence: 1, Body: body})
		var event map[string]any
		json.Unmarshal(raw, &event)
		event["type"] = "message"
		conn.WriteJSON(event)
		conn.ReadMessage()
	}))
	defer srv.Close()
	c := Client{URL: srv.URL, Token: "local-buffer-token", Subscriptions: []string{"conversation"}, OnMessage: func(m storage.Message) error {
		if m.Body == body {
			received.Store(true)
		}
		cancel()
		return nil
	}}
	c.Run(ctx)
	if !received.Load() || c.Cursors["conversation"] != 1 {
		t.Fatal("large fragmented message was lost", c.Cursors)
	}
}
