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

func TestHTTPRecoveryOutageKeepsSocketAndRepairsGap(t *testing.T) {
	for _, status := range []int{503, 401} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var sockets, reads, retries atomic.Int32
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
					conn.WriteJSON(map[string]string{"type": "resumed"})
					conn.WriteJSON(map[string]string{"type": "message", "conversation_id": "conversation", "sequence": "3"})
					conn.ReadMessage()
					return
				}
				if reads.Add(1) == 1 || status == 401 {
					w.WriteHeader(status)
					return
				}
				json.NewEncoder(w).Encode([]storage.Message{{ConversationID: "conversation", Sequence: 1}, {ConversationID: "conversation", Sequence: 2}, {ConversationID: "conversation", Sequence: 3}})
			}))
			defer srv.Close()
			var delivered []int64
			c := Client{URL: srv.URL, Token: "local-recovery-token", Subscriptions: []string{"conversation"}, RecoveryInterval: 30 * time.Millisecond,
				OnMessage: func(m storage.Message) error {
					delivered = append(delivered, m.Sequence)
					if m.Sequence == 3 {
						cancel()
					}
					return nil
				},
				OnState: func(state State) {
					if state.Kind == "recovery_retry" {
						retries.Add(1)
					}
				}}
			err := c.Run(ctx)
			if status == 503 {
				if !reflect.DeepEqual(delivered, []int64{1, 2, 3}) || retries.Load() == 0 || sockets.Load() != 1 {
					t.Fatal("transient repair failure lost ordering or forced reconnect", delivered, retries.Load(), sockets.Load(), err)
				}
			} else if err != statusError(401) || len(delivered) != 0 || sockets.Load() != 1 || retries.Load() != 0 {
				t.Fatal("authorization failure was ignored", err, delivered, sockets.Load())
			}
		})
	}
}
