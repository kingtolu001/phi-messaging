//go:build compose

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

// Run explicitly against the three running Compose processes and load balancer.
func TestComposeThreeNodeDelivery(t *testing.T) {
	base := os.Getenv("COMPOSE_BASE_URL")
	if base == "" {
		base = "http://127.0.0.1"
	}
	nodes := []string{base + ":8081", base + ":8082", base + ":8083", base + ":8080"}
	tokens := []string{"local-alice-token-0000000000000001", "local-bob-token-000000000000000002"}
	call := func(node, token, method, path string, body any, status int, out any) {
		t.Helper()
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(method, node+path, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		client := http.Client{Timeout: 5 * time.Second}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != status {
			t.Fatalf("%s: %d %s", path, res.StatusCode, data)
		}
		if out != nil {
			if err = json.Unmarshal(data, out); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, node := range nodes {
		call(node, tokens[0], "GET", "/health/ready", nil, 200, nil)
	}
	var c storage.Conversation
	call(nodes[3], tokens[0], "POST", "/v1/conversations", map[string]any{"kind": "group", "member_ids": []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"}}, 201, &c)
	connect := func(node string, cursor int64) *websocket.Conn {
		t.Helper()
		conn, res, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(node, "http")+"/v1/ws", http.Header{"Authorization": []string{"Bearer " + tokens[1]}})
		if err != nil {
			if res != nil {
				res.Body.Close()
			}
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var ready map[string]string
		if err = conn.ReadJSON(&ready); err != nil || ready["type"] != "ready" {
			t.Fatal(ready, err)
		}
		if err = conn.WriteJSON(map[string]any{"type": "resume", "cursors": map[string]string{c.ID: fmt.Sprint(cursor)}}); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	next := func(conn *websocket.Conn) storage.Message {
		t.Helper()
		for {
			var frame struct {
				Type string `json:"type"`
				storage.Message
			}
			if err := conn.ReadJSON(&frame); err != nil {
				t.Fatal(err)
			}
			if frame.Type == "message" {
				return frame.Message
			}
			if frame.Type == "error" {
				t.Fatal("resume rejected")
			}
		}
	}
	live := connect(nodes[1], 0)
	defer live.Close()
	var first storage.Message
	call(nodes[0], tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "app1 to app2"}, 201, &first)
	got := next(live)
	if got.Sequence != 1 || got.ClientMessageID != first.ClientMessageID {
		t.Fatal("cross-process delivery mismatch", got)
	}
	live.Close()
	var second storage.Message
	call(nodes[2], tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "app3 while receiver offline"}, 201, &second)
	resumed := connect(nodes[3], 1)
	defer resumed.Close()
	got = next(resumed)
	if got.Sequence != 2 || got.ClientMessageID != second.ClientMessageID {
		t.Fatal("reconnect recovery mismatch", got)
	}
	var history []storage.Message
	call(nodes[3], tokens[1], "GET", "/v1/conversations/"+c.ID+"/messages?after=0", nil, 200, &history)
	if len(history) != 2 {
		t.Fatal(history)
	}
}
