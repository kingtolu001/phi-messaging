//go:build compose

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"phi-messaging/internal/exercise"
	"phi-messaging/internal/storage"
)

func TestComposeProxyErrorsUseProtocolJSON(t *testing.T) {
	req, err := http.NewRequest("POST", "http://127.0.0.1:8080/v1/conversations/"+uuid.NewString()+"/messages", strings.NewReader(strings.Repeat("x", 33000)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer local-alice-token-0000000000000001")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body struct {
		Code string `json:"code"`
	}
	if err = json.NewDecoder(res.Body).Decode(&body); err != nil || res.StatusCode != 413 || body.Code != "too_large" {
		t.Fatal("proxy violated error contract", res.StatusCode, body, err)
	}
}

func TestComposeConcurrentOrderingAndRetries(t *testing.T) {
	ctx := context.Background()
	nodes := []exercise.API{exercise.NewAPI("http://127.0.0.1:8081"), exercise.NewAPI("http://127.0.0.1:8082"), exercise.NewAPI("http://127.0.0.1:8083")}
	token := "local-alice-token-0000000000000001"
	c, err := nodes[0].Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 64)
	start := make(chan struct{})
	retryKey := uuid.NewString()
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			key := retryKey
			if i < 24 {
				key = uuid.NewString()
			}
			var m storage.Message
			_, e := nodes[i%3].Call(ctx, token, "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": key, "body": "concurrent"}, &m)
			errors <- e
		}(i)
	}
	close(start)
	wg.Wait()
	close(errors)
	for e := range errors {
		if e != nil {
			t.Error(e)
		}
	}
	history, err := nodes[1].History(ctx, c.ID)
	if err != nil || len(history) != 25 {
		t.Fatalf("rows=%d err=%v", len(history), err)
	}
	count := 0
	for _, m := range history {
		if m.ClientMessageID == retryKey {
			count++
		}
	}
	if count != 1 {
		t.Fatal("retry duplicated", count)
	}
	code, _ := nodes[2].Call(ctx, token, "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": retryKey, "body": "different"}, nil)
	if code != 409 {
		t.Fatal("conflict", code)
	}
	ids := make(chan string, 16)
	errors = make(chan error, 16)
	start = make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var direct storage.Conversation
			_, e := nodes[i%3].Call(ctx, token, "POST", "/v1/conversations", map[string]any{"kind": "direct", "member_ids": []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"}}, &direct)
			if e == nil {
				ids <- direct.ID
			}
			errors <- e
		}(i)
	}
	close(start)
	wg.Wait()
	close(ids)
	close(errors)
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	for e := range errors {
		if e != nil {
			t.Error(e)
		}
	}
	if len(unique) != 1 {
		t.Fatal(fmt.Sprint("direct race IDs: ", unique))
	}
}
