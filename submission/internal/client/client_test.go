package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"phi-messaging/internal/storage"
)

func TestDevicesReuseAndReleaseRecoveryConnections(t *testing.T) {
	const devices = 20
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var opened, active, requests atomic.Int64
	first := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		if n == devices {
			close(first)
		}
		if n <= devices {
			// Force simultaneous devices without leaving a stuck handler on failure.
			select {
			case <-first:
			case <-ctx.Done():
				return
			}
		}
		w.Write([]byte("[]"))
		if n == devices*3 {
			cancel()
		}
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
			active.Add(1)
		} else if state == http.StateClosed {
			active.Add(-1)
		}
	}
	srv.Start()
	defer srv.Close()
	var workers sync.WaitGroup
	for i := 0; i < devices; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			c := &Client{URL: srv.URL, Token: "device-token", OnMessage: func(storage.Message) error { return nil }}
			c.Run(ctx)
		}()
	}
	workers.Wait()
	if requests.Load() < devices*3 || opened.Load() != devices {
		t.Fatalf("recovery connection churn: requests=%d connections=%d", requests.Load(), opened.Load())
	}
	deadline := time.Now().Add(time.Second)
	for active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 {
		t.Fatal("device shutdown retained idle recovery connections", active.Load())
	}
}

func TestRecoveryMergesDuplicatesAndRejectsGaps(t *testing.T) {
	delivered := []int64{}
	c := Client{Cursors: map[string]int64{"conversation": 0}, OnMessage: func(m storage.Message) error { delivered = append(delivered, m.Sequence); return nil }}
	if err := c.Accept(storage.Message{ConversationID: "conversation", Sequence: 2}); !errors.Is(err, ErrGap) {
		t.Fatal("gap advanced cursor")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing bearer")
		}
		json.NewEncoder(w).Encode([]storage.Message{{ConversationID: "conversation", Sequence: 1}, {ConversationID: "conversation", Sequence: 2}})
	}))
	defer srv.Close()
	c.URL = srv.URL
	c.Token = "test-token"
	c.HTTP = srv.Client()
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(storage.Message{ConversationID: "conversation", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(storage.Message{ConversationID: "conversation", Sequence: 3}); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 3 || c.Cursors["conversation"] != 3 {
		t.Fatal(delivered, c.Cursors)
	}
	c.OnMessage = func(storage.Message) error { return errors.New("persistence failure") }
	if err := c.Accept(storage.Message{ConversationID: "conversation", Sequence: 4}); err == nil || c.Cursors["conversation"] != 3 {
		t.Fatal("advanced before successful processing")
	}
}
