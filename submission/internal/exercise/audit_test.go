package exercise

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"phi-messaging/internal/storage"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHistoryAuditRetriesReadButRequiresEveryAcknowledgement(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode([]storage.Message{{ConversationID: "conversation", Sequence: 1, ClientMessageID: "committed"}})
	}))
	defer srv.Close()
	report := LoadReport{Config: LoadConfig{URL: srv.URL}, Conversation: "conversation", Records: []*DeliveryRecord{{ID: "committed", Ack: true, Sequence: 1}, {ID: "missing", Ack: true, Sequence: 2}}}
	result := Audit(context.Background(), report)
	if result.ReadAttempts != 2 || result.Error != "" || result.Acknowledged != 2 || result.MissingAcknowledged != 1 {
		t.Fatal(result)
	}
}

func TestAuditChecksEachConversationIndependently(t *testing.T) {
	requests := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.Split(r.URL.Path, "/")[3]
		requests[id] = true
		json.NewEncoder(w).Encode([]storage.Message{{ConversationID: id, Sequence: 1, ClientMessageID: id + "-message"}})
	}))
	defer srv.Close()
	report := LoadReport{Config: LoadConfig{URL: srv.URL}, Records: []*DeliveryRecord{
		{ID: "one-message", Conversation: "one", Ack: true, Sequence: 1},
		{ID: "two-message", Conversation: "two", Ack: true, Sequence: 1},
		{ID: "not-present", Conversation: "two", Ack: true, Sequence: 2},
	}}
	result := Audit(context.Background(), report)
	if result.Error != "" || result.Acknowledged != 3 || result.MissingAcknowledged != 1 || !requests["one"] || !requests["two"] {
		t.Fatal(result, requests)
	}
}

func TestHistoryAuditRejectsAnotherConversationsRows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]storage.Message{{ConversationID: "wrong", Sequence: 1, ClientMessageID: "expected"}})
	}))
	defer srv.Close()
	r := LoadReport{Config: LoadConfig{URL: srv.URL}, Conversation: "expected", Records: []*DeliveryRecord{{ID: "expected", Ack: true, Sequence: 1}}}
	result := Audit(context.Background(), r)
	if result.Error != "durable history conversation mismatch" {
		t.Fatal("audit accepted cross-conversation history", result)
	}
}
