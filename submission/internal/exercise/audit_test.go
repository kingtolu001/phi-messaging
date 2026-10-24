package exercise

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"phi-messaging/internal/storage"
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
