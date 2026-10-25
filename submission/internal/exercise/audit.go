package exercise

import (
	"context"
	"phi-messaging/internal/storage"
	"time"
)

type HistoryAudit struct {
	ReadAttempts            int    `json:"history_read_attempts"`
	Rows                    int    `json:"durable_rows"`
	Acknowledged            int    `json:"acknowledged_checked"`
	MissingAcknowledged     int    `json:"missing_or_changed_acknowledged"`
	UnacknowledgedCommitted int    `json:"unacknowledged_but_committed"`
	Error                   string `json:"error,omitempty"`
}

func Audit(ctx context.Context, r LoadReport) HistoryAudit {
	a := HistoryAudit{}
	readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	all := map[string]bool{}
	for _, record := range r.Records {
		id := record.Conversation
		if id == "" {
			id = r.Conversation
		}
		all[id] = true
	}
	if len(all) == 0 {
		all[r.Conversation] = true
	}
	durable := map[string]storage.Message{}
	var e error
	api := NewAPI(r.Config.URL)
	defer api.HTTP.CloseIdleConnections()
	for id := range all {
		var page []storage.Message
		for attempt := 0; attempt < 5; attempt++ {
			a.ReadAttempts++
			page, e = api.history(readCtx, id, aliceToken)
			if e == nil {
				break
			}
			select {
			case <-readCtx.Done():
				a.Error = e.Error()
				return a
			case <-time.After(time.Duration(100*(1<<attempt)) * time.Millisecond):
			}
		}
		if e != nil {
			a.Error = e.Error()
			return a
		}
		a.Rows += len(page)
		for _, m := range page {
			durable[m.ClientMessageID] = m
		}
	}
	for _, rec := range r.Records {
		message, exists := durable[rec.ID]
		if rec.Ack {
			a.Acknowledged++
			id := rec.Conversation
			if id == "" {
				id = r.Conversation
			}
			if !exists || message.Sequence != rec.Sequence || message.ConversationID != id {
				a.MissingAcknowledged++
			}
		} else if exists {
			a.UnacknowledgedCommitted++
		}
	}
	return a
}
