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
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var page []storage.Message
	var e error
	api := NewAPI(r.Config.URL)
	for attempt := 0; attempt < 5; attempt++ {
		a.ReadAttempts++
		page, e = api.History(readCtx, r.Conversation)
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
	a.Rows = len(page)
	durable := map[string]int64{}
	for _, m := range page {
		durable[m.ClientMessageID] = m.Sequence
	}
	for _, rec := range r.Records {
		sequence, exists := durable[rec.ID]
		if rec.Ack {
			a.Acknowledged++
			if !exists || sequence != rec.Sequence {
				a.MissingAcknowledged++
			}
		} else if exists {
			a.UnacknowledgedCommitted++
		}
	}
	return a
}
