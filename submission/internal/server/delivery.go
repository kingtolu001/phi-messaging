package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"phi-messaging/internal/storage"
)

const (
	deliveryWorkers  = 4
	maxDeliveryFeeds = 128
	deliveryWindow   = 128
)

type deliverySnapshot struct {
	Head     int64
	Messages []storage.Message
	Receipts []storage.Receipt
}
type conversationFeed struct {
	id        string
	sessions  map[*session]struct{}
	snapshot  *deliverySnapshot // Immutable after publication under hub.mu.
	scheduled bool
	dirty     bool
}
type deliveryHub struct {
	server    *Server
	mu        sync.Mutex
	feeds     map[string]*conversationFeed
	jobs      chan *conversationFeed
	refreshes atomic.Uint64
}

func newDeliveryHub(s *Server) *deliveryHub {
	h := &deliveryHub{server: s, feeds: map[string]*conversationFeed{}, jobs: make(chan *conversationFeed, maxDeliveryFeeds)}
	s.wg.Add(deliveryWorkers + 1)
	for i := 0; i < deliveryWorkers; i++ {
		go h.worker()
	}
	go func() {
		defer s.wg.Done()
		timer := time.NewTicker(s.config.PollInterval)
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
				h.wakeAll()
			}
		}
	}()
	return h
}

// queueLocked coalesces hints during both queued and in-flight refreshes.
func (h *deliveryHub) queueLocked(f *conversationFeed) {
	if f.scheduled {
		f.dirty = true
		return
	}
	f.scheduled = true
	select {
	case h.jobs <- f:
	default:
		f.scheduled = false
	} // The periodic sweep retries a full queue.
}
func (h *deliveryHub) wake(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if f := h.feeds[id]; f != nil {
		h.queueLocked(f)
	}
}
func (h *deliveryHub) wakeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.feeds {
		h.queueLocked(f)
	}
}
func (h *deliveryHub) attach(c *session) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	needed := 0
	for id := range c.cursors {
		if h.feeds[id] == nil {
			needed++
		}
	}
	if len(h.feeds)+needed > maxDeliveryFeeds {
		return false
	}
	for id := range c.cursors {
		f := h.feeds[id]
		if f == nil {
			f = &conversationFeed{id: id, sessions: map[*session]struct{}{}}
			h.feeds[id] = f
		}
		f.sessions[c] = struct{}{}
		if f.snapshot == nil {
			h.queueLocked(f)
		} else {
			select {
			case c.wake <- struct{}{}:
			default:
			}
		}
	}
	return true
}
func (h *deliveryHub) detach(c *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id := range c.cursors {
		if f := h.feeds[id]; f != nil {
			delete(f.sessions, c)
			if len(f.sessions) == 0 {
				delete(h.feeds, id)
			}
		}
	}
}
func (h *deliveryHub) snapshot(id string) *deliverySnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	if f := h.feeds[id]; f != nil {
		return f.snapshot
	}
	return nil
}

func (h *deliveryHub) worker() {
	defer h.server.wg.Done()
	for {
		select {
		case <-h.server.ctx.Done():
			return
		case f := <-h.jobs:
			h.refresh(f)
		}
	}
}
func (h *deliveryHub) refresh(f *conversationFeed) {
	h.mu.Lock()
	if h.feeds[f.id] != f {
		h.mu.Unlock()
		return
	}
	previous := f.snapshot
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(h.server.ctx, 5*time.Second)
	defer cancel()
	h.refreshes.Add(1)
	state, err := h.server.store.DeliveryState(ctx, f.id)
	var next *deliverySnapshot
	if err == nil {
		next = &deliverySnapshot{Head: state.Head, Receipts: state.Receipts}
		if previous != nil && state.Head < previous.Head {
			err = storage.ErrInvalid
		}
		after := int64(0)
		if previous != nil {
			after = previous.Head
			next.Messages = append([]storage.Message(nil), previous.Messages...)
		}
		if state.Head-after > deliveryWindow {
			after = state.Head - deliveryWindow
			next.Messages = nil
		}
		if state.Head > after {
			var page []storage.Message
			page, err = h.server.store.DeliveryMessages(ctx, f.id, after, state.Head, deliveryWindow)
			if err == nil {
				for _, m := range page {
					if m.Sequence != after+1 {
						err = storage.ErrInvalid
						break
					}
					after = m.Sequence
				}
				if after != state.Head {
					err = storage.ErrInvalid
				}
			}
			next.Messages = append(next.Messages, page...)
		}
		if len(next.Messages) > deliveryWindow {
			next.Messages = append([]storage.Message(nil), next.Messages[len(next.Messages)-deliveryWindow:]...)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.feeds[f.id] != f {
		return
	}
	f.scheduled = false
	if err != nil {
		for c := range f.sessions {
			c.stop()
		}
		return
	}
	changed := previous == nil || previous.Head != next.Head || !sameReceipts(previous.Receipts, next.Receipts)
	f.snapshot = next
	if changed {
		for c := range f.sessions {
			select {
			case c.wake <- struct{}{}:
			default:
			}
		}
	}
	if f.dirty {
		f.dirty = false
		h.queueLocked(f)
	}
}
func sameReceipts(a, b []storage.Receipt) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// page requires complete coverage through the caller's freshly authorized head.
// A miss must fall back to PostgreSQL, never return a falsely empty catch-up page.
func cachedPage(snapshot *deliverySnapshot, after, head int64, limit int) ([]storage.Message, bool) {
	if after >= head {
		return []storage.Message{}, true
	}
	target := head
	if head-after > int64(limit) {
		target = after + int64(limit)
	}
	if snapshot == nil || snapshot.Head < target || len(snapshot.Messages) == 0 || after < snapshot.Messages[0].Sequence-1 {
		return nil, false
	}
	out := make([]storage.Message, 0, limit)
	for _, m := range snapshot.Messages {
		if m.Sequence > after && m.Sequence <= head {
			out = append(out, m)
			if len(out) == limit {
				break
			}
		}
	}
	return out, true
}
func (s *Server) recoveryPage(ctx context.Context, actor, id string, after int64, limit int) ([]storage.Message, error) {
	// Public recovery still performs a current DB membership/head check per call.
	head, err := s.store.Head(ctx, actor, id)
	if err != nil {
		return nil, err
	}
	if page, ok := cachedPage(s.delivery.snapshot(id), after, head, limit); ok {
		return page, nil
	}
	return s.store.History(ctx, actor, id, storage.HistoryQuery{After: &after, Limit: limit})
}
