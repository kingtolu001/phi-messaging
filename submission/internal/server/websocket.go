package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

type session struct {
	server    *Server
	conn      *websocket.Conn
	actor, id string
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
	wake      chan struct{}
	outbound  chan any
	cursors   map[string]int64
	receipts  map[string]map[string]int64
}

func (c *session) stop() { c.once.Do(func() { c.cancel(); c.conn.Close() }) }
func (c *session) enqueue(event any) bool {
	select {
	case <-c.ctx.Done():
		return false
	default:
	}
	select {
	case c.outbound <- event:
		return true
	default:
		c.server.diagnostics.queueFull.Add(1)
		c.stop()
		return false
	}
}
func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	// Count failed authentication attempts too. This is a bounded per-node limiter.
	s.mu.Lock()
	now := time.Now()
	if now.Sub(s.rateStart) >= time.Second {
		s.rateStart = now
		s.attempts = 0
	}
	s.attempts++
	limited := s.attempts > s.config.HandshakesPerSecond
	s.mu.Unlock()
	if limited {
		failure(w, 429, "rate_limited", "WebSocket handshake rate exceeded")
		return
	}
	s.auth(s.acceptWebSocket)(w, r)
}
func (s *Server) acceptWebSocket(w http.ResponseWriter, r *http.Request, actor string) {
	if e := query(r); e != nil {
		storageError(w, e)
		return
	}
	origin := r.Header.Get("Origin")
	trusted := origin == ""
	for _, allowed := range s.config.Origins {
		trusted = trusted || origin == allowed
	}
	if !trusted {
		failure(w, 403, "origin_rejected", "WebSocket origin is not allowed")
		return
	}
	s.mu.Lock()
	if s.closing || s.slots >= s.config.MaxConnections {
		s.mu.Unlock()
		failure(w, 503, "unavailable", "WebSocket capacity unavailable")
		return
	}
	s.slots++
	s.wg.Add(1)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.slots--; s.mu.Unlock(); s.wg.Done() }()
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	id := uuid.NewString()
	leaseDeadline := time.Now().Add(s.config.LeaseTTL)
	err := s.leases.Touch(ctx, actor, id)
	cancel()
	if err != nil {
		storageError(w, err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.leases.Remove(ctx, actor, id)
	}()
	upgrader := websocket.Upgrader{HandshakeTimeout: 5 * time.Second, CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sessionCtx, sessionCancel := context.WithCancel(s.ctx)
	c := &session{server: s, conn: conn, actor: actor, id: id, ctx: sessionCtx, cancel: sessionCancel, wake: make(chan struct{}, 1), outbound: make(chan any, s.config.QueueSize), receipts: make(map[string]map[string]int64)}
	defer c.stop()
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.sessions[c] = struct{}{}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.sessions, c); s.mu.Unlock() }()
	conn.SetReadLimit(32768)
	conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	if err = conn.WriteJSON(map[string]string{"type": "ready", "instance_id": s.config.InstanceID}); err != nil {
		return
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		return
	}
	var resume struct {
		Type    string            `json:"type"`
		Cursors map[string]string `json:"cursors"`
	}
	if json.Unmarshal(raw, &resume) != nil || resume.Type != "resume" || len(resume.Cursors) == 0 || len(resume.Cursors) > 100 {
		c.reject("invalid_input", "Expected resume with 1-100 conversation cursors")
		return
	}
	c.cursors = make(map[string]int64, len(resume.Cursors))
	for conversation, raw := range resume.Cursors {
		cursor, e := number(raw)
		if e != nil {
			c.reject("invalid_input", "Invalid resume cursor")
			return
		}
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		head, e := s.store.Head(ctx, actor, conversation)
		cancel()
		if e != nil {
			code := "unavailable"
			if errors.Is(e, storage.ErrNotFound) {
				code = "not_found"
			} else if errors.Is(e, storage.ErrInvalid) {
				code = "invalid_input"
			}
			c.reject(code, "Cannot resume conversation")
			return
		}
		if cursor > head {
			c.reject("invalid_input", "Resume cursor exceeds conversation head")
			return
		}
		c.cursors[conversation] = cursor
	}
	if !s.delivery.attach(c) {
		c.reject("unavailable", "Conversation delivery capacity unavailable")
		return
	}
	defer s.delivery.detach(c)
	conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	if conn.WriteJSON(map[string]any{"type": "resumed", "durable_reconciliation": true}) != nil {
		return
	}
	conn.SetReadDeadline(leaseDeadline)
	conn.SetPongHandler(func(string) error {
		nextDeadline := time.Now().Add(s.config.LeaseTTL)
		ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
		defer cancel()
		if err := s.renewLease(ctx, actor, id); err != nil {
			s.diagnostics.recordLeaseError(err)
			// A failed renewal does not invalidate the last successful lease.
			// Keep its existing deadline; another pong may renew before expiry.
			return nil
		}
		return conn.SetReadDeadline(nextDeadline)
	})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); c.writeLoop() }()
	go func() { defer workers.Done(); c.recoverLoop() }()
	// This goroutine must keep reading to process pong and close control frames.
	_, _, readErr := conn.ReadMessage() // Resume is the only application frame; control frames are handled while reading.
	if c.ctx.Err() == nil {
		var network net.Error
		if errors.As(readErr, &network) && network.Timeout() {
			s.diagnostics.readTimeout.Add(1)
		} else {
			s.diagnostics.readError.Add(1)
		}
	}
	c.stop()
	workers.Wait()
}
func (c *session) reject(code, message string) {
	c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	c.conn.WriteJSON(map[string]string{"type": "error", "code": code, "message": message})
	c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Invalid resume"), time.Now().Add(time.Second))
}
func (c *session) writeLoop() {
	defer c.stop()
	ticker := time.NewTicker(c.server.config.LeaseTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case event := <-c.outbound:
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
			if c.conn.WriteJSON(event) != nil {
				if c.ctx.Err() == nil {
					c.server.diagnostics.writeError.Add(1)
				}
				return
			}
		case <-ticker.C:
			if c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.server.config.WriteTimeout)) != nil {
				if c.ctx.Err() == nil {
					c.server.diagnostics.writeError.Add(1)
				}
				return
			}
		}
	}
}
func (c *session) recoverLoop() {
	defer c.stop()
	for {
		if !c.catchUp() {
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
		}
	}
}
func (c *session) catchUp() bool {
	for conversation, cursor := range c.cursors {
		snapshot := c.server.delivery.snapshot(conversation)
		if snapshot == nil {
			continue
		} // First shared refresh will wake this session.
		member := false
		for _, r := range snapshot.Receipts {
			member = member || r.UserID == c.actor
		}
		if !member {
			return false
		}
		page, hit := cachedPage(snapshot, cursor, snapshot.Head, 100)
		if !hit {
			ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			var err error
			page, err = c.server.store.History(ctx, c.actor, conversation, storage.HistoryQuery{After: &cursor, Limit: 100})
			cancel()
			if err != nil {
				c.server.diagnostics.historyError.Add(1)
				return false
			}
		}
		for _, m := range page {
			if m.Sequence != c.cursors[conversation]+1 {
				return false
			}
			if !c.enqueue(struct {
				Type string `json:"type"`
				storage.Message
			}{"message", m}) {
				return false
			}
			c.cursors[conversation] = m.Sequence
		}
		if len(page) == 100 {
			select {
			case c.wake <- struct{}{}:
			default:
			}
		}
		if c.receipts[conversation] == nil {
			c.receipts[conversation] = make(map[string]int64)
		}
		for _, r := range snapshot.Receipts {
			old, exists := c.receipts[conversation][r.UserID]
			if exists && r.ReadSequence <= old {
				continue
			}
			if !c.enqueue(struct {
				Type           string `json:"type"`
				ConversationID string `json:"conversation_id"`
				storage.Receipt
			}{"read", conversation, r}) {
				return false
			}
			c.receipts[conversation][r.UserID] = r.ReadSequence
		}
	}
	return true
}
