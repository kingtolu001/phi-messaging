package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"phi-messaging/internal/realtime"
	"phi-messaging/internal/storage"
)

func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, status int, code, message string) {
	write(w, status, struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message})
}
func storageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrInvalid):
		failure(w, 400, "invalid_input", "Invalid input")
	case errors.Is(err, storage.ErrNotFound):
		failure(w, 404, "not_found", "Conversation or user not found")
	case errors.Is(err, storage.ErrConflict):
		failure(w, 409, "conflict", "Client message ID is already used for another payload")
	default:
		failure(w, 503, "unavailable", "Service temporarily unavailable")
	}
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 32768)
	raw, err := io.ReadAll(r.Body)
	var size *http.MaxBytesError
	if errors.As(err, &size) {
		failure(w, 413, "too_large", "Request body is too large")
		return false
	}
	if err != nil || !utf8.Valid(raw) {
		failure(w, 400, "invalid_input", "Invalid JSON body")
		return false
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err = d.Decode(v); err == nil {
		err = d.Decode(new(any))
		if errors.Is(err, io.EOF) {
			return true
		}
	}
	failure(w, 400, "invalid_input", "Expected one JSON object with known fields")
	return false
}
func query(r *http.Request, allowed ...string) error {
	// ParseQuery errors must not silently discard malformed parameters.
	values, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil {
		return storage.ErrInvalid
	}
	for key, v := range values {
		found := false
		for _, a := range allowed {
			found = found || key == a
		}
		if !found || len(v) != 1 || v[0] == "" {
			return storage.ErrInvalid
		}
	}
	return nil
}
func number(raw string) (int64, error) {
	if raw == "" {
		return 0, storage.ErrInvalid
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, storage.ErrInvalid
		}
	}
	v, e := strconv.ParseInt(raw, 10, 64)
	if e != nil {
		return 0, storage.ErrInvalid
	}
	return v, nil
}
func pageLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, nil
	}
	n, e := number(raw)
	if e != nil || n < 1 || n > 100 {
		return 0, storage.ErrInvalid
	}
	return int(n), nil
}
func (s *Server) auth(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		actor, exists := s.config.Tokens[token]
		if !ok || !exists {
			failure(w, 401, "unauthorized", "Valid bearer authentication required")
			return
		}
		if r.URL.Path == "/v1/ws" {
			next(w, r, actor)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		next(w, r.WithContext(ctx), actor)
	}
}
func (s *Server) routes() {
	s.mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "live"}) })
	s.mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if s.ctx.Err() != nil || s.store.Ping(ctx) != nil || s.redis.Ping(ctx).Err() != nil || !s.subscribed.Load() {
			failure(w, 503, "unavailable", "Dependencies are not ready")
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	s.mux.HandleFunc("POST /v1/conversations", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r); e != nil {
			storageError(w, e)
			return
		}
		var body struct {
			Kind      string   `json:"kind"`
			MemberIDs []string `json:"member_ids"`
		}
		if !decode(w, r, &body) {
			return
		}
		c, e := s.store.CreateConversation(r.Context(), actor, body.Kind, body.MemberIDs)
		if e != nil {
			storageError(w, e)
			return
		}
		write(w, 201, c)
	}))
	s.mux.HandleFunc("GET /v1/conversations", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r, "after", "limit"); e != nil {
			storageError(w, e)
			return
		}
		n, e := pageLimit(r)
		if e != nil {
			storageError(w, e)
			return
		}
		v, e := s.store.ListConversations(r.Context(), actor, r.URL.Query().Get("after"), n)
		if e != nil {
			storageError(w, e)
			return
		}
		write(w, 200, v)
	}))
	s.mux.HandleFunc("POST /v1/conversations/{id}/messages", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r); e != nil {
			storageError(w, e)
			return
		}
		var b struct {
			ClientID string `json:"client_message_id"`
			Body     string `json:"body"`
		}
		if !decode(w, r, &b) {
			return
		}
		m, created, e := s.store.Send(r.Context(), actor, r.PathValue("id"), b.ClientID, b.Body)
		if e != nil {
			storageError(w, e)
			return
		}
		status := 200
		if created {
			status = 201
		}
		write(w, status, m)
	}))
	s.mux.HandleFunc("GET /v1/conversations/{id}/messages", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r, "before", "after", "limit"); e != nil {
			storageError(w, e)
			return
		}
		n, e := pageLimit(r)
		if e != nil {
			storageError(w, e)
			return
		}
		q := storage.HistoryQuery{Limit: n}
		for key, target := range map[string]**int64{"before": &q.Before, "after": &q.After} {
			if raw := r.URL.Query().Get(key); raw != "" {
				v, e := number(raw)
				if e != nil {
					storageError(w, e)
					return
				}
				*target = &v
			}
		}
		var v []storage.Message
		if q.After != nil && q.Before == nil {
			limit := q.Limit
			if limit == 0 {
				limit = 100
			}
			v, e = s.recoveryPage(r.Context(), actor, r.PathValue("id"), *q.After, limit)
		} else {
			v, e = s.store.History(r.Context(), actor, r.PathValue("id"), q)
		}
		if e != nil {
			storageError(w, e)
			return
		}
		write(w, 200, v)
	}))
	s.mux.HandleFunc("PUT /v1/conversations/{id}/read", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r); e != nil {
			storageError(w, e)
			return
		}
		var b struct {
			Sequence string `json:"sequence"`
		}
		if !decode(w, r, &b) {
			return
		}
		n, e := number(b.Sequence)
		if e != nil {
			storageError(w, e)
			return
		}
		v, e := s.store.MarkRead(r.Context(), actor, r.PathValue("id"), n)
		if e != nil {
			storageError(w, e)
			return
		}
		write(w, 200, v)
	}))
	s.mux.HandleFunc("GET /v1/conversations/{id}/receipts", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r); e != nil {
			storageError(w, e)
			return
		}
		v, e := s.store.Receipts(r.Context(), actor, r.PathValue("id"))
		if e != nil {
			storageError(w, e)
			return
		}
		write(w, 200, v)
	}))
	s.mux.HandleFunc("GET /v1/presence", s.auth(func(w http.ResponseWriter, r *http.Request, actor string) {
		if e := query(r, "user_ids"); e != nil {
			storageError(w, e)
			return
		}
		ids := strings.Split(r.URL.Query().Get("user_ids"), ",")
		if e := s.store.CanSeePresence(r.Context(), actor, ids); e != nil {
			storageError(w, e)
			return
		}
		out := make([]realtime.Presence, 0, len(ids))
		for _, id := range ids {
			p, e := s.leases.Get(r.Context(), id)
			if e != nil {
				storageError(w, e)
				return
			}
			out = append(out, p)
		}
		write(w, 200, out)
	}))
	s.mux.HandleFunc("GET /v1/ws", s.websocket)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { failure(w, 404, "not_found", "Endpoint not found") })
}
