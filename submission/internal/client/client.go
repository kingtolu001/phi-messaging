// Package client provides a small recovery-aware client for the local exercise.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

var ErrGap = errors.New("noncontiguous message sequence")
var errConversationLimit = errors.New("sample client supports at most 100 conversations")

type processingError struct{ error }

type statusError int

func (e statusError) Error() string { return fmt.Sprintf("server returned HTTP %d", int(e)) }

type Client struct {
	URL, Token string
	HTTP       *http.Client
	// Cursors belong to this client device, not to the user's read receipts.
	// Run owns this map. Persist it in a real client after processing each message.
	Cursors   map[string]int64
	OnMessage func(storage.Message) error
	// Explicit subscriptions avoid discovering unrelated conversations in tools.
	Subscriptions []string
	// Pin the first socket for failure tests; all retries use URL's load balancer.
	FirstWebSocketURL string
	OnState           func(State)
	firstAttempted    bool
}

type State struct {
	Kind       string
	InstanceID string
	Delay      time.Duration
	Status     int
}

func (c *Client) state(s State) {
	if c.OnState != nil {
		c.OnState(s)
	}
}
func retryDelay(base time.Duration) time.Duration {
	return base/2 + time.Duration(rand.Int64N(int64(base-base/2)))
}

func (c *Client) Accept(m storage.Message) error {
	cursor, known := c.Cursors[m.ConversationID]
	if !known || m.Sequence < 1 {
		return processingError{errors.New("invalid or unsolicited message")}
	}
	if m.Sequence <= cursor {
		c.state(State{Kind: "duplicate"})
		return nil
	}
	if m.Sequence != cursor+1 {
		c.state(State{Kind: "gap"})
		return ErrGap
	}
	if err := c.OnMessage(m); err != nil {
		return processingError{err}
	}
	c.Cursors[m.ConversationID] = m.Sequence
	return nil
}
func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return statusError(res.StatusCode)
	}
	return json.NewDecoder(res.Body).Decode(out)
}
func (c *Client) discover(ctx context.Context) (bool, error) {
	if len(c.Subscriptions) > 0 {
		return false, nil
	}
	changed := false
	after := ""
	for {
		var list []storage.Conversation
		if err := c.get(ctx, "/v1/conversations?limit=100"+after, &list); err != nil {
			return false, err
		}
		for _, v := range list {
			if _, ok := c.Cursors[v.ID]; !ok {
				if len(c.Cursors) >= 100 {
					return false, errConversationLimit
				}
				c.Cursors[v.ID] = 0
				changed = true
			}
		}
		if len(list) < 100 {
			break
		}
		after = "&after=" + list[len(list)-1].ID
	}
	return changed, nil
}
func (c *Client) Recover(ctx context.Context) error {
	for id := range c.Cursors {
		for {
			before := c.Cursors[id]
			var page []storage.Message
			if err := c.get(ctx, "/v1/conversations/"+id+"/messages?after="+strconv.FormatInt(c.Cursors[id], 10)+"&limit=100", &page); err != nil {
				return err
			}
			for _, m := range page {
				if m.ConversationID != id {
					return processingError{errors.New("history conversation mismatch")}
				}
				if err := c.Accept(m); err != nil {
					return err
				}
			}
			if len(page) < 100 {
				break
			}
			if c.Cursors[id] == before {
				return processingError{errors.New("history page made no progress")}
			}
		}
	}
	return nil
}
func (c *Client) Run(ctx context.Context) error {
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.User != nil {
		return errors.New("invalid server URL")
	}
	c.URL = strings.TrimRight(c.URL, "/")
	if c.Token == "" || c.OnMessage == nil {
		return errors.New("token and message callback are required")
	}
	if c.HTTP == nil {
		// A device owns its recovery connections. Sharing DefaultTransport's
		// two idle slots across thousands of devices creates connection churn.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = 2
		transport.MaxIdleConnsPerHost = 2
		transport.MaxConnsPerHost = 2
		transport.IdleConnTimeout = 30 * time.Second
		c.HTTP = &http.Client{Timeout: 5 * time.Second, Transport: transport}
		defer transport.CloseIdleConnections()
	}
	if c.Cursors == nil {
		c.Cursors = make(map[string]int64)
	}
	if len(c.Subscriptions) > 100 {
		return errConversationLimit
	}
	for _, id := range c.Subscriptions {
		if _, ok := c.Cursors[id]; !ok {
			c.Cursors[id] = 0
		}
	}
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		started := time.Now()
		err = c.once(ctx)
		var processing processingError
		if errors.As(err, &processing) || errors.Is(err, errConversationLimit) {
			return err
		}
		var status statusError
		if errors.As(err, &status) && status >= 400 && status < 500 && status != 429 {
			return err
		}
		if ctx.Err() != nil {
			break
		}
		if time.Since(started) > 10*time.Second {
			backoff = 100 * time.Millisecond
		}
		delay := retryDelay(backoff)
		status = 0
		errors.As(err, &status)
		c.state(State{Kind: "retry", Delay: delay, Status: int(status)})
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	return ctx.Err()
}
func (c *Client) once(ctx context.Context) error {
	if _, err := c.discover(ctx); err != nil {
		return err
	}
	if len(c.Cursors) == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	}
	endpoint := "ws" + strings.TrimPrefix(c.URL, "http") + "/v1/ws"
	established := false
	defer func() {
		if !established && ctx.Err() == nil {
			c.state(State{Kind: "connection_failed"})
		}
	}()
	if !c.firstAttempted && c.FirstWebSocketURL != "" {
		endpoint = "ws" + strings.TrimPrefix(strings.TrimRight(c.FirstWebSocketURL, "/"), "http") + "/v1/ws"
	}
	c.firstAttempted = true
	conn, res, err := websocket.DefaultDialer.DialContext(ctx, endpoint, http.Header{"Authorization": []string{"Bearer " + c.Token}})
	if err != nil {
		if res != nil {
			res.Body.Close()
			return statusError(res.StatusCode)
		}
		return err
	}
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopClose()
	conn.SetReadLimit(32768)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ready struct {
		Type       string `json:"type"`
		InstanceID string `json:"instance_id"`
	}
	if err = conn.ReadJSON(&ready); err != nil {
		return err
	}
	if ready.Type != "ready" {
		return errors.New("missing registration frame")
	}
	cursors := make(map[string]string, len(c.Cursors))
	for id, n := range c.Cursors {
		cursors[id] = strconv.FormatInt(n, 10)
	}
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err = conn.WriteJSON(map[string]any{"type": "resume", "cursors": cursors}); err != nil {
		return err
	}
	var resumed struct {
		Type string `json:"type"`
		Code string `json:"code"`
	}
	if err = conn.ReadJSON(&resumed); err != nil {
		return err
	}
	if resumed.Type != "resumed" {
		if resumed.Code == "unavailable" {
			return statusError(503)
		}
		return statusError(400)
	}
	c.state(State{Kind: "connected", InstanceID: ready.InstanceID})
	established = true
	defer c.state(State{Kind: "disconnected", InstanceID: ready.InstanceID})
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPingHandler(func(data string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})
	type frame struct {
		Type string `json:"type"`
		storage.Message
	}
	frames := make(chan frame, 128)
	readDone := make(chan struct{})
	readerErr := make(chan error, 1)
	go func() {
		defer close(readDone)
		for {
			var f frame
			if e := conn.ReadJSON(&f); e != nil {
				readerErr <- e
				return
			}
			select {
			case frames <- f:
			case <-ctx.Done():
				return
			default:
				readerErr <- errors.New("client queue full")
				return
			}
		}
	}()
	defer func() { conn.Close(); <-readDone }()
	// The socket is already registered. HTTP and buffered live events may overlap;
	// Accept suppresses duplicates and advances only contiguous positions.
	if err = c.Recover(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-readerErr:
			return err
		case f := <-frames:
			if f.Type == "error" {
				return errors.New("WebSocket resume rejected")
			}
			if f.Type != "message" {
				continue
			}
			if err = c.Accept(f.Message); errors.Is(err, ErrGap) {
				if err = c.Recover(ctx); err != nil {
					return err
				}
				err = c.Accept(f.Message)
			}
			if err != nil {
				return err
			}
		case <-ticker.C:
			changed, e := c.discover(ctx)
			if e != nil {
				return e
			}
			if changed {
				return nil
			}
			if err = c.Recover(ctx); err != nil {
				return err
			}
		}
	}
}
