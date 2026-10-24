package exercise

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"phi-messaging/internal/storage"
)

type Assertion struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}
type FailureReport struct {
	Passed             bool        `json:"passed"`
	Load               LoadReport  `json:"active_messaging"`
	Assertions         []Assertion `json:"assertions"`
	CrashLeaseExpiryMS float64     `json:"crash_lease_expiry_ms"`
}

func RunFailure(ctx context.Context, c LoadConfig) FailureReport {
	r := FailureReport{}
	check := func(name string, ok bool, detail string) {
		r.Assertions = append(r.Assertions, Assertion{name, ok, detail})
	}
	// Always restore only this exercise's replicas/dependencies, preserving volumes.
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		command(restore, "compose", "start", "postgres", "redis", "app1", "app2", "app3")
	}()
	api := NewAPI(c.URL)
	if !waitReady(ctx, c.URL, 20*time.Second) {
		check("ready before failure fixture", false, "dependencies did not become ready")
		return r
	}
	conversation, e := api.Create(ctx)
	if e != nil {
		check("create failure fixture", false, e.Error())
		return r
	}
	path := "/v1/conversations/" + conversation.ID + "/messages"
	// A forwarding proxy deliberately drops an actual committed HTTP response.
	target, _ := url.Parse(c.URL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var dropped atomic.Bool
	proxy.ModifyResponse = func(res *http.Response) error {
		if res.StatusCode == 201 && !dropped.Swap(true) {
			return fmt.Errorf("injected lost acknowledgement")
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(502) }
	lost := httptest.NewServer(proxy)
	key := uuid.NewString()
	body := map[string]string{"client_message_id": key, "body": "lost acknowledgement"}
	code, _ := NewAPI(lost.URL).Call(ctx, aliceToken, "POST", path, body, nil)
	lost.Close()
	var retried storage.Message
	retryCode, retryErr := api.Call(ctx, aliceToken, "POST", path, body, &retried)
	check("lost acknowledgement retry", code == 502 && dropped.Load() && retryCode == 200 && retryErr == nil && retried.Sequence == 1, fmt.Sprintf("dropped_status=%d retry_status=%d sequence=%d", code, retryCode, retried.Sequence))
	body["body"] = "conflict"
	code, _ = api.Call(ctx, aliceToken, "POST", path, body, nil)
	check("conflicting payload", code == 409, fmt.Sprint(code))
	// A Carol socket exists only on app1. It will not reconnect after SIGKILL.
	conn, res, e := websocket.DefaultDialer.Dial("ws://127.0.0.1:8081/v1/ws", http.Header{"Authorization": []string{"Bearer " + carolToken}})
	if e != nil {
		if res != nil {
			res.Body.Close()
		}
		check("crash-only device connection", false, e.Error())
		return r
	}
	defer conn.Close()
	var frame map[string]any
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	e = conn.ReadJSON(&frame)
	if e == nil {
		e = conn.WriteJSON(map[string]any{"type": "resume", "cursors": map[string]string{conversation.ID: "1"}})
	}
	if e == nil {
		e = conn.ReadJSON(&frame)
	}
	check("crash-only device subscribed", e == nil && frame["type"] == "resumed", fmt.Sprint(e))
	// Consume control frames to renew the lease until the process dies.
	conn.SetReadDeadline(time.Time{})
	go func() {
		for {
			if conn.ReadJSON(&frame) != nil {
				return
			}
		}
	}()
	r.Load = RunLoad(ctx, c)
	check("active messaging and affected reconnect", r.Load.Passed, r.Load.Failure)
	check("other device remains online after node kill", r.Load.OtherDeviceOnlineAfterKill, "Bob has connections on surviving nodes")
	check("different original instances", len(r.Load.Instances) >= 2, fmt.Sprint(r.Load.Instances))
	validDelays := len(r.Load.RetryDelaysMS) > 0
	for _, delay := range r.Load.RetryDelaysMS {
		validDelays = validDelays && delay >= 50 && delay < 10000
	}
	check("bounded jittered reconnect delays", validDelays, fmt.Sprint(r.Load.RetryDelaysMS))
	history, e := NewAPI("http://127.0.0.1:8082").History(ctx, r.Load.Conversation)
	durable := map[string]storage.Message{}
	senders := map[string]bool{}
	for _, m := range history {
		durable[m.ClientMessageID] = m
		senders[m.SenderID] = true
	}
	all := e == nil
	acks := 0
	for _, record := range r.Load.Records {
		if record.Ack {
			acks++
			m, ok := durable[record.ID]
			all = all && ok && m.Sequence == record.Sequence
		}
	}
	check("every acknowledged message in PostgreSQL history", all, fmt.Sprintf("acknowledged=%d durable_rows=%d error=%v", acks, len(history), e))
	// PostgreSQL remains the recovery source after the recipient's app was killed.
	check("contiguous durable ordering", e == nil, fmt.Sprint(e))
	check("both users send during active exchange", senders[alice] && senders[bob], fmt.Sprint(senders))
	expiryStart := time.Now()
	offline := false
	for time.Since(expiryStart) < 40*time.Second {
		var presence []struct {
			Online   bool       `json:"online"`
			LastSeen *time.Time `json:"last_seen"`
		}
		_, err := NewAPI("http://127.0.0.1:8082").Call(ctx, bobToken, "GET", "/v1/presence?user_ids="+carol, nil, &presence)
		if err == nil && len(presence) == 1 && !presence[0].Online && presence[0].LastSeen != nil {
			offline = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !r.Load.KillAt.IsZero() {
		r.CrashLeaseExpiryMS = float64(time.Since(r.Load.KillAt)) / float64(time.Millisecond)
	}
	check("crashed device lease expires with last_seen", offline, "observation from kill command request; polling can overestimate expiry")
	// Stop Redis completely. Sends commit while presence is unavailable; pending
	// hints must drain after Redis returns, and receipts restore through PostgreSQL.
	_, e = command(ctx, "compose", "stop", "redis")
	check("stop Redis", e == nil, fmt.Sprint(e))
	api = NewAPI("http://127.0.0.1:8082")
	var outage storage.Message
	key = uuid.NewString()
	body = map[string]string{"client_message_id": key, "body": "during Redis outage"}
	code, e = api.Call(ctx, aliceToken, "POST", path, body, &outage)
	check("Redis outage still commits", e == nil && code == 201, fmt.Sprint(code, e))
	code, _ = api.Call(ctx, bobToken, "GET", "/v1/presence?user_ids="+carol, nil, nil)
	check("Redis outage presence unavailable", code == 503, fmt.Sprint(code))
	_, e = api.Call(ctx, bobToken, "PUT", "/v1/conversations/"+conversation.ID+"/read", map[string]string{"sequence": fmt.Sprint(outage.Sequence)}, nil)
	check("durable read during Redis outage", e == nil, fmt.Sprint(e))
	pending, e := pendingOutbox(ctx, conversation.ID)
	check("Redis outage leaves durable pending outbox", e == nil && pending > 0, fmt.Sprint(pending, e))
	_, e = command(ctx, "compose", "start", "redis")
	check("restart Redis", e == nil, fmt.Sprint(e))
	waitReady(ctx, "http://127.0.0.1:8082", 15*time.Second)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pending, e = pendingOutbox(ctx, conversation.ID)
		if e == nil && pending == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	check("outbox drains after Redis restart", e == nil && pending == 0, fmt.Sprint(pending, e))
	var receipts []storage.Receipt
	_, e = api.Call(ctx, aliceToken, "GET", "/v1/conversations/"+conversation.ID+"/receipts", nil, &receipts)
	readRestored := false
	for _, v := range receipts {
		if v.UserID == bob && v.ReadSequence == outage.Sequence {
			readRestored = true
		}
	}
	check("receipt recovery after missed live hint", e == nil && readRestored, fmt.Sprint(e))
	history, e = api.History(ctx, conversation.ID)
	check("missed outage event recoverable", e == nil && len(history) == 2, fmt.Sprintf("rows=%d error=%v", len(history), e))
	_, e = command(ctx, "compose", "stop", "postgres")
	check("stop PostgreSQL", e == nil, fmt.Sprint(e))
	key = uuid.NewString()
	body = map[string]string{"client_message_id": key, "body": "database unavailable"}
	code, _ = api.Call(ctx, aliceToken, "POST", path, body, nil)
	check("PostgreSQL unavailable never acknowledges", code == 503, fmt.Sprint(code))
	_, e = command(ctx, "compose", "start", "postgres")
	check("restart PostgreSQL", e == nil, fmt.Sprint(e))
	waitReady(ctx, api.URL, 20*time.Second)
	code, e = api.Call(ctx, aliceToken, "POST", path, body, nil)
	check("same key works after PostgreSQL recovery", e == nil && code == 201, fmt.Sprint(code, e))
	r.Passed = true
	for _, a := range r.Assertions {
		r.Passed = r.Passed && a.Passed
	}
	return r
}
func pendingOutbox(ctx context.Context, id string) (int, error) {
	raw, e := command(ctx, "compose", "exec", "-T", "postgres", "psql", "-U", "phi", "-d", "phi", "-At", "-c", "SELECT count(*) FROM outbox WHERE published_at IS NULL AND conversation_id='"+id+"'")
	if e != nil {
		return 0, e
	}
	var n int
	_, e = fmt.Sscan(raw, &n)
	return n, e
}
func waitReady(ctx context.Context, address string, timeout time.Duration) bool {
	api := NewAPI(address)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, e := api.Call(ctx, aliceToken, "GET", "/health/ready", nil, nil)
		if code == 200 && e == nil {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}
