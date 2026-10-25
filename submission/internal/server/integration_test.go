package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"phi-messaging/internal/client"
	"phi-messaging/internal/storage"
)

type environment struct {
	db     *sql.DB
	store  *storage.Store
	redis  *redis.Client
	users  []string
	tokens []string
	config Config
}

func setup(t *testing.T) *environment {
	t.Helper()
	dsn, redisURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_URL")
	if dsn == "" || redisURL == "" {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatal("TEST_DATABASE_URL and TEST_REDIS_URL required")
		}
		t.Skip("PostgreSQL and Redis integration URLs are required")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(32)
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Error(err)
		}
	})
	_, file, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../../migrations/001_initial.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	opts.MaxRetries = -1
	opts.ContextTimeoutEnabled = true
	opts.DialTimeout = 300 * time.Millisecond
	opts.ReadTimeout = 300 * time.Millisecond
	opts.WriteTimeout = 300 * time.Millisecond
	r := redis.NewClient(opts)
	if err = r.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := schema + ":"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		keys, err := r.Keys(ctx, prefix+"*").Result()
		if err != nil {
			t.Error(err)
		} else if len(keys) > 0 {
			r.Del(ctx, keys...)
		}
		r.Close()
	})
	e := &environment{db: db, store: storage.New(db), redis: r}
	tokens := map[string]string{}
	for i := 0; i < 3; i++ {
		id, token := uuid.NewString(), uuid.NewString()
		e.users = append(e.users, id)
		e.tokens = append(e.tokens, token)
		tokens[token] = id
		if _, err = db.Exec("INSERT INTO users(id,display_name) VALUES($1,$2)", id, fmt.Sprint("user", i)); err != nil {
			t.Fatal(err)
		}
	}
	e.config = Config{Tokens: tokens, Origins: []string{"http://trusted.local"}, Prefix: prefix, PollInterval: 50 * time.Millisecond, LeaseTTL: 900 * time.Millisecond, WriteTimeout: 300 * time.Millisecond, QueueSize: 128}
	return e
}
func node(t *testing.T, e *environment, c Config) (*Server, *httptest.Server) {
	t.Helper()
	app, err := New(e.store, e.redis, c)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(app)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Close(ctx); err != nil {
			t.Error(err)
		}
		httpServer.Close()
	})
	eventually(t, 3*time.Second, func() bool { return app.subscribed.Load() })
	return app, httpServer
}
func eventually(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true before deadline")
}
func request(t *testing.T, url, token, method, path string, body any, status int, target any) {
	t.Helper()
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := http.Client{Timeout: 5 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != status {
		t.Fatalf("%s %s: got %d want %d: %s", method, path, res.StatusCode, status, raw)
	}
	if target != nil {
		if err = json.Unmarshal(raw, target); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
}
func conversation(t *testing.T, e *environment) storage.Conversation {
	t.Helper()
	c, err := e.store.CreateConversation(context.Background(), e.users[0], "direct", e.users[:2])
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type event struct {
	Type string `json:"type"`
	storage.Message
	UserID       string `json:"user_id"`
	ReadSequence int64  `json:"read_sequence,string"`
	Code         string `json:"code"`
}
type socket struct {
	conn   *websocket.Conn
	events chan event
}

func connect(t *testing.T, url, token string, cursors map[string]string) *socket {
	t.Helper()
	h := http.Header{"Authorization": []string{"Bearer " + token}, "Origin": []string{"http://trusted.local"}}
	conn, res, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http")+"/v1/ws", h)
	if err != nil {
		if res != nil {
			res.Body.Close()
		}
		t.Fatal(err)
	}
	var ready event
	if err = conn.ReadJSON(&ready); err != nil || ready.Type != "ready" {
		t.Fatal(ready, err)
	}
	if err = conn.WriteJSON(map[string]any{"type": "resume", "cursors": cursors}); err != nil {
		t.Fatal(err)
	}
	s := &socket{conn: conn, events: make(chan event, 256)}
	done := make(chan struct{})
	go func() {
		defer close(s.events)
		defer close(done)
		for {
			var e event
			if conn.ReadJSON(&e) != nil {
				return
			}
			select {
			case s.events <- e:
			default:
				return
			}
		}
	}()
	t.Cleanup(func() {
		conn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("socket reader did not stop")
		}
	})
	return s
}
func next(t *testing.T, s *socket, kind string) event {
	t.Helper()
	timer := time.NewTimer(4 * time.Second)
	defer timer.Stop()
	for {
		select {
		case e, ok := <-s.events:
			if !ok {
				t.Fatal("WebSocket closed before event")
			}
			if e.Type == "error" {
				t.Fatalf("WebSocket error: %+v", e)
			}
			if e.Type == kind {
				return e
			}
		case <-timer.C:
			t.Fatal("event timeout")
		}
	}
}

func TestHTTPAuthenticationMembershipAndValidation(t *testing.T) {
	e := setup(t)
	_, n := node(t, e, e.config)
	for _, path := range []string{"/v1/conversations", "/v1/presence?user_ids=" + e.users[0]} {
		request(t, n.URL, "", "GET", path, nil, 401, nil)
		request(t, n.URL, "wrong", "GET", path, nil, 401, nil)
	}
	request(t, n.URL, e.tokens[0], "POST", "/v1/conversations", map[string]any{"kind": "direct", "member_ids": e.users[:2]}, 201, new(storage.Conversation))
	c := conversation(t, e)
	base := "/v1/conversations/" + c.ID
	request(t, n.URL, e.tokens[2], "POST", base+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "hidden"}, 404, nil)
	for _, path := range []string{base + "/messages", base + "/receipts", "/v1/presence?user_ids=" + e.users[0]} {
		request(t, n.URL, e.tokens[2], "GET", path, nil, 404, nil)
	}
	request(t, n.URL, e.tokens[2], "PUT", base+"/read", map[string]string{"sequence": "0"}, 404, nil)
	key := uuid.NewString()
	body := map[string]string{"client_message_id": key, "body": "hello"}
	var first, retry storage.Message
	request(t, n.URL, e.tokens[0], "POST", base+"/messages", body, 201, &first)
	request(t, n.URL, e.tokens[0], "POST", base+"/messages", body, 200, &retry)
	if first.Sequence != 1 || first != retry {
		t.Fatal("retry changed committed message")
	}
	body["body"] = "conflict"
	request(t, n.URL, e.tokens[0], "POST", base+"/messages", body, 409, nil)
	var list []storage.Conversation
	request(t, n.URL, e.tokens[1], "GET", "/v1/conversations", nil, 200, &list)
	if len(list) != 1 || list[0].UnreadCount != 1 {
		t.Fatal(list)
	}
	request(t, n.URL, e.tokens[0], "GET", "/v1/conversations", nil, 200, &list)
	if list[0].UnreadCount != 0 {
		t.Fatal("own send unread")
	}
	request(t, n.URL, e.tokens[1], "PUT", base+"/read", map[string]string{"sequence": "1"}, 200, nil)
	request(t, n.URL, e.tokens[1], "PUT", base+"/read", map[string]string{"sequence": "0"}, 200, nil)
	var receipts []storage.Receipt
	request(t, n.URL, e.tokens[0], "GET", base+"/receipts", nil, 200, &receipts)
	for _, r := range receipts {
		if r.UserID == e.users[1] && r.ReadSequence != 1 {
			t.Fatal(receipts)
		}
	}
	for _, path := range []string{base + "/messages?before=1&after=0", base + "/messages?after=-1", base + "/messages?limit=0", base + "/messages?after=0&after=1", base + "/messages?after=%zz"} {
		request(t, n.URL, e.tokens[0], "GET", path, nil, 400, nil)
	}
	request(t, n.URL, e.tokens[0], "POST", base+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "fake actor", "sender_id": e.users[1]}, 400, nil)
	request(t, n.URL, e.tokens[0], "POST", base+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": strings.Repeat("x", 33000)}, 413, nil)
	request(t, n.URL, e.tokens[0], "PUT", base+"/read", map[string]any{"sequence": 1}, 400, nil)
	request(t, n.URL, e.tokens[0], "GET", "/health/ready", nil, 200, nil)
}

func TestWebSocketAuthenticationOriginAndMembership(t *testing.T) {
	e := setup(t)
	_, n := node(t, e, e.config)
	c := conversation(t, e)
	for _, test := range []struct {
		token, origin, query string
		status               int
	}{{"", "", "", 401}, {"wrong", "", "", 401}, {e.tokens[0], "http://untrusted.local", "", 403}, {e.tokens[0], "", "?token=secret", 400}} {
		h := http.Header{}
		if test.token != "" {
			h.Set("Authorization", "Bearer "+test.token)
		}
		h.Set("Origin", test.origin)
		conn, res, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(n.URL, "http")+"/v1/ws"+test.query, h)
		if conn != nil {
			conn.Close()
		}
		if err == nil || res == nil || res.StatusCode != test.status {
			t.Fatalf("upgrade status: %v %v", res, err)
		}
		res.Body.Close()
	}
	nonmember := connect(t, n.URL, e.tokens[2], map[string]string{c.ID: "0"})
	var denied event
	select {
	case denied = <-nonmember.events:
	case <-time.After(time.Second):
		t.Fatal("missing denial")
	}
	if denied.Type != "error" || denied.Code != "not_found" {
		t.Fatal(denied)
	}
	ahead := connect(t, n.URL, e.tokens[0], map[string]string{c.ID: "1"})
	select {
	case denied = <-ahead.events:
	case <-time.After(time.Second):
		t.Fatal("missing denial")
	}
	if denied.Code != "invalid_input" {
		t.Fatal(denied)
	}
}

func TestCrossInstanceOrderedDeliveryAndReceipts(t *testing.T) {
	e := setup(t)
	cfg := e.config
	cfg.PollInterval = time.Hour // This test must rely on Redis, not periodic repair.
	_, a := node(t, e, cfg)
	_, b := node(t, e, cfg)
	c := conversation(t, e)
	receiver := connect(t, b.URL, e.tokens[1], map[string]string{c.ID: "0"})
	next(t, receiver, "read")
	next(t, receiver, "read")
	for i := 1; i <= 8; i++ {
		var ack storage.Message
		request(t, a.URL, e.tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": fmt.Sprint(i)}, 201, &ack)
		got := next(t, receiver, "message")
		if got.Sequence != int64(i) || got.ClientMessageID != ack.ClientMessageID {
			t.Fatalf("wrong delivery %+v", got)
		}
	}
	request(t, a.URL, e.tokens[1], "PUT", "/v1/conversations/"+c.ID+"/read", map[string]string{"sequence": "8"}, 200, nil)
	got := next(t, receiver, "read")
	if got.ReadSequence != 8 || got.UserID != e.users[1] {
		t.Fatal(got)
	}
	// Duplicated or arbitrarily repeated hints must not duplicate durable events.
	for i := 0; i < 3; i++ {
		e.redis.Publish(context.Background(), cfg.Prefix+"events", c.ID)
	}
	select {
	case got := <-receiver.events:
		t.Fatalf("duplicate event: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestMissedHintAndReconnectRecovery(t *testing.T) {
	e := setup(t)
	_, n := node(t, e, e.config)
	c := conversation(t, e)
	receiver := connect(t, n.URL, e.tokens[1], map[string]string{c.ID: "0"})
	next(t, receiver, "read")
	next(t, receiver, "read")
	// Simulate an outbox publish while no subscriber received it. There is no
	// Redis hint for these rows; the active connection must repair periodically.
	if _, err := e.db.Exec(`CREATE FUNCTION lose_hint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.published_at=now(); RETURN NEW; END $$; CREATE TRIGGER lose_hint BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION lose_hint()`); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		request(t, n.URL, e.tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "missed"}, 201, nil)
		if got := next(t, receiver, "message"); got.Sequence != int64(i) {
			t.Fatal(got)
		}
	}
	request(t, n.URL, e.tokens[0], "PUT", "/v1/conversations/"+c.ID+"/read", map[string]string{"sequence": "3"}, 200, nil)
	if receipt := next(t, receiver, "read"); receipt.UserID != e.users[0] || receipt.ReadSequence != 3 {
		t.Fatal("missed read hint was not repaired", receipt)
	}
	receiver.conn.Close()
	for i := 0; i < 3; i++ {
		request(t, n.URL, e.tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "offline"}, 201, nil)
	}
	resumed := connect(t, n.URL, e.tokens[1], map[string]string{c.ID: "3"})
	for i := 4; i <= 6; i++ {
		if got := next(t, resumed, "message"); got.Sequence != int64(i) {
			t.Fatal(got)
		}
	}
	var history []storage.Message
	request(t, n.URL, e.tokens[1], "GET", "/v1/conversations/"+c.ID+"/messages?after=3", nil, 200, &history)
	if len(history) != 3 || history[0].Sequence != 4 {
		t.Fatal(history)
	}
}

func TestMultiDevicePresenceAndLeaseExpiry(t *testing.T) {
	e := setup(t)
	a, na := node(t, e, e.config)
	_, nb := node(t, e, e.config)
	c := conversation(t, e)
	one := connect(t, na.URL, e.tokens[0], map[string]string{c.ID: "0"})
	two := connect(t, nb.URL, e.tokens[0], map[string]string{c.ID: "0"})
	next(t, one, "read")
	next(t, two, "read")
	online := func() bool {
		var p []struct {
			Online   bool       `json:"online"`
			LastSeen *time.Time `json:"last_seen"`
		}
		request(t, nb.URL, e.tokens[1], "GET", "/v1/presence?user_ids="+e.users[0], nil, 200, &p)
		return len(p) == 1 && p[0].Online && p[0].LastSeen != nil
	}
	if !online() {
		t.Fatal("devices not online")
	}
	one.conn.Close()
	time.Sleep(e.config.LeaseTTL + 100*time.Millisecond)
	if !online() {
		t.Fatal("disconnecting one device hid another")
	}
	two.conn.Close()
	eventually(t, time.Second, func() bool { return !online() })
	// An unremoved lease models a process crash. Redis time, not a local clock,
	// expires it while keeping the observed last heartbeat.
	if err := a.leases.Touch(context.Background(), e.users[0], "crashed-connection"); err != nil {
		t.Fatal(err)
	}
	if !online() {
		t.Fatal("lease missing")
	}
	eventually(t, 2*time.Second, func() bool { return !online() })
	var p []map[string]any
	request(t, nb.URL, e.tokens[1], "GET", "/v1/presence?user_ids="+e.users[0], nil, 200, &p)
	if p[0]["last_seen"] == nil {
		t.Fatal("last_seen lost")
	}
}

func TestBoundedQueueAndGracefulShutdown(t *testing.T) {
	// Deterministically exercise the queue boundary without relying on OS socket
	// buffers to fill at a particular speed.
	ctx, cancel := context.WithCancel(context.Background())
	left, right := netPipeWebSocket(t)
	defer right.Close()
	c := &session{server: &Server{}, conn: left, ctx: ctx, cancel: cancel, outbound: make(chan any, 1)}
	if !c.enqueue("one") || c.enqueue("two") || ctx.Err() == nil {
		t.Fatal("full queue did not disconnect")
	}
	if c.server.diagnostics.queueFull.Load() != 1 {
		t.Fatal("slow-consumer closure was not counted")
	}
	e := setup(t)
	cfg := e.config
	cfg.LogDiagnostics = true
	app, n := node(t, e, cfg)
	conv := conversation(t, e)
	live := connect(t, n.URL, e.tokens[0], map[string]string{conv.ID: "0"})
	next(t, live, "read")
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := app.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { app.mu.Lock(); defer app.mu.Unlock(); return app.slots == 0 && len(app.sessions) == 0 })
	request(t, n.URL, e.tokens[0], "GET", "/health/ready", nil, 503, nil)
}

func TestDependencyFailuresPreserveDurability(t *testing.T) {
	e := setup(t)
	c := conversation(t, e)
	// A separate Redis client is deliberately closed. The database remains healthy.
	brokenRedis := redis.NewClient(e.redis.Options())
	brokenRedis.Close()
	app, err := New(e.store, brokenRedis, e.config)
	if err != nil {
		t.Fatal(err)
	}
	n := httptest.NewServer(app)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := app.Close(ctx); err != nil {
			t.Error(err)
		}
		n.Close()
	})
	path := "/v1/conversations/" + c.ID + "/messages"
	request(t, n.URL, e.tokens[0], "POST", path, map[string]string{"client_message_id": uuid.NewString(), "body": "Redis unavailable"}, 201, nil)
	request(t, n.URL, e.tokens[1], "GET", path+"?after=0", nil, 200, nil)
	request(t, n.URL, e.tokens[1], "GET", "/v1/presence?user_ids="+e.users[0], nil, 503, nil)
	request(t, n.URL, e.tokens[0], "GET", "/health/ready", nil, 503, nil)
	var pending int
	if err = e.db.QueryRow("SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&pending); err != nil || pending != 1 {
		t.Fatal("Redis failure lost pending outbox", pending, err)
	}
	// The same node's future worker can retry after Redis recovers.
	// Stop the failed publisher before claiming; SKIP LOCKED may skip its row.
	shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
	if err := app.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	stop()
	if ok, err := e.store.PublishNext(context.Background(), func(ctx context.Context, id string) error {
		return e.redis.Publish(ctx, e.config.Prefix+"events", id).Err()
	}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Rejecting outbox insertion must fail the HTTP send, never acknowledge it.
	if _, err = e.db.Exec(`CREATE FUNCTION fail_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; CREATE TRIGGER fail_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_event()`); err != nil {
		t.Fatal(err)
	}
	request(t, n.URL, e.tokens[0], "POST", path, map[string]string{"client_message_id": uuid.NewString(), "body": "must roll back"}, 503, nil)
	var head int
	if err = e.db.QueryRow("SELECT last_sequence FROM conversations WHERE id=$1", c.ID).Scan(&head); err != nil || head != 1 {
		t.Fatal("failed send changed head", head, err)
	}
	// PostgreSQL unavailable prevents acknowledgement even with healthy Redis.
	closedDB, err := sql.Open("pgx", os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	closedDB.Close()
	unavailable, err := New(storage.New(closedDB), e.redis, e.config)
	if err != nil {
		t.Fatal(err)
	}
	other := httptest.NewServer(unavailable)
	defer other.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := unavailable.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	request(t, other.URL, e.tokens[0], "POST", path, map[string]string{"client_message_id": uuid.NewString(), "body": "no database"}, 503, nil)
}

func TestWebSocketCapacityAndHandshakeLimit(t *testing.T) {
	e := setup(t)
	cfg := e.config
	cfg.MaxConnections = 1
	app, n := node(t, e, cfg)
	c := conversation(t, e)
	live := connect(t, n.URL, e.tokens[0], map[string]string{c.ID: "0"})
	next(t, live, "read")
	request(t, n.URL, e.tokens[0], "GET", "/v1/ws", nil, 503, nil)
	live.conn.Close()
	eventually(t, time.Second, func() bool { app.mu.Lock(); defer app.mu.Unlock(); return app.slots == 0 })
	cfg.HandshakesPerSecond = 1
	_, limited := node(t, e, cfg)
	request(t, limited.URL, "", "GET", "/v1/ws", nil, 401, nil)
	request(t, limited.URL, "", "GET", "/v1/ws", nil, 429, nil)
}

func TestRecoveryClientSurvivesNodeShutdown(t *testing.T) {
	e := setup(t)
	a, na := node(t, e, e.config)
	_, nb := node(t, e, e.config)
	c := conversation(t, e)
	first, _ := url.Parse(na.URL)
	second, _ := url.Parse(nb.URL)
	proxyA, proxyB := httputil.NewSingleHostReverseProxy(first), httputil.NewSingleHostReverseProxy(second)
	var alternate atomic.Bool
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if alternate.Load() {
			proxyB.ServeHTTP(w, r)
		} else {
			proxyA.ServeHTTP(w, r)
		}
	}))
	defer front.Close()
	received := make(chan int64, 20)
	consumer := client.Client{URL: front.URL, Token: e.tokens[1], OnMessage: func(m storage.Message) error { received <- m.Sequence; return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("recovery client did not stop")
		}
	})
	for i := 1; i <= 5; i++ {
		if i == 2 {
			alternate.Store(true)
			shutdown, stop := context.WithTimeout(context.Background(), time.Second)
			err := a.Close(shutdown)
			stop()
			if err != nil {
				t.Fatal(err)
			}
		}
		request(t, nb.URL, e.tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "recover after node shutdown"}, 201, nil)
		select {
		case sequence := <-received:
			if sequence != int64(i) {
				t.Fatalf("duplicate or gap: got %d want %d", sequence, i)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("acknowledged message was not recovered")
		}
	}
	select {
	case sequence := <-received:
		t.Fatalf("duplicate delivery %d", sequence)
	case <-time.After(150 * time.Millisecond):
	}
}

func netPipeWebSocket(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := websocket.Upgrader{}
		c, e := u.Upgrade(w, r, nil)
		if e == nil {
			accepted <- c
		}
	}))
	defer srv.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	return <-accepted, client
}
