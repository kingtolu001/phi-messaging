package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"phi-messaging/internal/storage"
)

func TestCachedPageRequiresCompleteCoverage(t *testing.T) {
	snapshot := &deliverySnapshot{Head: 200, Messages: []storage.Message{}}
	for i := int64(73); i <= 200; i++ {
		snapshot.Messages = append(snapshot.Messages, storage.Message{Sequence: i})
	}
	if _, ok := cachedPage(snapshot, 0, 200, 100); ok {
		t.Fatal("old history incorrectly served from bounded tail")
	}
	if _, ok := cachedPage(snapshot, 190, 201, 100); ok {
		t.Fatal("stale cache hid newly committed message")
	}
	page, ok := cachedPage(snapshot, 180, 200, 100)
	if !ok || len(page) != 20 || page[0].Sequence != 181 {
		t.Fatal(page, ok)
	}
	max := int64(1<<63 - 1)
	snapshot = &deliverySnapshot{Head: max, Messages: []storage.Message{{Sequence: max}}}
	page, ok = cachedPage(snapshot, max-1, max, 100)
	if !ok || len(page) != 1 {
		t.Fatal("bigint boundary", page, ok)
	}
}

func TestFeedAdmissionAndCleanupAreBounded(t *testing.T) {
	h := &deliveryHub{feeds: map[string]*conversationFeed{}, jobs: make(chan *conversationFeed, maxDeliveryFeeds)}
	c := &session{cursors: map[string]int64{}, wake: make(chan struct{}, 1)}
	for i := 0; i < maxDeliveryFeeds; i++ {
		c.cursors[uuid.NewString()] = 0
	}
	if !h.attach(c) || len(h.feeds) != maxDeliveryFeeds {
		t.Fatal("admission")
	}
	extra := &session{cursors: map[string]int64{uuid.NewString(): 0}, wake: make(chan struct{}, 1)}
	if h.attach(extra) || len(h.feeds) != maxDeliveryFeeds {
		t.Fatal("overcommitted feeds")
	}
	h.detach(c)
	if len(h.feeds) != 0 {
		t.Fatal("retained feeds after last subscriber")
	}
}

func TestSharedFeedTargetsHintsAndKeepsRemainingDevice(t *testing.T) {
	e := setup(t)
	cfg := e.config
	cfg.PollInterval = time.Hour
	app, node := node(t, e, cfg)
	c := conversation(t, e)
	other, err := e.store.CreateConversation(context.Background(), e.users[0], "group", e.users[:2])
	if err != nil {
		t.Fatal(err)
	}
	one := connect(t, node.URL, e.tokens[1], map[string]string{c.ID: "0"})
	two := connect(t, node.URL, e.tokens[1], map[string]string{c.ID: "0"})
	unrelated := connect(t, node.URL, e.tokens[1], map[string]string{other.ID: "0"})
	for _, device := range []*socket{one, two, unrelated} {
		next(t, device, "read")
		next(t, device, "read")
	}
	before := app.delivery.snapshot(other.ID)
	one.conn.Close()
	request(t, node.URL, e.tokens[0], "POST", "/v1/conversations/"+c.ID+"/messages", map[string]string{"client_message_id": uuid.NewString(), "body": "remaining device"}, 201, nil)
	if got := next(t, two, "message"); got.Sequence != 1 {
		t.Fatal(got)
	}
	if app.delivery.snapshot(other.ID) != before {
		t.Fatal("unrelated hint refreshed another conversation")
	}
	two.conn.Close()
	unrelated.conn.Close()
	eventually(t, time.Second, func() bool {
		app.delivery.mu.Lock()
		defer app.delivery.mu.Unlock()
		return len(app.delivery.feeds) == 0
	})
}

func TestRecoveryCacheFallsBackAndChecksCurrentMembership(t *testing.T) {
	e := setup(t)
	cfg := e.config
	cfg.PollInterval = time.Hour
	app, node := node(t, e, cfg)
	c := conversation(t, e)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		if _, _, err := e.store.Send(ctx, e.users[0], c.ID, uuid.NewString(), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	conn := connect(t, node.URL, e.tokens[1], map[string]string{c.ID: "200"})
	next(t, conn, "read")
	next(t, conn, "read")
	snapshot := app.delivery.snapshot(c.ID)
	if len(snapshot.Messages) != deliveryWindow {
		t.Fatal("unbounded or missing tail", len(snapshot.Messages))
	}
	base := "/v1/conversations/" + c.ID + "/messages"
	var page []storage.Message
	request(t, node.URL, e.tokens[1], "GET", base+"?after=0&limit=50", nil, 200, &page)
	if len(page) != 50 || page[0].Sequence != 1 {
		t.Fatal("old history fallback", page)
	}
	request(t, node.URL, e.tokens[2], "GET", base+"?after=180", nil, 404, nil)
	if _, err := e.db.Exec(`CREATE FUNCTION lose_hint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.published_at=now(); RETURN NEW; END $$; CREATE TRIGGER lose_hint BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION lose_hint()`); err != nil {
		t.Fatal(err)
	}
	request(t, node.URL, e.tokens[0], "POST", base, map[string]string{"client_message_id": uuid.NewString(), "body": "fresh commit without hint"}, 201, nil)
	request(t, node.URL, e.tokens[1], "GET", base+"?after=200", nil, 200, &page)
	if len(page) != 1 || page[0].Sequence != 201 {
		t.Fatal("stale cache hid commit", page)
	}
	// A warm cache cannot bypass the public endpoint's current membership check.
	if _, err := e.db.Exec("DELETE FROM members WHERE conversation_id=$1 AND user_id=$2", c.ID, e.users[1]); err != nil {
		t.Fatal(err)
	}
	request(t, node.URL, e.tokens[1], "GET", base+"?after=180", nil, 404, nil)
	if err := e.db.Close(); err != nil {
		t.Fatal(err)
	}
	request(t, node.URL, e.tokens[0], "GET", base+"?after=180", nil, 503, nil)
}

func TestManyDevicesShareOnePeriodicReconciliation(t *testing.T) {
	e := setup(t)
	cfg := e.config
	cfg.PollInterval = 200 * time.Millisecond
	app, node := node(t, e, cfg)
	c := conversation(t, e)
	for i := 0; i < 20; i++ {
		device := connect(t, node.URL, e.tokens[1], map[string]string{c.ID: "0"})
		next(t, device, "read")
		next(t, device, "read")
	}
	initial := app.delivery.refreshes.Load()
	time.Sleep(650 * time.Millisecond)
	delta := app.delivery.refreshes.Load() - initial
	if delta < 2 || delta > 6 {
		t.Fatalf("periodic work scaled with devices: refreshes=%d", delta)
	}
	app.delivery.mu.Lock()
	feeds := len(app.delivery.feeds)
	app.delivery.mu.Unlock()
	if feeds != 1 {
		t.Fatal(feeds)
	}
}
