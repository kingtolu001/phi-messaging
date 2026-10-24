package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func fixture(t *testing.T) (*Store, []string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatal("TEST_DATABASE_URL is required")
		}
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := "test_" + uuid.New().String()
	schema = "\"" + schema + "\""
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	// Each test has its own schema, including concurrent test runs.
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(32)
	t.Cleanup(func() {
		db.Close()
		if _, e := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); e != nil {
			t.Error(e)
		}
	})
	_, file, _, _ := runtime.Caller(0)
	migration, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../../migrations/001_initial.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(migration)); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 101)
	for i := range ids {
		ids[i] = uuid.NewString()
		if _, err = db.Exec("INSERT INTO users(id,display_name) VALUES($1,$2)", ids[i], fmt.Sprint("user", i)); err != nil {
			t.Fatal(err)
		}
	}
	return New(db), ids
}
func concurrent(t *testing.T, n int, fn func(int) error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; errs <- fn(i) }(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}
func create(t *testing.T, s *Store, ids []string, kind string) Conversation {
	t.Helper()
	c, err := s.CreateConversation(context.Background(), ids[0], kind, ids)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func send(t *testing.T, s *Store, actor, id string) Message {
	t.Helper()
	m, created, err := s.Send(context.Background(), actor, id, uuid.NewString(), "hello 🌍")
	if err != nil || !created {
		t.Fatalf("send: created=%v err=%v", created, err)
	}
	return m
}
func count(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConcurrentDirectCreation(t *testing.T) {
	s, u := fixture(t)
	ctx := context.Background()
	var winner sync.Map
	concurrent(t, 24, func(i int) error {
		members := []string{u[0], u[1]}
		if i%2 == 0 {
			members[0], members[1] = members[1], members[0]
		}
		c, e := s.CreateConversation(ctx, u[i%2], "direct", members)
		if e == nil {
			winner.Store(c.ID, true)
		}
		return e
	})
	n := 0
	winner.Range(func(_, _ any) bool { n++; return true })
	if n != 1 || count(t, s, "conversations") != 1 || count(t, s, "members") != 2 {
		t.Fatal("direct pair was not canonical and atomic")
	}
}
func TestConcurrentIdenticalSends(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "direct")
	key := uuid.NewString()
	var created atomic.Int32
	concurrent(t, 32, func(int) error {
		m, new, e := s.Send(context.Background(), u[0], c.ID, key, "same")
		if new {
			created.Add(1)
		}
		if e == nil && (m.Sequence != 1 || m.Body != "same") {
			return fmt.Errorf("wrong retry: %+v", m)
		}
		return e
	})
	if created.Load() != 1 || count(t, s, "messages") != 1 || count(t, s, "outbox") != 1 {
		t.Fatal("retry produced duplicate durable writes")
	}
	if _, _, err := s.Send(context.Background(), u[0], c.ID, key, "different"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
}
func TestConcurrentCrossConversationKey(t *testing.T) {
	s, u := fixture(t)
	a := create(t, s, u[:2], "direct")
	b := create(t, s, []string{u[0], u[2]}, "direct")
	key := uuid.NewString()
	var wins, conflicts atomic.Int32
	concurrent(t, 24, func(i int) error {
		id := a.ID
		if i%2 == 1 {
			id = b.ID
		}
		_, created, e := s.Send(context.Background(), u[0], id, key, "same")
		if created {
			wins.Add(1)
		}
		if errors.Is(e, ErrConflict) {
			conflicts.Add(1)
			return nil
		}
		return e
	})
	if wins.Load() != 1 || conflicts.Load() != 12 || count(t, s, "messages") != 1 || count(t, s, "outbox") != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins.Load(), conflicts.Load())
	}
	var total int
	if err := s.db.QueryRow("SELECT sum(last_sequence) FROM conversations").Scan(&total); err != nil || total != 1 {
		t.Fatalf("rolled-back head: %d %v", total, err)
	}
}
func TestConcurrentOrderingAndReads(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "group")
	ctx := context.Background()
	concurrent(t, 64, func(i int) error { _, _, e := s.Send(ctx, u[i%2], c.ID, uuid.NewString(), "ordered"); return e })
	zero := int64(0)
	messages, err := s.History(ctx, u[0], c.ID, HistoryQuery{After: &zero, Limit: 100})
	if err != nil || len(messages) != 64 {
		t.Fatalf("history len=%d err=%v", len(messages), err)
	}
	for i, m := range messages {
		if m.Sequence != int64(i+1) {
			t.Fatal("sequence gap")
		}
	}
	list, err := s.ListConversations(ctx, u[0], "", 50)
	if err != nil || len(list) != 1 || list[0].UnreadCount != 32 || list[0].LastSequence != 64 {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	concurrent(t, 64, func(i int) error { _, e := s.MarkRead(ctx, u[0], c.ID, int64(i+1)); return e })
	receipts, err := s.Receipts(ctx, u[1], c.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range receipts {
		if r.UserID == u[0] && r.ReadSequence != 64 {
			t.Fatal("receipt regressed")
		}
	}
	r, err := s.MarkRead(ctx, u[0], c.ID, 1)
	if err != nil || r.ReadSequence != 64 {
		t.Fatal("stale receipt regressed")
	}
	if _, err = s.MarkRead(ctx, u[0], c.ID, 65); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	list, err = s.ListConversations(ctx, u[0], "", 0)
	if err != nil || list[0].UnreadCount != 0 {
		t.Fatal("read did not clear unread")
	}
	var bad int
	if err = s.db.QueryRow(`SELECT count(*) FROM outbox WHERE event_type='read' AND (payload->>'read_sequence')::bigint>64`).Scan(&bad); err != nil || bad != 0 {
		t.Fatal("invalid receipt event")
	}
}
func TestHistoryAndAuthorization(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "direct")
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		send(t, s, u[0], c.ID)
	}
	page, err := s.History(ctx, u[1], c.ID, HistoryQuery{Limit: 2})
	if err != nil || len(page) != 2 || page[0].Sequence != 5 {
		t.Fatal(page, err)
	}
	send(t, s, u[0], c.ID)
	before := page[1].Sequence
	page, err = s.History(ctx, u[1], c.ID, HistoryQuery{Before: &before, Limit: 2})
	if err != nil || len(page) != 2 || page[0].Sequence != 3 || page[1].Sequence != 2 {
		t.Fatal(page, err)
	}
	after := int64(4)
	page, err = s.History(ctx, u[1], c.ID, HistoryQuery{After: &after})
	if err != nil || len(page) != 2 || page[0].Sequence != 5 || page[1].Sequence != 6 {
		t.Fatal(page, err)
	}
	for _, id := range []string{c.ID, uuid.NewString()} {
		if _, e := s.History(ctx, u[2], id, HistoryQuery{}); !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
		if _, _, e := s.Send(ctx, u[2], id, uuid.NewString(), "hidden"); !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
		if _, e := s.MarkRead(ctx, u[2], id, 0); !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
		if _, e := s.Receipts(ctx, u[2], id); !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
	}
	list, err := s.ListConversations(ctx, u[2], "", 0)
	if err != nil || len(list) != 0 {
		t.Fatal(list, err)
	}
	if count(t, s, "messages") != 6 || count(t, s, "outbox") != 6 {
		t.Fatal("unauthorized writes")
	}
}
func TestValidationAndConversationPagination(t *testing.T) {
	s, u := fixture(t)
	ctx := context.Background()
	for _, members := range [][]string{u[:1], u, u[:0], {u[0], u[0]}, {u[1], u[2]}, {u[0], uuid.NewString()}} {
		if _, e := s.CreateConversation(ctx, u[0], "group", members); !errors.Is(e, ErrInvalid) {
			t.Fatalf("members=%d err=%v", len(members), e)
		}
	}
	create(t, s, u[:100], "group")
	c := create(t, s, u[:2], "direct")
	create(t, s, u[:3], "group")
	first, e := s.ListConversations(ctx, u[0], "", 2)
	if e != nil || len(first) != 2 {
		t.Fatal(first, e)
	}
	last, e := s.ListConversations(ctx, u[0], first[1].ID, 2)
	if e != nil || len(last) != 1 || last[0].ID <= first[1].ID {
		t.Fatal(last, e)
	}
	for _, body := range []string{"", string([]byte{0xff}), "\x00", strings.Repeat("🌍", 4001)} {
		if _, _, e = s.Send(ctx, u[0], c.ID, uuid.NewString(), body); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	if _, _, e = s.Send(ctx, u[0], c.ID, uuid.NewString(), strings.Repeat("🌍", 4000)); e != nil {
		t.Fatal("4000 Unicode characters must be accepted:", e)
	}
	if _, e = s.History(ctx, u[0], c.ID, HistoryQuery{Limit: 101}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	zero := int64(0)
	if _, e = s.History(ctx, u[0], c.ID, HistoryQuery{Before: &zero, After: &zero}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}
func TestOutboxFailureRollsBackSendAndRead(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "direct")
	send(t, s, u[0], c.ID)
	ctx := context.Background()
	if _, e := s.db.Exec(`CREATE FUNCTION reject_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$; CREATE TRIGGER reject_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_event()`); e != nil {
		t.Fatal(e)
	}
	if _, _, e := s.Send(ctx, u[0], c.ID, uuid.NewString(), "rollback"); e == nil {
		t.Fatal("expected outbox error")
	}
	if _, e := s.MarkRead(ctx, u[1], c.ID, 1); e == nil {
		t.Fatal("expected outbox error")
	}
	if count(t, s, "messages") != 1 || count(t, s, "outbox") != 1 {
		t.Fatal("partial send committed")
	}
	receipts, e := s.Receipts(ctx, u[0], c.ID)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range receipts {
		if r.ReadSequence != 0 {
			t.Fatal("partial read committed")
		}
	}
	if _, e = s.db.Exec("DROP TRIGGER reject_event ON outbox"); e != nil {
		t.Fatal(e)
	}
	if m := send(t, s, u[0], c.ID); m.Sequence != 2 {
		t.Fatal("failed transaction consumed sequence")
	}
}
