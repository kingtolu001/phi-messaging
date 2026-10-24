package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrInvalid  = errors.New("invalid input")
	ErrNotFound = errors.New("conversation not found")
	ErrConflict = errors.New("client message ID conflicts with an existing message")
)

// Store accepts an authenticated user ID from its caller, never from request bodies.
type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

type Conversation struct {
	ID           string    `json:"id"`
	Kind         string    `json:"kind"`
	LastSequence int64     `json:"last_sequence,string"`
	CreatedAt    time.Time `json:"created_at"`
	UnreadCount  int64     `json:"unread_count,string"`
}
type Message struct {
	ConversationID  string    `json:"conversation_id"`
	Sequence        int64     `json:"sequence,string"`
	SenderID        string    `json:"sender_id"`
	ClientMessageID string    `json:"client_message_id"`
	Body            string    `json:"body"`
	CreatedAt       time.Time `json:"created_at"`
}
type Receipt struct {
	UserID       string `json:"user_id"`
	ReadSequence int64  `json:"read_sequence,string"`
}

func validID(s string) bool { u, e := uuid.Parse(s); return e == nil && u.String() == s }
func limitValue(n int) (int, error) {
	if n == 0 {
		return 50, nil
	}
	if n < 1 || n > 100 {
		return 0, ErrInvalid
	}
	return n, nil
}
func (s *Store) CreateConversation(ctx context.Context, actor, kind string, members []string) (Conversation, error) {
	var c Conversation
	if !validID(actor) || (kind != "direct" && kind != "group") {
		return c, ErrInvalid
	}
	ids := append([]string(nil), members...)
	sort.Strings(ids)
	found := false
	for i, id := range ids {
		if !validID(id) || (i > 0 && id == ids[i-1]) {
			return c, ErrInvalid
		}
		found = found || id == actor
	}
	if !found || len(ids) < 2 || len(ids) > 100 || (kind == "direct" && len(ids) != 2) {
		return c, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return c, err
	}
	defer tx.Rollback()
	// A shared lock prevents deletion of validated identities before member insertion.
	for _, id := range ids {
		var exists string
		if err = tx.QueryRowContext(ctx, "SELECT id FROM users WHERE id=$1 FOR KEY SHARE", id).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			return c, ErrInvalid
		} else if err != nil {
			return c, err
		}
	}
	var key any
	if kind == "direct" {
		key = strings.Join(ids, ":")
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO conversations(id,kind,direct_key) VALUES($1,$2,$3)
 ON CONFLICT(direct_key) DO UPDATE SET direct_key=EXCLUDED.direct_key
 RETURNING id,kind,last_sequence,created_at`, uuid.NewString(), kind, key).Scan(&c.ID, &c.Kind, &c.LastSequence, &c.CreatedAt)
	if err != nil {
		return c, err
	}
	for _, id := range ids {
		if _, err = tx.ExecContext(ctx, "INSERT INTO members(conversation_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", c.ID, id); err != nil {
			return c, err
		}
	}
	err = tx.Commit()
	return c, err
}

func lockConversation(ctx context.Context, tx *sql.Tx, actor, id string) (int64, error) {
	var head int64
	err := tx.QueryRowContext(ctx, `SELECT c.last_sequence FROM conversations c WHERE c.id=$1
 AND EXISTS(SELECT 1 FROM members m WHERE m.conversation_id=c.id AND m.user_id=$2) FOR UPDATE`, id, actor).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return head, err
}

const messageColumns = "conversation_id,sequence,sender_id,client_message_id,body,created_at"

type scanner interface{ Scan(...any) error }

func scanMessage(r scanner) (Message, error) {
	var m Message
	err := r.Scan(&m.ConversationID, &m.Sequence, &m.SenderID, &m.ClientMessageID, &m.Body, &m.CreatedAt)
	return m, err
}
func sameMessage(m Message, conversation, body string) (Message, bool, error) {
	if m.ConversationID != conversation || m.Body != body {
		return Message{}, false, ErrConflict
	}
	return m, false, nil
}
func outbox(ctx context.Context, tx *sql.Tx, id, kind string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO outbox(conversation_id,event_type,payload) VALUES($1,$2,$3::jsonb)", id, kind, string(b))
	return err
}

// Send returns created=false for an identical committed retry. No success precedes commit.
func (s *Store) Send(ctx context.Context, actor, conversation, clientID, body string) (Message, bool, error) {
	if !validID(actor) || !validID(conversation) || !validID(clientID) || !utf8.ValidString(body) || utf8.RuneCountInString(body) < 1 || utf8.RuneCountInString(body) > 4000 || strings.ContainsRune(body, 0) {
		return Message{}, false, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Message{}, false, err
	}
	defer tx.Rollback()
	head, err := lockConversation(ctx, tx, actor, conversation)
	if err != nil {
		return Message{}, false, err
	}
	m, err := scanMessage(tx.QueryRowContext(ctx, "SELECT "+messageColumns+" FROM messages WHERE sender_id=$1 AND client_message_id=$2", actor, clientID))
	if err == nil {
		return sameMessage(m, conversation, body)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Message{}, false, err
	}
	if head == int64(1<<63-1) {
		return Message{}, false, ErrInvalid
	}
	m, err = scanMessage(tx.QueryRowContext(ctx, "INSERT INTO messages(conversation_id,sequence,sender_id,client_message_id,body) VALUES($1,$2,$3,$4,$5) RETURNING "+messageColumns, conversation, head+1, actor, clientID, body))
	if err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "23505" && pg.ConstraintName == "messages_sender_id_client_message_id_key" {
			tx.Rollback()
			winner, e := scanMessage(s.db.QueryRowContext(ctx, "SELECT "+messageColumns+" FROM messages WHERE sender_id=$1 AND client_message_id=$2", actor, clientID))
			if e != nil {
				return Message{}, false, e
			}
			return sameMessage(winner, conversation, body)
		}
		return Message{}, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE conversations SET last_sequence=$2 WHERE id=$1", conversation, m.Sequence); err != nil {
		return Message{}, false, err
	}
	if err = outbox(ctx, tx, conversation, "message", m); err != nil {
		return Message{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Message{}, false, err
	}
	return m, true, nil
}

func (s *Store) ListConversations(ctx context.Context, actor, after string, limit int) ([]Conversation, error) {
	n, err := limitValue(limit)
	if err != nil || !validID(actor) || (after != "" && !validID(after)) {
		return nil, ErrInvalid
	}
	var cursor any
	if after != "" {
		cursor = after
	}
	rows, err := s.db.QueryContext(ctx, `SELECT c.id,c.kind,c.last_sequence,c.created_at,
 (SELECT count(*) FROM messages x WHERE x.conversation_id=c.id AND x.sequence>m.read_sequence AND x.sender_id<>$1)
 FROM conversations c JOIN members m ON m.conversation_id=c.id AND m.user_id=$1
 WHERE ($2::uuid IS NULL OR c.id>$2) ORDER BY c.id LIMIT $3`, actor, cursor, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Conversation{}
	for rows.Next() {
		var c Conversation
		if err = rows.Scan(&c.ID, &c.Kind, &c.LastSequence, &c.CreatedAt, &c.UnreadCount); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

type HistoryQuery struct {
	Before, After *int64
	Limit         int
}

func (s *Store) History(ctx context.Context, actor, id string, q HistoryQuery) ([]Message, error) {
	if q.After != nil && q.Limit == 0 {
		q.Limit = 100
	}
	n, err := limitValue(q.Limit)
	if err != nil || !validID(actor) || !validID(id) || (q.Before != nil && q.After != nil) || (q.Before != nil && *q.Before < 0) || (q.After != nil && *q.After < 0) {
		return nil, ErrInvalid
	}
	// One repeatable snapshot covers both authorization/head capture and the page.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var head int64
	err = tx.QueryRowContext(ctx, `SELECT c.last_sequence FROM conversations c JOIN members m ON m.conversation_id=c.id WHERE c.id=$1 AND m.user_id=$2`, id, actor).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	query := "SELECT " + messageColumns + " FROM messages WHERE conversation_id=$1 AND sequence<=$2"
	args := []any{id, head}
	if q.After != nil {
		query += " AND sequence>$3 ORDER BY sequence ASC"
		args = append(args, *q.After)
	} else if q.Before != nil {
		query += " AND sequence<$3 ORDER BY sequence DESC"
		args = append(args, *q.Before)
	} else {
		query += " ORDER BY sequence DESC"
	}
	query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
	args = append(args, n)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	out := []Message{}
	for rows.Next() {
		m, e := scanMessage(rows)
		if e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) MarkRead(ctx context.Context, actor, id string, sequence int64) (Receipt, error) {
	r := Receipt{UserID: actor}
	if !validID(actor) || !validID(id) || sequence < 0 {
		return r, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	head, err := lockConversation(ctx, tx, actor, id)
	if err != nil {
		return r, err
	}
	if sequence > head {
		return r, ErrInvalid
	}
	var old int64
	err = tx.QueryRowContext(ctx, "SELECT read_sequence FROM members WHERE conversation_id=$1 AND user_id=$2", id, actor).Scan(&old)
	if err != nil {
		return r, err
	}
	r.ReadSequence = old
	if sequence > old {
		r.ReadSequence = sequence
		if _, err = tx.ExecContext(ctx, "UPDATE members SET read_sequence=$3 WHERE conversation_id=$1 AND user_id=$2", id, actor, sequence); err != nil {
			return r, err
		}
		if err = outbox(ctx, tx, id, "read", struct {
			ConversationID string `json:"conversation_id"`
			Receipt
		}{id, r}); err != nil {
			return r, err
		}
	}
	err = tx.Commit()
	return r, err
}
func (s *Store) Receipts(ctx context.Context, actor, id string) ([]Receipt, error) {
	if !validID(actor) || !validID(id) {
		return nil, ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT user_id,read_sequence FROM members WHERE conversation_id=$1 AND EXISTS(SELECT 1 FROM members WHERE conversation_id=$1 AND user_id=$2) ORDER BY user_id`, id, actor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Receipt{}
	for rows.Next() {
		var r Receipt
		if err = rows.Scan(&r.UserID, &r.ReadSequence); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}
