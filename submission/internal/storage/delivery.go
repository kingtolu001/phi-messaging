package storage

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) PoolStats() sql.DBStats { return s.db.Stats() }

func (s *Store) Head(ctx context.Context, actor, id string) (int64, error) {
	if !validID(actor) || !validID(id) {
		return 0, ErrInvalid
	}
	var head int64
	err := s.db.QueryRowContext(ctx, `SELECT c.last_sequence FROM conversations c JOIN members m ON m.conversation_id=c.id WHERE c.id=$1 AND m.user_id=$2`, id, actor).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return head, err
}

// Presence visibility is restricted to users sharing at least one conversation.
func (s *Store) CanSeePresence(ctx context.Context, actor string, ids []string) error {
	if !validID(actor) || len(ids) == 0 || len(ids) > 100 {
		return ErrInvalid
	}
	for _, id := range ids {
		if !validID(id) {
			return ErrInvalid
		}
		var allowed bool
		err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM members a JOIN members b ON a.conversation_id=b.conversation_id WHERE a.user_id=$1 AND b.user_id=$2)`, actor, id).Scan(&allowed)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrNotFound
		}
	}
	return nil
}

// PublishNext holds a row lock until publish and marking succeed. A crash after
// publish but before commit repeats the hint; consumers never trust hint ordering.
func (s *Store) PublishNext(ctx context.Context, publish func(context.Context, string) error) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var id int64
	var conversation string
	err = tx.QueryRowContext(ctx, `SELECT id,conversation_id FROM outbox WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id, &conversation)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = publish(ctx, conversation); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE outbox SET published_at=now() WHERE id=$1", id); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
