package storage

import (
	"context"
)

// DeliveryState is internal dispatcher input, not an authorization-free API.
// Membership is fixed; receipt rows also identify the authorized members.
type DeliveryState struct {
	Head     int64
	Receipts []Receipt
}

func (s *Store) DeliveryState(ctx context.Context, id string) (DeliveryState, error) {
	state := DeliveryState{Receipts: []Receipt{}}
	rows, err := s.db.QueryContext(ctx, `SELECT c.last_sequence,m.user_id,m.read_sequence FROM conversations c JOIN members m ON m.conversation_id=c.id WHERE c.id=$1 ORDER BY m.user_id`, id)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	for rows.Next() {
		var r Receipt
		if err = rows.Scan(&state.Head, &r.UserID, &r.ReadSequence); err != nil {
			return state, err
		}
		state.Receipts = append(state.Receipts, r)
	}
	if err = rows.Err(); err != nil {
		return state, err
	}
	if len(state.Receipts) == 0 {
		return state, ErrNotFound
	}
	return state, nil
}

// DeliveryMessages reads immutable rows up to a previously observed committed head.
// Only internal dispatchers call this; public history retains actor authorization.
func (s *Store) DeliveryMessages(ctx context.Context, id string, after, head int64, limit int) ([]Message, error) {
	if after < 0 || head < after || limit < 1 || limit > 256 {
		return nil, ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+messageColumns+" FROM messages WHERE conversation_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence LIMIT $4", id, after, head, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		m, e := scanMessage(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
