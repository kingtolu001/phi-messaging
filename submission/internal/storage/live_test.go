package storage

import (
	"context"
	"testing"
)

func TestDeliverySnapshotRespectsCapturedHead(t *testing.T) {
	s, u := fixture(t)
	c := create(t, s, u[:2], "direct")
	send(t, s, u[0], c.ID)
	ctx := context.Background()
	state, err := s.DeliveryState(ctx, c.ID)
	if err != nil || state.Head != 1 || len(state.Receipts) != 2 {
		t.Fatal(state, err)
	}
	send(t, s, u[0], c.ID)
	page, err := s.DeliveryMessages(ctx, c.ID, 0, state.Head, 128)
	if err != nil || len(page) != 1 || page[0].Sequence != 1 {
		t.Fatal("new write crossed captured head", page, err)
	}
}
