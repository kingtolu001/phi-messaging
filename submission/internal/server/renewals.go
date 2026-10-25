package server

import (
	"context"
	"time"

	"phi-messaging/internal/realtime"
)

type renewalRequest struct {
	ctx   context.Context
	lease realtime.Renewal
	done  chan error
}

// One bounded queue and worker per node replace one Redis round trip per pong.
// Callers retain their original deadline unless their own renewal succeeds.
func (s *Server) renewLease(ctx context.Context, user, connection string) error {
	request := renewalRequest{ctx: ctx, lease: realtime.Renewal{User: user, Connection: connection}, done: make(chan error, 1)}
	select {
	case s.renewals <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) renewLeases() {
	defer s.wg.Done()
	for {
		var first renewalRequest
		select {
		case <-s.ctx.Done():
			return
		case first = <-s.renewals:
		}
		batch := []renewalRequest{first}
		timer := time.NewTimer(10 * time.Millisecond)
	collect:
		for len(batch) < 128 {
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case request := <-s.renewals:
				batch = append(batch, request)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		active := batch[:0]
		for _, request := range batch {
			if err := request.ctx.Err(); err != nil {
				request.done <- err
			} else {
				active = append(active, request)
			}
		}
		batch = active
		if len(batch) == 0 {
			continue
		}
		leases := make([]realtime.Renewal, len(batch))
		for i, request := range batch {
			leases[i] = request.lease
		}
		ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		results := s.leases.TouchBatch(ctx, leases)
		cancel()
		for i, request := range batch {
			request.done <- results[i]
		}
	}
}
