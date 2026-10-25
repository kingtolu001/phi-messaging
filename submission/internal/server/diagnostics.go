package server

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

type diagnostics struct {
	queueFull, readTimeout, readError, writeError      atomic.Uint64
	leaseError, feedError, historyError                atomic.Uint64
	leaseDeadline, leaseNetwork, leasePool, leaseOther atomic.Uint64
}

func (d *diagnostics) recordLeaseError(err error) {
	d.leaseError.Add(1)
	var network net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		d.leaseDeadline.Add(1)
	case strings.Contains(err.Error(), "connection pool timeout"):
		d.leasePool.Add(1)
	case errors.As(err, &network):
		d.leaseNetwork.Add(1)
	default:
		d.leaseOther.Add(1)
	}
}

// Optional aggregate counters contain no identities, tokens, URLs or message data.
func (s *Server) logDiagnostics() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			active := len(s.sessions)
			s.mu.Unlock()
			pool := s.store.PoolStats()
			cache := s.redis.PoolStats()
			log.Printf("lease_diagnostics instance=%s deadline=%d network=%d pool=%d other=%d redis_pool_timeouts=%d redis_connections=%d redis_idle=%d", s.config.InstanceID, s.diagnostics.leaseDeadline.Load(), s.diagnostics.leaseNetwork.Load(), s.diagnostics.leasePool.Load(), s.diagnostics.leaseOther.Load(), cache.Timeouts, cache.TotalConns, cache.IdleConns)
			log.Printf("diagnostics instance=%s active=%d queue_full=%d read_timeout=%d read_error=%d write_error=%d lease_error=%d feed_error=%d history_error=%d sql_open=%d sql_in_use=%d sql_waits=%d sql_wait_ms=%d", s.config.InstanceID, active, s.diagnostics.queueFull.Load(), s.diagnostics.readTimeout.Load(), s.diagnostics.readError.Load(), s.diagnostics.writeError.Load(), s.diagnostics.leaseError.Load(), s.diagnostics.feedError.Load(), s.diagnostics.historyError.Load(), pool.OpenConnections, pool.InUse, pool.WaitCount, pool.WaitDuration.Milliseconds())
		}
	}
}
