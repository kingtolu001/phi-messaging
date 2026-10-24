package server

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"phi-messaging/internal/realtime"
	"phi-messaging/internal/storage"
)

type Config struct {
	InstanceID          string
	Tokens              map[string]string
	Origins             []string
	Prefix              string
	PollInterval        time.Duration
	LeaseTTL            time.Duration
	WriteTimeout        time.Duration
	QueueSize           int
	MaxConnections      int
	HandshakesPerSecond int
}
type Server struct {
	store      *storage.Store
	redis      *redis.Client
	config     Config
	leases     realtime.Leases
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	sessions   map[*session]struct{}
	slots      int
	closing    bool
	rateStart  time.Time
	attempts   int
	subscribed atomic.Bool
	mux        *http.ServeMux
	delivery   *deliveryHub
}

func New(store *storage.Store, r *redis.Client, c Config) (*Server, error) {
	if len(c.Tokens) == 0 {
		return nil, errors.New("mock bearer identities are required")
	}
	tokens := make(map[string]string, len(c.Tokens))
	for token, user := range c.Tokens {
		u, err := uuid.Parse(user)
		if err != nil || u.String() != user || len(token) < 16 {
			return nil, errors.New("invalid mock identity configuration")
		}
		tokens[token] = user
	}
	c.Tokens = tokens
	c.Origins = append([]string(nil), c.Origins...)
	if c.Prefix == "" {
		c.Prefix = "phi:"
	}
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.QueueSize == 0 {
		c.QueueSize = 128
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 1000
	}
	if c.HandshakesPerSecond == 0 {
		c.HandshakesPerSecond = 100
	}
	if c.PollInterval <= 0 || c.LeaseTTL < 300*time.Millisecond || c.WriteTimeout <= 0 || c.QueueSize < 1 || c.MaxConnections < 1 || c.HandshakesPerSecond < 1 {
		return nil, errors.New("invalid server limits")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{store: store, redis: r, config: c, leases: realtime.Leases{Redis: r, Prefix: c.Prefix, TTL: c.LeaseTTL}, ctx: ctx, cancel: cancel, sessions: make(map[*session]struct{}), mux: http.NewServeMux()}
	s.delivery = newDeliveryHub(s)
	s.routes()
	s.wg.Add(2)
	go s.listen()
	go s.publish()
	return s, nil
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }
func (s *Server) wake() {
	s.delivery.wakeAll()
}
func (s *Server) wait(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (s *Server) listen() {
	defer s.wg.Done()
	for s.ctx.Err() == nil {
		sub := s.redis.Subscribe(s.ctx, s.config.Prefix+"events")
		// ReceiveMessage can be blocked in a socket read when the context ends.
		// Closing the subscription explicitly unblocks it during shutdown.
		stopClose := context.AfterFunc(s.ctx, func() { sub.Close() })
		ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		_, err := sub.Receive(ctx)
		cancel()
		if err == nil {
			s.subscribed.Store(true)
			s.wake()
			for {
				var hint *redis.Message
				hint, err = sub.ReceiveMessage(s.ctx)
				if err != nil {
					break
				}
				s.delivery.wake(hint.Payload)
			}
		}
		s.subscribed.Store(false)
		sub.Close()
		stopClose()
		if !s.wait(time.Second) {
			return
		}
	}
}
func (s *Server) publish() {
	defer s.wg.Done()
	backoff := 100 * time.Millisecond
	for s.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		published, err := s.store.PublishNext(ctx, func(ctx context.Context, id string) error {
			return s.redis.Publish(ctx, s.config.Prefix+"events", id).Err()
		})
		cancel()
		if err != nil {
			if !s.wait(min(backoff+time.Duration(rand.Int64N(int64(backoff/2))), 5*time.Second)) {
				return
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		if !published && !s.wait(100*time.Millisecond) {
			return
		}
	}
}

// Close also closes hijacked WebSockets, which http.Server.Shutdown does not do.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.cancel()
	for c := range s.sessions {
		c.stop()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
