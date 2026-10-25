package exercise

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"phi-messaging/internal/client"
	"phi-messaging/internal/storage"
)

type LoadConfig struct {
	ReadyTimeout      time.Duration `json:"ready_timeout_ns"`
	UserOffset        int           `json:"user_offset"`
	ReadyFile         string        `json:"ready_file,omitempty"`
	StartBarrier      string        `json:"start_barrier,omitempty"`
	RateScope         string        `json:"send_rate_scope"`
	HTTPPool          string        `json:"device_http_pool"`
	URL               string        `json:"url"`
	Connections       int           `json:"connections"`
	Rate              int           `json:"logical_messages_per_second"`
	Warmup            time.Duration `json:"warmup_ns"`
	Duration          time.Duration `json:"measurement_ns"`
	Ramp              time.Duration `json:"connection_start_interval_ns"`
	Drain             time.Duration `json:"drain_ns"`
	KillAfter         time.Duration `json:"kill_after_ns,omitempty"`
	DistinctUsers     bool          `json:"distinct_user_per_recipient"`
	RecoveryInterval  time.Duration `json:"recovery_interval_ns"`
	SharedHTTP        bool          `json:"shared_generator_http_pool"`
	FirstWebSocketURL string        `json:"first_websocket_url,omitempty"`
	SurvivorURL       string        `json:"survivor_url,omitempty"`
}
type DeliveryRecord struct {
	SenderID     string `json:"sender_id"`
	ID           string `json:"client_message_id"`
	Measured     bool   `json:"measured"`
	Ack          bool   `json:"acknowledged"`
	Sequence     int64  `json:"sequence"`
	Delivered    int    `json:"delivered_devices"`
	Conversation string `json:"conversation_id,omitempty"`
	Expected     int    `json:"expected_devices,omitempty"`
	start        time.Time
	seen         map[int]bool
}
type LoadReport struct {
	ClockDiscontinuity             bool              `json:"clock_discontinuity_detected"`
	WallMeasuredElapsed            float64           `json:"wall_measurement_and_drain_seconds"`
	SharedReconciliationResumes    int               `json:"resumes_with_shared_durable_reconciliation"`
	MeasurementStarted             time.Time         `json:"measurement_started_at"`
	MeasurementEnds                time.Time         `json:"measurement_ends_at"`
	DeliveryHistogram              map[int64]int     `json:"delivery_latency_ms_upper_bound_histogram"`
	AckHistogram                   map[int64]int     `json:"ack_latency_ms_upper_bound_histogram"`
	GeneratorEnvironment           map[string]any    `json:"generator_environment"`
	QueryWindowSeconds             float64           `json:"database_counter_window_seconds_monotonic"`
	QueriesBefore                  QuerySnapshot     `json:"database_queries_before"`
	QueriesAfter                   QuerySnapshot     `json:"database_queries_after"`
	QueriesPerSecond               *float64          `json:"database_statements_per_second_full_run,omitempty"`
	MissingWarmup                  int               `json:"missing_warmup_deliveries"`
	Audit                          HistoryAudit      `json:"history_audit"`
	MinConnected                   int               `json:"min_resumed_during_measurement"`
	SchedulerLatency               Percentiles       `json:"scheduler_lateness"`
	ConnectionFailures             int               `json:"websocket_establishment_failures"`
	Disconnects                    int               `json:"websocket_disconnects"`
	MeasuredElapsed                float64           `json:"measurement_and_drain_seconds"`
	Before                         Diagnostics       `json:"diagnostics_before"`
	After                          Diagnostics       `json:"diagnostics_after"`
	Config                         LoadConfig        `json:"config"`
	Started                        time.Time         `json:"started"`
	Conversation                   string            `json:"conversation"`
	ConversationCount              int               `json:"conversation_count"`
	DistinctRecipientUsers         int               `json:"distinct_recipient_users"`
	RecipientsWithMeasuredDelivery int               `json:"recipients_with_measured_delivery"`
	Passed                         bool              `json:"passed"`
	Failure                        string            `json:"failure,omitempty"`
	MaxConnected                   int               `json:"max_simultaneously_resumed"`
	ConnectedAtStart               int               `json:"connected_at_measurement_start"`
	NeverConnected                 int               `json:"never_connected"`
	ConnectionRetries              int               `json:"connection_retries"`
	RetryStatuses                  map[int]int       `json:"retry_http_statuses"`
	ClientErrors                   int               `json:"client_terminal_errors"`
	ClientErrorKinds               map[string]int    `json:"client_error_kinds"`
	HTTPAttemptErrors              int               `json:"http_attempt_errors"`
	HTTPRecoveryRetries            int               `json:"http_recovery_retries"`
	MessageErrors                  int               `json:"unacknowledged_logical_messages"`
	DroppedSends                   int               `json:"scheduler_queue_drops"`
	Unscheduled                    int               `json:"unscheduled_rate_slots"`
	MeasuredMessages               int               `json:"measured_logical_messages"`
	Acknowledged                   int               `json:"measured_acknowledged_messages"`
	ExpectedDeliveries             int               `json:"expected_measured_deliveries"`
	ReceivedDeliveries             int               `json:"received_measured_deliveries"`
	MissingDeliveries              int               `json:"missing_measured_deliveries"`
	DuplicateCallbacks             int               `json:"duplicate_callbacks"`
	SuppressedDuplicates           int               `json:"suppressed_duplicates"`
	Gaps                           int               `json:"gap_recovery_triggers"`
	AckLatency                     Percentiles       `json:"sender_ack_latency"`
	DeliveryLatency                Percentiles       `json:"recipient_delivery_latency"`
	AckThroughput                  float64           `json:"acknowledged_messages_per_second"`
	DeliveryThroughput             float64           `json:"recipient_deliveries_per_second_including_drain"`
	RecoveryMS                     *float64          `json:"kill_to_affected_reconnect_ms,omitempty"`
	KillAt                         time.Time         `json:"kill_requested_at,omitempty"`
	OtherDeviceOnlineAfterKill     bool              `json:"other_device_online_after_kill"`
	RetryDelaysMS                  []float64         `json:"retry_delays_ms"`
	Instances                      map[string]int    `json:"connections_by_instance"`
	Resources                      []ResourceSample  `json:"resource_samples"`
	Records                        []*DeliveryRecord `json:"records"`
}
type loadState struct {
	sync.Mutex
	report                *LoadReport
	records               map[string]*DeliveryRecord
	latencies, acks       []time.Duration
	connected, ever       []bool
	deliveryCounts        []int
	instance              []string
	active                int
	killAt                time.Time
	killedInstance        string
	initialPinnedInstance string
	recovered             bool
	measuring             bool
}

func scheduleOffset(slot, rate int) time.Duration {
	return time.Duration(int64(slot) * int64(time.Second) / int64(rate))
}

func (s *loadState) state(device int, v client.State) {
	s.Lock()
	defer s.Unlock()
	r := s.report
	switch v.Kind {
	case "connected":
		if v.DurableReconciliation {
			r.SharedReconciliationResumes++
		}
		if !s.connected[device] {
			s.active++
		}
		s.connected[device] = true
		s.ever[device] = true
		s.instance[device] = v.InstanceID
		if device == 0 && v.Pinned && s.initialPinnedInstance == "" {
			s.initialPinnedInstance = v.InstanceID
		}
		r.MaxConnected = max(r.MaxConnected, s.active)
		r.Instances[v.InstanceID]++
		if !s.killAt.IsZero() && !s.recovered && v.InstanceID != s.killedInstance && device == 0 {
			elapsed := float64(time.Since(s.killAt)) / float64(time.Millisecond)
			r.RecoveryMS = &elapsed
			s.recovered = true
		}
	case "disconnected":
		r.Disconnects++
		if s.connected[device] {
			s.active--
		}
		s.connected[device] = false
		if s.measuring {
			r.MinConnected = min(r.MinConnected, s.active)
		}
	case "retry":
		r.ConnectionRetries++
		r.RetryStatuses[v.Status]++
		r.RetryDelaysMS = append(r.RetryDelaysMS, float64(v.Delay)/float64(time.Millisecond))
	case "duplicate":
		r.SuppressedDuplicates++
	case "gap":
		r.Gaps++
	case "connection_failed":
		r.ConnectionFailures++
	case "recovery_retry":
		r.HTTPRecoveryRetries++
	}
}
func (s *loadState) receive(device int, m storage.Message) error {
	now := time.Now()
	s.Lock()
	defer s.Unlock()
	record := s.records[m.ClientMessageID]
	if record == nil {
		return fmt.Errorf("unexpected message")
	}
	if record.Conversation != "" && record.Conversation != m.ConversationID {
		return fmt.Errorf("message delivered to wrong conversation")
	}
	if record.seen[device] {
		s.report.DuplicateCallbacks++
		return fmt.Errorf("duplicate callback")
	}
	record.seen[device] = true
	record.Delivered++
	if record.Measured {
		s.latencies = append(s.latencies, now.Sub(record.start))
		s.deliveryCounts[device]++
	}
	return nil
}

func RunLoad(ctx context.Context, c LoadConfig) LoadReport {
	c.RateScope = "aggregate_across_workload_not_per_device"
	c.HTTPPool = "per_device_max_2_connections_owned_until_shutdown"
	r := LoadReport{Config: c, Started: time.Now().UTC(), RetryStatuses: map[int]int{}, Instances: map[string]int{}, ClientErrorKinds: map[string]int{}}
	portRange, _ := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	r.GeneratorEnvironment = map[string]any{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "gomemlimit": os.Getenv("GOMEMLIMIT"), "ephemeral_port_range": strings.TrimSpace(string(portRange))}
	if c.Connections < 1 || c.Connections > 20000 || c.UserOffset < 0 || c.UserOffset+c.Connections > 20000 || c.Rate < 1 || c.Rate > 10000 || c.Duration <= 0 || c.Drain <= 0 || c.Warmup < 0 || c.Ramp < 0 || c.RecoveryInterval < 0 || c.KillAfter < 0 {
		r.Failure = "invalid workload parameters"
		return r
	}
	fanout := c.Connections
	if c.DistinctUsers {
		fanout = min(99, fanout)
	}
	if (c.Warmup.Seconds()+c.Duration.Seconds())*float64(c.Rate)*float64(fanout) > 10000000 {
		r.Failure = "workload exceeds the generator's 10-million-delivery memory bound"
		return r
	}
	api := NewAPI(c.URL)
	defer api.HTTP.CloseIdleConnections()
	if !waitReady(ctx, c.URL, 20*time.Second) {
		r.Failure = "dependencies did not become ready"
		return r
	}
	var conversations []storage.Conversation
	var sizes map[string]int
	var recipients []recipient
	var e error
	r.DistinctRecipientUsers = 1
	if c.DistinctUsers {
		recipients, conversations, sizes, e = api.userConversations(ctx, c.Connections, c.UserOffset)
		if e != nil {
			r.Failure = e.Error()
			return r
		}
		r.DistinctRecipientUsers = c.Connections
	} else {
		conversation, err := api.Create(ctx)
		if err != nil {
			r.Failure = err.Error()
			return r
		}
		conversations = []storage.Conversation{conversation}
		sizes = map[string]int{conversation.ID: c.Connections}
		recipients = make([]recipient, c.Connections)
		for i := range recipients {
			recipients[i] = recipient{Token: bobToken, Conversation: conversation.ID}
		}
	}
	r.Conversation = conversations[0].ID
	r.ConversationCount = len(conversations)
	var sharedHTTP *http.Client
	if c.SharedHTTP {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = 256
		transport.MaxIdleConnsPerHost = 256
		transport.MaxConnsPerHost = 256
		sharedHTTP = &http.Client{Timeout: 5 * time.Second, Transport: transport}
		defer transport.CloseIdleConnections()
		r.Config.HTTPPool = "shared_generator_max_256_connections"
	}
	r.Before = diagnostics()
	queryStarted := time.Now()
	r.QueriesBefore = queryStats()
	s := &loadState{report: &r, records: map[string]*DeliveryRecord{}, connected: make([]bool, c.Connections), ever: make([]bool, c.Connections), deliveryCounts: make([]int, c.Connections), instance: make([]string, c.Connections)}
	runCtx, cancel := context.WithCancel(ctx)
	var clients sync.WaitGroup
	for i := 0; i < c.Connections && runCtx.Err() == nil; i++ {
		device := i
		consumer := &client.Client{URL: c.URL, Token: recipients[i].Token, HTTP: sharedHTTP, RecoveryInterval: c.RecoveryInterval, Subscriptions: []string{recipients[i].Conversation}, OnMessage: func(m storage.Message) error { return s.receive(device, m) }, OnState: func(v client.State) { s.state(device, v) }}
		if c.KillAfter > 0 && i == 0 {
			consumer.FirstWebSocketURL = "http://127.0.0.1:8081"
			if c.FirstWebSocketURL != "" {
				consumer.FirstWebSocketURL = c.FirstWebSocketURL
			}
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			if err := consumer.Run(runCtx); err != nil && runCtx.Err() == nil {
				s.Lock()
				r.ClientErrors++
				r.ClientErrorKinds[err.Error()]++
				s.Unlock()
			}
		}()
		if c.Ramp > 0 {
			select {
			case <-runCtx.Done():
			case <-time.After(c.Ramp):
			}
		}
	}
	readyTimeout := c.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = 30 * time.Second
	}
	readyDeadline := time.Now().Add(readyTimeout)
	for time.Now().Before(readyDeadline) && ctx.Err() == nil {
		s.Lock()
		active := s.active
		s.Unlock()
		if active == c.Connections {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if c.ReadyFile != "" {
		s.Lock()
		active := s.active
		s.Unlock()
		if err := Save(c.ReadyFile, map[string]int{"connected": active, "target": c.Connections, "user_offset": c.UserOffset}); err != nil {
			r.Failure = err.Error()
			cancel()
			clients.Wait()
			return r
		}
	}
	if c.StartBarrier != "" {
		for {
			if _, err := os.Stat(c.StartBarrier); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				r.Failure = "start barrier canceled"
				cancel()
				clients.Wait()
				return r
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	s.Lock()
	r.ConnectedAtStart = s.active
	r.MinConnected = s.active
	s.measuring = true
	s.Unlock()
	// Resources are sampled separately so docker stats never blocks the scheduler.
	resourceDone := make(chan struct{})
	stopResources := make(chan struct{})
	go func() {
		defer close(resourceDone)
		for {
			sample := resourceSample()
			s.Lock()
			r.Resources = append(r.Resources, sample)
			s.Unlock()
			select {
			case <-stopResources:
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
	scheduleStart := time.Now()
	measurementStart := scheduleStart.Add(c.Warmup)
	r.MeasurementStarted, r.MeasurementEnds = measurementStart.UTC(), measurementStart.Add(c.Duration).UTC()
	jobs := make(chan *DeliveryRecord, 128)
	var senders sync.WaitGroup
	for i := 0; i < 16; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for record := range jobs {
				s.Lock()
				record.start = time.Now()
				s.Unlock()
				body := map[string]string{"client_message_id": record.ID, "body": strings.Repeat("x", 64)}
				senderToken := aliceToken
				if record.SenderID == bob {
					senderToken = bobToken
				}
				var message storage.Message
				var err error
				for attempt := 0; attempt < 3; attempt++ {
					_, err = api.Call(ctx, senderToken, "POST", "/v1/conversations/"+record.Conversation+"/messages", body, &message)
					if err == nil {
						break
					}
					s.Lock()
					r.HTTPAttemptErrors++
					s.Unlock()
					time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
				}
				s.Lock()
				if err != nil {
					r.MessageErrors++
				} else {
					record.Ack = true
					record.Sequence = message.Sequence
					if record.Measured {
						r.Acknowledged++
						s.acks = append(s.acks, time.Since(record.start))
					}
				}
				s.Unlock()
			}
		}()
	}
	killed := false
	killDone := make(chan struct{})
	close(killDone)
	totalSlots := int((c.Warmup + c.Duration).Seconds() * float64(c.Rate))
	lateness := []time.Duration{}
	for slot := 0; slot < totalSlots && ctx.Err() == nil; slot++ {
		scheduled := scheduleStart.Add(scheduleOffset(slot, c.Rate))
		if wait := time.Until(scheduled); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
		lateness = append(lateness, max(time.Duration(0), time.Since(scheduled)))
		if c.KillAfter > 0 && !killed && time.Since(scheduleStart) >= c.KillAfter {
			killed = true
			s.Lock()
			s.killAt = time.Now()
			r.KillAt = s.killAt
			s.killedInstance = s.initialPinnedInstance
			if s.initialPinnedInstance == "" || s.instance[0] != s.initialPinnedInstance {
				r.Failure = "pinned device had left the target instance before kill"
			}
			s.Unlock()
			killDone = make(chan struct{})
			go func() {
				defer close(killDone)
				killCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				defer stop()
				if _, err := command(killCtx, "compose", "kill", "-s", "SIGKILL", "app1"); err != nil {
					s.Lock()
					r.Failure = "docker kill failed"
					s.Unlock()
				}
				if c.DistinctUsers {
					return
				}
				time.Sleep(time.Second)
				var p []struct {
					Online bool `json:"online"`
				}
				survivor := c.SurvivorURL
				if survivor == "" {
					survivor = "http://127.0.0.1:8082"
				}
				presenceAPI := NewAPI(survivor)
				defer presenceAPI.HTTP.CloseIdleConnections()
				_, err := presenceAPI.Call(killCtx, aliceToken, "GET", "/v1/presence?user_ids="+bob, nil, &p)
				s.Lock()
				r.OtherDeviceOnlineAfterKill = err == nil && len(p) == 1 && p[0].Online
				s.Unlock()
			}()
		}
		record := &DeliveryRecord{ID: uuid.NewString(), Measured: !scheduled.Before(measurementStart), seen: map[int]bool{}}
		record.Conversation = conversations[slot%len(conversations)].ID
		record.Expected = sizes[record.Conversation]
		record.SenderID = alice
		if alternateLoadSender(c, slot) {
			record.SenderID = bob
		}
		s.Lock()
		s.records[record.ID] = record
		r.Records = append(r.Records, record)
		if record.Measured {
			r.MeasuredMessages++
		}
		s.Unlock()
		select {
		case jobs <- record:
		default:
			s.Lock()
			r.DroppedSends++
			r.MessageErrors++
			s.Unlock()
		}
	}
	close(jobs)
	senders.Wait()
	<-killDone
	r.Unscheduled = max(0, totalSlots-len(r.Records))
	r.SchedulerLatency = summarize(lateness)
	// Keep the full declared measurement window even when the last scheduled send
	// occurs one interval before its end.
	if wait := time.Until(measurementStart.Add(c.Duration)); wait > 0 {
		time.Sleep(wait)
	}
	deadline := time.Now().Add(c.Drain)
	for time.Now().Before(deadline) {
		s.Lock()
		complete := true
		for _, rec := range r.Records {
			if rec.Ack && rec.Delivered < rec.Expected {
				complete = false
				break
			}
		}
		s.Unlock()
		if complete {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(measurementStart)
	r.MeasuredElapsed = elapsed.Seconds()
	r.WallMeasuredElapsed = time.Now().UTC().Sub(r.MeasurementStarted).Seconds()
	drift := r.WallMeasuredElapsed - r.MeasuredElapsed
	r.ClockDiscontinuity = drift > 1 || drift < -1
	s.Lock()
	s.measuring = false
	s.Unlock()
	cancel()
	clients.Wait()
	close(stopResources)
	<-resourceDone
	r.After = diagnostics()
	r.QueriesAfter = queryStats()
	r.QueryWindowSeconds = time.Since(queryStarted).Seconds()
	r.QueriesPerSecond = statementRate(r.QueriesBefore, r.QueriesAfter, r.QueryWindowSeconds)
	r.Audit = Audit(ctx, r)
	s.Lock()
	defer s.Unlock()
	for _, ever := range s.ever {
		if !ever {
			r.NeverConnected++
		}
	}
	for _, count := range s.deliveryCounts {
		if count > 0 {
			r.RecipientsWithMeasuredDelivery++
		}
	}
	for _, rec := range r.Records {
		if rec.Measured {
			r.ExpectedDeliveries += rec.Expected
			r.ReceivedDeliveries += rec.Delivered
		} else {
			r.MissingWarmup += rec.Expected - rec.Delivered
		}
	}
	r.MissingDeliveries = r.ExpectedDeliveries - r.ReceivedDeliveries
	r.AckLatency = summarize(s.acks)
	r.DeliveryLatency = summarize(s.latencies)
	r.DeliveryHistogram = durationHistogram(s.latencies)
	r.AckHistogram = durationHistogram(s.acks)
	r.AckThroughput = float64(r.Acknowledged) / elapsed.Seconds()
	r.DeliveryThroughput = float64(r.ReceivedDeliveries) / elapsed.Seconds()
	r.Passed = r.Failure == "" && r.ConnectedAtStart == c.Connections && r.MissingDeliveries == 0 && r.MessageErrors == 0 && r.ClientErrors == 0 && r.DuplicateCallbacks == 0 && r.Unscheduled == 0
	r.Passed = r.Passed && r.Audit.Error == "" && r.Audit.MissingAcknowledged == 0
	r.Passed = r.Passed && r.MissingWarmup == 0
	r.Passed = r.Passed && !r.ClockDiscontinuity
	if c.DistinctUsers {
		r.Passed = r.Passed && r.RecipientsWithMeasuredDelivery == c.Connections
	}
	if c.KillAfter > 0 && !s.recovered {
		r.Passed = false
		r.Failure = "pinned client did not reconnect to another instance"
	}
	if !r.Passed && r.Failure == "" {
		r.Failure = "connection, delivery or message assertions failed; inspect counters"
	}
	return r
}

// Bob belongs to the legacy shared-device conversation. Distinct-user groups
// contain Alice and their own recipient cohort, so only Alice spans all groups.
func alternateLoadSender(c LoadConfig, slot int) bool {
	return c.KillAfter > 0 && !c.DistinctUsers && slot%2 == 1
}
