package exercise

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"sort"
	"time"
)

type Percentiles struct {
	Samples int     `json:"samples"`
	P50     float64 `json:"p50_ms"`
	P95     float64 `json:"p95_ms"`
	P99     float64 `json:"p99_ms"`
	Max     float64 `json:"max_ms"`
}

func summarize(values []time.Duration) Percentiles {
	p := Percentiles{Samples: len(values)}
	if len(values) == 0 {
		return p
	}
	v := append([]time.Duration(nil), values...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	at := func(q float64) float64 {
		return float64(v[int(math.Ceil(float64(len(v))*q))-1]) / float64(time.Millisecond)
	}
	p.P50 = at(.5)
	p.P95 = at(.95)
	p.P99 = at(.99)
	p.Max = at(1)
	return p
}

type ResourceSample struct {
	At          time.Time `json:"at"`
	DockerStats string    `json:"docker_stats"`
	Error       string    `json:"error,omitempty"`
}

type Diagnostics struct {
	Postgres string   `json:"postgres"`
	Redis    string   `json:"redis"`
	Errors   []string `json:"errors,omitempty"`
}

func diagnostics() Diagnostics {
	d := Diagnostics{}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var e error
	d.Postgres, e = command(ctx, "compose", "exec", "-T", "postgres", "psql", "-U", "phi", "-d", "phi", "-c", `SELECT numbackends,xact_commit,xact_rollback,tup_returned,tup_fetched,deadlocks,sessions,sessions_abandoned,sessions_fatal FROM pg_stat_database WHERE datname='phi'; SELECT state,wait_event_type,count(*) FROM pg_stat_activity WHERE datname='phi' GROUP BY state,wait_event_type;`)
	if e != nil {
		d.Errors = append(d.Errors, "PostgreSQL diagnostics: "+e.Error())
	}
	d.Redis, e = command(ctx, "compose", "exec", "-T", "redis", "redis-cli", "INFO", "stats")
	if e != nil {
		d.Errors = append(d.Errors, "Redis diagnostics: "+e.Error())
	}
	return d
}

func command(ctx context.Context, args ...string) (string, error) {
	raw, e := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return string(raw), e
}
func resourceSample() ResourceSample {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	raw, e := command(ctx, "stats", "--no-stream", "--format", "{{json .}}", "phi-messaging-app1-1", "phi-messaging-app2-1", "phi-messaging-app3-1", "phi-messaging-postgres-1", "phi-messaging-redis-1", "phi-messaging-load-balancer-1")
	s := ResourceSample{At: time.Now().UTC(), DockerStats: raw}
	if e != nil {
		s.Error = e.Error()
	}
	return s
}
func Save(path string, value any) error {
	raw, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if e != nil {
		return e
	}
	_, e = f.Write(append(raw, '\n'))
	closeErr := f.Close()
	if e != nil {
		return e
	}
	return closeErr
}
