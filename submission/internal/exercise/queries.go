package exercise

import (
	"context"
	"strconv"
	"strings"
	"time"
)

type QuerySnapshot struct {
	At          time.Time `json:"at"`
	Calls       int64     `json:"statements"`
	ExecutionMS float64   `json:"execution_ms"`
	Error       string    `json:"error,omitempty"`
}

func queryStats() QuerySnapshot {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	raw, e := command(ctx, "compose", "exec", "-T", "postgres", "psql", "-U", "phi", "-d", "phi", "-At", "-F", "|", "-c", `SELECT coalesce(sum(calls),0),coalesce(sum(total_exec_time),0) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname='phi')`)
	s := QuerySnapshot{At: time.Now().UTC()}
	if e != nil {
		s.Error = "pg_stat_statements unavailable"
		return s
	}
	fields := strings.Split(strings.TrimSpace(raw), "|")
	if len(fields) != 2 {
		s.Error = "invalid query statistics"
		return s
	}
	s.Calls, e = strconv.ParseInt(fields[0], 10, 64)
	if e != nil {
		s.Error = e.Error()
		return s
	}
	s.ExecutionMS, e = strconv.ParseFloat(fields[1], 64)
	if e != nil {
		s.Error = e.Error()
	}
	return s
}

func statementRate(before, after QuerySnapshot, seconds float64) *float64 {
	if seconds <= 0 || before.Error != "" || after.Error != "" || after.Calls < before.Calls {
		return nil
	}
	rate := float64(after.Calls-before.Calls) / seconds
	return &rate
}
