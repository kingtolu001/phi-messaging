package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestLeaseDiagnosticsClassifiesFailures(t *testing.T) {
	var d diagnostics
	for _, err := range []error{
		fmt.Errorf("renewal: %w", context.DeadlineExceeded),
		errors.New("redis: connection pool timeout"),
		&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")},
		errors.New("redis: client is closed"),
	} {
		d.recordLeaseError(err)
	}
	if d.leaseError.Load() != 4 || d.leaseDeadline.Load() != 1 || d.leasePool.Load() != 1 || d.leaseNetwork.Load() != 1 || d.leaseOther.Load() != 1 {
		t.Fatal("Lease error categories must count each failure exactly once")
	}
}
