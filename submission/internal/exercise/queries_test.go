package exercise

import (
	"testing"
	"time"
)

func TestQueryRateUsesMonotonicWindowNotWallClock(t *testing.T) {
	before := QuerySnapshot{At: time.Now(), Calls: 10}
	after := QuerySnapshot{At: before.At.Add(2 * time.Hour), Calls: 110}
	rate := statementRate(before, after, 2)
	if rate == nil || *rate != 50 {
		t.Fatal(rate)
	}
	if statementRate(after, before, 2) != nil {
		t.Fatal("counter reset fabricated a rate")
	}
}
