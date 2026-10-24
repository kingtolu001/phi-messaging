package exercise

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReportsCannotOverwriteFailureEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := Save(path, map[string]bool{"passed": false}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if err := Save(path, map[string]bool{"passed": true}); err == nil {
		t.Fatal("overwrote failure")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("failure evidence changed")
	}
}

func TestPercentilesRetainTail(t *testing.T) {
	p := summarize([]time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, time.Second})
	if p.Samples != 4 || p.P50 != 2 || p.P95 != 1000 || p.P99 != 1000 {
		t.Fatal(p)
	}
}

func TestScheduleExcludesEndBoundary(t *testing.T) {
	for _, rate := range []int{2, 3, 5} {
		measured := 0
		for i := 0; i < 25*rate; i++ {
			offset := scheduleOffset(i, rate)
			if offset >= 25*time.Second {
				t.Fatal("included end")
			}
			if offset >= 5*time.Second {
				measured++
			}
		}
		if measured != 20*rate {
			t.Fatal(rate, measured)
		}
	}
}
