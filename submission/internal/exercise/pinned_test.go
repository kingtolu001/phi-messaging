package exercise

import (
	"phi-messaging/internal/client"
	"testing"
)

func TestRecoverySubjectMustHaveResumedOnPinnedEndpoint(t *testing.T) {
	r := &LoadReport{Instances: map[string]int{}}
	s := &loadState{report: r, connected: make([]bool, 1), ever: make([]bool, 1), instance: make([]string, 1)}
	s.state(0, client.State{Kind: "connected", InstanceID: "survivor"})
	if s.initialPinnedInstance != "" {
		t.Fatal("fallback connection was mistaken for the killed node")
	}
	s.state(0, client.State{Kind: "disconnected"})
	s.state(0, client.State{Kind: "connected", InstanceID: "target", Pinned: true})
	if s.initialPinnedInstance != "target" {
		t.Fatal("accepted pinned session was not recorded")
	}
}
