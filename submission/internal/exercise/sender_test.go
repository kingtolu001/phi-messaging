package exercise

import (
	"testing"
	"time"
)

func TestFaultWorkloadSendersAreConversationMembers(t *testing.T) {
	groups := []map[string]bool{{"alice": true, "recipient1": true}, {"alice": true, "recipient2": true}}
	config := LoadConfig{DistinctUsers: true, KillAfter: time.Second}
	for slot := 0; slot < 100; slot++ {
		sender := "alice"
		if alternateLoadSender(config, slot) {
			sender = "bob"
		}
		if !groups[slot%len(groups)][sender] {
			t.Fatalf("fault send %d uses nonmember %s", slot, sender)
		}
	}
	config.DistinctUsers = false
	if !alternateLoadSender(config, 1) || alternateLoadSender(config, 0) {
		t.Fatal("legacy multi-device fault workload lost its second sender")
	}
}
