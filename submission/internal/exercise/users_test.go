package exercise

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"phi-messaging/internal/storage"
)

func TestDistinctUserFixturesAndGroupMembership(t *testing.T) {
	identities := map[string]bool{}
	tokens := map[string]bool{}
	for i := 0; i < 20000; i++ {
		u := scaleUser(i)
		if identities[u.ID] || tokens[u.Token] || uuid.Validate(u.ID) != nil || u != scaleUser(i) {
			t.Fatal("fixture identities are not unique and deterministic", i)
		}
		identities[u.ID], tokens[u.Token] = true, true
	}
	groupSizes := []int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Kind    string   `json:"kind"`
			Members []string `json:"member_ids"`
		}
		if r.Header.Get("Authorization") != "Bearer "+aliceToken || json.NewDecoder(r.Body).Decode(&body) != nil || body.Kind != "group" || len(body.Members) > 100 || body.Members[0] != alice {
			t.Error("invalid authorized group creation")
			w.WriteHeader(400)
			return
		}
		groupSizes = append(groupSizes, len(body.Members))
		json.NewEncoder(w).Encode(storage.Conversation{ID: uuid.NewString()})
	}))
	defer srv.Close()
	api := NewAPI(srv.URL)
	defer api.HTTP.CloseIdleConnections()
	devices, groups, sizes, err := api.userConversations(context.Background(), 200, 10000)
	if err != nil || len(groups) != 3 || len(devices) != 200 || groupSizes[0] != 100 || groupSizes[2] != 3 {
		t.Fatal(err, len(groups), groupSizes)
	}
	for i, device := range devices {
		if device.Token != scaleUser(10000+i).Token || sizes[device.Conversation] != min(99, 200-(i/99)*99) {
			t.Fatal("recipient routed to wrong group", i)
		}
	}
}

func TestUserFixturesRefuseOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := WriteUserFixtures(dir, 2); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "tokens.json"))
	if err != nil {
		t.Fatal(err)
	}
	if WriteUserFixtures(dir, 3) == nil {
		t.Fatal("overwrote fixtures")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "tokens.json"))
	if string(before) != string(after) {
		t.Fatal("changed evidence")
	}
}
