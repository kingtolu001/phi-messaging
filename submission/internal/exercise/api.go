package exercise

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"phi-messaging/internal/storage"
)

const alice = "11111111-1111-4111-8111-111111111111"
const bob = "22222222-2222-4222-8222-222222222222"
const carol = "33333333-3333-4333-8333-333333333333"
const aliceToken = "local-alice-token-0000000000000001"
const bobToken = "local-bob-token-000000000000000002"
const carolToken = "local-carol-token-0000000000000003"

type API struct {
	URL  string
	HTTP *http.Client
}

func NewAPI(url string) API {
	return API{url, &http.Client{Timeout: 7 * time.Second, Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 128}}}
}
func (a API) Call(ctx context.Context, token, method, path string, body, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		raw, e := json.Marshal(body)
		if e != nil {
			return 0, e
		}
		reader = bytes.NewReader(raw)
	}
	req, e := http.NewRequestWithContext(ctx, method, a.URL+path, reader)
	if e != nil {
		return 0, e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, e := a.HTTP.Do(req)
	if e != nil {
		return 0, fmt.Errorf("request transport failure")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		io.Copy(io.Discard, res.Body)
		return res.StatusCode, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	if out != nil {
		e = json.NewDecoder(res.Body).Decode(out)
	} else {
		_, e = io.Copy(io.Discard, res.Body)
	}
	return res.StatusCode, e
}
func (a API) Create(ctx context.Context) (storage.Conversation, error) {
	var c storage.Conversation
	_, e := a.Call(ctx, aliceToken, "POST", "/v1/conversations", map[string]any{"kind": "group", "member_ids": []string{alice, bob, carol}}, &c)
	return c, e
}
func (a API) History(ctx context.Context, id string) ([]storage.Message, error) {
	return a.history(ctx, id, bobToken)
}
func (a API) history(ctx context.Context, id, token string) ([]storage.Message, error) {
	all := []storage.Message{}
	cursor := int64(0)
	for {
		var page []storage.Message
		_, e := a.Call(ctx, token, "GET", fmt.Sprintf("/v1/conversations/%s/messages?after=%d&limit=100", id, cursor), nil, &page)
		if e != nil {
			return all, e
		}
		for _, m := range page {
			if m.ConversationID != id {
				return all, fmt.Errorf("durable history conversation mismatch")
			}
			if m.Sequence != cursor+1 {
				return all, fmt.Errorf("durable history gap")
			}
			cursor = m.Sequence
			all = append(all, m)
		}
		if len(page) < 100 {
			return all, nil
		}
	}
}
