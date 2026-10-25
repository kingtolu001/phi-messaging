package exercise

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"phi-messaging/internal/storage"
)

type userFixture struct {
	ID, Token string
}

func scaleUser(index int) userFixture {
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("phi-scale-user-%d", index))).String()
	return userFixture{ID: id, Token: "local-scale-" + id}
}

// Fixtures are local mock identities, never production credentials.
func WriteUserFixtures(directory string, count int) error {
	if count < 1 || count > 20000 {
		return fmt.Errorf("users must be between 1 and 20000")
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	tokens := map[string]string{aliceToken: alice, bobToken: bob, carolToken: carol}
	var sql strings.Builder
	sql.WriteString("BEGIN;\nINSERT INTO users(id, display_name) VALUES\n")
	for i := 0; i < count; i++ {
		u := scaleUser(i)
		tokens[u.Token] = u.ID
		if i > 0 {
			sql.WriteString(",\n")
		}
		fmt.Fprintf(&sql, "('%s','Scale user %d')", u.ID, i)
	}
	sql.WriteString("\nON CONFLICT (id) DO NOTHING;\nCOMMIT;\n")
	if err := Save(filepath.Join(directory, "tokens.json"), tokens); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(directory, "users.sql"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(sql.String())
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

type recipient struct {
	Token, Conversation string
}

func (a API) userConversations(ctx context.Context, count, offset int) ([]recipient, []storage.Conversation, map[string]int, error) {
	devices := make([]recipient, count)
	conversations := []storage.Conversation{}
	sizes := map[string]int{}
	// The sender occupies the hundredth membership slot.
	for first := 0; first < count; first += 99 {
		last := min(first+99, count)
		members := []string{alice}
		for i := first; i < last; i++ {
			members = append(members, scaleUser(offset+i).ID)
		}
		var conversation storage.Conversation
		if _, err := a.Call(ctx, aliceToken, "POST", "/v1/conversations", map[string]any{"kind": "group", "member_ids": members}, &conversation); err != nil {
			return nil, nil, nil, fmt.Errorf("create user group: %w", err)
		}
		conversations = append(conversations, conversation)
		sizes[conversation.ID] = last - first
		for i := first; i < last; i++ {
			devices[i] = recipient{Token: scaleUser(offset + i).Token, Conversation: conversation.ID}
		}
	}
	return devices, conversations, sizes, nil
}
