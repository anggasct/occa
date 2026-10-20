package discord

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

type mentionPayload struct {
	Content         string `json:"content"`
	AllowedMentions struct {
		Parse []string `json:"parse"`
		Users []string `json:"users"`
	} `json:"allowed_mentions"`
	Components []struct {
		Components []struct {
			Label    string `json:"label"`
			Disabled bool   `json:"disabled"`
		} `json:"components"`
	} `json:"components"`
}

func TestSendNotificationMentionAllowsOnlyTheResolvedUser(t *testing.T) {
	var sentBody string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/channels/chan-1/messages") {
			b, _ := io.ReadAll(r.Body)
			sentBody = string(b)
			return []byte(`{"id":"msg-123","channel_id":"chan-1"}`), http.StatusOK
		}
		return []byte(`{}`), http.StatusOK
	})

	a := &Adapter{session: session}
	if _, err := a.SendNotificationMention("chan-1", "🔴 acme/widget#181 — blocked needs you\n@operator", "user-42"); err != nil {
		t.Fatalf("SendNotificationMention: %v", err)
	}
	var payload mentionPayload
	if err := json.Unmarshal([]byte(sentBody), &payload); err != nil {
		t.Fatalf("decode payload: %v (%s)", err, sentBody)
	}
	if len(payload.AllowedMentions.Users) != 1 || payload.AllowedMentions.Users[0] != "user-42" {
		t.Fatalf("allowed users = %v, want the resolved user id", payload.AllowedMentions.Users)
	}
	if len(payload.AllowedMentions.Parse) != 1 || payload.AllowedMentions.Parse[0] != "users" {
		t.Fatalf("allowed parse = %v, want users", payload.AllowedMentions.Parse)
	}
	if !strings.Contains(payload.Content, "needs you") {
		t.Errorf("escalation copy = %q, want the required copy", payload.Content)
	}
}

func TestOtherNotificationPathsCarryNoMention(t *testing.T) {
	var bodies []string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		return []byte(`{"id":"msg-123","channel_id":"chan-1"}`), http.StatusOK
	})
	a := &Adapter{session: session}
	if _, err := a.SendNotification("chan-1", "📌 Status — 🔨 fix"); err != nil {
		t.Fatalf("SendNotification: %v", err)
	}
	if _, err := a.SendNotificationWithButtons("chan-1", "parent", []channel.Button{{Label: "Open PR", URL: "https://example.com"}}); err != nil {
		t.Fatalf("SendNotificationWithButtons: %v", err)
	}
	if err := a.EditNotification("chan-1", "msg-123", "edited"); err != nil {
		t.Fatalf("EditNotification: %v", err)
	}
	if _, err := a.ReplyNotification("chan-1", "msg-123", "reply"); err != nil {
		t.Fatalf("ReplyNotification: %v", err)
	}
	if len(bodies) != 4 {
		t.Fatalf("captured bodies = %d, want 4", len(bodies))
	}
	for i, body := range bodies {
		var payload mentionPayload
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			t.Fatalf("decode payload %d: %v", i, err)
		}
		if len(payload.AllowedMentions.Users) != 0 || len(payload.AllowedMentions.Parse) != 0 {
			t.Errorf("body %d must not allow mentions: %s", i, body)
		}
	}
}

func TestDisabledButtonIsRendered(t *testing.T) {
	var sentBody string
	session := fakeDiscordSession(t, func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/channels/chan-1/messages") {
			b, _ := io.ReadAll(r.Body)
			sentBody = string(b)
			return []byte(`{"id":"msg-123","channel_id":"chan-1"}`), http.StatusOK
		}
		return []byte(`{}`), http.StatusOK
	})
	a := &Adapter{session: session}
	buttons := []channel.Button{
		{Label: "Open PR", URL: "https://example.com"},
		{Label: "Merge", Value: "task:merge:acme/widget#feat/x", Row: 1, Disabled: true},
	}
	if _, err := a.SendNotificationWithButtons("chan-1", "🔨 acme/widget#181 — fix", buttons); err != nil {
		t.Fatalf("SendNotificationWithButtons: %v", err)
	}
	var payload mentionPayload
	if err := json.Unmarshal([]byte(sentBody), &payload); err != nil {
		t.Fatalf("decode payload: %v (%s)", err, sentBody)
	}
	labels := map[string]bool{}
	for _, row := range payload.Components {
		for _, btn := range row.Components {
			labels[btn.Label] = btn.Disabled
		}
	}
	if disabled, ok := labels["Merge"]; !ok || !disabled {
		t.Fatalf("merge button must be rendered disabled: %s", sentBody)
	}
	if disabled, ok := labels["Open PR"]; !ok || disabled {
		t.Fatalf("link button must stay enabled: %s", sentBody)
	}
}
