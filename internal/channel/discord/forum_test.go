package discord

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

// forumAdapter wires an adapter whose channelLookup reports forumID as a forum
// channel and everything else as a plain text channel.
func forumAdapter(t *testing.T, forumID string, handle func(*http.Request) ([]byte, int)) *Adapter {
	t.Helper()
	a := &Adapter{session: fakeDiscordSession(t, handle)}
	a.setBotID("bot")
	a.channelLookup = func(channelID string) (*discordgo.Channel, error) {
		if channelID == forumID {
			return &discordgo.Channel{ID: forumID, Type: discordgo.ChannelTypeGuildForum}, nil
		}
		return &discordgo.Channel{ID: channelID, Type: discordgo.ChannelTypeGuildText}, nil
	}
	return a
}

func TestSendNotificationWithButtonsOpensForumPost(t *testing.T) {
	var body map[string]any
	a := forumAdapter(t, "forum-1", func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/channels/forum-1/threads") {
			raw, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Fatalf("decode thread body: %v", err)
			}
			return []byte(`{"id":"post-1","type":11}`), http.StatusOK
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		return []byte(`{}`), http.StatusInternalServerError
	})

	id, err := a.SendNotificationWithButtons(
		"forum-1",
		"**aura#217 · review**\nqueued: 0 · next: run",
		[]channel.Button{{Label: "Retry", Value: "task:retry:1"}},
	)
	if err != nil {
		t.Fatalf("SendNotificationWithButtons: %v", err)
	}
	if id != "post-1" {
		t.Fatalf("post id = %q, want post-1", id)
	}
	if got, _ := body["name"].(string); got != "aura#217 · review" {
		t.Fatalf("post title = %q, want the first line", got)
	}
	msg, ok := body["message"].(map[string]any)
	if !ok {
		t.Fatalf("thread payload missing starter message: %+v", body)
	}
	if got, _ := msg["content"].(string); !strings.Contains(got, "queued: 0") {
		t.Fatalf("starter content = %q", got)
	}
	if rows, _ := msg["components"].([]any); len(rows) == 0 {
		t.Fatalf("starter message has no buttons: %+v", msg)
	}
	if !a.isTrackedThread("post-1") {
		t.Fatal("forum post is not tracked as an owned thread")
	}
}

func TestEditNotificationOnForumTargetsPost(t *testing.T) {
	a := forumAdapter(t, "forum-1", func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/channels/post-7/messages/post-7") {
			return []byte(`{"id":"post-7"}`), http.StatusOK
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		return []byte(`{}`), http.StatusInternalServerError
	})

	if err := a.EditNotification("forum-1", "post-7", "updated card"); err != nil {
		t.Fatalf("EditNotification: %v", err)
	}
	if err := a.EditNotificationWithButtons("forum-1", "post-7", "updated card", nil); err != nil {
		t.Fatalf("EditNotificationWithButtons: %v", err)
	}
}

func TestStartThreadOnForumKeepsPostAsThread(t *testing.T) {
	a := forumAdapter(t, "forum-1", func(r *http.Request) ([]byte, int) {
		t.Errorf("forum StartThread must not create a thread: %s %s", r.Method, r.URL.Path)
		return []byte(`{}`), http.StatusInternalServerError
	})

	id, err := a.StartThread("forum-1", "post-9", "PR #9: review")
	if err != nil {
		t.Fatalf("StartThread: %v", err)
	}
	if id != "post-9" {
		t.Fatalf("thread id = %q, want the forum post id", id)
	}
}

func TestStartThreadOnForumWithoutPostFails(t *testing.T) {
	a := forumAdapter(t, "forum-1", func(*http.Request) ([]byte, int) {
		t.Error("unexpected request")
		return []byte(`{}`), http.StatusInternalServerError
	})

	if _, err := a.StartThread("forum-1", "", "PR #9: review"); err == nil {
		t.Fatal("StartThread with no post id must fail")
	}
}

func TestTextChannelKeepsMessageSendPath(t *testing.T) {
	a := forumAdapter(t, "forum-1", func(r *http.Request) ([]byte, int) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/channels/chan-1/messages") {
			return []byte(`{"id":"msg-1","channel_id":"chan-1"}`), http.StatusOK
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		return []byte(`{}`), http.StatusInternalServerError
	})

	id, err := a.SendNotification("chan-1", "hello")
	if err != nil {
		t.Fatalf("SendNotification: %v", err)
	}
	if id != "msg-1" {
		t.Fatalf("message id = %q, want msg-1", id)
	}
	if a.isTrackedThread("msg-1") {
		t.Fatal("text channel message must not be tracked as a thread")
	}
}

func TestForumPostTitle(t *testing.T) {
	cases := map[string]string{
		"**aura#217 · review**\nqueued: 0": "aura#217 · review",
		"\n\n   \nplain title":             "plain title",
		"":                                 "run",
		strings.Repeat("x", 140):           strings.Repeat("x", 100),
	}
	for input, want := range cases {
		if got := forumPostTitle(input); got != want {
			t.Fatalf("forumPostTitle(%q) = %q, want %q", input, got, want)
		}
	}
}
