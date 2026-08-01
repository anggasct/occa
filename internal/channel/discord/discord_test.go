package discord

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/render"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

func TestNormalizeMessageResolvesThreadScopeOnce(t *testing.T) {
	calls := 0
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			calls++
			return &discordgo.Channel{ID: channelID, ParentID: "parent", Type: discordgo.ChannelTypeGuildPublicThread}, nil
		},
	}
	a.setBotID("bot")

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "thread",
		Author:    &discordgo.User{ID: "user"},
		Content:   "hello",
	})

	if calls != 1 {
		t.Fatalf("channel lookup calls = %d, want 1", calls)
	}
	if got.ParentChannelID != "parent" || !got.IsThread || got.ChannelScopeUnresolved {
		t.Fatalf("unexpected normalized scope: %+v", got)
	}
}

func TestNormalizeMessageMarksFailedScopeLookup(t *testing.T) {
	a := &Adapter{
		channelLookup: func(string) (*discordgo.Channel, error) {
			return nil, errors.New("lookup failed")
		},
	}
	a.setBotID("bot")

	got := a.normalizeMessage(&discordgo.Message{
		GuildID:   "guild",
		ChannelID: "thread",
		Author:    &discordgo.User{ID: "user"},
	})

	if !got.ChannelScopeUnresolved || got.ParentChannelID != "" {
		t.Fatalf("unexpected failed scope normalization: %+v", got)
	}
}

func TestSplitMessageShort(t *testing.T) {
	chunks := render.Split("hello", 2000)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}

func TestSplitMessageLong(t *testing.T) {
	para1 := strings.Repeat("a", 1500)
	para2 := strings.Repeat("b", 1500)
	text := para1 + "\n\n" + para2

	chunks := render.Split(text, 2000)
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 2000 {
			t.Fatalf("chunk %d exceeds 2000: %d", i, len(chunk))
		}
	}
}

func TestComponentRowsEmptyRemovesButtons(t *testing.T) {
	if components := componentRows(nil); components == nil || len(components) != 0 {
		t.Fatalf("empty components = %+v", components)
	}
	components := componentRows([]channel.Button{{Label: "Allow", Value: "allow"}})
	if len(components) != 1 {
		t.Fatalf("button components = %+v", components)
	}
}

func TestDownloadAttachmentTimeout(t *testing.T) {
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // never respond until the client gives up
	}))
	defer blocked.Close()

	a := &Adapter{downloadClient: &http.Client{Timeout: 200 * time.Millisecond}}
	start := time.Now()
	atts := a.downloadAttachments(&discordgo.Message{
		Attachments: []*discordgo.MessageAttachment{
			{Filename: "stalled.bin", ContentType: "application/octet-stream", URL: blocked.URL},
		},
	})
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("download blocked for %v", elapsed)
	}
	if len(atts) != 0 {
		t.Fatalf("expected stalled attachment dropped, got %d", len(atts))
	}
}

func TestDownloadAttachmentSucceeds(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("file-data"))
	}))
	defer ts.Close()

	a := &Adapter{downloadClient: &http.Client{Timeout: 5 * time.Second}}
	atts := a.downloadAttachments(&discordgo.Message{
		Attachments: []*discordgo.MessageAttachment{
			{Filename: "ok.txt", ContentType: "text/plain", URL: ts.URL},
		},
	})
	if len(atts) != 1 || string(atts[0].Data) != "file-data" {
		t.Fatalf("unexpected attachments: %+v", atts)
	}
}
