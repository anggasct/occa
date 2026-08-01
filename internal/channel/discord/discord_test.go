package discord

import (
	"errors"
	"strings"
	"testing"

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
	chunks := splitMessage("hello", 2000)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}

func TestSplitMessageLong(t *testing.T) {
	para1 := strings.Repeat("a", 1500)
	para2 := strings.Repeat("b", 1500)
	text := para1 + "\n\n" + para2

	chunks := splitMessage(text, 2000)
	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > 2000 {
			t.Fatalf("chunk %d exceeds 2000: %d", i, len(chunk))
		}
	}
}

func TestFindBreakPoint(t *testing.T) {
	text := "hello world\n\nsecond paragraph"
	bp := findBreakPoint(text, 20)
	if bp != 11 {
		t.Fatalf("expected break at 11, got %d", bp)
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
