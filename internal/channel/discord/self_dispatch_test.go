package discord

import (
	"fmt"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

func TestOwnDispatchSummaryDeliveredOnce(t *testing.T) {
	a := New("fake-token", nil, Config{DownloadTimeout: 60 * 1000000000, MaxDownloadSize: 10 * 1024 * 1024})
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}
	summary := "⏳ Dispatched acme/widget#feat/x · task-1792210842511548349-1\nSpec: specs/feat-x.md"
	var got []channel.IncomingMessage
	deliver := func(m channel.IncomingMessage) { got = append(got, m) }
	mk := func(content string) *discordgo.MessageCreate {
		return &discordgo.MessageCreate{Message: &discordgo.Message{
			ChannelID: "chan",
			Author:    &discordgo.User{ID: "bot-42"},
			Content:   content,
		}}
	}
	a.onMessage(mk(summary), deliver)
	a.onMessage(mk(summary), deliver)
	a.onMessage(mk("echo"), deliver)
	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1", len(got))
	}
	if !got[0].IsMention || !got[0].IsSelfDispatch {
		t.Fatalf("self dispatch must force mention and self-dispatch flag: %+v", got[0])
	}
	if repo, branch, taskID, ok := channel.ParseDispatchSummary(got[0].Text); !ok || repo != "acme/widget" || branch != "feat/x" || taskID != "task-1792210842511548349-1" {
		t.Fatalf("unexpected parsed trigger: %q %q %q %v", repo, branch, taskID, ok)
	}
}

func TestDispatchClaimSetBounded(t *testing.T) {
	a := New("fake-token", nil, Config{})
	for i := 0; i < maxSeenDispatchTasks+100; i++ {
		if !a.tryClaimDispatchTask(fmt.Sprintf("task-%d", i)) {
			t.Fatalf("claim task-%d should succeed", i)
		}
	}
	if n := len(a.seenDispatchTask); n > maxSeenDispatchTasks {
		t.Fatalf("claimed set size = %d, want <= %d", n, maxSeenDispatchTasks)
	}
	if !a.tryClaimDispatchTask("task-0") {
		t.Fatalf("evicted task-0 should be claimable again")
	}
}

func TestOwnDispatchSummaryRoutesIntoTaskThread(t *testing.T) {
	a := New("fake-token", nil, Config{})
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}
	a.SetTaskThreadLookup(func(repo, branch string) (string, bool) {
		if repo == "acme/widget" && branch == "feat/x" {
			return "thread-456", true
		}
		return "", false
	})
	summary := "⏳ Dispatched acme/widget#feat/x · task-1792210842511548349-1\nSpec: specs/feat-x.md"
	var got []channel.IncomingMessage
	deliver := func(m channel.IncomingMessage) { got = append(got, m) }
	mk := func(content string) *discordgo.MessageCreate {
		return &discordgo.MessageCreate{Message: &discordgo.Message{
			ChannelID: "chan-1",
			GuildID:   "guild-1",
			Author:    &discordgo.User{ID: "bot-42"},
			Content:   content,
		}}
	}
	a.onMessage(mk(summary), deliver)
	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1", len(got))
	}
	msg := got[0]
	if msg.ThreadID != "thread-456" || !msg.IsThread || msg.ParentChannelID != "chan-1" {
		t.Fatalf("thread routing = %+v, want thread-456/chan-1/isThread", msg)
	}
	if msg.ChannelID != "chan-1" {
		t.Fatalf("ChannelID = %q, want parent chan-1", msg.ChannelID)
	}
	rc, ok := msg.ReplyCtx.(*taskThreadReplyContext)
	if !ok {
		t.Fatalf("ReplyCtx type = %T, want taskThreadReplyContext", msg.ReplyCtx)
	}
	if rc.thread.channelID != "thread-456" || rc.parent.channelID != "chan-1" {
		t.Fatalf("reply targets = %q/%q, want thread-456/chan-1", rc.thread.channelID, rc.parent.channelID)
	}
}

func TestOwnDispatchSummaryWithoutThreadStaysTopLevel(t *testing.T) {
	a := New("fake-token", nil, Config{})
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}
	summary := "⏳ Dispatched acme/widget#feat/y · task-999\nSpec: specs/y.md"
	var got []channel.IncomingMessage
	deliver := func(m channel.IncomingMessage) { got = append(got, m) }
	mk := func(content string) *discordgo.MessageCreate {
		return &discordgo.MessageCreate{Message: &discordgo.Message{
			ChannelID: "chan-1",
			Author:    &discordgo.User{ID: "bot-42"},
			Content:   content,
		}}
	}
	a.onMessage(mk(summary), deliver)
	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1", len(got))
	}
	if got[0].ThreadID != "" || got[0].IsThread {
		t.Fatalf("without lookup must stay top-level: %+v", got[0])
	}
	if _, ok := got[0].ReplyCtx.(*taskThreadReplyContext); ok {
		t.Fatalf("without thread must not use task thread reply context")
	}
}

func TestOwnDispatchSummaryHostPrefixRoutesIntoThread(t *testing.T) {
	a := New("fake-token", nil, Config{})
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}
	a.SetTaskThreadLookup(func(repo, branch string) (string, bool) {
		normalized := repo
		if rest, ok := cutoffPrefix(normalized); ok {
			normalized = rest
		}
		if normalized == "acme/widget" && branch == "feat/hosted" {
			return "thread-789", true
		}
		return "", false
	})
	summary := "⏳ Dispatched github.com/acme/widget#feat/hosted · task-1792217637155287504-1\nSpec: specs/x.md"
	var got []channel.IncomingMessage
	deliver := func(m channel.IncomingMessage) { got = append(got, m) }
	a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "chan-1",
		GuildID:   "guild-1",
		Author:    &discordgo.User{ID: "bot-42"},
		Content:   summary,
	}}, deliver)
	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1", len(got))
	}
	if got[0].ThreadID != "thread-789" || !got[0].IsThread {
		t.Fatalf("host summary must route: %+v", got[0])
	}
}

func cutoffPrefix(s string) (string, bool) {
	for _, p := range []string{"github.com/", "https://github.com/", "http://github.com/"} {
		if len(s) >= len(p) && equalFoldPrefix(s[:len(p)], p) {
			return s[len(p):], true
		}
	}
	return s, false
}

func equalFoldPrefix(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func TestOwnDispatchSummaryMissDetailPath(t *testing.T) {
	a := New("fake-token", nil, Config{})
	a.onReady(&discordgo.Ready{User: &discordgo.User{ID: "bot-42"}})
	a.channelLookup = func(string) (*discordgo.Channel, error) {
		return &discordgo.Channel{Type: discordgo.ChannelTypeGuildText}, nil
	}
	a.SetTaskThreadLookup(func(string, string) (string, bool) { return "", false })
	called := false
	a.SetTaskThreadMissDetail(func(repo, branch string) (string, string, string, bool, bool) {
		called = true
		return repo + "#" + branch, "acme/widget#feat/x", "thread-1", true, false
	})
	summary := "⏳ Dispatched acme/widget#feat/y · task-999\nSpec: specs/y.md"
	var got []channel.IncomingMessage
	a.onMessage(&discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "chan-1",
		Author:    &discordgo.User{ID: "bot-42"},
		Content:   summary,
	}}, func(m channel.IncomingMessage) { got = append(got, m) })
	if len(got) != 1 || got[0].IsThread {
		t.Fatalf("miss must stay top-level: %+v", got)
	}
	if !called {
		t.Fatalf("miss detail must be consulted on lookup miss")
	}
}
