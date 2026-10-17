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
