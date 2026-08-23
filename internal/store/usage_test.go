package store

import (
	"context"
	"testing"
)

func TestUsageProjectionStoresDeltasAndUnknownCost(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.UsageRepo()

	first := UsageSnapshot{
		Platform: "telegram", ChannelID: "chat1", UserID: "alice", SessionID: "sess-a",
		Model: "openai/gpt", Workdir: "/repo", Input: 100, Output: 20, Reasoning: 4,
		CacheRead: 30, CacheWrite: 5, Cost: 0.10, CostKnown: true, RecordedAt: 1000,
	}
	if err := repo.RecordSnapshot(ctx, first); err != nil {
		t.Fatalf("record first: %v", err)
	}
	second := first
	second.Input = 160
	second.Output = 35
	second.Reasoning = 9
	second.CacheRead = 50
	second.CacheWrite = 8
	second.Cost = 0.25
	second.RecordedAt = 1010
	if err := repo.RecordSnapshot(ctx, second); err != nil {
		t.Fatalf("record second: %v", err)
	}
	unknown := second
	unknown.Input = 200
	unknown.Cost = 0
	unknown.CostKnown = false
	unknown.RecordedAt = 1020
	if err := repo.RecordSnapshot(ctx, unknown); err != nil {
		t.Fatalf("record unknown: %v", err)
	}

	report, err := repo.Query(ctx, UsageQuery{Platform: "telegram", ChannelID: "chat1", ThreadID: "", UserID: "alice", Limit: 10})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if report.Totals.Input != 200 || report.Totals.Output != 35 || report.Totals.Reasoning != 9 || report.Totals.CacheRead != 50 || report.Totals.CacheWrite != 8 {
		t.Fatalf("totals = %+v", report.Totals)
	}
	if report.Totals.CostKnown {
		t.Fatal("expected unknown cost after an unpriced snapshot")
	}
	if len(report.Breakdowns) != 1 || report.Breakdowns[0].Model != "openai/gpt" {
		t.Fatalf("breakdowns = %+v", report.Breakdowns)
	}
}

func TestUsageProjectionScopesConversationAndAdminChannel(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.UsageRepo()
	for _, snapshot := range []UsageSnapshot{
		{Platform: "discord", ChannelID: "channel", UserID: "alice", SessionID: "alice-session", Model: "model-a", Input: 10, RecordedAt: 100},
		{Platform: "discord", ChannelID: "channel", UserID: "bob", SessionID: "bob-session", Model: "model-b", Input: 20, RecordedAt: 100},
		{Platform: "discord", ChannelID: "other", UserID: "alice", SessionID: "other-session", Model: "model-c", Input: 40, RecordedAt: 100},
	} {
		if err := repo.RecordSnapshot(ctx, snapshot); err != nil {
			t.Fatalf("record %s: %v", snapshot.SessionID, err)
		}
	}

	conversation, err := repo.Query(ctx, UsageQuery{Platform: "discord", ChannelID: "channel", UserID: "alice", Limit: 10})
	if err != nil {
		t.Fatalf("conversation query: %v", err)
	}
	if conversation.Totals.Input != 10 || conversation.BreakdownTotal != 1 {
		t.Fatalf("conversation report = %+v", conversation)
	}
	channel, err := repo.Query(ctx, UsageQuery{Platform: "discord", ChannelID: "channel", ChannelWide: true, Limit: 10})
	if err != nil {
		t.Fatalf("channel query: %v", err)
	}
	if channel.Totals.Input != 30 || channel.BreakdownTotal != 2 {
		t.Fatalf("channel report = %+v", channel)
	}
}

func TestUsageProjectionPaginationAndRetention(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.UsageRepo()
	for i := 0; i < 7; i++ {
		snapshot := UsageSnapshot{
			Platform: "telegram", ChannelID: "chat", UserID: "user", SessionID: string(rune('a' + i)),
			Model: string(rune('a' + i)), Input: int64(i + 1), RecordedAt: 1000,
		}
		if err := repo.RecordSnapshot(ctx, snapshot); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	page, err := repo.Query(ctx, UsageQuery{Platform: "telegram", ChannelID: "chat", UserID: "user", Limit: 5, Offset: 5})
	if err != nil {
		t.Fatalf("page query: %v", err)
	}
	if page.BreakdownTotal != 7 || len(page.Breakdowns) != 2 {
		t.Fatalf("page report = %+v", page)
	}

	old := UsageSnapshot{Platform: "telegram", ChannelID: "chat", UserID: "user", SessionID: "old", Model: "old", Input: 9, RecordedAt: 1000}
	if err := repo.RecordSnapshot(ctx, old); err != nil {
		t.Fatalf("record old: %v", err)
	}
	fresh := old
	fresh.SessionID = "fresh"
	fresh.RecordedAt = 90*24*60*60 + 1001
	if err := repo.RecordSnapshot(ctx, fresh); err != nil {
		t.Fatalf("record fresh: %v", err)
	}
	retained, err := repo.Query(ctx, UsageQuery{Platform: "telegram", ChannelID: "chat", UserID: "user", Limit: 20})
	if err != nil {
		t.Fatalf("retention query: %v", err)
	}
	if retained.Totals.Input != 7*0+9 {
		t.Fatalf("retention totals = %+v", retained.Totals)
	}
}
