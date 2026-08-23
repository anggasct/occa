package router

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

type fakeUsageRepo struct {
	queries   []store.UsageQuery
	report    store.UsageReport
	records   []store.UsageSnapshot
	queryErr  error
	recordErr error
}

func (f *fakeUsageRepo) RecordSnapshot(_ context.Context, snapshot store.UsageSnapshot) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	f.records = append(f.records, snapshot)
	return nil
}

func (f *fakeUsageRepo) Query(_ context.Context, query store.UsageQuery) (store.UsageReport, error) {
	f.queries = append(f.queries, query)
	return f.report, f.queryErr
}

var _ store.UsageRepo = (*fakeUsageRepo)(nil)

func TestUsageViewUsesConversationScopeAndNeverNeedsAgent(t *testing.T) {
	r, _, reply, overrides := newTestRouterWithAccess()
	usage := &fakeUsageRepo{report: store.UsageReport{
		Totals:         store.UsageTotals{Input: 120, Output: 30, Reasoning: 4, CacheRead: 8, CacheWrite: 2, Cost: 0.12, CostKnown: true},
		Breakdowns:     []store.UsageBreakdown{{Model: "openai/gpt", Workdir: "/repo", Input: 120, Cost: 0.12, CostKnown: true}},
		BreakdownTotal: 1,
	}}
	r.store.(*fakeStore).usage = usage
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Role: "allow"}

	if err := r.Route(context.Background(), msg("/usage", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || len(reply.buttons) != 1 {
		t.Fatalf("reply = sends %d buttons %d", len(reply.sends), len(reply.buttons))
	}
	if !contains(reply.sends[0], "Input: 0.1k cumulative") || !contains(reply.sends[0], "Estimated cost: $0.12") {
		t.Fatalf("usage text = %q", reply.sends[0])
	}
	if len(usage.queries) != 1 {
		t.Fatalf("queries = %d", len(usage.queries))
	}
	query := usage.queries[0]
	if query.ChannelWide || query.Platform != "telegram" || query.ChannelID != "chat1" || query.UserID != "user1" || query.ThreadID != "" {
		t.Fatalf("query scope = %+v", query)
	}
	if r.instances.(*fakeInstanceProvider).calls != 0 {
		t.Fatal("/usage must not resolve an agent instance")
	}
}

func TestUsageAdminGetsChannelScopeAndUnknownCost(t *testing.T) {
	r, _, _, overrides := newTestRouterWithAccess()
	usage := &fakeUsageRepo{report: store.UsageReport{Totals: store.UsageTotals{Input: 10, CostKnown: false}, BreakdownTotal: 1}}
	r.store.(*fakeStore).usage = usage
	overrides.overrides["telegram:chat1:admin"] = &store.UserOverride{Role: "admin"}
	msg := msgFrom("admin", "/usage 7d", &fakeReplyCtx{})

	if _, err := r.handleUsage(context.Background(), msg, "7d"); err != errReplied {
		t.Fatalf("handle usage err = %v, want errReplied", err)
	}
	if len(usage.queries) != 1 || !usage.queries[0].ChannelWide {
		t.Fatalf("admin query = %+v", usage.queries)
	}
	if !contains(msg.ReplyCtx.(*fakeReplyCtx).sends[0], "Estimated cost: unknown") {
		t.Fatalf("unknown cost text = %q", msg.ReplyCtx.(*fakeReplyCtx).sends[0])
	}
}

func TestUsageCallbackPaginatesAndKeepsScope(t *testing.T) {
	r, _, _, overrides := newTestRouterWithAccess()
	usage := &fakeUsageRepo{report: store.UsageReport{
		Totals:         store.UsageTotals{Input: 1},
		BreakdownTotal: 7,
	}}
	r.store.(*fakeStore).usage = usage
	overrides.overrides["telegram:chat1:user1"] = &store.UserOverride{Role: "allow"}
	reply := &fakeReplyCtx{}
	msg := msgFrom("user1", "", reply)
	msg.IsCallback = true
	msg.CallbackData = "usage:7d:2"
	msg.CallbackRef = fakeRef{id: "usage-message"}

	if err := r.Route(context.Background(), msg); err != nil {
		t.Fatalf("Route callback: %v", err)
	}
	if len(reply.edits) != 1 || len(usage.queries) != 1 {
		t.Fatalf("edits=%d queries=%d", len(reply.edits), len(usage.queries))
	}
	if usage.queries[0].Offset != usagePageSize || usage.queries[0].Since == 0 || usage.queries[0].ChannelWide {
		t.Fatalf("callback query = %+v", usage.queries[0])
	}
}

func contains(text, part string) bool {
	for i := 0; i+len(part) <= len(text); i++ {
		if text[i:i+len(part)] == part {
			return true
		}
	}
	return false
}

var _ channel.ReplyContext = (*fakeReplyCtx)(nil)
