package webhook

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

const providerLimitDetail = "monthly usage limit reached (provider opencode-go, next attempt 2026-11-18T08:00:00Z)"

// IMP-115 AC-04: a provider limit blocks the PR at once with the provider's
// message; no retry is spent.
func TestProviderLimitRunHandsOffWithoutRetry(t *testing.T) {
	run := runFixLane(t, false, func(workCtx *WebhookWorkContext) error {
		return fmt.Errorf("%w: %s", relay.ErrWebhookProviderLimit, providerLimitDetail)
	})
	if calls := run.callCount(); calls != 1 {
		t.Fatalf("executor calls = %d, want 1 (no retry)", calls)
	}
	row := run.row(t)
	if row.Status != store.PRStatusBlocked || row.RetryCount != 0 {
		t.Fatalf("pr row = %+v, want blocked with no retry spent", row)
	}
	if want := "fix stopped on a provider limit — operator action required: " + providerLimitDetail; row.BlockedReason != want {
		t.Fatalf("blocked reason = %q, want %q", row.BlockedReason, want)
	}
	receipt, err := run.store.WebhookDeliveryRepo().Get(context.Background(), "github-widget-pr-fix", "del-nowork")
	if err != nil || receipt == nil || !strings.HasPrefix(receipt.ErrorSummary, "webhook provider limit") {
		t.Fatalf("receipt = %+v %v, want a failure naming the provider limit", receipt, err)
	}
	logs := run.logs.String()
	if !strings.Contains(logs, "PR handed off, provider limit") || strings.Contains(logs, "retry scheduled") {
		t.Fatalf("logs: %s", logs)
	}
}

// IMP-115 AC-04: classification and operator copy.
func TestProviderLimitClassAndCopy(t *testing.T) {
	err := fmt.Errorf("executor: %w", fmt.Errorf("%w: %s", relay.ErrWebhookProviderLimit, providerLimitDetail))
	if got := classifyFailure(err, nil, 0, false); got != failureProviderLimit {
		t.Fatalf("class = %v, want provider limit", got.label())
	}
	c := describeBlock("review stopped on a provider limit — operator action required: " + providerLimitDetail)
	if c.Headline != "The review run was refused by the model provider (limit reached)." || c.Detail != "Provider said: "+providerLimitDetail || !strings.Contains(c.Next, "Restore the provider quota or switch the model") {
		t.Fatalf("copy = %+v", c)
	}
}
