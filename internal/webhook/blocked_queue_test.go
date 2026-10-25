package webhook

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

type surfaceLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *surfaceLog) add(text string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, text)
}

func (l *surfaceLog) find(needle string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines {
		if strings.Contains(line, needle) {
			out = append(out, line)
		}
	}
	return out
}

func waitForLoggedSurface(t *testing.T, log *surfaceLog, needle string, want int) []string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		found := log.find(needle)
		if len(found) >= want {
			return found
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("surface %q never reached %d lines", needle, want)
	return nil
}

func TestBlockedQueueSkipNamesPRAndBlockReason(t *testing.T) {
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		return fmt.Errorf("model blew up: %w", relay.ErrWebhookAgentResponse)
	}
	srv, st, url := newRetryHarness(t, exec)
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureModel: {10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond},
		failureStall: {0},
		failureBuild: {10 * time.Millisecond, 10 * time.Millisecond},
	}
	parents := &surfaceLog{}
	threads := &surfaceLog{}
	notices := &surfaceLog{}
	srv.SetSenderWithButtons(func(_ context.Context, _, _, text string, _ []channel.Button) (string, error) {
		parents.add(text)
		return "parent-1", nil
	})
	srv.SetEditorWithButtons(func(_ context.Context, _, _, _ string, text string, _ []channel.Button) error {
		parents.add(text)
		return nil
	})
	srv.SetSender(func(_ context.Context, _, _, text string) (string, error) {
		threads.add(text)
		return "status-1", nil
	})
	srv.SetEditor(func(_ context.Context, _, _, _ string, text string) error {
		threads.add(text)
		return nil
	})
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		notices.add(text)
		return nil
	})

	first := prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-blocked-1")
	if resp := post(t, url+"/review?secret=s3cret", "del-blocked-1", "pull_request_review", first); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-blocked-1", store.WebhookStatusFailed)

	row, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil || row.Status != store.PRStatusBlocked {
		t.Fatalf("pr row = %+v, %v; want a blocked queue", row, err)
	}
	reason := strings.TrimSpace(row.BlockedReason)
	if reason == "" {
		t.Fatal("a blocked queue must carry the block reason")
	}

	second := prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-blocked-2")
	if resp := post(t, url+"/review?secret=s3cret", "del-blocked-2", "pull_request_review", second); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-blocked-2", store.WebhookStatusSkipped)

	if got := atomic.LoadInt64(&calls); got != 4 {
		t.Fatalf("executor calls = %d, want 1 + 3 retries, the blocked queue must not run again", got)
	}

	if skips := notices.find("REVIEW skipped"); len(skips) != 0 {
		t.Errorf("a skipped delivery must stay silent, got %v", skips)
	}

	states := threads.find(threadStatusPrefix)
	if len(states) == 0 {
		t.Fatal("the blocked PR must carry a state line")
	}
	status := states[len(states)-1]
	for _, want := range []string{"acme/widget#7", reason, "ACTION:"} {
		if !strings.Contains(status, want) {
			t.Errorf("state line %q must carry %q", status, want)
		}
	}
	if strings.Contains(status, "waiting: review run active") {
		t.Errorf("state line %q must not name a run that is not running", status)
	}
}
