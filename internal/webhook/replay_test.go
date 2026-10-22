package webhook

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestReplayExecutesRejectedDelivery(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	srv.SetReplayChecker(func(context.Context, store.WebhookReplay) (replayDecision, error) {
		return replayGo, nil
	})
	ctx := context.Background()
	deliveryID := "del-rejected-replay"
	if _, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{
		Endpoint:     "github",
		DeliveryID:   deliveryID,
		EventType:    "pull_request",
		PayloadHash:  "hash-rejected",
		Status:       store.WebhookStatusRejected,
		ErrorSummary: "state lane at capacity",
	}); err != nil {
		t.Fatalf("create rejected delivery: %v", err)
	}
	now := time.Now().Unix()
	if _, _, err := st.WebhookReplayRepo().Create(ctx, store.WebhookReplay{
		Endpoint:      "github",
		DeliveryID:    deliveryID,
		EventType:     "pull_request",
		Body:          []byte(`{"action":"opened"}`),
		Reason:        "state lane at capacity",
		NextAttemptAt: now - 1,
		ExpiresAt:     now + 600,
		Status:        store.WebhookReplayPending,
	}); err != nil {
		t.Fatalf("create replay: %v", err)
	}

	srv.replayPending(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && exec.callCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if exec.callCount() != 1 {
		t.Fatalf("replayed delivery executions = %d, want 1", exec.callCount())
	}
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, "github", deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("replayed receipt missing: %v", err)
	}
	if receipt.Status == store.WebhookStatusRejected {
		t.Fatal("replayed receipt still rejected")
	}
	replayed, err := st.WebhookReplayRepo().Count(ctx, store.WebhookReplayReplayed)
	if err != nil || replayed != 1 {
		t.Fatalf("replayed rows = %d, err = %v, want 1", replayed, err)
	}
}

func TestReplayBackoffSequence(t *testing.T) {
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
	})
	srv.SetWebhookReplayRepo(st.WebhookReplayRepo())
	ctx := context.Background()
	now := time.Now().Unix()
	if got := srv.replayBackoffFor(0); got != 30*time.Second {
		t.Fatalf("first backoff = %s, want 30s", got)
	}
	if _, _, err := st.WebhookReplayRepo().Create(ctx, store.WebhookReplay{
		Endpoint:      "github",
		DeliveryID:    "del-backoff",
		EventType:     "pull_request",
		Body:          []byte(`{"action":"opened"}`),
		Reason:        "state lane at capacity",
		NextAttemptAt: now + int64(srv.replayBackoffFor(0)/time.Second),
		ExpiresAt:     now + 24*3600,
		Status:        store.WebhookReplayPending,
	}); err != nil {
		t.Fatalf("create replay: %v", err)
	}

	row := mustPendingReplay(t, st, "del-backoff")
	if delta := row.NextAttemptAt - now; delta != 30 {
		t.Fatalf("initial next attempt in %ds, want 30s", delta)
	}

	before := time.Now().Unix()
	srv.rescheduleReplay(ctx, *row)
	row = mustPendingReplay(t, st, "del-backoff")
	assertReplayDelay(t, row.NextAttemptAt, before, 2*time.Minute)

	before = time.Now().Unix()
	srv.rescheduleReplay(ctx, *row)
	row = mustPendingReplay(t, st, "del-backoff")
	assertReplayDelay(t, row.NextAttemptAt, before, 10*time.Minute)

	srv.rescheduleReplay(ctx, *row)
	if pending, err := st.WebhookReplayRepo().Count(ctx, store.WebhookReplayPending); err != nil || pending != 0 {
		t.Fatalf("pending rows = %d, err = %v, want 0", pending, err)
	}
	if expired, err := st.WebhookReplayRepo().Count(ctx, store.WebhookReplayExpired); err != nil || expired != 1 {
		t.Fatalf("expired rows = %d, err = %v, want 1", expired, err)
	}
}

func mustPendingReplay(t *testing.T, st *store.SQLiteStore, deliveryID string) *store.WebhookReplay {
	t.Helper()
	rows, err := st.WebhookReplayRepo().ListDue(context.Background(), time.Now().Add(48*time.Hour).Unix(), 10)
	if err != nil {
		t.Fatalf("list due: %v", err)
	}
	for i := range rows {
		if rows[i].DeliveryID == deliveryID {
			return &rows[i]
		}
	}
	t.Fatalf("pending replay %s not found: %+v", deliveryID, rows)
	return nil
}

func assertReplayDelay(t *testing.T, next, before int64, want time.Duration) {
	t.Helper()
	delta := next - before
	wantSeconds := int64(want / time.Second)
	if delta < wantSeconds-1 || delta > wantSeconds+1 {
		t.Fatalf("next attempt in %ds, want %s", delta, want)
	}
}

func TestReplayRelevanceDefaultChecker(t *testing.T) {
	prev := replayGHRun
	t.Cleanup(func() { replayGHRun = prev })

	type ghResponse struct {
		match string
		out   string
		err   error
	}
	manyReviewsPage1 := func() string {
		reviews := []string{`{"id":10,"state":"CHANGES_REQUESTED"}`}
		for i := 0; i < 99; i++ {
			reviews = append(reviews, fmt.Sprintf(`{"id":%d,"state":"COMMENTED"}`, 100+i))
		}
		return "[" + strings.Join(reviews, ",") + "]"
	}()
	tests := []struct {
		name      string
		eventType string
		body      string
		responses []ghResponse
		want      replayDecision
		wantErr   bool
	}{
		{
			name:      "open PR on current head",
			eventType: "pull_request",
			body:      `{"pull_request":{"head":{"sha":"aaaaaaa"}}}`,
			responses: []ghResponse{{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`}},
			want:      replayGo,
		},
		{
			name:      "open PR on newer descendant head",
			eventType: "pull_request",
			body:      `{"pull_request":{"head":{"sha":"aaaaaaa"}}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"bbbbbbb"}`},
				{match: "compare/aaaaaaa...bbbbbbb", out: `{"status":"ahead"}`},
			},
			want: replayGo,
		},
		{
			name:      "open PR on stale head",
			eventType: "pull_request",
			body:      `{"pull_request":{"head":{"sha":"bbbbbbb"}}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`},
				{match: "compare/bbbbbbb...aaaaaaa", out: `{"status":"behind"}`},
			},
			want: replaySuperseded,
		},
		{
			name:      "open PR on diverged head",
			eventType: "pull_request",
			body:      `{"pull_request":{"head":{"sha":"bbbbbbb"}}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`},
				{match: "compare/bbbbbbb...aaaaaaa", out: `{"status":"diverged"}`},
			},
			want: replaySuperseded,
		},
		{
			name:      "ancestry undecidable fails closed",
			eventType: "pull_request",
			body:      `{"pull_request":{"head":{"sha":"aaaaaaa"}}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"bbbbbbb"}`},
				{match: "compare/aaaaaaa...bbbbbbb", err: fmt.Errorf("gh api: HTTP 500")},
			},
			want:    replayUndecidable,
			wantErr: true,
		},
		{
			name:      "missing stored head fails closed",
			eventType: "pull_request",
			body:      `{"pull_request":{"head":{}}}`,
			responses: []ghResponse{{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`}},
			want:      replayUndecidable,
		},
		{
			name:      "check suite on stale head",
			eventType: "check_suite",
			body:      `{"check_suite":{"head_sha":"bbbbbbb"}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`},
				{match: "compare/bbbbbbb...aaaaaaa", out: `{"status":"behind"}`},
			},
			want: replaySuperseded,
		},
		{
			name:      "latest blocking review",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{{match: "pulls/7/reviews", out: `[{"id":10,"state":"CHANGES_REQUESTED"}]`}},
			want:      replayGo,
		},
		{
			name:      "dismissed review",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{{match: "pulls/7/reviews", out: `[{"id":10,"state":"DISMISSED"}]`}},
			want:      replaySuperseded,
		},
		{
			name:      "non-verdict review",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{{match: "pulls/7/reviews", out: `[{"id":10,"state":"COMMENTED"}]`}},
			want:      replaySuperseded,
		},
		{
			name:      "later comment review does not supersede verdict",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{{match: "pulls/7/reviews", out: `[{"id":10,"state":"APPROVED"},{"id":11,"state":"COMMENTED"}]`}},
			want:      replayGo,
		},
		{
			name:      "another reviewer comment does not supersede verdict",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{{match: "pulls/7/reviews", out: `[{"id":10,"state":"CHANGES_REQUESTED"},{"id":11,"state":"COMMENTED","user":{"login":"other"}}]`}},
			want:      replayGo,
		},
		{
			name:      "later actionable review supersedes verdict",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{{match: "pulls/7/reviews", out: `[{"id":10,"state":"CHANGES_REQUESTED"},{"id":11,"state":"APPROVED"}]`}},
			want:      replaySuperseded,
		},
		{
			name:      "reviews past the first page",
			eventType: "pull_request_review",
			body:      `{"review":{"id":10}}`,
			responses: []ghResponse{
				{match: "page=1", out: manyReviewsPage1},
				{match: "page=2", out: `[{"id":200,"state":"COMMENTED"}]`},
			},
			want: replayGo,
		},
		{
			name:      "existing issue comment",
			eventType: "issue_comment",
			body:      `{"comment":{"id":99}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`},
				{match: "issues/comments/99", out: `{"id":99}`},
			},
			want: replayGo,
		},
		{
			name:      "deleted issue comment",
			eventType: "issue_comment",
			body:      `{"comment":{"id":99}}`,
			responses: []ghResponse{
				{match: "pr view", out: `{"state":"OPEN","headRefOid":"aaaaaaa"}`},
				{match: "issues/comments/99", err: fmt.Errorf("gh api: HTTP 404: Not Found")},
			},
			want: replaySuperseded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replayGHRun = func(_ context.Context, args ...string) ([]byte, error) {
				joined := strings.Join(args, " ")
				for _, response := range tt.responses {
					if strings.Contains(joined, response.match) {
						return []byte(response.out), response.err
					}
				}
				return nil, fmt.Errorf("unexpected gh invocation: %s", joined)
			}
			got, err := defaultReplayChecker(context.Background(), store.WebhookReplay{
				PRKey:     "acme/widget#7",
				EventType: tt.eventType,
				Body:      []byte(tt.body),
			})
			if err != nil && !tt.wantErr {
				t.Fatalf("checker error: %v", err)
			}
			if err == nil && tt.wantErr {
				t.Fatal("checker returned no error, want an undecidable result")
			}
			if got != tt.want {
				t.Fatalf("decision = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRejectionNotificationDoesNotHoldAdmissionLock(t *testing.T) {
	srv, _, _ := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github", Path: "/github", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Analyze"},
		{Name: "github-review", Path: "/review", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "Review", Workflow: "review"},
	})
	srv.maxQueuedPerKey = 1
	srv.processingTimeout = 50 * time.Millisecond

	started := make(chan struct{})
	var once sync.Once
	srv.SetNotifier(func(ctx context.Context, _, _, _ string) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	})

	rejected := laneItem("custom", "del-blocked-notify", 3, "pull_request", "edited", "")
	rejected.ep = srv.endpointsByName["github"]
	go func() {
		_, _, _ = srv.enqueue(rejected.ep, WebhookExecutionKey{}, rejected)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("rejection notification never started")
	}

	admitted := make(chan bool, 1)
	go func() {
		second := laneItem("review", "del-while-notify", 1, "pull_request_review", "submitted", "")
		second.ep = srv.endpointsByName["github-review"]
		_, ok, _ := srv.enqueue(second.ep, WebhookExecutionKey{}, second)
		admitted <- ok
	}()
	select {
	case ok := <-admitted:
		if !ok {
			t.Fatal("second delivery was not admitted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admission lock held while notification was blocked")
	}
}
