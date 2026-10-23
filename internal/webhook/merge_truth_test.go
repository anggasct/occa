package webhook

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

func TestMergeLaneRecordsMergedOnlyAfterAPIConfirmation(t *testing.T) {
	logs := captureServerLogs(t)
	srv, st, _ := newSurfaceServer(t)
	setPRState(srv, PRStateOpen, nil)
	ctx := context.Background()
	prKey := "acme/widget#181"
	ep := srv.endpoints["/merge"]
	ep.Admit = defaultCatchAllAdmit()
	srv.endpoints["/merge"] = ep
	body := []byte(prReviewBody(181, "feat/x", "owner", "approved", "c0ffee"))
	deliveryID := "del-merge-refused"
	receiptID := seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request_review")

	if err := srv.executeDelivery(ep, body, receiptID, deliveryID, "pull_request_review", 1, nil, func() bool { return false }, nil); err != nil {
		t.Fatalf("execute merge lane: %v", err)
	}

	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseMerge {
		t.Fatalf("phase = %q (known=%v), want %q for an unconfirmed merge", phase, ok, PhaseMerge)
	}
	warnings := logs.all("webhook: merge not confirmed")
	if len(warnings) != 1 {
		t.Fatalf("merge not confirmed warnings = %d, want exactly one", len(warnings))
	}

	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "root-9"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	srv.SetReconcileFacts(func(context.Context, string, string) (ReconcileFacts, error) {
		return ReconcileFacts{
			State:         "OPEN",
			HeadSHA:       "c0ffee",
			HeadRef:       "feat/x",
			BaseRef:       "main",
			Title:         "T",
			URL:           "https://github.com/acme/widget/pull/181",
			CIGreen:       true,
			ReviewVerdict: "approved",
		}, nil
	})
	if srv.reconcileLanePending(ctx, prKey, "merge") {
		t.Fatal("a refused merge must stay eligible for reconcile")
	}
	srv.shutdownCancel()
	srv.reconcileOnce(ctx)
	if len(logs.all("webhook: reconcile enqueued")) == 0 {
		t.Fatal("the next reconcile tick must enqueue the refused merge")
	}
}

func TestMergedDocsUpdatePostsIntoPRThreadBinding(t *testing.T) {
	turn := make(chan WebhookWorkContext, 1)
	exec := func(_ context.Context, _, _ string, prompt string, workCtx *WebhookWorkContext) error {
		turn <- *workCtx
		return nil
	}
	srv, st, _, _ := newIsolationServer(t, exec)
	setPRState(srv, PRStateMerged, nil)
	ctx := context.Background()
	prKey := "acme/widget#7"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-7", "root-7"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	ep := srv.endpoints["/merged"]
	deliveryID := "del-merged-thread"
	seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request")
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, ep.Name, deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	item := dispatchItem{ep: ep, body: []byte(mergedBody(7)), deliveryID: deliveryID, eventType: "pull_request", payloadHash: deliveryID, prKey: prKey}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, srv.verdicts, false, "", ep.CommentTrigger)
	srv.handleMergedDelivery(item, receipt, envelope, nil)

	select {
	case workCtx := <-turn:
		if workCtx.ThreadID != "thread-7" {
			t.Fatalf("merged docs thread = %q, want the persisted binding thread-7", workCtx.ThreadID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the post-merge work never ran")
	}
}

func TestWebhookACKP99UnderConcurrentWrites(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	srv.SetSpoolDir(filepath.Join(t.TempDir(), "spool"))

	writeCtx, stopWrites := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; writeCtx.Err() == nil; i++ {
			_ = st.SessionRepo().SetActive(writeCtx, "telegram", "chat1", "thread-w", "user-w", fmt.Sprintf("ses-%d", i), 4242)
			time.Sleep(time.Millisecond)
		}
	}()

	const deliveries = 20
	latencies := make([]time.Duration, 0, deliveries)
	for i := 0; i < deliveries; i++ {
		start := time.Now()
		resp := post(t, url+"/review?secret=s3cret",
			fmt.Sprintf("del-ack-%02d", i),
			"pull_request_review",
			prReviewBody(100+i, "feat/x", "owner", "commented", fmt.Sprintf("c0ffee%02d", i)))
		latencies = append(latencies, time.Since(start))
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			stopWrites()
			wg.Wait()
			t.Fatalf("delivery %d status = %d, want 200 or 202", i, resp.StatusCode)
		}
	}
	stopWrites()
	wg.Wait()
	srv.drainSpool(context.Background())

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[int(math.Ceil(0.99*float64(len(latencies))))-1]
	t.Logf("AC-01 p99 = %s over %d deliveries under concurrent writes", p99, deliveries)
	if raceDetectorEnabled {
		t.Logf("AC-01 p99 bound skipped under the race detector")
	} else if p99 >= 2*time.Second {
		t.Fatalf("AC-01 p99 = %s, want < 2s", p99)
	}
	for i := 0; i < deliveries; i++ {
		got, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-review", fmt.Sprintf("del-ack-%02d", i))
		if err != nil || got == nil {
			t.Fatalf("delivery %d receipt missing: %+v %v", i, got, err)
		}
	}
}
