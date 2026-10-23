package webhook

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/store"
)

func TestMergedDeliveryRetriesLookupFailure(t *testing.T) {
	turns := make(chan string, 4)
	srv, st, _, _ := newIsolationServer(t, func(_ context.Context, _, _ string, _ string, workCtx *WebhookWorkContext) error {
		turns <- workCtx.ThreadID
		return nil
	})
	var lookups atomic.Int32
	srv.SetPRStateResolver(func(context.Context, string, string) (string, error) {
		if lookups.Add(1) == 1 {
			return "", errors.New("gh unavailable")
		}
		return PRStateMerged, nil
	})
	srv.replayBackoff = []time.Duration{200 * time.Millisecond}

	ctx := context.Background()
	prKey := "acme/widget#7"
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-7", "root-7"); err != nil {
		t.Fatalf("ensure pr binding: %v", err)
	}
	ep := srv.endpoints["/merged"]
	deliveryID := "del-merged-retry"
	seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request")
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, ep.Name, deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	item := dispatchItem{ep: ep, body: []byte(mergedBody(7)), deliveryID: deliveryID, eventType: "pull_request", payloadHash: deliveryID, prKey: prKey}
	envelope := normalizeWebhook(item.body, item.eventType, item.deliveryID, srv.verdicts, false, "", ep.CommentTrigger)

	srv.handleMergedDelivery(item, receipt, envelope, nil)

	after, err := st.WebhookDeliveryRepo().Get(ctx, ep.Name, deliveryID)
	if err != nil || after == nil {
		t.Fatalf("get receipt after lookup failure: %v", err)
	}
	if after.Status == store.WebhookStatusSkipped || after.Status == store.WebhookStatusFailed {
		t.Fatalf("lookup failure became terminal: status = %s", after.Status)
	}

	waitForDeliveryStatus(t, st, ep.Name, deliveryID, store.WebhookStatusCompleted)
	select {
	case threadID := <-turns:
		if threadID != "thread-7" {
			t.Fatalf("merged docs thread = %q, want thread-7", threadID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("post-merge work never ran after the lookup recovered")
	}
	if got := lookups.Load(); got < 2 {
		t.Fatalf("lookups = %d, want a retry", got)
	}
}
