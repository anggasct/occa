package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func webhookDelivery(overrides func(*WebhookDelivery)) WebhookDelivery {
	d := WebhookDelivery{
		Endpoint:    "github-review",
		DeliveryID:  "abc-123",
		EventType:   "pull_request",
		PayloadHash: "deadbeef",
		Attempt:     1,
	}
	if overrides != nil {
		overrides(&d)
	}
	return d
}

func TestWebhookDeliveryCreateAndGet(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	created, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("expected first create to report created=true")
	}

	d, err := s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if d == nil {
		t.Fatal("Get returned nil for created receipt")
	}
	if d.Endpoint != "github-review" || d.DeliveryID != "abc-123" || d.EventType != "pull_request" || d.PayloadHash != "deadbeef" {
		t.Fatalf("unexpected receipt fields: %+v", d)
	}
	if d.Status != WebhookStatusReceived {
		t.Fatalf("status = %q, want received", d.Status)
	}
	if d.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", d.Attempt)
	}
	if d.CreatedAt == 0 || d.UpdatedAt == 0 {
		t.Fatalf("timestamps not set: %+v", d)
	}
}

func TestWebhookDeliveryDuplicateBumpsAttempt(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if created, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil)); err != nil || !created {
		t.Fatalf("first Create: created=%v err=%v", created, err)
	}
	for i := 0; i < 3; i++ {
		created, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil))
		if err != nil {
			t.Fatalf("duplicate Create %d: %v", i, err)
		}
		if created {
			t.Fatalf("duplicate Create %d reported created=true", i)
		}
	}

	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM webhook_delivery WHERE endpoint = 'github-review' AND delivery_id = 'abc-123'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("receipt count = %d, want exactly 1", count)
	}

	d, err := s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if d.Attempt != 4 {
		t.Fatalf("attempt = %d, want 4 after three replays", d.Attempt)
	}
}

func TestWebhookDeliveryTransitionCAS(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if _, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	d, err := s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusReceived, WebhookStatusAccepted}, WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("received->processing: ok=%v err=%v", ok, err)
	}
	d, _ = s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if d.Status != WebhookStatusProcessing || d.StartedAt == 0 {
		t.Fatalf("after claim: status=%q started_at=%d", d.Status, d.StartedAt)
	}

	// A duplicate claim from an unexpected state must not move the row.
	if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusReceived}, WebhookStatusProcessing, ""); err != nil || ok {
		t.Fatalf("stale claim: ok=%v err=%v, want no-op", ok, err)
	}

	if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusProcessing}, WebhookStatusCompleted, ""); err != nil || !ok {
		t.Fatalf("processing->completed: ok=%v err=%v", ok, err)
	}
	d, _ = s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if d.Status != WebhookStatusCompleted || d.CompletedAt == 0 {
		t.Fatalf("after complete: status=%q completed_at=%d", d.Status, d.CompletedAt)
	}

	if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusProcessing}, WebhookStatusFailed, "late failure"); err != nil || ok {
		t.Fatalf("terminal->terminal transition: ok=%v err=%v, want no-op", ok, err)
	}
}

func TestWebhookDeliveryFailCarriesRedactedSummary(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	if _, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	d, _ := s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusReceived, WebhookStatusAccepted}, WebhookStatusProcessing, ""); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusProcessing}, WebhookStatusFailed, "agent unreachable"); err != nil || !ok {
		t.Fatalf("fail: ok=%v err=%v", ok, err)
	}
	d, _ = s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if d.Status != WebhookStatusFailed || d.ErrorSummary != "agent unreachable" || d.CompletedAt == 0 {
		t.Fatalf("failed receipt: %+v", d)
	}
}

func TestWebhookDeliveryListNewestFirst(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(func(d *WebhookDelivery) {
			d.DeliveryID = fmt.Sprintf("del-%d", i)
			d.EventType = fmt.Sprintf("event-%d", i)
		})); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	all, err := s.WebhookDeliveryRepo().List(ctx, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("List len = %d, want 5", len(all))
	}
	if all[0].DeliveryID != "del-4" || all[4].DeliveryID != "del-0" {
		t.Fatalf("List not newest-first: %+v", all)
	}

	limited, err := s.WebhookDeliveryRepo().List(ctx, 2)
	if err != nil {
		t.Fatalf("List limited: %v", err)
	}
	if len(limited) != 2 || limited[0].DeliveryID != "del-4" || limited[1].DeliveryID != "del-3" {
		t.Fatalf("List limit=2: %+v", limited)
	}
}

func TestWebhookDeliveryPruneBounded(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	// Insert rows in order; sqlite autoincrement ids follow insertion order.
	for i := 0; i < 10; i++ {
		d := webhookDelivery(func(d *WebhookDelivery) {
			d.DeliveryID = fmt.Sprintf("del-%d", i)
		})
		if _, err := s.WebhookDeliveryRepo().Create(ctx, d); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Only rows 0..5 are older than cutoff. Pruning with keep=4 must delete
	// every old row that is not among the newest 4 rows overall (ids 6..9),
	// which stay for forensic audit.
	if _, err := s.db.Exec(`UPDATE webhook_delivery SET created_at = 1000 WHERE delivery_id IN ('del-0','del-1','del-2','del-3','del-4','del-5')`); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	pruned, err := s.WebhookDeliveryRepo().Prune(ctx, 2000, 4)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 6 {
		t.Fatalf("pruned = %d, want 6 (del-0..del-5 are old and outside the kept set)", pruned)
	}

	ids := make(map[string]bool)
	rows, err := s.WebhookDeliveryRepo().List(ctx, 20)
	if err != nil {
		t.Fatalf("List after prune: %v", err)
	}
	for _, d := range rows {
		ids[d.DeliveryID] = true
	}
	for _, id := range []string{"del-0", "del-1", "del-2", "del-3", "del-4", "del-5"} {
		if ids[id] {
			t.Fatalf("%s should have been pruned", id)
		}
	}
	for _, id := range []string{"del-6", "del-7", "del-8", "del-9"} {
		if !ids[id] {
			t.Fatalf("%s should be preserved by the keep bound", id)
		}
	}
}

func TestWebhookDeliveryFailStaleRecoversInFlight(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	mk := func(id string, status WebhookStatus) int64 {
		if _, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(func(d *WebhookDelivery) {
			d.DeliveryID = id
		})); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
		d, _ := s.WebhookDeliveryRepo().Get(ctx, "github-review", id)
		if ok, err := s.WebhookDeliveryRepo().Transition(ctx, d.ID, []WebhookStatus{WebhookStatusReceived}, status, ""); err != nil || !ok {
			t.Fatalf("transition %s to %s: ok=%v err=%v", id, status, ok, err)
		}
		return d.ID
	}
	stale := mk("stale", WebhookStatusProcessing)
	mk("fresh", WebhookStatusProcessing)

	if _, err := s.db.Exec(`UPDATE webhook_delivery SET updated_at = 1 WHERE id = ?`, stale); err != nil {
		t.Fatalf("backdate stale row: %v", err)
	}

	failed, err := s.WebhookDeliveryRepo().FailStale(ctx, 500, "interrupted by restart")
	if err != nil {
		t.Fatalf("FailStale: %v", err)
	}
	if failed != 1 {
		t.Fatalf("FailStale recovered %d rows, want 1", failed)
	}

	d, _ := s.WebhookDeliveryRepo().Get(ctx, "github-review", "stale")
	if d.Status != WebhookStatusFailed || d.ErrorSummary != "interrupted by restart" || d.CompletedAt == 0 {
		t.Fatalf("stale receipt not failed: %+v", d)
	}
	fresh, _ := s.WebhookDeliveryRepo().Get(ctx, "github-review", "fresh")
	if fresh.Status != WebhookStatusProcessing {
		t.Fatalf("fresh receipt disturbed: %+v", fresh)
	}
}

func TestWebhookDeliveryRestartRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhooks.db")

	s, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	if _, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	d, err := s2.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if d == nil || d.Endpoint != "github-review" || d.DeliveryID != "abc-123" {
		t.Fatalf("receipt did not survive restart: %+v", d)
	}
	assertVersion(t, s2, schemaVersion)
}

func TestWebhookDeliveryConcurrentDuplicates(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	const workers = 12
	var wg sync.WaitGroup
	createdCount := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil))
			if err != nil {
				t.Errorf("concurrent Create: %v", err)
				return
			}
			createdCount <- created
		}()
	}
	wg.Wait()
	close(createdCount)

	createdTotal := 0
	for created := range createdCount {
		if created {
			createdTotal++
		}
	}
	if createdTotal != 1 {
		t.Fatalf("concurrent creates produced %d receipts, want exactly 1", createdTotal)
	}

	d, err := s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if d.Attempt != workers {
		t.Fatalf("attempt = %d, want %d (one bump per duplicate)", d.Attempt, workers)
	}
}
