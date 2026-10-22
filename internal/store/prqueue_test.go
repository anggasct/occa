package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPRQueueSchemaVersion(t *testing.T) {
	s := tempStore(t)
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != 21 {
		t.Fatalf("schema version = %d, want 21", version)
	}
}

func TestPRQueueEnsureIdempotent(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()

	first, err := repo.EnsurePR(ctx, "acme/widget#42", "thread-1", "root-1")
	if err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if first.ThreadID != "thread-1" || first.RootMsgID != "root-1" || first.Status != PRStatusActive {
		t.Fatalf("unexpected row: %+v", first)
	}
	second, err := repo.EnsurePR(ctx, "acme/widget#42", "thread-2", "root-2")
	if err != nil {
		t.Fatalf("EnsurePR again: %v", err)
	}
	if second.ThreadID != "thread-1" {
		t.Fatalf("second ensure overwrote thread: %+v", second)
	}
}

func TestPRQueuePriorityOrder(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()
	const key = "acme/widget#7"

	for _, item := range []struct {
		endpoint string
		priority int
		delivery int64
	}{
		{"github-acme-merge", 4, 1},
		{"github-acme-fix", 3, 2},
		{"github-acme-review", 2, 3},
		{"github-acme-fix", 3, 4},
		{"dispatch", 1, 5},
	} {
		if _, err := repo.Enqueue(ctx, key, item.endpoint, "check_suite", item.delivery, item.priority); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	var order []int64
	for range 5 {
		next, err := repo.DequeueNext(ctx, key)
		if err != nil {
			t.Fatalf("DequeueNext: %v", err)
		}
		if next == nil {
			t.Fatalf("queue drained early after %d pops", len(order))
		} else {
			order = append(order, next.DeliveryID)
		}
	}
	for i, want := range []int64{5, 3, 2, 4, 1} {
		if order[i] != want {
			t.Fatalf("pop %d delivery = %d, want %d (order %v)", i, order[i], want, order)
		}
	}
	if next, err := repo.DequeueNext(ctx, key); err != nil || next != nil {
		t.Fatalf("DequeueNext on empty = %v, %v; want nil, nil", next, err)
	}
}

func TestPRQueueActiveLifecycle(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()
	const key = "acme/widget#9"

	if _, err := repo.EnsurePR(ctx, key, "thread-9", "root-9"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if err := repo.SetActive(ctx, key, "fix", "session-1", 11); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	run, err := repo.Get(ctx, key)
	if err != nil || run == nil {
		t.Fatalf("Get: %v (%+v)", err, run)
	}
	if run.ActiveKind != "fix" || run.ActiveSession != "session-1" || run.ActiveDelivery != 11 || run.Status != PRStatusActive {
		t.Fatalf("unexpected active row: %+v", run)
	}
	if err := repo.SetActive(ctx, "acme/widget#missing", "fix", "s", 1); err == nil {
		t.Fatalf("SetActive on unknown pr_key succeeded, want error")
	}

	if _, err := repo.Enqueue(ctx, key, "github-acme-merge", "check_suite", 12, 4); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := repo.ClearActive(ctx, key); err != nil {
		t.Fatalf("ClearActive: %v", err)
	}
	run, _ = repo.Get(ctx, key)
	if run.Status != PRStatusActive || run.ActiveSession != "" {
		t.Fatalf("clear with queued item should stay active and empty: %+v", run)
	}
	if _, err := repo.DequeueNext(ctx, key); err != nil {
		t.Fatalf("DequeueNext: %v", err)
	}
	if err := repo.ClearActive(ctx, key); err != nil {
		t.Fatalf("ClearActive: %v", err)
	}
	run, _ = repo.Get(ctx, key)
	if run.Status != PRStatusDrained {
		t.Fatalf("clear with empty queue should drain: %+v", run)
	}
}

func TestPRQueueBlocked(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()
	const key = "acme/widget#11"

	if _, err := repo.EnsurePR(ctx, key, "thread-11", "root-11"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if err := repo.SetBlocked(ctx, key, "provider stall ×3"); err != nil {
		t.Fatalf("SetBlocked: %v", err)
	}
	run, _ := repo.Get(ctx, key)
	if run.Status != PRStatusBlocked || run.BlockedReason != "provider stall ×3" {
		t.Fatalf("unexpected blocked row: %+v", run)
	}
	if err := repo.SetActive(ctx, key, "fix", "session-2", 13); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
	run, _ = repo.Get(ctx, key)
	if run.Status != PRStatusActive || run.BlockedReason != "" {
		t.Fatalf("SetActive should clear blocked state: %+v", run)
	}
	if err := repo.SetBlocked(ctx, "acme/widget#missing", "x"); err == nil {
		t.Fatalf("SetBlocked on unknown pr_key succeeded, want error")
	}
}

func TestPRQueueListAndPrune(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()
	const key = "acme/widget#13"

	if _, err := repo.Enqueue(ctx, key, "github-acme-review", "pull_request_review", 21, 2); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := repo.Enqueue(ctx, key, "github-acme-merge", "check_suite", 22, 4); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	items, err := repo.ListItems(ctx, key)
	if err != nil || len(items) != 2 {
		t.Fatalf("ListItems: %v (n=%d)", err, len(items))
	}
	if items[0].DeliveryID != 21 || items[1].DeliveryID != 22 {
		t.Fatalf("ListItems order wrong: %+v", items)
	}
	if got, err := repo.Get(ctx, key); err != nil || got == nil || got.ThreadID != "" {
		t.Fatalf("Enqueue-only row should carry empty thread: %+v %v", got, err)
	}

	if _, err := repo.EnsurePR(ctx, "acme/widget#old", "t", "r"); err != nil {
		t.Fatalf("EnsurePR: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE pr_queue SET status = 'drained', updated_at = 1 WHERE pr_key = 'acme/widget#old'`); err != nil {
		t.Fatalf("age drained row: %v", err)
	}
	n, err := repo.PruneDrained(ctx, 100)
	if err != nil || n != 1 {
		t.Fatalf("PruneDrained = %d, %v; want 1", n, err)
	}
	if got, _ := repo.Get(ctx, "acme/widget#old"); got != nil {
		t.Fatalf("drained row survived prune: %+v", got)
	}
	if got, _ := repo.Get(ctx, key); got == nil {
		t.Fatal("active row pruned, want kept")
	}
}

func TestPRQueueSetPRKey(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	created, err := s.WebhookDeliveryRepo().Create(ctx, webhookDelivery(nil))
	if err != nil || !created {
		t.Fatalf("Create: %v", err)
	}
	d, err := s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil || d == nil {
		t.Fatalf("Get: %v", err)
	}
	if d.PRKey != "" {
		t.Fatalf("PRKey = %q, want empty default", d.PRKey)
	}
	if err := s.WebhookDeliveryRepo().SetPRKey(ctx, d.ID, "acme/widget#42"); err != nil {
		t.Fatalf("SetPRKey: %v", err)
	}
	d, err = s.WebhookDeliveryRepo().Get(ctx, "github-review", "abc-123")
	if err != nil || d == nil || d.PRKey != "acme/widget#42" {
		t.Fatalf("Get after SetPRKey: %+v %v", d, err)
	}
}

func TestPRQueueConcurrentEnqueue(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()
	const key = "acme/widget#99"

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := repo.Enqueue(ctx, key, "github-acme-fix", "issue_comment", int64(i), 3)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	items, err := repo.ListItems(ctx, key)
	if err != nil || len(items) != workers {
		t.Fatalf("ListItems: %v (n=%d, want %d)", err, len(items), workers)
	}
}

func TestRepairInvariantsDrainsPhantomActiveRow(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	now := time.Now().Unix()
	if _, err := s.db.Exec(
		`INSERT INTO pr_queue (pr_key, thread_id, root_msg_id, status, active_kind, active_session, active_delivery, active_since, retry_count, next_retry_at, blocked_reason, created_at, updated_at)
		 VALUES (?, '', '', ?, '', '', 0, 0, 0, 0, '', ?, ?)`,
		"acme/widget#9", string(PRStatusActive), now, now,
	); err != nil {
		t.Fatalf("seed phantom row: %v", err)
	}

	repaired, err := s.PRQueueRepo().RepairInvariants(ctx)
	if err != nil {
		t.Fatalf("repair invariants: %v", err)
	}
	if len(repaired) != 1 || repaired[0] != "acme/widget#9" {
		t.Fatalf("repaired rows = %v, want the phantom row", repaired)
	}
	row, err := s.PRQueueRepo().Get(ctx, "acme/widget#9")
	if err != nil || row == nil {
		t.Fatalf("get repaired row: %v", err)
	}
	if row.Status != PRStatusDrained || row.ActiveKind != "" || row.ActiveSince != 0 {
		t.Fatalf("phantom row not repaired: %+v", row)
	}
}

func TestRepairInvariantsKeepsActiveQueueAndRun(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()
	repo := s.PRQueueRepo()
	if _, err := repo.EnsurePR(ctx, "acme/widget#10", "thread-10", "root-10"); err != nil {
		t.Fatalf("ensure queued row: %v", err)
	}
	if _, err := repo.Enqueue(ctx, "acme/widget#10", "github-widget-fix", "pull_request_review", 0, 3); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := repo.EnsurePR(ctx, "acme/widget#11", "thread-11", "root-11"); err != nil {
		t.Fatalf("ensure running row: %v", err)
	}
	if err := repo.SetActive(ctx, "acme/widget#11", "fix", "sess-11", 1); err != nil {
		t.Fatalf("set active: %v", err)
	}

	repaired, err := repo.RepairInvariants(ctx)
	if err != nil {
		t.Fatalf("repair invariants: %v", err)
	}
	if len(repaired) != 0 {
		t.Fatalf("legitimate rows repaired: %v", repaired)
	}
}
