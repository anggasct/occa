package webhook

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type failingCreateStore struct {
	DeliveryStore
	fail atomic.Bool
}

func (f *failingCreateStore) Create(ctx context.Context, d store.WebhookDelivery) (bool, error) {
	if f.fail.Load() {
		return false, errors.New("database busy")
	}
	return f.DeliveryStore.Create(ctx, d)
}

func TestSpoolFallbackReturns202AndDrainsExactlyOnce(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	spoolDir := filepath.Join(t.TempDir(), "spool")
	srv.SetSpoolDir(spoolDir)
	failing := &failingCreateStore{DeliveryStore: srv.deliveries}
	failing.fail.Store(true)
	srv.deliveries = failing

	deliveryID := "del-spool-1"
	resp := post(t, url+"/review?secret=s3cret", deliveryID, "pull_request_review",
		prReviewBody(301, "feat/x", "owner", "commented", "c0ffee301"))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 when the receipt write misses the ACK budget", resp.StatusCode)
	}
	files, err := filepath.Glob(filepath.Join(spoolDir, "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("spool files = %v, want exactly one (%v)", files, err)
	}
	path := files[0]
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat spool file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("spool mode = %o, want 600", perm)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read spool file: %v", err)
	}

	failing.fail.Store(false)
	srv.drainSpool(context.Background())
	waitForDeliveryStatus(t, st, "github-widget-review", deliveryID, store.WebhookStatusCompleted)
	waitForCalls(t, rec, 1)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("spool file still exists after drain: %v", err)
	}

	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("restore spool file: %v", err)
	}
	srv.drainSpool(context.Background())
	time.Sleep(100 * time.Millisecond)
	rec.mu.Lock()
	calls := len(rec.order)
	rec.mu.Unlock()
	if calls != 1 {
		t.Fatalf("executor calls = %d, want exactly one across duplicate drains", calls)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("duplicate spool file was not removed: %v", err)
	}
}
