package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func newRecoveryHarness(t *testing.T) (*Server, *store.SQLiteStore) {
	t.Helper()
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		return nil
	}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults(nil),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return srv, st
}

func TestRecoverPRQueueInterruptsAndBlocks(t *testing.T) {
	srv, st := newRecoveryHarness(t)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-1", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", 0); err != nil {
		t.Fatal(err)
	}
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{Endpoint: "e", DeliveryID: "del-1", EventType: "pull_request", PayloadHash: "h"})
	if err != nil || !created {
		t.Fatalf("create receipt: %v %v", created, err)
	}
	receipt, err := st.WebhookDeliveryRepo().Get(ctx, "e", "del-1")
	if err != nil || receipt == nil {
		t.Fatalf("get receipt: %v", err)
	}
	ok, err := st.WebhookDeliveryRepo().Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, "")
	if err != nil || !ok {
		t.Fatalf("transition to processing: %v %v", ok, err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", int64(receipt.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PRQueueRepo().Enqueue(ctx, "acme/widget#7", "github-acme-review", "pull_request_review", 999, 2); err != nil {
		t.Fatal(err)
	}
	msg := srv.RecoverPRQueue()
	want := "recovered 1 active (resumed 1, failed 1), dropped 1 queued, blocked 0"
	if msg != want {
		t.Fatalf("recover log = %q, want %q", msg, want)
	}
	rec, err := st.WebhookDeliveryRepo().Get(ctx, "e", "del-1")
	if err != nil || rec == nil || rec.Status != store.WebhookStatusFailed {
		t.Fatalf("active receipt = %+v, %v; want failed", rec, err)
	}
	run, err := st.PRQueueRepo().Get(ctx, "acme/widget#7")
	if err != nil || run == nil || run.Status != store.PRStatusDrained {
		t.Fatalf("pr row = %+v, %v; want drained", run, err)
	}
	if run.BlockedReason != "resume_pending" || run.ActiveKind != "fix" || run.ActiveDelivery != int64(receipt.ID) {
		t.Fatalf("resume marker = %+v", run)
	}
	items, err := st.PRQueueRepo().ListItems(ctx, "acme/widget#7")
	if err != nil || len(items) != 0 {
		t.Fatalf("queued items not dropped: %+v %v", items, err)
	}
}

func TestRecoverPRQueueKeepsBlocked(t *testing.T) {
	srv, st := newRecoveryHarness(t)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#8", "thread-8", "root-8"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#8", "exhausted"); err != nil {
		t.Fatal(err)
	}
	msg := srv.RecoverPRQueue()
	if msg != "recovered 0 active (resumed 0, failed 0), dropped 0 queued, blocked 1" {
		t.Fatalf("recover log = %q", msg)
	}
}

func TestRecoverPRQueueNoStore(t *testing.T) {
	srv := &Server{}
	if got := srv.RecoverPRQueue(); !strings.Contains(got, "no store") {
		t.Fatalf("no-store recovery = %q", got)
	}
}
