package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func newThreadBindingServer(t *testing.T, exec Executor) (*Server, *store.SQLiteStore) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{prTestEndpoint("fix", "/fix", config.AutomationFull)}),
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetSessionStore(st.SessionRepo())
	srv.SetThreadStarter(func(context.Context, string, string, string, string) (string, error) {
		return "thread-new", nil
	})
	return srv, st
}

func TestRunUsesPersistedPRThreadAndWritesSessionBinding(t *testing.T) {
	var gotThread string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		gotThread = workCtx.ThreadID
		workCtx.ReportSession("ses-recovered")
		return nil
	}
	srv, st := newThreadBindingServer(t, exec)
	ctx := context.Background()
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-saved", "root-saved"); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-binding", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-binding", store.WebhookStatusCompleted)
	if gotThread != "thread-saved" {
		t.Fatalf("run thread = %q, want persisted thread-saved", gotThread)
	}
	channel, err := st.SessionRepo().ThreadChannel(ctx, "telegram", "thread-saved")
	if err != nil || channel != "chat1" {
		t.Fatalf("session binding = %q, %v; want chat1", channel, err)
	}
}

func TestRunWithoutResolvableThreadFailsLoudly(t *testing.T) {
	var calls int64
	var notices int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return nil
	}
	srv, st := newThreadBindingServer(t, exec)
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		if strings.Contains(text, "RUN REFUSED") {
			atomic.AddInt64(&notices, 1)
		}
		return nil
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-unbound", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	receipt := waitForReceiptDeliveryID(t, st, "del-unbound", store.WebhookStatusFailed)
	if receipt.ErrorSummary != "run refused: PR thread binding unresolved (re-run /retry or re-trigger)" {
		t.Fatalf("unexpected failure summary: %q", receipt.ErrorSummary)
	}
	if atomic.LoadInt64(&calls) != 0 {
		t.Fatalf("unbound run executed %d times", calls)
	}
	if atomic.LoadInt64(&notices) != 1 {
		t.Fatalf("notices = %d, want exactly 1", notices)
	}
}

func TestRestartRetryResumesWithSameThread(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook.db")
	ctx := context.Background()
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)

	openServer := func(st *store.SQLiteStore, exec Executor) *Server {
		cfg := config.WebhookConfig{
			Bind:      "127.0.0.1:0",
			Policy:    testPolicy(),
			Runtime:   testRuntime(),
			Endpoints: withAdmitDefaults([]config.EndpointConfig{ep}),
		}
		srv := New(cfg, exec, st.WebhookDeliveryRepo())
		srv.SetChannelStore(st.ChannelRepo())
		srv.SetPRQueueRepo(st.PRQueueRepo())
		srv.SetSessionStore(st.SessionRepo())
		srv.SetThreadStarter(func(context.Context, string, string, string, string) (string, error) {
			return "thread-new", nil
		})
		setOpenPRState(srv)
		return srv
	}

	stA, err := store.OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open store A: %v", err)
	}
	enteredA := make(chan struct{})
	blockA := make(chan struct{})
	execA := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx.ThreadID != "thread-pr" {
			t.Errorf("server A run thread = %q, want thread-pr", workCtx.ThreadID)
		}
		workCtx.ReportSession("sess-a")
		close(enteredA)
		<-blockA
		return nil
	}
	srvA := openServer(stA, execA)
	if _, err := stA.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatalf("seed pr queue: %v", err)
	}
	muxA := http.NewServeMux()
	muxA.HandleFunc("/", srvA.handleRequest)
	tsA := httptest.NewServer(muxA)
	resp := post(t, tsA.URL+"/fix?secret=s3cret", "del-midrun", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("server A post status = %d", resp.StatusCode)
	}
	select {
	case <-enteredA:
	case <-time.After(5 * time.Second):
		t.Fatalf("server A run did not start")
	}
	tsA.Close()
	if err := stA.Close(); err != nil {
		t.Fatalf("close store A: %v", err)
	}

	stB, err := store.OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open store B: %v", err)
	}
	defer func() { _ = stB.Close() }()

	threadsB := make(chan string, 4)
	execB := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		threadsB <- workCtx.ThreadID
		workCtx.ReportSession("sess-b")
		return nil
	}
	srvB := openServer(stB, execB)
	srvB.SetOperatorAuthorizer(func(platform, userID string) bool { return true })
	srvB.prAuthorResolver = func(ctx context.Context, repo, number string) (string, error) {
		return "owner", nil
	}
	rec := srvB.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
	srvB.tasks.LinkPRThread("acme/widget#7", rec)

	if msg := srvB.RecoverPRQueue(); !strings.Contains(msg, "recovered 1 active") {
		t.Fatalf("recovery = %q, want one active run resumed", msg)
	}
	row, err := stB.PRQueueRepo().Get(ctx, "acme/widget#7")
	if err != nil || row == nil {
		t.Fatalf("resume row = %+v, %v", row, err)
	}
	if row.Status != store.PRStatusDrained || row.BlockedReason != "resume_pending" || row.ThreadID != "thread-pr" {
		t.Fatalf("resume row = %+v, want drained/resume_pending on thread-pr", row)
	}

	srvB.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
		if repo != "acme/widget" || number != "7" || kind != "fix" {
			t.Fatalf("retry builder got %q %q %q", repo, number, kind)
		}
		return issueCommentEventBodyForRepo(t, "acme/widget", 7, "please retry", "abc1234def5678"), "issue_comment", nil
	}
	reply, handled, err := srvB.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
	if err != nil || !handled {
		t.Fatalf("retry = %q %v %v", reply, handled, err)
	}
	if !strings.Contains(reply, "Re-running fix") {
		t.Fatalf("retry reply = %q, want a fix re-run", reply)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		all, lerr := stB.WebhookDeliveryRepo().List(ctx, 20)
		if lerr != nil {
			t.Fatalf("list deliveries: %v", lerr)
		}
		done := false
		for _, d := range all {
			if strings.HasPrefix(d.DeliveryID, "op-retry-") && d.Status == store.WebhookStatusCompleted {
				done = true
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("synthetic retry never completed: %+v", all)
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case got := <-threadsB:
		if got != "thread-pr" {
			t.Fatalf("resumed run thread = %q, want thread-pr", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("resumed run did not execute")
	}
	session, _, err := stB.SessionRepo().Active(ctx, "telegram", "chat1", "thread-pr", "")
	if err != nil {
		t.Fatalf("read session binding: %v", err)
	}
	if session != "sess-b" {
		t.Fatalf("session binding = %q, want sess-b on thread-pr", session)
	}
}
