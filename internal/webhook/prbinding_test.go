package webhook

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
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

func newPRBindingServer(t *testing.T, ep config.EndpointConfig, exec Executor) (*Server, *store.SQLiteStore) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults([]config.EndpointConfig{ep}),
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

func prOpenedBody(number int, branch string) string {
	return fmt.Sprintf(`{"action":"opened","pull_request":{"number":%d,"state":"open","user":{"login":"owner"},"head":{"ref":%q,"sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"repository":{"full_name":"acme/widget"}}`, number, branch)
}

func TestRunBridgesPRBindingFromBranchRegistry(t *testing.T) {
	var gotThread string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		gotThread = workCtx.ThreadID
		return nil
	}
	srv, st := newPRBindingServer(t, prTestEndpoint("fix", "/fix", config.AutomationFull), exec)
	srv.tasks.Upsert("acme/widget", "feat/x", "root-branch", "thread-branch", TaskPROpened)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-bridge", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-bridge", store.WebhookStatusCompleted)
	if gotThread != "thread-branch" {
		t.Fatalf("run thread = %q, want thread-branch", gotThread)
	}
	row, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil {
		t.Fatalf("pr queue row: %+v, %v", row, err)
	}
	if row.ThreadID != "thread-branch" || row.RootMsgID != "root-branch" {
		t.Fatalf("persisted binding = thread %q root %q", row.ThreadID, row.RootMsgID)
	}
	if rec, ok := srv.tasks.ResolvePRThread("acme/widget#7"); !ok || rec.ThreadID != "thread-branch" {
		t.Fatalf("pr binding = %+v, %v", rec, ok)
	}
}

func TestRunBridgesPRBindingFromHeadBranchLookup(t *testing.T) {
	var gotThread string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		gotThread = workCtx.ThreadID
		return nil
	}
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)
	ep.Repository = "github.com/acme/widget"
	ep.Workspace = config.EndpointWorkspace{}
	ep.CommentTrigger = []string{"please fix review"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour}
	srv, st := newPRBindingServer(t, ep, exec)
	srv.prAuthorResolver = func(ctx context.Context, repo, number string) (string, error) {
		return "owner", nil
	}
	resolver := &fakePRHeadResolver{head: PRHead{Branch: "feat/gh"}}
	srv.SetPRHeadResolver(resolver)
	srv.tasks.Upsert("acme/widget", "feat/gh", "root-gh", "thread-gh", TaskPROpened)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := strings.ReplaceAll(string(realisticIssueCommentBody(7, "please fix review")), "anggasct/occa", "acme/widget")
	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-gh-bridge", "issue_comment", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-gh-bridge", store.WebhookStatusCompleted)
	if resolver.calls != 1 {
		t.Fatalf("head lookup calls = %d, want 1", resolver.calls)
	}
	if gotThread != "thread-gh" {
		t.Fatalf("run thread = %q, want thread-gh", gotThread)
	}
	row, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil || row.ThreadID != "thread-gh" {
		t.Fatalf("persisted binding = %+v, %v", row, err)
	}
}

func TestRunKeepsExistingPRBindingOnConflict(t *testing.T) {
	var gotThread string
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		gotThread = workCtx.ThreadID
		return nil
	}
	srv, st := newPRBindingServer(t, prTestEndpoint("fix", "/fix", config.AutomationFull), exec)
	srv.tasks.Upsert("acme/widget", "feat/x", "root-branch", "thread-branch", TaskPROpened)
	if _, err := st.PRQueueRepo().EnsurePR(context.Background(), "acme/widget#7", "thread-existing", "root-existing"); err != nil {
		t.Fatal(err)
	}
	if seeded, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7"); err != nil || seeded == nil || seeded.ThreadID != "thread-existing" {
		t.Fatalf("seeded row = %+v, %v", seeded, err)
	}

	env := normalizeWebhook([]byte(prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678")), "pull_request_review", "del-conflict", srv.verdicts, false, "", nil)
	if direct := srv.resolvePRBinding(context.Background(), "acme/widget#7", "pull_request_review", env); direct.ThreadID != "thread-existing" {
		t.Fatalf("direct resolve = %+v, want thread-existing", direct)
	}
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/fix?secret=s3cret", "del-conflict", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "abc1234def5678"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-conflict", store.WebhookStatusCompleted)
	if gotThread != "thread-existing" {
		t.Fatalf("run thread = %q, want thread-existing", gotThread)
	}
	row, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || row == nil || row.ThreadID != "thread-existing" {
		t.Fatalf("binding rebound: %+v, %v", row, err)
	}
	for _, want := range []string{"pr thread binding conflict", "thread-existing", "thread-branch"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("conflict log missing %q: %s", want, logs.String())
		}
	}
}

func TestOpenedReviewWithoutBindingRuns(t *testing.T) {
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return nil
	}
	srv, st := newPRBindingServer(t, prTestEndpoint("review", "/review", config.AutomationFull), exec)

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp := post(t, ts.URL+"/review?secret=s3cret", "del-opened", "pull_request", prOpenedBody(7, "feat/new"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	waitForReceiptDeliveryID(t, st, "del-opened", store.WebhookStatusCompleted)
	if atomic.LoadInt64(&calls) != 1 {
		t.Fatalf("opened review run calls = %d, want 1 (no refusal)", calls)
	}
}
