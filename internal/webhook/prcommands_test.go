package webhook

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

func TestParseOperatorCommand(t *testing.T) {
	cases := []struct {
		in   string
		name string
		arg  string
		ok   bool
	}{
		{"/queue", "queue", "", true},
		{"/retry", "retry", "", true},
		{"/skip #7", "skip", "7", true},
		{"/cancel 42", "cancel", "42", true},
		{"/status", "status", "", true},
		{"/STATUS", "status", "", true},
		{"  /queue  ", "queue", "", true},
		{"hello", "", "", false},
		{"/unknown", "", "", false},
		{"/", "", "", false},
		{"/webhooks", "", "", false},
	}
	for _, tc := range cases {
		name, arg, ok := parseOperatorCommand(tc.in)
		if name != tc.name || arg != tc.arg || ok != tc.ok {
			t.Fatalf("parse(%q) = %q %q %v, want %q %q %v", tc.in, name, arg, ok, tc.name, tc.arg, tc.ok)
		}
	}
}

func operatorTestServer(t *testing.T, authorizer func(platform, userID string) bool, endpoints ...config.EndpointConfig) (*Server, *store.SQLiteStore, func()) {
	t.Helper()
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		return nil
	}
	if len(endpoints) == 0 {
		endpoints = []config.EndpointConfig{
			prTestEndpoint("review", "/review", config.AutomationFull),
			prTestEndpoint("fix", "/fix", config.AutomationFull),
			prTestEndpoint("merge", "/merge", config.AutomationFull),
			prTestEndpoint("merged", "/merged", config.AutomationFull),
		}
	}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults(endpoints),
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
	if authorizer != nil {
		srv.SetOperatorAuthorizer(authorizer)
	}
	ts := httptest.NewServer(http.HandlerFunc(srv.handleRequest))
	t.Cleanup(ts.Close)
	_ = ts
	return srv, st, func() {}
}

func operatorMsg(threadID, text string) channel.IncomingMessage {
	return channel.IncomingMessage{
		Platform:  "discord",
		ChannelID: "chan-1",
		ThreadID:  threadID,
		UserID:    "op-1",
		Text:      text,
		IsThread:  threadID != "",
	}
}

func linkPRThread(t *testing.T, srv *Server, prKey, threadID string) {
	t.Helper()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", threadID, TaskClaimed)
	srv.tasks.LinkPRThread(prKey, rec)
}

func TestOperatorAuthDenied(t *testing.T) {
	srv, _, _ := operatorTestServer(t, func(platform, userID string) bool { return false })
	linkPRThread(t, srv, "acme/widget#7", "thread-pr")
	reply, handled, err := srv.HandleOperatorCommand(context.Background(), operatorMsg("thread-pr", "/queue"), "queue", "")
	if err != nil || !handled || !strings.Contains(reply, "Not allowed") {
		t.Fatalf("denied = %q %v %v, want not-allowed reply", reply, handled, err)
	}
}

func TestOperatorNotPRThread(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	srv, _, _ := operatorTestServer(t, allow)
	reply, handled, _ := srv.HandleOperatorCommand(context.Background(), operatorMsg("other-thread", "/queue"), "queue", "")
	if !handled || !strings.Contains(reply, "inside the PR thread") {
		t.Fatalf("outside PR thread = %q %v", reply, handled)
	}
	reply, handled, _ = srv.HandleOperatorCommand(context.Background(), operatorMsg("", "/retry"), "retry", "")
	if !handled || !strings.Contains(reply, "inside the PR thread") {
		t.Fatalf("no thread = %q %v", reply, handled)
	}
	reply, handled, _ = srv.HandleOperatorCommand(context.Background(), operatorMsg("thread-pr", "/status"), "status", "")
	if handled {
		t.Fatalf("plain /status in PR thread without link must fall through to health, got %q", reply)
	}
}

func TestOperatorQueueAndStatus(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	srv, st, _ := operatorTestServer(t, allow)
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", 42); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PRQueueRepo().Enqueue(ctx, "acme/widget#7", "github-widget-merge", "check_suite", 0, 4); err != nil {
		t.Fatal(err)
	}
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/queue"), "queue", "")
	if err != nil || !handled {
		t.Fatalf("queue = %q %v %v", reply, handled, err)
	}
	for _, want := range []string{"acme/widget#7", "active", "sess-9", "merge"} {
		if !strings.Contains(reply, want) {
			t.Fatalf("queue reply missing %q: %q", want, reply)
		}
	}
	reply, handled, err = srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/status"), "status", "")
	if err != nil || !handled {
		t.Fatalf("status = %q %v %v", reply, handled, err)
	}
	for _, want := range []string{"acme/widget#7", "sess-9", "fix"} {
		if !strings.Contains(reply, want) {
			t.Fatalf("status reply missing %q: %q", want, reply)
		}
	}
}

func TestOperatorStatusScopesQueueToPR(t *testing.T) {
	srv, st, _ := operatorTestServer(t, func(platform, userID string) bool { return true })
	ctx := context.Background()
	for _, key := range []string{"acme/widget#7", "acme/widget#8"} {
		if _, err := st.PRQueueRepo().EnsurePR(ctx, key, "", ""); err != nil {
			t.Fatal(err)
		}
		d := newDispatcher(srv, "pr:"+key)
		if !d.push(dispatchItem{ep: config.EndpointConfig{Name: "github-widget-merge"}, prKey: key}) {
			t.Fatal("push failed")
		}
		srv.dispatchMu.Lock()
		srv.dispatchers[d.key] = d
		srv.dispatchMu.Unlock()
	}
	reply := srv.operatorStatus(ctx, "acme/widget#7")
	if !strings.Contains(reply, "queue: 1/8") {
		t.Fatalf("status must report the PR queue against its budget: %q", reply)
	}
	if strings.Contains(reply, "queue: 2/8") {
		t.Fatalf("status reported the process-wide queue: %q", reply)
	}
}

func TestOperatorRetryUnblocksAndRequeues(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	srv, st, _ := operatorTestServer(t, allow)
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", 11); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetRetryState(ctx, "acme/widget#7", 2, time.Now().Add(time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "model error exhausted"); err != nil {
		t.Fatal(err)
	}
	srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
		if repo != "acme/widget" || number != "7" || kind != "fix" {
			t.Fatalf("retry builder got %q %q %q", repo, number, kind)
		}
		return []byte(`{"action":"created","synthetic":"retry"}`), "issue_comment", nil
	}
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
	if err != nil || !handled {
		t.Fatalf("retry = %q %v %v", reply, handled, err)
	}
	row, rerr := st.PRQueueRepo().Get(ctx, "acme/widget#7")
	if rerr != nil || row == nil || row.Status != store.PRStatusActive || row.RetryCount != 0 {
		t.Fatalf("after retry: %+v %v", row, rerr)
	}
	if !strings.Contains(reply, "fix") {
		t.Fatalf("retry reply should name the kind: %q", reply)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		all, lerr := st.WebhookDeliveryRepo().List(ctx, 20)
		if lerr != nil {
			t.Fatalf("list: %v", lerr)
		}
		for _, d := range all {
			if strings.HasPrefix(d.DeliveryID, "op-retry-") && d.Status == store.WebhookStatusCompleted {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("synthetic retry never completed: %+v", all)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOperatorRetryInfersKindFromLastDelivery(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	srv, st, _ := operatorTestServer(t, allow)
	ctx := context.Background()
	linkPRThread(t, srv, "acme/widget#7", "thread-pr")
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{
		Endpoint:     "github-widget-pr-fix",
		DeliveryID:   "del-interrupted",
		EventType:    "pull_request_review",
		ErrorSummary: "Review verdict: REQUEST_CHANGES",
		Status:       store.WebhookStatusFailed,
	})
	if err != nil || !created {
		t.Fatalf("create delivery: %v %v", created, err)
	}
	rec, err := st.WebhookDeliveryRepo().Get(ctx, "github-widget-pr-fix", "del-interrupted")
	if err != nil || rec == nil {
		t.Fatalf("get delivery: %v", err)
	}
	if err := st.WebhookDeliveryRepo().SetPRKey(ctx, rec.ID, "acme/widget#7"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "interrupted by restart — re-run via /retry or re-trigger"); err != nil {
		t.Fatal(err)
	}
	srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
		if repo != "acme/widget" || number != "7" || kind != "fix" {
			t.Fatalf("retry builder got %q %q %q", repo, number, kind)
		}
		return []byte(`{"action":"created","synthetic":"retry"}`), "issue_comment", nil
	}
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
	if err != nil || !handled {
		t.Fatalf("retry = %q %v %v", reply, handled, err)
	}
	if !strings.Contains(reply, "fix") || strings.Contains(reply, "Nothing to retry") {
		t.Fatalf("retry reply = %q", reply)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		all, lerr := st.WebhookDeliveryRepo().List(ctx, 20)
		if lerr != nil {
			t.Fatalf("list: %v", lerr)
		}
		for _, d := range all {
			if strings.HasPrefix(d.DeliveryID, "op-retry-") && d.Status == store.WebhookStatusCompleted {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("synthetic retry never completed: %+v", all)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOperatorRetryInfersReviewFromDeliveryEndpoint(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	srv, st, _ := operatorTestServer(t, allow)
	ctx := context.Background()
	linkPRThread(t, srv, "acme/widget#7", "thread-pr")
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{
		Endpoint:     "github-widget-review",
		DeliveryID:   "del-review-comment",
		EventType:    "issue_comment",
		ErrorSummary: "skipped: comment trigger",
		Status:       store.WebhookStatusFailed,
	})
	if err != nil || !created {
		t.Fatalf("create delivery: %v %v", created, err)
	}
	rec, err := st.WebhookDeliveryRepo().Get(ctx, "github-widget-review", "del-review-comment")
	if err != nil || rec == nil {
		t.Fatalf("get delivery: %v", err)
	}
	if err := st.WebhookDeliveryRepo().SetPRKey(ctx, rec.ID, "acme/widget#7"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "resume_pending"); err != nil {
		t.Fatal(err)
	}
	srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
		if repo != "acme/widget" || number != "7" || kind != "review" {
			t.Fatalf("retry builder got %q %q %q", repo, number, kind)
		}
		return []byte(`{"action":"created","synthetic":"retry"}`), "issue_comment", nil
	}
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
	if err != nil || !handled {
		t.Fatalf("retry = %q %v %v", reply, handled, err)
	}
	if !strings.Contains(reply, "review") || strings.Contains(reply, "Nothing to retry") {
		t.Fatalf("retry reply = %q", reply)
	}
}

func TestOperatorRetryBypassesSameHeadReviewDedupe(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	reviewEp := prTestEndpoint("review", "/review", config.AutomationFull)
	reviewEp.CommentTrigger = []string{"please re-review"}
	reviewEp.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
	srv, st, _ := operatorTestServer(t, allow, reviewEp)
	ctx := context.Background()
	linkPRThread(t, srv, "acme/widget#7", "thread-pr")
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}

	const head = "5932515abc123"
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{
		Endpoint:    "github-widget-review",
		DeliveryID:  "del-prior-review",
		EventType:   "issue_comment",
		PayloadHash: "prior-review",
	})
	if err != nil || !created {
		t.Fatalf("create prior delivery: created=%v err=%v", created, err)
	}
	prior, err := st.WebhookDeliveryRepo().Get(ctx, "github-widget-review", "del-prior-review")
	if err != nil || prior == nil {
		t.Fatalf("get prior delivery: %v %v", prior, err)
	}
	if err := st.WebhookDeliveryRepo().SetReviewKey(ctx, prior.ID, "acme/widget#7@"+head); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WebhookDeliveryRepo().Transition(ctx, prior.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusCompleted, ""); err != nil {
		t.Fatal(err)
	}

	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "review", "sess-9", 11); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "interrupted by restart — re-run via /retry or re-trigger"); err != nil {
		t.Fatal(err)
	}

	var calls int64
	srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		atomic.AddInt64(&calls, 1)
		return nil
	}
	srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
		if repo != "acme/widget" || number != "7" || kind != "review" {
			t.Fatalf("retry builder got %q %q %q", repo, number, kind)
		}
		return internalReviewCommentBody("acme/widget", "7", "please re-review", "https://github.com/acme/widget/pull/7", "feat/x", head), "issue_comment", nil
	}

	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
	if err != nil || !handled || !strings.Contains(reply, "Re-running review") {
		t.Fatalf("retry = %q %v %v", reply, handled, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		all, lerr := st.WebhookDeliveryRepo().List(ctx, 50)
		if lerr != nil {
			t.Fatalf("list: %v", lerr)
		}
		for _, d := range all {
			if !strings.HasPrefix(d.DeliveryID, "op-retry-") {
				continue
			}
			if d.Status == store.WebhookStatusSkipped {
				t.Fatalf("operator retry was deduped: %+v", d)
			}
			if d.Status == store.WebhookStatusCompleted {
				if got := atomic.LoadInt64(&calls); got != 1 {
					t.Fatalf("executor calls = %d, want 1", got)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("synthetic retry never completed: %+v", all)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func operatorSyntheticCommentBody(trigger, prAuthor, head string) []byte {
	return []byte(fmt.Sprintf(`{"action":"created","issue":{"number":7,"state":"open","title":"widget","user":{"login":%q},"pull_request":{"html_url":"https://github.com/acme/widget/pull/7"}},"comment":{"body":%q,"user":{"login":"operator"}},"repository":{"full_name":"acme/widget"},"branch":"feat/x","head_revision":%q}`, prAuthor, trigger, head))
}

func waitSyntheticRetryCompleted(t *testing.T, ctx context.Context, st *store.SQLiteStore, calls *int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		all, err := st.WebhookDeliveryRepo().List(ctx, 50)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, d := range all {
			if !strings.HasPrefix(d.DeliveryID, operatorRetryDeliveryPrefix) {
				continue
			}
			if d.Status == store.WebhookStatusSkipped {
				t.Fatalf("operator retry was skipped: %+v", d)
			}
			if d.Status == store.WebhookStatusCompleted {
				if got := atomic.LoadInt64(calls); got != 1 {
					t.Fatalf("executor calls = %d, want 1", got)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("synthetic retry never completed: %+v", all)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOperatorRetryAdmittedWithCommentAuthorsAllowlist(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	const head = "5932515abc123"

	t.Run("review", func(t *testing.T) {
		reviewEp := prTestEndpoint("review", "/review", config.AutomationFull)
		reviewEp.CommentTrigger = []string{"please re-review"}
		reviewEp.CommentAuthors = []string{"anggasct"}
		reviewEp.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
		srv, st, _ := operatorTestServer(t, allow, reviewEp)
		ctx := context.Background()
		linkPRThread(t, srv, "acme/widget#7", "thread-pr")
		if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
			t.Fatal(err)
		}
		if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "review", "sess-9", 11); err != nil {
			t.Fatal(err)
		}
		if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "interrupted by restart — re-run via /retry or re-trigger"); err != nil {
			t.Fatal(err)
		}

		var calls int64
		srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
			atomic.AddInt64(&calls, 1)
			return nil
		}
		srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
			if repo != "acme/widget" || number != "7" || kind != "review" {
				t.Fatalf("retry builder got %q %q %q", repo, number, kind)
			}
			return operatorSyntheticCommentBody("please re-review", "anggasct", head), "issue_comment", nil
		}

		reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
		if err != nil || !handled || !strings.Contains(reply, "Re-running review") {
			t.Fatalf("retry = %q %v %v", reply, handled, err)
		}
		waitSyntheticRetryCompleted(t, ctx, st, &calls)
	})

	t.Run("fix review-only", func(t *testing.T) {
		fixEp := prTestEndpoint("fix", "/fix", config.AutomationReviewOnly)
		fixEp.CommentTrigger = []string{"/fix"}
		fixEp.CommentAuthors = []string{"anggasct"}
		fixEp.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
		srv, st, _ := operatorTestServer(t, allow, fixEp)
		ctx := context.Background()
		linkPRThread(t, srv, "acme/widget#7", "thread-pr")
		if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
			t.Fatal(err)
		}
		if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", 11); err != nil {
			t.Fatal(err)
		}
		if err := st.PRQueueRepo().SetBlocked(ctx, "acme/widget#7", "interrupted by restart — re-run via /retry or re-trigger"); err != nil {
			t.Fatal(err)
		}
		srv.prAuthorResolver = func(ctx context.Context, repo, number string) (string, error) {
			return "unlisted-author", nil
		}

		var calls int64
		srv.executor = func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
			atomic.AddInt64(&calls, 1)
			return nil
		}
		srv.retryBodyBuilder = func(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
			if repo != "acme/widget" || number != "7" || kind != "fix" {
				t.Fatalf("retry builder got %q %q %q", repo, number, kind)
			}
			return operatorSyntheticCommentBody("/fix", "unlisted-author", head), "issue_comment", nil
		}

		reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/retry"), "retry", "")
		if err != nil || !handled || !strings.Contains(reply, "Re-running fix") {
			t.Fatalf("retry = %q %v %v", reply, handled, err)
		}
		waitSyntheticRetryCompleted(t, ctx, st, &calls)
	})
}

func TestOperatorSkipNoActive(t *testing.T) {
	allow := func(platform, userID string) bool { return true }
	srv, _, _ := operatorTestServer(t, allow)
	linkPRThread(t, srv, "acme/widget#7", "thread-pr")
	reply, handled, err := srv.HandleOperatorCommand(context.Background(), operatorMsg("thread-pr", "/skip"), "skip", "")
	if err != nil || !handled || !strings.Contains(reply, "No live run") {
		t.Fatalf("skip idle = %q %v %v", reply, handled, err)
	}
}

func backoffHarness(t *testing.T, failFirst bool) (*Server, *store.SQLiteStore, *int64, string) {
	t.Helper()
	var calls int64
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		n := atomic.AddInt64(&calls, 1)
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		if failFirst && n == 1 {
			return fmt.Errorf("wrapping: %w", relay.ErrWebhookAgentResponse)
		}
		return nil
	}
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
	setOpenPRState(srv)
	srv.SetOperatorAuthorizer(func(platform, userID string) bool { return true })
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureModel: {30 * time.Second},
		failureStall: {0},
		failureBuild: {30 * time.Second},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return srv, st, &calls, ts.URL
}

func waitForRetryScheduled(t *testing.T, st *store.SQLiteStore, prKey string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		row, err := st.PRQueueRepo().Get(context.Background(), prKey)
		if err == nil && row != nil && row.RetryCount > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("retry never scheduled")
}

func postFix(t *testing.T, url, deliveryID string) {
	t.Helper()
	if resp := post(t, url+"/fix?secret=s3cret", deliveryID, "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee"+deliveryID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post %s: status %d", deliveryID, resp.StatusCode)
	}
}

func TestOperatorSkipDuringBackoff(t *testing.T) {
	srv, st, calls, url := backoffHarness(t, true)
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	postFix(t, url, "del-skip-1")
	waitForRetryScheduled(t, st, "acme/widget#7")
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/skip"), "skip", "")
	if err != nil || !handled || !strings.Contains(reply, "equeuing next") {
		t.Fatalf("skip = %q %v %v", reply, handled, err)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-skip-1", store.WebhookStatusSkipped)
	if got := atomic.LoadInt64(calls); got != 1 {
		t.Fatalf("skipped retry must not re-execute, calls = %d", got)
	}
	waitForPRQueueStatus(t, st, "acme/widget#7", store.PRStatusDrained)
}

func TestOperatorCancelDuringBackoff(t *testing.T) {
	srv, st, calls, url := backoffHarness(t, true)
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	postFix(t, url, "del-cancel-1")
	waitForRetryScheduled(t, st, "acme/widget#7")
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/cancel"), "cancel", "")
	if err != nil || !handled || !strings.Contains(reply, "Cancelled") {
		t.Fatalf("cancel = %q %v %v", reply, handled, err)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-cancel-1", store.WebhookStatusSkipped)
	if got := atomic.LoadInt64(calls); got != 1 {
		t.Fatalf("cancelled retry must not re-execute, calls = %d", got)
	}
	waitForPRQueueStatus(t, st, "acme/widget#7", store.PRStatusDrained)
}

func TestOperatorCancelClears(t *testing.T) {
	aborted := false
	var abortedSession, abortedWorkdir string
	allow := func(platform, userID string) bool { return true }
	fixEp := prTestEndpoint("fix", "/fix", config.AutomationFull)
	fixEp.Workspace = config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/tmp/widget", Mode: config.WorkspaceModeIsolated}
	fixEp.Repository = "github.com/acme/widget"
	srv, st, _ := operatorTestServer(t, allow, prTestEndpoint("review", "/review", config.AutomationFull), fixEp)
	srv.sessionAborter = func(ctx context.Context, sessionID, workdir string) error {
		aborted = true
		abortedSession, abortedWorkdir = sessionID, workdir
		return nil
	}
	ctx := context.Background()
	rec := srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-pr", TaskClaimed)
	srv.tasks.LinkPRThread("acme/widget#7", rec)
	if _, err := st.PRQueueRepo().EnsurePR(ctx, "acme/widget#7", "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.PRQueueRepo().SetActive(ctx, "acme/widget#7", "fix", "sess-9", 11); err != nil {
		t.Fatal(err)
	}
	d := newDispatcher(srv, "pr:acme/widget#7")
	srv.dispatchMu.Lock()
	srv.dispatchers["pr:acme/widget#7"] = d
	srv.dispatchMu.Unlock()
	reply, handled, err := srv.HandleOperatorCommand(ctx, operatorMsg("thread-pr", "/cancel"), "cancel", "")
	if err != nil || !handled {
		t.Fatalf("cancel = %q %v %v", reply, handled, err)
	}
	if !aborted {
		t.Fatal("cancel must attempt session abort")
	}
	if abortedSession != "sess-9" || abortedWorkdir != "/tmp/widget" {
		t.Fatalf("abort target = %q @ %q", abortedSession, abortedWorkdir)
	}
	select {
	case <-d.notify:
	default:
		t.Fatal("cancel must wake the dispatcher")
	}
}
