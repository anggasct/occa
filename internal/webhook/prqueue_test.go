package webhook

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestLaneOfWorkflow(t *testing.T) {
	cases := map[string]int{
		"dispatch": 0,
		"merged":   0,
		"review":   1,
		"fix":      2,
		"merge":    3,
		"custom":   3,
		"":         3,
	}
	for workflow, want := range cases {
		if got := laneOfWorkflow(workflow); got != want {
			t.Fatalf("laneOfWorkflow(%q) = %d, want %d", workflow, got, want)
		}
	}
}

func TestPRKeyForEndpoint(t *testing.T) {
	ep := config.EndpointConfig{Workflow: "review", Automation: &config.RepoAutomation{Default: config.AutomationFull}}
	body := []byte(`{"action":"opened","pull_request":{"number":7,"base":{"repo":{"full_name":"acme/widget"}},"head":{"ref":"feat/x","repo":{"full_name":"acme/widget"}}},"repository":{"full_name":"acme/widget"}}`)
	envelope := normalizeWebhook(body, "pull_request", "del-1", testVerdicts(), false, "", nil)
	key := ExtractExecutionKey(body)
	prKey, ok := prKeyForEndpoint(ep, key, envelope)
	if !ok || prKey != "acme/widget#7" {
		t.Fatalf("prKey = %q, %v; want acme/widget#7, true", prKey, ok)
	}
	plain := config.EndpointConfig{Workflow: "review"}
	if _, ok := prKeyForEndpoint(plain, key, envelope); ok {
		t.Fatal("endpoint without automation must not resolve a pr key")
	}
	empty := normalizeWebhook([]byte(`{}`), "ping", "del-2", testVerdicts(), false, "", nil)
	if _, ok := prKeyForEndpoint(ep, WebhookExecutionKey{}, empty); ok {
		t.Fatal("envelope without PR number must not resolve a pr key")
	}
}

func TestAutomationMode(t *testing.T) {
	if got := automationMode(config.EndpointConfig{}, "anyone"); got != config.AutomationFull {
		t.Fatalf("nil automation = %q, want full", got)
	}
	ep := config.EndpointConfig{Automation: &config.RepoAutomation{Default: config.AutomationReviewOnly, Authors: map[string]string{"Owner": config.AutomationFull}}}
	if got := automationMode(ep, "stranger"); got != config.AutomationReviewOnly {
		t.Fatalf("unlisted = %q, want review-only", got)
	}
	if got := automationMode(ep, "owner"); got != config.AutomationFull {
		t.Fatalf("listed (case-insensitive) = %q, want full", got)
	}
	if got := automationMode(ep, ""); got != config.AutomationReviewOnly {
		t.Fatalf("empty author = %q, want review-only", got)
	}
}

func TestLinkResolvePRThread(t *testing.T) {
	reg := NewTaskThreadRegistry()
	if _, ok := reg.ResolvePRThread(""); ok {
		t.Fatal("empty pr key must miss")
	}
	if _, ok := reg.ResolvePRThread("acme/widget#9"); ok {
		t.Fatal("unknown pr key must miss")
	}
	rec := reg.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskClaimed)
	reg.LinkPRThread("acme/widget#9", rec)
	got, ok := reg.ResolvePRThread("acme/widget#9")
	if !ok || got.ThreadID != "thread-1" || got.RootMessageID != "root-1" {
		t.Fatalf("resolve = %+v, %v", got, ok)
	}
}

func prTestEndpoint(workflow, path, mode string) config.EndpointConfig {
	return config.EndpointConfig{
		Name:       "github-widget-" + workflow,
		Path:       path,
		Secret:     "s3cret",
		Platform:   "telegram",
		ChannelID:  "chat1",
		Prompt:     "do it",
		Workflow:   workflow,
		Thread:     true,
		Repository: "github.com/acme/widget",
		Admit:      defaultCatchAllAdmit(),
		Automation: &config.RepoAutomation{Default: mode},
	}
}

func prReviewBody(number int, branch, author, state, commit string) string {
	return fmt.Sprintf(`{"action":"submitted","pull_request":{"number":%d,"state":"open","user":{"login":%q},"head":{"ref":%q,"sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"review":{"state":%q,"commit_id":%q,"user":{"login":"reviewer"}},"repository":{"full_name":"acme/widget"}}`,
		number, author, branch, state, commit)
}

type prRecorder struct {
	mu       sync.Mutex
	order    []string
	created  map[string]bool
	entered  int
	peak     int
	release  chan struct{}
	threadID string
}

func newPRRecorder(threadID string, release chan struct{}) *prRecorder {
	return &prRecorder{created: map[string]bool{}, release: release, threadID: threadID}
}

func (r *prRecorder) exec(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
	r.mu.Lock()
	r.order = append(r.order, workCtx.DeliveryID)
	r.entered++
	if r.entered > r.peak {
		r.peak = r.entered
	}
	created := false
	if workCtx.ThreadID == "" {
		workCtx.ThreadID = r.threadID
		created = true
	}
	r.created[workCtx.DeliveryID] = created
	r.mu.Unlock()
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			r.mu.Lock()
			r.entered--
			r.mu.Unlock()
			return ctx.Err()
		}
	}
	r.mu.Lock()
	r.entered--
	r.mu.Unlock()
	return nil
}

func newPRTestServer(t *testing.T, rec *prRecorder, endpoints []config.EndpointConfig) (*Server, *store.SQLiteStore, string) {
	t.Helper()
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: endpoints,
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, rec.exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	setOpenPRState(srv)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return srv, st, ts.URL
}

func waitForCalls(t *testing.T, rec *prRecorder, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		got := len(rec.order)
		rec.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("executor calls = %d, want %d", len(rec.order), n)
}

func TestPRQueueSerialSamePR(t *testing.T) {
	rec := newPRRecorder("thread-pr", make(chan struct{}))
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	postPR := func(deliveryID, commit string) {
		if resp := post(t, url+"/review?secret=s3cret", deliveryID, "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", commit)); resp.StatusCode != http.StatusOK {
			t.Fatalf("post %s: status %d", deliveryID, resp.StatusCode)
		}
	}
	postPR("del-serial-1", "c0ffee1a")
	postPR("del-serial-2", "c0ffee1b")
	time.Sleep(200 * time.Millisecond)
	rec.mu.Lock()
	if len(rec.order) != 1 || rec.order[0] != "del-serial-1" {
		rec.mu.Unlock()
		t.Fatalf("second same-PR run started before first finished: %v", rec.order)
	}
	rec.mu.Unlock()
	close(rec.release)
	waitForCalls(t, rec, 2)
	waitForDeliveryStatus(t, st, "github-widget-review", "del-serial-1", store.WebhookStatusCompleted)
	waitForDeliveryStatus(t, st, "github-widget-review", "del-serial-2", store.WebhookStatusCompleted)
	if rec.order[0] != "del-serial-1" || rec.order[1] != "del-serial-2" {
		t.Fatalf("FIFO order broken: %v", rec.order)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
		if err == nil && run != nil && run.Status == store.PRStatusDrained {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("pr queue row never drained")
}

func TestPRQueueParallelDifferentPRs(t *testing.T) {
	rec := newPRRecorder("thread-x", make(chan struct{}))
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	for _, del := range []struct {
		id   string
		body string
	}{
		{"del-par-1", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")},
		{"del-par-2", prReviewBody(8, "feat/y", "owner", "commented", "c0ffee3")},
	} {
		if resp := post(t, url+"/review?secret=s3cret", del.id, "pull_request_review", del.body); resp.StatusCode != http.StatusOK {
			t.Fatalf("post %s: status %d", del.id, resp.StatusCode)
		}
	}
	waitForCalls(t, rec, 2)
	rec.mu.Lock()
	peak := rec.peak
	rec.mu.Unlock()
	close(rec.release)
	waitForDeliveryStatus(t, st, "github-widget-review", "del-par-1", store.WebhookStatusCompleted)
	waitForDeliveryStatus(t, st, "github-widget-review", "del-par-2", store.WebhookStatusCompleted)
	if peak != 2 {
		t.Fatalf("peak concurrency = %d, want 2 parallel PR runs", peak)
	}
}

func TestPRQueuePriorityReviewBeforeMerge(t *testing.T) {
	rec := newPRRecorder("thread-pr", make(chan struct{}))
	_, _, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
		prTestEndpoint("merge", "/merge", config.AutomationFull),
	})
	if resp := post(t, url+"/merge?secret=s3cret", "del-prio-merge-1", "pull_request_review", prReviewBody(7, "feat/x", "owner", "approved", "c0ffee2")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post merge: status %d", resp.StatusCode)
	}
	waitForCalls(t, rec, 1)
	if resp := post(t, url+"/merge?secret=s3cret", "del-prio-merge-2", "pull_request_review", prReviewBody(7, "feat/x", "owner", "approved", "c0ffee7")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post merge 2: status %d", resp.StatusCode)
	}
	if resp := post(t, url+"/review?secret=s3cret", "del-prio-review", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post review: status %d", resp.StatusCode)
	}
	close(rec.release)
	waitForCalls(t, rec, 3)
	want := []string{"del-prio-merge-1", "del-prio-review", "del-prio-merge-2"}
	for i, id := range want {
		if rec.order[i] != id {
			t.Fatalf("execution order = %v, want %v", rec.order, want)
		}
	}
}

func TestPRThreadReuseSameThread(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	if resp := post(t, url+"/review?secret=s3cret", "del-thread-1", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post del-thread-1: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-thread-1", store.WebhookStatusCompleted)
	if resp := post(t, url+"/review?secret=s3cret", "del-thread-2", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee8")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post del-thread-2: status %d", resp.StatusCode)
	}
	waitForCalls(t, rec, 2)
	if rec.created["del-thread-2"] {
		t.Fatal("second same-PR delivery opened a new thread instead of reusing the first")
	}
	if !rec.created["del-thread-1"] {
		t.Fatal("first delivery should have created the thread")
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || run == nil || run.ThreadID != "thread-pr" {
		t.Fatalf("pr queue thread = %+v, %v; want thread-pr", run, err)
	}
	d1, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-review", "del-thread-1")
	if err != nil || d1 == nil || d1.PRKey != "acme/widget#7" {
		t.Fatalf("receipt pr_key = %+v, %v; want acme/widget#7", d1, err)
	}
}

func TestAutomationGateReviewOnly(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("fix", "/fix", config.AutomationReviewOnly),
		prTestEndpoint("review", "/review", config.AutomationReviewOnly),
	})
	if resp := post(t, url+"/fix?secret=s3cret", "del-gate-fix", "pull_request_review", prReviewBody(9, "feat/z", "stranger", "changes_requested", "c0ffee4")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post fix: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-fix", "del-gate-fix", store.WebhookStatusSkipped)
	time.Sleep(200 * time.Millisecond)
	if n := len(rec.order); n != 0 {
		t.Fatalf("review-only author must not execute fix, calls = %v", rec.order)
	}
	if resp := post(t, url+"/review?secret=s3cret", "del-gate-review", "pull_request_review", prReviewBody(9, "feat/z", "stranger", "commented", "c0ffee5")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post review: status %d", resp.StatusCode)
	}
	waitForCalls(t, rec, 1)
}

func waitForDeliveryStatus(t *testing.T, st *store.SQLiteStore, endpoint, deliveryID string, status store.WebhookStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		d, err := st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
		if err == nil && d != nil && d.Status == status {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("delivery %s never reached %s", deliveryID, status)
}

func TestAutomationGateFullAuthor(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	ep := prTestEndpoint("fix", "/fix", config.AutomationReviewOnly)
	ep.Automation = &config.RepoAutomation{Default: config.AutomationReviewOnly, Authors: map[string]string{"owner": config.AutomationFull}}
	_, _, url := newPRTestServer(t, rec, []config.EndpointConfig{ep})
	if resp := post(t, url+"/fix?secret=s3cret", "del-gate-full", "pull_request_review", prReviewBody(9, "feat/z", "owner", "changes_requested", "c0ffee6")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post fix: status %d", resp.StatusCode)
	}
	waitForCalls(t, rec, 1)
}

func TestPRKeyLockIsolation(t *testing.T) {
	ep := config.EndpointConfig{Workflow: "review", Automation: &config.RepoAutomation{Default: config.AutomationFull}}
	body := []byte(`{"action":"created","issue":{"number":5,"pull_request":{"html_url":"https://github.com/acme/widget/pull/5"}},"comment":{"body":"please re-review"},"repository":{"full_name":"acme/widget"}}`)
	envelope := normalizeWebhook(body, "issue_comment", "del-ic", testVerdicts(), false, "", []string{"please re-review"})
	key := ExtractExecutionKey(body)
	prKey, ok := prKeyForEndpoint(ep, key, envelope)
	if !ok || prKey != "acme/widget#5" {
		t.Fatalf("issue_comment prKey = %q, %v", prKey, ok)
	}
	if got := laneOfWorkflow("fix"); got != 2 {
		t.Fatalf("fix lane = %d", got)
	}
	if !strings.HasPrefix("pr:"+prKey, "pr:acme/widget#") {
		t.Fatalf("pr lock prefix wrong: %q", prKey)
	}
}

func prClosedBody(number int, merged bool) string {
	return fmt.Sprintf(`{"action":"closed","pull_request":{"number":%d,"state":"closed","merged":%v,"user":{"login":"owner"},"head":{"ref":"feat/x","sha":"abc1234def5678","repo":{"full_name":"acme/widget"}},"base":{"ref":"main","repo":{"full_name":"acme/widget"}}},"repository":{"full_name":"acme/widget"}}`,
		number, merged)
}

func TestMergeDrainRequeueThenBlocked(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	srv, st, _ := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
		prTestEndpoint("merge", "/merge", config.AutomationFull),
	})
	d := newDispatcher(srv, "pr:acme/widget#7")
	reviewItem := dispatchItem{
		ep:          config.EndpointConfig{Name: "github-widget-review", Workflow: "review", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "p", Admit: defaultCatchAllAdmit(), Automation: &config.RepoAutomation{Default: config.AutomationFull}},
		body:        []byte(prReviewBody(7, "feat/x", "owner", "commented", "c0ffee9")),
		deliveryID:  "del-drain-review",
		eventType:   "pull_request_review",
		payloadHash: "h",
		prKey:       "acme/widget#7",
		lane:        1,
	}
	if !d.push(reviewItem) {
		t.Fatal("review push failed")
	}
	mergeEp := config.EndpointConfig{Name: "github-widget-merge", Workflow: "merge", Secret: "s3cret", Platform: "telegram", ChannelID: "chat1", Prompt: "p", Admit: defaultCatchAllAdmit(), Automation: &config.RepoAutomation{Default: config.AutomationFull}}
	mergeBody := []byte(prReviewBody(7, "feat/x", "owner", "approved", "c0ffeeb"))
	mergeItem := dispatchItem{ep: mergeEp, body: mergeBody, deliveryID: "del-drain-merge", eventType: "pull_request_review", payloadHash: "h", prKey: "acme/widget#7", lane: 3}
	popLane3 := func() dispatchItem {
		t.Helper()
		d.lanes[3].mu.Lock()
		defer d.lanes[3].mu.Unlock()
		if len(d.lanes[3].items) == 0 {
			t.Fatal("merge item not requeued at back of lane 3")
			return dispatchItem{}
		}
		next := d.lanes[3].items[0]
		d.lanes[3].items = d.lanes[3].items[1:]
		return next
	}
	backdateReceipt := func() {
		t.Helper()
		if _, err := st.DB().Exec(`UPDATE webhook_delivery SET updated_at = 1 WHERE endpoint = 'github-widget-merge' AND delivery_id = 'del-drain-merge'`); err != nil {
			t.Fatalf("backdate receipt: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		d.handle(mergeItem)
		if len(rec.order) != 0 {
			t.Fatalf("merge must not execute while higher lanes are queued (iter %d)", i)
		}
		mergeItem = popLane3()
		if mergeItem.requeues != i+1 {
			t.Fatalf("requeues = %d, want %d", mergeItem.requeues, i+1)
		}
		backdateReceipt()
	}
	d.handle(mergeItem)
	if len(rec.order) != 0 {
		t.Fatal("merge must not execute after exhausting requeues")
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "acme/widget#7")
	if err != nil || run == nil || run.Status != store.PRStatusBlocked {
		t.Fatalf("pr queue = %+v, %v; want blocked", run, err)
	}
	d1, err := st.WebhookDeliveryRepo().Get(context.Background(), "github-widget-merge", "del-drain-merge")
	if err != nil || d1 == nil || d1.Status != store.WebhookStatusSkipped {
		t.Fatalf("merge receipt = %+v, %v; want skipped", d1, err)
	}
}

func TestMergedEventDrainsQueue(t *testing.T) {
	rec := newPRRecorder("thread-pr", make(chan struct{}))
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("fix", "/fix", config.AutomationFull),
		prTestEndpoint("merged", "/merged", config.AutomationFull),
	})
	if resp := post(t, url+"/fix?secret=s3cret", "del-mq-fix", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee10")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post fix: status %d", resp.StatusCode)
	}
	waitForCalls(t, rec, 1)
	if resp := post(t, url+"/merged?secret=s3cret", "del-mq-merged", "pull_request", prClosedBody(7, true)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post merged: status %d", resp.StatusCode)
	}
	if resp := post(t, url+"/fix?secret=s3cret", "del-mq-fix-2", "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee11")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post fix 2: status %d", resp.StatusCode)
	}
	close(rec.release)
	waitForDeliveryStatus(t, st, "github-widget-merged", "del-mq-merged", store.WebhookStatusCompleted)
	waitForCalls(t, rec, 2)
	rec.mu.Lock()
	order := append([]string(nil), rec.order...)
	rec.mu.Unlock()
	if len(order) != 2 || order[0] != "del-mq-fix" || order[1] != "del-mq-merged" {
		t.Fatalf("merged must run after drain, dropping queued fix; calls = %v", order)
	}
	waitForPRStatus(t, st, "acme/widget#7", store.PRStatusDrained)
}

// waitForPRStatus waits until the PR queue row of a key reaches a status. The
// delivery is marked completed before the queue is drained, so the drain is
// only observable after it happened.
func waitForPRStatus(t *testing.T, st *store.SQLiteStore, prKey string, want store.PRStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := st.PRQueueRepo().Get(context.Background(), prKey)
		if err == nil && run != nil && run.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pr queue row = %+v, %v; want %s", run, err, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClosedPRSkipsAndDrains(t *testing.T) {
	rec := newPRRecorder("thread-pr", nil)
	_, st, url := newPRTestServer(t, rec, []config.EndpointConfig{
		prTestEndpoint("review", "/review", config.AutomationFull),
	})
	if resp := post(t, url+"/review?secret=s3cret", "del-closed", "pull_request", prClosedBody(7, false)); resp.StatusCode != http.StatusOK {
		t.Fatalf("post closed: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, "github-widget-review", "del-closed", store.WebhookStatusSkipped)
	if len(rec.order) != 0 {
		t.Fatalf("closed PR must not execute, calls = %v", rec.order)
	}
}
