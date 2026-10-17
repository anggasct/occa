package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func dispatchEndpoints(repo string) []config.EndpointConfig {
	workflows := []string{"review", "fix", "merge", "merged"}
	out := make([]config.EndpointConfig, 0, len(workflows))
	for _, workflow := range workflows {
		out = append(out, config.EndpointConfig{
			Name:       "ep-" + workflow,
			Path:       "/webhooks/ep-" + workflow,
			Auth:       "legacy_bearer",
			Secret:     "secret-" + workflow,
			Workflow:   workflow,
			Platform:   "discord",
			ChannelID:  "chan-1",
			Prompt:     "prompt",
			Repository: repo,
			Workspace:  config.EndpointWorkspace{Type: "none"},
		})
	}
	return out
}

func newDispatchServer(t *testing.T, endpoints []config.EndpointConfig) *Server {
	t.Helper()
	exec := &fakeExecutor{}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: withAdmitDefaults(endpoints),
		Dispatch: config.DispatchConfig{
			Enabled: true,
			Path:    "/dispatch",
			Token:   "dispatch-token",
		},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "dispatch.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec.exec, st.WebhookDeliveryRepo())
	srv.SetFallbackPollAfter(time.Hour)
	return srv
}

func postDispatch(t *testing.T, srv *Server, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	srv.handleDispatch(rec, req)
	return rec
}

const validDispatchBody = `{"repo":"acme/widget","base_sha":"abcdef1234567890","branch":"feat/x","spec_path":"specs/feat-x.md","scope":"ship it","verify":"go test ./..."}`

func TestHandleDispatchSuccess(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.HasPrefix(payload["task_id"], "task-") {
		t.Errorf("task_id = %q, want task- prefix", payload["task_id"])
	}
	task, ok := srv.dispatchTasks.Get(payload["task_id"])
	if !ok || task.Repo != "acme/widget" || task.Branch != "feat/x" || task.SpecPath != "specs/feat-x.md" {
		t.Errorf("stored task = %+v, %v", task, ok)
	}
	rec2, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok || rec2.Status != TaskDispatched {
		t.Errorf("registry = %+v, %v; want dispatched record", rec2, ok)
	}
	summary := FormatDispatchSummary(task, nil)
	lines := strings.Split(strings.TrimSpace(summary), "\n")
	if len(lines) != 2 {
		t.Errorf("dispatch summary must be 2 lines, got %q", summary)
	}
	if !strings.Contains(summary, "⏳") || !strings.Contains(summary, task.ID) {
		t.Errorf("summary = %q", summary)
	}
}

func TestHandleDispatchTokenHeader(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(validDispatchBody))
	req.Header.Set("X-Dispatch-Token", "dispatch-token")
	rec := httptest.NewRecorder()
	srv.handleDispatch(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("header token status = %d, want 201", rec.Code)
	}
}

func TestHandleDispatchPostsSummary(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	var mu sync.Mutex
	var posts []string
	var platform, channel string
	srv.SetNotifier(func(_ context.Context, p, c, text string) error {
		mu.Lock()
		posts = append(posts, text)
		platform, channel = p, c
		mu.Unlock()
		return nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(posts) != 1 {
		t.Fatalf("summary posts = %d, want 1", len(posts))
	}
	if platform != "discord" || channel != "chan-1" {
		t.Errorf("target = %s/%s, want discord/chan-1", platform, channel)
	}
	if !strings.Contains(posts[0], "Dispatched") || !strings.Contains(posts[0], payload["task_id"]) {
		t.Errorf("summary = %q, want Dispatched with %q", posts[0], payload["task_id"])
	}
	if !strings.Contains(posts[0], "acme/widget#feat/x") || !strings.Contains(posts[0], "specs/feat-x.md") {
		t.Errorf("summary missing repo/branch/spec: %q", posts[0])
	}
}

func TestHandleDispatchAuth(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	if rec := postDispatch(t, srv, "", validDispatchBody); rec.Code != http.StatusUnauthorized {
		t.Errorf("missing token status = %d, want 401", rec.Code)
	}
	if rec := postDispatch(t, srv, "wrong", validDispatchBody); rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong token status = %d, want 401", rec.Code)
	}
}

func TestHandleDispatchValidation(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	cases := map[string]string{
		"malformed json": `{"repo":`,
		"missing branch": `{"repo":"acme/widget","spec_path":"specs/x.md"}`,
		"bad repo":       `{"repo":"not a repo!!!","branch":"feat/x","spec_path":"specs/x.md"}`,
		"bad sha":        `{"repo":"acme/widget","branch":"feat/x","spec_path":"specs/x.md","base_sha":"zzz"}`,
		"traversal spec": `{"repo":"acme/widget","branch":"feat/x","spec_path":"../evil.md"}`,
		"unknown field":  `{"repo":"acme/widget","branch":"feat/x","spec_path":"specs/x.md","nope":1}`,
		"invalid branch": `{"repo":"acme/widget","branch":"-bad","spec_path":"specs/x.md"}`,
	}
	for name, body := range cases {
		if rec := postDispatch(t, srv, "dispatch-token", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400: %s", name, rec.Code, rec.Body.String())
		}
	}
}

func TestHandleDispatchMissingWebhooks(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/other"))
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	missing, _ := payload["missing"].([]any)
	if len(missing) != 4 {
		t.Errorf("missing = %v, want all 4 workflows", payload["missing"])
	}
}

func TestHandleDispatchMethodAndDisabled(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	req := httptest.NewRequest(http.MethodGet, "/dispatch", nil)
	req.Header.Set("Authorization", "Bearer dispatch-token")
	rec := httptest.NewRecorder()
	srv.handleDispatch(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", rec.Code)
	}

	plain := &fakeExecutor{}
	cfg := config.WebhookConfig{Bind: "127.0.0.1:0", Policy: testPolicy(), Runtime: testRuntime()}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "plain.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	off := New(cfg, plain.exec, st.WebhookDeliveryRepo())
	rec = postDispatch(t, off, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusNotFound {
		t.Errorf("disabled dispatch status = %d, want 404", rec.Code)
	}
}

func TestDispatchRequestValidate(t *testing.T) {
	req := DispatchRequest{Repo: "acme/widget", Branch: "feat/x", SpecPath: "specs/x.md"}
	if err := req.Validate(); err != nil {
		t.Errorf("valid request: %v", err)
	}
	req.BaseSHA = "ABCDEF1234567"
	if err := req.Validate(); err != nil {
		t.Errorf("uppercase sha: %v", err)
	}
}

func TestDispatchRecordClaimLookupBothDirections(t *testing.T) {
	reg := NewTaskThreadRegistry()
	task := DispatchTask{ID: "task-1", Repo: "github.com/acme/widget", Branch: "feat/hosted", SpecPath: "specs/x.md"}
	reg.Upsert(task.Repo, task.Branch, "root-1", "thread-1", TaskDispatched)
	summary := FormatDispatchSummary(task, nil)
	repo, branch, taskID, ok := channel.ParseDispatchSummary(summary)
	if !ok || taskID != "task-1" {
		t.Fatalf("parse summary %q = %q %q %q %v", summary, repo, branch, taskID, ok)
	}
	if rec, ok := reg.Lookup(repo, branch); !ok || rec.ThreadID != "thread-1" {
		t.Fatalf("host record short lookup miss: %+v %v (parsed %q %q)", rec, ok, repo, branch)
	}
	reg2 := NewTaskThreadRegistry()
	task2 := DispatchTask{ID: "task-2", Repo: "acme/widget", Branch: "feat/short", SpecPath: "specs/x.md"}
	reg2.Upsert(task2.Repo, task2.Branch, "root-2", "thread-2", TaskDispatched)
	summary2 := FormatDispatchSummary(task2, nil)
	repo2, branch2, _, ok := channel.ParseDispatchSummary(summary2)
	if !ok {
		t.Fatalf("parse short summary %q failed", summary2)
	}
	if rec, ok := reg2.Lookup("github.com/"+repo2, branch2); !ok || rec.ThreadID != "thread-2" {
		t.Fatalf("short record host lookup miss: %+v %v", rec, ok)
	}
}

func postDispatchAt(t *testing.T, srv *Server, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	srv.handleDispatch(rec, req)
	return rec
}

func TestHandleDispatchIngressPrefix(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	for _, path := range []string{"/dispatch", "/occa/dispatch"} {
		if rec := postDispatchAt(t, srv, path, "dispatch-token", validDispatchBody); rec.Code != http.StatusCreated {
			t.Errorf("%s: status = %d, want 201: %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestScheduleDispatchFallbackFires(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetFallbackPollAfter(20 * time.Millisecond)
	var branchCalls atomic.Int64
	srv.SetBranchChecker(func(ctx context.Context, repo, branch string) (BranchPRResult, error) {
		branchCalls.Add(1)
		return BranchPRResult{}, nil
	})
	var mu sync.Mutex
	var senderPosts []string
	var posts []string
	var platforms []string
	var channels []string
	srv.SetSender(func(_ context.Context, _, _, text string) (string, error) {
		mu.Lock()
		senderPosts = append(senderPosts, text)
		mu.Unlock()
		return "parent-123", nil
	})
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		return "thread-456", nil
	})
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		posts = append(posts, text)
		platforms = append(platforms, platform)
		channels = append(channels, channelID)
		mu.Unlock()
		return nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("dispatch status = %d, want 201", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	taskID := payload["task_id"]
	mu.Lock()
	if len(senderPosts) != 1 {
		mu.Unlock()
		t.Fatalf("dispatch summary posts = %d, want 1 synchronous post", len(senderPosts))
	}
	if !strings.Contains(senderPosts[0], "Dispatched") || !strings.Contains(senderPosts[0], taskID) {
		t.Errorf("dispatch summary = %q, want Dispatched with %q", senderPosts[0], taskID)
	}
	mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for branchCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := branchCalls.Load(); got != 1 {
		t.Errorf("branch checks = %d, want exactly 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(posts) != 1 {
		t.Fatalf("finding posts = %d, want exactly 1 thread-only fallback", len(posts))
	}
	if !strings.Contains(posts[0], "Fallback poll") {
		t.Errorf("posts missing fallback finding: %q", posts[0])
	}
	if strings.Contains(posts[0], "Dispatched") {
		t.Errorf("fallback must not repeat dispatch summary: %q", posts[0])
	}
	for i := range platforms {
		if platforms[i] != "discord" || channels[i] != "thread-456" {
			t.Errorf("post %d target = %s/%s, want discord/thread-456", i, platforms[i], channels[i])
		}
	}
}

func TestScheduleDispatchFallbackNoPRWithoutThreadSuppressed(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetFallbackPollAfter(20 * time.Millisecond)
	var branchCalls atomic.Int64
	srv.SetBranchChecker(func(ctx context.Context, repo, branch string) (BranchPRResult, error) {
		branchCalls.Add(1)
		return BranchPRResult{}, nil
	})
	var mu sync.Mutex
	var posts []string
	srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error {
		mu.Lock()
		posts = append(posts, text)
		mu.Unlock()
		return nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("dispatch status = %d, want 201", rec.Code)
	}
	deadline := time.Now().Add(2 * time.Second)
	for branchCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := branchCalls.Load(); got != 1 {
		t.Errorf("branch checks = %d, want exactly 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(posts) != 1 {
		t.Fatalf("posts = %d, want 1 summary only with no-PR thread-only suppressed", len(posts))
	}
	if !strings.Contains(posts[0], "Dispatched") {
		t.Errorf("posts missing dispatch summary: %q", posts[0])
	}
	if strings.Contains(strings.Join(posts, "\n"), "Fallback poll") {
		t.Errorf("no-PR fallback must never post top-level without thread: %q", posts)
	}
}

func TestScheduleDeliveryFallbackTerminalSkips(t *testing.T) {
	newFallbackServer := func(t *testing.T) (*Server, *atomic.Int64) {
		t.Helper()
		srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
		srv.SetFallbackPollAfter(20 * time.Millisecond)
		var calls atomic.Int64
		srv.SetBranchChecker(func(ctx context.Context, repo, branch string) (BranchPRResult, error) {
			calls.Add(1)
			return BranchPRResult{}, nil
		})
		srv.SetNotifier(func(ctx context.Context, platform, channelID, text string) error { return nil })
		return srv, &calls
	}
	deliveryCtx := func(repo, branch, threadID string) *WebhookWorkContext {
		return &WebhookWorkContext{
			Key:      WebhookExecutionKey{Repository: repo, Branch: branch},
			Envelope: WebhookEnvelope{"repository": repo, "head_branch": branch},
			ThreadID: threadID,
		}
	}

	srv, calls := newFallbackServer(t)
	srv.tasks.Upsert("acme/widget", "feat/merged", "root-1", "thread-1", TaskMerged)
	srv.scheduleDeliveryFallback(dispatchEndpoints("acme/widget")[2], deliveryCtx("acme/widget", "feat/merged", "thread-1"))
	time.Sleep(150 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Errorf("merged delivery branch checks = %d, want 0", got)
	}

	srv2, calls2 := newFallbackServer(t)
	srv2.tasks.Upsert("acme/widget", "feat/active", "root-2", "thread-2", TaskClaimed)
	srv2.scheduleDeliveryFallback(dispatchEndpoints("acme/widget")[0], deliveryCtx("acme/widget", "feat/active", "thread-2"))
	deadline := time.Now().Add(2 * time.Second)
	for calls2.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := calls2.Load(); got != 1 {
		t.Errorf("claimed delivery branch checks = %d, want exactly 1", got)
	}
}

func TestPollBranchOnceDoesNotAdoptCrossRepoThread(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	var mu sync.Mutex
	var targets []string
	srv.SetNotifier(func(_ context.Context, _, channelID, _ string) error {
		mu.Lock()
		targets = append(targets, channelID)
		mu.Unlock()
		return nil
	})
	srv.SetBranchChecker(func(_ context.Context, _, _ string) (BranchPRResult, error) {
		return BranchPRResult{}, nil
	})
	srv.pollBranchOnce("other/widget", "feat/x", "discord", "chan-1", "")
	mu.Lock()
	defer mu.Unlock()
	if len(targets) != 0 {
		t.Fatalf("cross-repo fallback posts = %v, want none suppressed without PR", targets)
	}
	for _, target := range targets {
		if target == "thread-1" {
			t.Fatalf("fallback adopted another repo's thread: %v", targets)
		}
	}
}
