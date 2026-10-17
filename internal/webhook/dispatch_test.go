package webhook

import (
	"context"
	"encoding/json"
	"errors"
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

func TestHandleDispatchIdempotentLiveRecord(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	var mu sync.Mutex
	var parents, threads int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		mu.Lock()
		parents++
		mu.Unlock()
		return "parent-1", nil
	})
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		threads++
		mu.Unlock()
		return "thread-1", nil
	})
	first := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201: %s", first.Code, first.Body.String())
	}
	var firstPayload map[string]string
	if err := json.Unmarshal(first.Body.Bytes(), &firstPayload); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	normalized := `{"repo":"github.com/ACME/widget","base_sha":"abcdef1234567890","branch":"FEAT/X","spec_path":"specs/feat-x.md","scope":"ship it","verify":"go test ./..."}`
	second := postDispatch(t, srv, "dispatch-token", normalized)
	if second.Code != http.StatusOK {
		t.Fatalf("retry status = %d, want 200: %s", second.Code, second.Body.String())
	}
	var secondPayload map[string]string
	if err := json.Unmarshal(second.Body.Bytes(), &secondPayload); err != nil {
		t.Fatalf("decode retry: %v", err)
	}
	if secondPayload["task_id"] != firstPayload["task_id"] {
		t.Errorf("retry task_id = %q, want existing %q", secondPayload["task_id"], firstPayload["task_id"])
	}
	mu.Lock()
	defer mu.Unlock()
	if parents != 1 || threads != 1 {
		t.Errorf("parents = %d threads = %d, want exactly 1 each", parents, threads)
	}
	if got := srv.dispatchTasks.Count(); got != 1 {
		t.Errorf("stored tasks = %d, want 1", got)
	}
}

func TestHandleDispatchFreshAfterTerminal(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	var mu sync.Mutex
	var parents int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		mu.Lock()
		parents++
		mu.Unlock()
		return "parent-1", nil
	})
	first := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201: %s", first.Code, first.Body.String())
	}
	var firstPayload map[string]string
	if err := json.Unmarshal(first.Body.Bytes(), &firstPayload); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	srv.tasks.Upsert("acme/widget", "feat/x", "", "", TaskFailed)
	second := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if second.Code != http.StatusCreated {
		t.Fatalf("retry-after-failed status = %d, want 201: %s", second.Code, second.Body.String())
	}
	var secondPayload map[string]string
	if err := json.Unmarshal(second.Body.Bytes(), &secondPayload); err != nil {
		t.Fatalf("decode retry: %v", err)
	}
	if secondPayload["task_id"] == "" || secondPayload["task_id"] == firstPayload["task_id"] {
		t.Errorf("retry-after-failed task_id = %q, want fresh id", secondPayload["task_id"])
	}
	srv.tasks.Upsert("acme/widget", "feat/x", "", "", TaskMerged)
	third := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if third.Code != http.StatusCreated {
		t.Fatalf("retry-after-merged status = %d, want 201: %s", third.Code, third.Body.String())
	}
	var thirdPayload map[string]string
	if err := json.Unmarshal(third.Body.Bytes(), &thirdPayload); err != nil {
		t.Fatalf("decode third: %v", err)
	}
	if thirdPayload["task_id"] == "" || thirdPayload["task_id"] == secondPayload["task_id"] {
		t.Errorf("retry-after-merged task_id = %q, want fresh id", thirdPayload["task_id"])
	}
	mu.Lock()
	defer mu.Unlock()
	if parents != 3 {
		t.Errorf("parents = %d, want 3 fresh posts", parents)
	}
}

func TestHandleDispatchParentFailure(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	var mu sync.Mutex
	parentFails := true
	var parents, threads int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		mu.Lock()
		parents++
		mu.Unlock()
		if parentFails {
			return "", errors.New("channel adapter unavailable")
		}
		return "parent-1", nil
	})
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		threads++
		mu.Unlock()
		return "thread-1", nil
	})
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, hasTask := payload["task_id"]; hasTask {
		t.Errorf("failed post accepted task_id %q, want no accept", payload["task_id"])
	}
	if !strings.Contains(payload["error"], "dispatch parent post failed") {
		t.Errorf("error = %q, want parent post reason", payload["error"])
	}
	mu.Lock()
	if parents != 1 || threads != 0 {
		t.Errorf("parents = %d threads = %d, want 1 parent attempt and no thread spawn", parents, threads)
	}
	mu.Unlock()
	if got := srv.dispatchTasks.Count(); got != 0 {
		t.Errorf("stored tasks = %d, want 0 after parent failure", got)
	}
	if live, ok := srv.tasks.Lookup("acme/widget", "feat/x"); ok && !isTaskTerminal(live.Status) {
		t.Errorf("registry holds live dead record %+v after parent failure", live)
	}
	mu.Lock()
	parentFails = false
	mu.Unlock()
	retry := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if retry.Code != http.StatusCreated {
		t.Fatalf("retry status = %d, want 201: %s", retry.Code, retry.Body.String())
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
	if rec, ok := reg.Lookup("github.com/acme/widget", "feat/hosted"); !ok || rec.ThreadID != "thread-1" {
		t.Fatalf("host record fallback host lookup miss: %+v %v", rec, ok)
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
	if rec, ok := reg2.Lookup(repo2, branch2); !ok || rec.ThreadID != "thread-2" {
		t.Fatalf("short record fallback short lookup miss: %+v %v", rec, ok)
	}
}

func TestDispatchCreateNormalizesRepoKey(t *testing.T) {
	store := NewDispatchStore()
	cases := map[string]string{
		"github.com/acme/widget":         "acme/widget",
		"acme/widget":                    "acme/widget",
		" GitHub.com/Acme/Widget ":       "acme/widget",
		"https://github.com/acme/widget": "acme/widget",
		"ACME/WIDGET":                    "acme/widget",
	}
	for in, want := range cases {
		task := store.Create(DispatchRequest{Repo: in, Branch: "feat/x", SpecPath: "specs/x.md"})
		if task.Repo != want {
			t.Errorf("Create(%q).Repo = %q, want %q", in, task.Repo, want)
		}
	}
}

func TestFormatDispatchSummaryNormalizesRepoKey(t *testing.T) {
	task := DispatchTask{ID: "task-9", Repo: "github.com/acme/widget", Branch: "feat/hosted", SpecPath: "specs/x.md"}
	summary := FormatDispatchSummary(task, nil)
	if !strings.Contains(summary, "acme/widget#feat/hosted") {
		t.Errorf("summary %q must contain normalized acme/widget#feat/hosted", summary)
	}
	if strings.Contains(summary, "github.com/acme/widget") {
		t.Errorf("summary %q must not contain raw host prefix", summary)
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
	first := postDispatchAt(t, srv, "/dispatch", "dispatch-token", validDispatchBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("/dispatch: status = %d, want 201: %s", first.Code, first.Body.String())
	}
	var firstPayload map[string]string
	if err := json.Unmarshal(first.Body.Bytes(), &firstPayload); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	second := postDispatchAt(t, srv, "/occa/dispatch", "dispatch-token", validDispatchBody)
	if second.Code != http.StatusOK {
		t.Fatalf("/occa/dispatch: status = %d, want 200: %s", second.Code, second.Body.String())
	}
	var secondPayload map[string]string
	if err := json.Unmarshal(second.Body.Bytes(), &secondPayload); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if secondPayload["task_id"] != firstPayload["task_id"] {
		t.Errorf("prefixed retry task_id = %q, want existing %q", secondPayload["task_id"], firstPayload["task_id"])
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

func TestHandleDispatchConcurrentDuplicatesYieldOneParent(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	var mu sync.Mutex
	var parents, threads int
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) {
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		parents++
		mu.Unlock()
		return "parent-1", nil
	})
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) {
		mu.Lock()
		threads++
		mu.Unlock()
		return "thread-1", nil
	})
	const n = 10
	codes := make([]int, n)
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(validDispatchBody))
			req.Header.Set("Authorization", "Bearer dispatch-token")
			rec := httptest.NewRecorder()
			srv.handleDispatch(rec, req)
			codes[idx] = rec.Code
			var payload map[string]string
			_ = json.Unmarshal(rec.Body.Bytes(), &payload)
			ids[idx] = payload["task_id"]
		}(i)
	}
	wg.Wait()
	for _, id := range ids {
		if id == "" {
			t.Fatalf("concurrent task_ids = %v, want all non-empty", ids)
		}
		if id != ids[0] {
			t.Fatalf("concurrent task_ids = %v, want single shared id", ids)
		}
	}
	created, ok := 0, false
	for _, c := range codes {
		if c == http.StatusCreated {
			created++
		} else if c != http.StatusOK {
			t.Fatalf("concurrent codes = %v, want only 201 and 200", codes)
		} else {
			ok = true
		}
	}
	if created != 1 || !ok {
		t.Fatalf("concurrent codes = %v, want exactly one 201 and rest 200", codes)
	}
	mu.Lock()
	defer mu.Unlock()
	if parents != 1 || threads != 1 {
		t.Errorf("parents = %d threads = %d, want exactly 1 each", parents, threads)
	}
	if got := srv.dispatchTasks.Count(); got != 1 {
		t.Errorf("stored tasks = %d, want 1", got)
	}
}

func TestHandleDispatchLiveWithoutTaskAllowsFresh(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "parent-1", nil })
	srv.tasks.Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	rec := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("stale live status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["task_id"] == "" {
		t.Errorf("stale live task_id empty, want fresh id")
	}
}
