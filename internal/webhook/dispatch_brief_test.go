package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/render"
)

func dispatchJSON(t *testing.T, value map[string]any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal dispatch body: %v", err)
	}
	return string(body)
}

func decodeTaskID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode task id: %v", err)
	}
	return payload["task_id"]
}

func TestDispatchAcceptsBriefOnly(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	brief := "Implement the dispatch brief payload.\n\nFiles:\n- internal/webhook/dispatch.go\n- internal/config/default_prompts.go"
	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  brief,
	}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	task, ok := srv.dispatchTasks.Get(decodeTaskID(t, rec))
	if !ok {
		t.Fatal("task not stored")
	}
	if task.Brief != brief {
		t.Errorf("brief = %q, want %q", task.Brief, brief)
	}
	if task.SpecPath != "" {
		t.Errorf("spec_path = %q, want empty", task.SpecPath)
	}
}

func TestDispatchLegacySpecPathDerivesBrief(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	rec := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x","spec_path":"specs/feat-x.md"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	task, ok := srv.dispatchTasks.Get(decodeTaskID(t, rec))
	if !ok {
		t.Fatal("task not stored")
	}
	if task.Brief != "Read specs/feat-x.md" {
		t.Errorf("brief = %q, want Read specs/feat-x.md", task.Brief)
	}
	if task.SpecPath != "specs/feat-x.md" {
		t.Errorf("spec_path = %q, want specs/feat-x.md", task.SpecPath)
	}
}

func TestDispatchBriefWinsOverSpecPath(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":      "acme/widget",
		"branch":    "feat/x",
		"brief":     "Do the thing.",
		"spec_path": "specs/feat-x.md",
	}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	task, ok := srv.dispatchTasks.Get(decodeTaskID(t, rec))
	if !ok {
		t.Fatal("task not stored")
	}
	if task.Brief != "Do the thing.\n\nRef: specs/feat-x.md" {
		t.Errorf("brief = %q, want brief followed by a Ref line", task.Brief)
	}
	if task.SpecPath != "specs/feat-x.md" {
		t.Errorf("spec_path = %q, want specs/feat-x.md", task.SpecPath)
	}
}

func TestDispatchBriefLengthBound(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	accepted := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  strings.Repeat("a", dispatchBriefMaxRunes),
	}))
	if accepted.Code != http.StatusCreated {
		t.Fatalf("8000-rune brief status = %d, want 201: %s", accepted.Code, accepted.Body.String())
	}
	rejected := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/y",
		"brief":  strings.Repeat("a", dispatchBriefMaxRunes+1),
	}))
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("8001-rune brief status = %d, want 400: %s", rejected.Code, rejected.Body.String())
	}
	if !strings.Contains(rejected.Body.String(), "brief is too long") {
		t.Errorf("error = %q, want brief is too long", rejected.Body.String())
	}
}

func TestDispatchRequiresBriefOrSpecPath(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	rec := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "brief is required") {
		t.Errorf("error = %q, want brief is required", rec.Body.String())
	}
}

func TestDispatchRejectsBlankBrief(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	rec := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x","brief":"   \n\t"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "brief is required") {
		t.Errorf("error = %q, want brief is required", rec.Body.String())
	}
}

func TestDispatchRejectsUnknownFields(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	rec := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x","brief":"Do it","nope":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestDispatchScopeIsDeprecatedAlias(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	onlyScope := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x","scope":"ship it"}`)
	if onlyScope.Code != http.StatusBadRequest {
		t.Fatalf("scope-only status = %d, want 400: %s", onlyScope.Code, onlyScope.Body.String())
	}
	if !strings.Contains(onlyScope.Body.String(), "brief is required") {
		t.Errorf("scope-only error = %q, want brief is required", onlyScope.Body.String())
	}
	folded := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x","brief":"Do it","scope":"ship it"}`)
	if folded.Code != http.StatusCreated {
		t.Fatalf("brief+scope status = %d, want 201: %s", folded.Code, folded.Body.String())
	}
	task, ok := srv.dispatchTasks.Get(decodeTaskID(t, folded))
	if !ok {
		t.Fatal("task not stored")
	}
	if !strings.Contains(task.Brief, "ship it") {
		t.Errorf("brief = %q, want the deprecated scope folded in", task.Brief)
	}
}

func TestDispatchRedispatchMode(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "root-1", nil })
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) { return "thread-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { return nil })
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) { return true, nil })
	first := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first dispatch status = %d, want 201: %s", first.Code, first.Body.String())
	}
	firstID := decodeTaskID(t, first)
	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  "Retry with a new brief",
		"mode":   "redispatch",
	}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("redispatch status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	newID := decodeTaskID(t, rec)
	if newID == firstID {
		t.Errorf("redispatch task id = %q, want a fresh task", newID)
	}
	task, ok := srv.dispatchTasks.Get(newID)
	if !ok || task.Brief != "Retry with a new brief" {
		t.Errorf("redispatch task = %+v, %v", task, ok)
	}
	stored, ok := srv.tasks.Lookup("acme/widget", "feat/x")
	if !ok || stored.ThreadID != "thread-1" {
		t.Errorf("registry = %+v, %v; want the same thread", stored, ok)
	}
}

func TestDispatchLegacyRedispatchScope(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "root-1", nil })
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) { return "thread-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	srv.SetNotifier(func(_ context.Context, _, _, _ string) error { return nil })
	srv.SetBranchExistsChecker(func(_ context.Context, _, _ string) (bool, error) { return true, nil })
	first := postDispatch(t, srv, "dispatch-token", validDispatchBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first dispatch status = %d, want 201: %s", first.Code, first.Body.String())
	}
	rec := postDispatch(t, srv, "dispatch-token", `{"repo":"acme/widget","branch":"feat/x","spec_path":"specs/placeholder.md","scope":"redispatch feat/x"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("legacy redispatch status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	task, ok := srv.dispatchTasks.Get(decodeTaskID(t, rec))
	if !ok {
		t.Fatal("task not stored")
	}
	if task.SpecPath != "specs/feat-x.md" {
		t.Errorf("redispatch spec = %q, want the original specs/feat-x.md", task.SpecPath)
	}
	if !strings.Contains(task.Brief, "redispatch feat/x") {
		t.Errorf("brief = %q, want the legacy redispatch marker folded in", task.Brief)
	}
}

func TestDispatchParentStaysOneLine(t *testing.T) {
	task := DispatchTask{ID: "task-1", Repo: "acme/widget", Branch: "feat/x", Brief: "line one\nline two\nline three"}
	summary := FormatDispatchSummary(task, nil)
	if strings.Contains(summary, "\n") {
		t.Fatalf("parent must be one line: %q", summary)
	}
	if strings.Contains(summary, "line one") {
		t.Fatalf("parent must not carry the brief: %q", summary)
	}
}

func TestDispatchBriefPostedInThreadChunked(t *testing.T) {
	srv := newDispatchServer(t, dispatchEndpoints("acme/widget"))
	srv.SetSender(func(_ context.Context, _, _, _ string) (string, error) { return "root-1", nil })
	srv.SetThreadStarter(func(_ context.Context, _, _, _, _ string) (string, error) { return "thread-1", nil })
	srv.SetEditor(func(_ context.Context, _, _, _, _ string) error { return nil })
	var posts []string
	srv.SetNotifier(func(_ context.Context, _, _, text string) error {
		posts = append(posts, text)
		return nil
	})
	brief := strings.Repeat("a", dispatchBriefMaxRunes)
	rec := postDispatch(t, srv, "dispatch-token", dispatchJSON(t, map[string]any{
		"repo":   "acme/widget",
		"branch": "feat/x",
		"brief":  brief,
	}))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	if len(posts) < 2 {
		t.Fatalf("thread posts = %d, want an opener plus brief chunks", len(posts))
	}
	limit := 2000 - continuationReserve
	expected := render.Split(brief, limit)
	if len(posts) < 1+len(expected) {
		t.Fatalf("thread posts = %d, want an opener plus %d brief chunks", len(posts), len(expected))
	}
	chunks := posts[1 : 1+len(expected)]
	for i, chunk := range chunks {
		if n := utf8.RuneCountInString(chunk); n > limit {
			t.Errorf("chunk %d has %d runes, want <= %d", i, n, limit)
		}
		if strings.Contains(chunk, "…") {
			t.Errorf("chunk %d was truncated: %q", i, chunk)
		}
		if chunk != expected[i] {
			t.Errorf("chunk %d = %q, want %q", i, chunk, expected[i])
		}
	}
	if joined := strings.Join(chunks, ""); joined != brief {
		t.Errorf("chunks concatenate to %d runes, want %d", utf8.RuneCountInString(joined), utf8.RuneCountInString(brief))
	}
}

func TestBuiltinPromptsRenderTaskBrief(t *testing.T) {
	store := NewDispatchStore()
	store.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", Brief: "Implement the multi-file brief.\nFiles: internal/webhook/dispatch.go", Verify: "go test ./..."})
	envelope := minimalEnvelope()
	injectTaskContext(envelope, WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}, store)
	for _, workflow := range []string{"review", "fix"} {
		rendered, err := renderTemplate(config.BuiltinPrompt(workflow), map[string]any{"webhook": envelope})
		if err != nil {
			t.Fatalf("%s render: %v", workflow, err)
		}
		if !strings.Contains(rendered, "Implement the multi-file brief.") || !strings.Contains(rendered, "internal/webhook/dispatch.go") {
			t.Errorf("%s prompt = %q, want the brief content", workflow, rendered)
		}
		if !strings.Contains(rendered, "Verify: go test ./...") {
			t.Errorf("%s prompt = %q, want the verify clause", workflow, rendered)
		}
	}
}

func TestTaskBriefKeyAlwaysPresent(t *testing.T) {
	envelope := normalizeWebhook([]byte(`{"repository":{"full_name":"o/r"}}`), "ping", "del-1", testVerdicts(), false, "", nil)
	value, ok := envelope["task_brief"]
	if !ok {
		t.Fatal("task_brief key missing from the default envelope")
	}
	if value != "" {
		t.Errorf("task_brief = %v, want empty", value)
	}
	rendered, err := renderTemplate("brief={{.webhook.task_brief}}", map[string]any{"webhook": envelope})
	if err != nil {
		t.Fatalf("template with task_brief: %v", err)
	}
	if rendered != "brief=" {
		t.Errorf("rendered = %q, want empty brief", rendered)
	}
}

func TestTaskSpecScopeStillInjected(t *testing.T) {
	store := NewDispatchStore()
	store.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", SpecPath: "specs/feat-x.md", Scope: "ship it", Verify: "go test ./..."})
	envelope := minimalEnvelope()
	injectTaskContext(envelope, WebhookExecutionKey{Repository: "acme/widget", Branch: "feat/x"}, store)
	if envelope["task_spec"] != "specs/feat-x.md" {
		t.Errorf("task_spec = %v, want the legacy spec path", envelope["task_spec"])
	}
	if envelope["task_scope"] != "ship it" {
		t.Errorf("task_scope = %v, want the legacy scope", envelope["task_scope"])
	}
	if !strings.Contains(envelope["task_brief"].(string), "Read specs/feat-x.md") {
		t.Errorf("task_brief = %v, want the derived brief", envelope["task_brief"])
	}
}

func TestRedispatchSeparatorFirstLineOnly(t *testing.T) {
	task := DispatchTask{ID: "task-1", Repo: "acme/widget", Branch: "feat/x", Brief: strings.Repeat("x", 200) + "\nsecond line"}
	separator := FormatRedispatchSeparator(task, nil)
	if !strings.Contains(separator, strings.Repeat("x", redispatchSeparatorBriefRunes)+"…") {
		t.Errorf("separator = %q, want the first line capped with an ellipsis", separator)
	}
	if strings.Contains(separator, strings.Repeat("x", redispatchSeparatorBriefRunes+1)) {
		t.Errorf("separator = %q, want at most %d brief runes", separator, redispatchSeparatorBriefRunes)
	}
	if strings.Contains(separator, "second line") {
		t.Errorf("separator = %q, must not carry the full brief", separator)
	}
}

func TestBriefNeverReachesShell(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, line := range strings.Split(string(source), "\n") {
			if strings.Contains(line, "exec.Command") && (strings.Contains(line, "Brief") || strings.Contains(line, "brief")) {
				t.Errorf("%s builds a shell command from the brief: %s", name, strings.TrimSpace(line))
			}
		}
	}
}

func TestDispatchBriefNotClippedAtCreate(t *testing.T) {
	store := NewDispatchStore()
	brief := strings.Repeat("a", dispatchBriefMaxRunes)
	task := store.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", Brief: brief})
	if task.Brief != brief {
		t.Fatalf("stored brief is %d runes, want %d byte-identical runes", utf8.RuneCountInString(task.Brief), utf8.RuneCountInString(brief))
	}
}
