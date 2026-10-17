package webhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func TestClassifyPorcelainStatus(t *testing.T) {
	tracked, untracked := classifyPorcelainStatus("")
	if tracked || len(untracked) != 0 {
		t.Fatalf("empty = %v %v, want false []", tracked, untracked)
	}
	tracked, untracked = classifyPorcelainStatus("?? scratch.log\n?? notes.tmp\n")
	if tracked {
		t.Fatal("untracked-only must not be tracked dirty")
	}
	if len(untracked) != 2 {
		t.Fatalf("untracked = %v, want 2", untracked)
	}
	tracked, untracked = classifyPorcelainStatus(" M README.md\n")
	if !tracked {
		t.Fatal("modified tracked file must be dirty")
	}
	if len(untracked) != 0 {
		t.Fatalf("untracked = %v, want empty", untracked)
	}
	tracked, untracked = classifyPorcelainStatus("?? scratch.log\n M README.md\n")
	if !tracked || len(untracked) != 1 {
		t.Fatalf("mixed = %v %v, want true + 1 untracked", tracked, untracked)
	}
}

func TestScratchOnlyDirtyProceeds(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	m := NewWorkspaceManager()
	key := WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main"}
	req := gitEndpointRequest(key, config.WorkspaceModeMutable)
	req.Path = root
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lease.Path, "scratch.log"), []byte("tmp\n"), 0644); err != nil {
		t.Fatalf("write scratch: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	lease2, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("scratch-only must proceed, got %v", err)
	}
	_ = lease2.Release(context.Background())
}

type threadCardCapture struct {
	mu       sync.Mutex
	notify   []string
	targets  []string
	editIDs  []string
	editText []string
}

func (c *threadCardCapture) notifyFn(_ context.Context, _, channelID, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notify = append(c.notify, text)
	c.targets = append(c.targets, channelID)
	return nil
}

func (c *threadCardCapture) editFn(_ context.Context, _, _, messageID, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.editIDs = append(c.editIDs, messageID)
	c.editText = append(c.editText, text)
	return nil
}

func fixBody(repo, branch string) string {
	return `{"repository":{"full_name":"` + repo + `"},"pull_request":{"base":{"repo":{"full_name":"` + repo + `"}},"head":{"repo":{"full_name":"` + repo + `"},"ref":"` + branch + `"}}}`
}

func TestActiveRunStandsDownQuiet(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github-fix", Path: "/github-fix", Secret: "s3cret", Platform: "discord", ChannelID: "chan-1", Prompt: "Analyze", Workflow: "fix",
			Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable}},
	})
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{failures: 1000})
	srv.workspaceRetryBackoff = []time.Duration{0, 0, 0}
	srv.workspaceRetrySleep = func(ctx context.Context, d time.Duration) bool { return true }
	cap := &threadCardCapture{}
	srv.SetNotifier(cap.notifyFn)
	srv.SetEditor(cap.editFn)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := fixBody("acme/widget", "feat/x")
	if resp := post(t, ts.URL+"/github-fix?secret=s3cret", "del-busy-1", "pull_request_review", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusSkipped)
	if !strings.Contains(receipt.ErrorSummary, "workspace leased") {
		t.Fatalf("summary = %q, want workspace leased", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.callCount())
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.notify) != 0 {
		t.Fatalf("quiet stand-down must post no card, got %d notifies: %q", len(cap.notify), cap.notify)
	}
	if len(cap.editIDs) != 0 {
		t.Fatalf("quiet stand-down must not edit parent, got %v", cap.editIDs)
	}
}

func TestConflictingDirtyRefusesLoud(t *testing.T) {
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{
		{Name: "github-fix", Path: "/github-fix", Secret: "s3cret", Platform: "discord", ChannelID: "chan-1", Prompt: "Analyze", Workflow: "fix",
			Workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable}},
	})
	srv.SetWorkspaceResolver(&fakeWorkspaceResolver{err: errors.New("workspace dirty: worktree at /tmp/wt has uncommitted changes")})
	srv.workspaceRetryBackoff = []time.Duration{0, 0, 0}
	srv.workspaceRetrySleep = func(ctx context.Context, d time.Duration) bool { return true }
	srv.TaskRegistry().Upsert("acme/widget", "feat/x", "root-1", "thread-1", TaskDispatched)
	cap := &threadCardCapture{}
	srv.SetNotifier(cap.notifyFn)
	srv.SetEditor(cap.editFn)
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := fixBody("acme/widget", "feat/x")
	if resp := post(t, ts.URL+"/github-fix?secret=s3cret", "del-dirty-1", "pull_request_review", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}
	receipt := waitForReceipt(t, st, store.WebhookStatusFailed)
	if !strings.Contains(receipt.ErrorSummary, "workspace dirty") {
		t.Fatalf("summary = %q, want workspace dirty", receipt.ErrorSummary)
	}
	if exec.callCount() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.callCount())
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.notify) == 0 {
		t.Fatal("loud refusal must post a thread card, got 0 notifies")
	}
	foundThread := false
	for i, target := range cap.targets {
		if target != "thread-1" {
			continue
		}
		text := cap.notify[i]
		hasAll := true
		for _, want := range []string{"acme/widget", "feat/x", "workspace dirty", "Action:"} {
			if !strings.Contains(text, want) {
				hasAll = false
				break
			}
		}
		if strings.Contains(text, "Delivery:") || strings.Contains(text, "Model:") || strings.Contains(text, "━━━") {
			continue
		}
		if strings.Contains(text, "del-dirty-1") {
			continue
		}
		if !hasAll || !strings.Contains(text, "❌") {
			continue
		}
		foundThread = true
		break
	}
	if !foundThread {
		t.Fatalf("no notify targeted thread-1, targets=%v", cap.targets)
	}
}
