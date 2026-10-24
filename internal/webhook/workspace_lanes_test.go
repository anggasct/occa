package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func initRepoWithBareRemote(t *testing.T, branch string) (root, remote string) {
	t.Helper()
	root = t.TempDir()
	remote = t.TempDir()
	runGit(t, remote, "init", "--bare")
	initTestGitRepoWithRemote(t, root, "https://github.com/testowner/myrepo.git")
	runGit(t, root, "remote", "set-url", "--push", "origin", remote)
	runGit(t, root, "push", "-u", "origin", "main")
	runGit(t, root, "branch", branch)
	runGit(t, root, "push", "origin", branch)
	return root, remote
}

func mutableLease(t *testing.T, m *WorkspaceManager, root, branch, delivery string) *WorkspaceLease {
	t.Helper()
	req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: branch}, config.WorkspaceModeMutable)
	req.Path = root
	req.DeliveryID = delivery
	req.Lane = "fix"
	lease, err := m.ResolveWorkspace(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return lease
}

func TestLaneWorkspacePolicies(t *testing.T) {
	review, ok := lanePolicyForWorkflow("review")
	if !ok || review.Type != config.WorkspaceTypeGit || review.Mode != config.WorkspaceModeIsolated || review.IsolatedTTL != 2*time.Hour || !review.RevisionKeyed {
		t.Fatalf("review policy = %+v", review)
	}
	fix, ok := lanePolicyForWorkflow("fix")
	if !ok || fix.Type != config.WorkspaceTypeGit || fix.Mode != config.WorkspaceModeMutable {
		t.Fatalf("fix policy = %+v", fix)
	}
	for _, lane := range []string{"merge", "merged"} {
		policy, ok := lanePolicyForWorkflow(lane)
		if !ok || policy.Type != config.WorkspaceTypeNone {
			t.Fatalf("%s policy = %+v", lane, policy)
		}
	}
	if _, ok := lanePolicyForWorkflow("custom"); ok {
		t.Fatal("custom lane must not have a lane policy")
	}
}

type recordingWorkspaceResolver struct {
	calls int
	last  WorkspaceRequest
	lease *WorkspaceLease
}

func (r *recordingWorkspaceResolver) ResolveWorkspace(_ context.Context, req WorkspaceRequest) (*WorkspaceLease, error) {
	r.calls++
	r.last = req
	if r.lease != nil {
		return r.lease, nil
	}
	return &WorkspaceLease{Path: "/projects/occa/lease", Mode: req.Mode}, nil
}

func TestResolveWorkspaceAppliesLanePolicy(t *testing.T) {
	tests := []struct {
		name      string
		workflow  string
		workspace config.EndpointWorkspace
		modeSet   bool
		wantErr   bool
		wantCalls int
		wantMode  string
		wantTTL   time.Duration
		wantLane  string
	}{
		{
			name:      "review isolated at head",
			workflow:  "review",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated},
			wantCalls: 1,
			wantMode:  config.WorkspaceModeIsolated,
			wantTTL:   2 * time.Hour,
			wantLane:  "review",
		},
		{
			name:      "review rejects a non-git workspace",
			workflow:  "review",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeNone},
			wantErr:   true,
		},
		{
			name:      "fix mutable branch lease",
			workflow:  "fix",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable},
			wantCalls: 1,
			wantMode:  config.WorkspaceModeMutable,
			wantLane:  "fix",
		},
		{
			name:      "fix override to isolated",
			workflow:  "fix",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeIsolated},
			modeSet:   true,
			wantCalls: 1,
			wantMode:  config.WorkspaceModeIsolated,
			wantLane:  "fix",
		},
		{
			name:      "review override to mutable",
			workflow:  "review",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable},
			modeSet:   true,
			wantCalls: 1,
			wantMode:  config.WorkspaceModeMutable,
			wantLane:  "review",
		},
		{
			name:      "merge has no workspace",
			workflow:  "merge",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeNone},
			wantCalls: 0,
		},
		{
			name:      "merge ignores a configured git workspace",
			workflow:  "merge",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: "/projects/occa", Mode: config.WorkspaceModeMutable},
			wantCalls: 0,
		},
		{
			name:      "merged has no workspace",
			workflow:  "merged",
			workspace: config.EndpointWorkspace{Type: config.WorkspaceTypeNone},
			wantCalls: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &recordingWorkspaceResolver{}
			s := &Server{workspaceResolver: resolver}
			item := dispatchItem{ep: config.EndpointConfig{
				Name:             "github-widget-" + tt.workflow,
				Workflow:         tt.workflow,
				Repository:       "github.com/testowner/myrepo",
				Workspace:        tt.workspace,
				WorkspaceModeSet: tt.modeSet,
			}, deliveryID: "del-1"}
			lease, err := s.resolveWorkspace(item)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveWorkspace = %+v, want error", lease)
				}
				if resolver.calls != 0 {
					t.Fatalf("resolver calls = %d, want 0 on lane type rejection", resolver.calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveWorkspace: %v", err)
			}
			if resolver.calls != tt.wantCalls {
				t.Fatalf("resolver calls = %d, want %d", resolver.calls, tt.wantCalls)
			}
			if tt.wantCalls == 0 {
				if lease != nil {
					t.Fatalf("non-git lane returned lease %+v", lease)
				}
				return
			}
			if lease == nil {
				t.Fatal("expected a lease")
			}
			if resolver.last.Mode != tt.wantMode || resolver.last.TTL != tt.wantTTL || resolver.last.Lane != tt.wantLane {
				t.Fatalf("request = %+v, want mode=%s ttl=%v lane=%s", resolver.last, tt.wantMode, tt.wantTTL, tt.wantLane)
			}
		})
	}
}

func TestWorkspaceFixLeaseChecksOutBranch(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	runGit(t, root, "branch", "fix/test-lane")

	lease := mutableLease(t, NewWorkspaceManager(), root, "fix/test-lane", "del-fix")
	if want := filepath.Join(root, ".worktree", "fix-test-lane"); filepath.Clean(lease.Path) != filepath.Clean(want) {
		t.Fatalf("fix lease path = %q, want %q", lease.Path, want)
	}
	if got := runGit(t, lease.Path, "symbolic-ref", "--short", "HEAD"); got != "fix/test-lane" {
		t.Fatalf("fix lease HEAD = %q, want branch checkout", got)
	}
	if lease.StartRevision == "" {
		t.Fatal("fix lease must record its start revision")
	}
}

func TestReviewCheckoutRecoversFromInvalidMetadata(t *testing.T) {
	root := t.TempDir()
	initTestGitRepo(t, root)
	head := runGit(t, root, "rev-parse", "HEAD")

	cases := []struct {
		name    string
		prepare func(t *testing.T, path string)
	}{
		{"missing", func(t *testing.T, target string) {
			if err := os.Remove(isolatedMetadataPath(target)); err != nil {
				t.Fatalf("remove metadata: %v", err)
			}
		}},
		{"unreadable", func(t *testing.T, target string) {
			if err := os.WriteFile(isolatedMetadataPath(target), []byte("not-json"), 0644); err != nil {
				t.Fatalf("corrupt metadata: %v", err)
			}
		}},
		{"drifted checkout", func(t *testing.T, target string) {
			runGit(t, target, "commit", "--allow-empty", "-m", "drift")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewWorkspaceManagerWithTTL(2 * time.Hour)
			req := gitEndpointRequest(WebhookExecutionKey{Repository: "testowner/myrepo", Branch: "main", HeadRevision: head}, config.WorkspaceModeIsolated)
			req.Path = root
			req.DeliveryID = "del-first"
			lease, err := m.ResolveWorkspace(context.Background(), req)
			if err != nil {
				t.Fatalf("first resolve: %v", err)
			}
			target := lease.Path
			if err := lease.Release(context.Background()); err != nil {
				t.Fatalf("release: %v", err)
			}

			tc.prepare(t, target)

			req.DeliveryID = "del-second"
			recovered, err := m.ResolveWorkspace(context.Background(), req)
			if err != nil {
				t.Fatalf("re-resolve after %s metadata: %v", tc.name, err)
			}
			defer func() { _ = recovered.Release(context.Background()) }()
			if filepath.Clean(recovered.Path) != filepath.Clean(target) {
				t.Fatalf("recovered path = %q, want %q", recovered.Path, target)
			}
			if got := runGit(t, target, "rev-parse", "HEAD"); got != head {
				t.Fatalf("recovered checkout at %s, want %s", got, head)
			}
			if _, err := readIsolatedMetadata(target); err != nil {
				t.Fatalf("metadata not restored: %v", err)
			}
		})
	}
}

func TestFinalizeCommitsDirtyTreeAndPushes(t *testing.T) {
	root, remote := initRepoWithBareRemote(t, "fix/test-lane")
	lease := mutableLease(t, NewWorkspaceManager(), root, "fix/test-lane", "del-42")
	start := lease.StartRevision

	if err := os.WriteFile(filepath.Join(lease.Path, "CHANGES.md"), []byte("work\n"), 0644); err != nil {
		t.Fatalf("write change: %v", err)
	}
	result, err := lease.Finalize(context.Background(), "fix", "del-42")
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if !result.Committed || !result.Pushed || result.Files != 1 {
		t.Fatalf("finalize result = %+v", result)
	}
	head := runGit(t, lease.Path, "rev-parse", "HEAD")
	if head == start {
		t.Fatal("harness commit did not advance HEAD")
	}
	if subject := runGit(t, lease.Path, "log", "-1", "--pretty=%s"); subject != "fix: 1 file(s) (delivery del-42)" {
		t.Fatalf("harness commit subject = %q", subject)
	}
	if count := runGit(t, lease.Path, "rev-list", "--count", start+"..HEAD"); count != "1" {
		t.Fatalf("harness commit count = %s, want 1", count)
	}
	if pushed := runGit(t, remote, "rev-parse", "refs/heads/fix/test-lane"); pushed != head {
		t.Fatalf("remote branch = %s, want %s", pushed, head)
	}
}

func TestFinalizePushesAgentCommitWithoutExtraCommit(t *testing.T) {
	root, remote := initRepoWithBareRemote(t, "fix/test-lane")
	lease := mutableLease(t, NewWorkspaceManager(), root, "fix/test-lane", "del-43")
	start := lease.StartRevision

	if err := os.WriteFile(filepath.Join(lease.Path, "CHANGES.md"), []byte("work\n"), 0644); err != nil {
		t.Fatalf("write change: %v", err)
	}
	runGit(t, lease.Path, "add", "-A")
	runGit(t, lease.Path, "commit", "-m", "agent commit")

	result, err := lease.Finalize(context.Background(), "fix", "del-43")
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if result.Committed {
		t.Fatal("finalize must not commit a clean tree")
	}
	if !result.Pushed {
		t.Fatal("finalize must push the agent commit")
	}
	head := runGit(t, lease.Path, "rev-parse", "HEAD")
	if count := runGit(t, lease.Path, "rev-list", "--count", start+"..HEAD"); count != "1" {
		t.Fatalf("commit count = %s, want only the agent commit", count)
	}
	if pushed := runGit(t, remote, "rev-parse", "refs/heads/fix/test-lane"); pushed != head {
		t.Fatalf("remote branch = %s, want %s", pushed, head)
	}
}

func TestFinalizeNoWorkFailsWithNoWorkClass(t *testing.T) {
	root, _ := initRepoWithBareRemote(t, "fix/test-lane")
	lease := mutableLease(t, NewWorkspaceManager(), root, "fix/test-lane", "del-44")

	_, err := lease.Finalize(context.Background(), "fix", "del-44")
	if !errors.Is(err, errNoWorkInLease) {
		t.Fatalf("Finalize error = %v, want no-work", err)
	}
	class := classifyFailure(err, &WebhookWorkContext{}, 2*time.Minute, false)
	if class != failureNoWork {
		t.Fatalf("class = %v, want no-work", class)
	}
	if got := class.label(); got != "no work in lease" {
		t.Fatalf("label = %q", got)
	}
	max, waits := retryPolicyFor(failureNoWork)
	if max != 2 || len(waits) != 2 || waits[0] != 60*time.Second || waits[1] != 5*time.Minute {
		t.Fatalf("no-work policy = %d %v", max, waits)
	}
}

func TestFinalizeSkipsReaderLanesAndFailedTurns(t *testing.T) {
	root, _ := initRepoWithBareRemote(t, "fix/test-lane")
	lease := mutableLease(t, NewWorkspaceManager(), root, "fix/test-lane", "del-45")

	if _, err := lease.Finalize(context.Background(), "review", "del-45"); err != nil {
		t.Fatalf("review finalize must be a no-op: %v", err)
	}
	if _, err := lease.Finalize(context.Background(), "merge", "del-45"); err != nil {
		t.Fatalf("merge finalize must be a no-op: %v", err)
	}
	if got := runGit(t, lease.Path, "rev-parse", "HEAD"); got != lease.StartRevision {
		t.Fatalf("reader lanes must not move HEAD, got %s", got)
	}
}

func TestRenderedEnvelopePublishesLeaseOnly(t *testing.T) {
	body := []byte(fixLaneReviewBody("testowner/myrepo", "feat/review-lane"))
	key := ExtractExecutionKey(body)
	envelope := normalizeWebhook(body, "pull_request_review", "del-render", testVerdicts(), false, "")

	reviewLease := filepath.Join("/projects/occa/.occa-workspaces", "review-8a7682e244-699397daa3")
	fixLease := filepath.Join("/projects/occa/.worktree", "fix-test-lane")

	tests := []struct {
		name            string
		workflow        string
		prompt          string
		lease           string
		wantLeaseInText bool
	}{
		{"review custom prompt", "review", "Workspace: {{.worktree}} Task: {{.webhook.task_spec}}", reviewLease, true},
		{"fix custom prompt", "fix", "Workspace: {{.worktree}}", fixLease, true},
		{"review builtin prompt", "review", config.BuiltinPrompt("review"), reviewLease, false},
		{"fix builtin prompt", "fix", config.BuiltinPrompt("fix"), fixLease, false},
		{"merge builtin prompt", "merge", config.BuiltinPrompt("merge"), "", false},
		{"merged builtin prompt", "merged", config.BuiltinPrompt("merged"), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workCtx := &WebhookWorkContext{Key: key, Worktree: tt.lease, Envelope: envelope}
			ep := config.EndpointConfig{Name: "render-" + tt.workflow, Workflow: tt.workflow, Prompt: tt.prompt}
			data, rendered, err := renderDeliveryPrompt(ep, body, workCtx)
			if err != nil {
				t.Fatalf("renderDeliveryPrompt: %v", err)
			}
			if data["worktree"] != tt.lease {
				t.Fatalf("template data worktree = %v, want %s", data["worktree"], tt.lease)
			}
			if tt.wantLeaseInText && !strings.Contains(rendered, tt.lease) {
				t.Fatalf("rendered prompt missing lease: %q", rendered)
			}
			if leftover := strings.ReplaceAll(rendered, tt.lease, ""); strings.Contains(leftover, ".worktree/") {
				t.Fatalf("rendered prompt carried a foreign worktree path: %q", rendered)
			}
		})
	}

	env := WebhookEnvelope{}
	injectTaskContext(env, WebhookExecutionKey{}, nil)
	if _, ok := env["worktree"]; ok {
		t.Fatal("task context must not publish a worktree path")
	}
}

func fixLaneReviewBody(repo, branch string) string {
	return fmt.Sprintf(`{"action":"submitted","pull_request":{"number":7,"state":"open","user":{"login":"owner"},"head":{"ref":%q,"sha":"abc1234def5678","repo":{"full_name":%q}},"base":{"ref":"main","repo":{"full_name":%q}}},"review":{"state":"changes_requested","commit_id":"abc1234def5678","user":{"login":"reviewer"}},"repository":{"full_name":%q}}`,
		branch, repo, repo, repo)
}

func TestNoWorkLeaseRetriesTwiceThenBlocks(t *testing.T) {
	root, _ := initRepoWithBareRemote(t, "fix/test-lane")
	ep := prTestEndpoint("fix", "/fix", config.AutomationFull)
	ep.Name = "github-widget-pr-fix"
	ep.Repository = testBinding
	ep.Workspace = config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: root, Mode: config.WorkspaceModeMutable}

	var calls int
	exec := func(ctx context.Context, platform, channelID, prompt string, workCtx *WebhookWorkContext) error {
		calls++
		if workCtx.ThreadID == "" {
			workCtx.ThreadID = "thread-pr"
		}
		return nil
	}
	cfg := config.WebhookConfig{
		Bind:      "127.0.0.1:0",
		Policy:    testPolicy(),
		Runtime:   testRuntime(),
		Endpoints: []config.EndpointConfig{ep},
	}
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "webhook.db"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(cfg, exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetWorkspaceResolver(NewWorkspaceManager())
	setOpenPRState(srv)
	srv.retryBackoffs = map[failureClass][]time.Duration{
		failureNoWork: {10 * time.Millisecond, 10 * time.Millisecond},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	if resp := post(t, ts.URL+"/fix?secret=s3cret", "del-nowork", "pull_request_review", fixLaneReviewBody("testowner/myrepo", "fix/test-lane")); resp.StatusCode != http.StatusOK {
		t.Fatalf("post: status %d", resp.StatusCode)
	}
	waitForDeliveryStatus(t, st, ep.Name, "del-nowork", store.WebhookStatusFailed)
	if calls != 3 {
		t.Fatalf("executor calls = %d, want 1 + 2 no-work retries", calls)
	}
	run, err := st.PRQueueRepo().Get(context.Background(), "testowner/myrepo#7")
	if err != nil || run == nil {
		t.Fatalf("pr row: %+v %v", run, err)
	}
	if run.Status != store.PRStatusBlocked || run.RetryCount != 2 {
		t.Fatalf("pr row = %+v, want blocked after 2 retries", run)
	}
	if !strings.Contains(run.BlockedReason, "no work in lease") {
		t.Fatalf("blocked reason = %q, want no work in lease", run.BlockedReason)
	}
}
