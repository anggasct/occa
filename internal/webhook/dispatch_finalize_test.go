package webhook

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
)

type fakeDispatchGH struct {
	mu        sync.Mutex
	calls     [][]string
	listOut   string
	createOut string
	createErr error
}

func (f *fakeDispatchGH) Run(_ context.Context, _ string, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]string(nil), args...))
	if len(args) >= 2 && args[0] == "pr" && args[1] == "list" {
		return f.listOut, nil
	}
	if len(args) >= 2 && args[0] == "pr" && args[1] == "create" {
		if f.createErr != nil {
			return f.createOut, f.createErr
		}
		if strings.TrimSpace(f.createOut) == "" {
			return "https://github.com/testowner/myrepo/pull/42\n", nil
		}
		return f.createOut, nil
	}
	return "", nil
}

func (f *fakeDispatchGH) callCount(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, call := range f.calls {
		if len(call) >= 2 && call[0] == "pr" && call[1] == kind {
			n++
		}
	}
	return n
}

func (f *fakeDispatchGH) lastCall(kind string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.calls) - 1; i >= 0; i-- {
		if len(f.calls[i]) >= 2 && f.calls[i][0] == "pr" && f.calls[i][1] == kind {
			return append([]string(nil), f.calls[i]...)
		}
	}
	return nil
}

type dispatchFixture struct {
	srv    *Server
	gh     *fakeDispatchGH
	root   string
	remote string
	wt     string
	repo   string
	branch string
	task   DispatchTask
	notes  []string
	mu     sync.Mutex
}

func newDispatchFixture(t *testing.T, dirty int) *dispatchFixture {
	t.Helper()
	f := &dispatchFixture{
		gh:     &fakeDispatchGH{},
		repo:   "testowner/myrepo",
		branch: "dispatch/test-branch",
	}
	f.root, f.remote = initRepoWithBareRemote(t, f.branch)
	f.wt = filepath.Join(f.root, ".worktree", "dispatch-test")
	if err := os.MkdirAll(filepath.Dir(f.wt), 0755); err != nil {
		t.Fatalf("mkdir worktree parent: %v", err)
	}
	runGit(t, f.root, "worktree", "add", f.wt, f.branch)
	start := runGit(t, f.wt, "rev-parse", "HEAD")

	ep := config.EndpointConfig{
		Name:       "dispatch-test",
		Repository: f.repo,
		Platform:   "discord",
		ChannelID:  "channel-1",
		Workflow:   "review",
		Workspace:  config.EndpointWorkspace{Type: config.WorkspaceTypeGit, Path: f.root, Mode: config.WorkspaceModeIsolated},
	}
	srv, _, st := newTestServerFull(t, []config.EndpointConfig{ep})
	srv.SetWorkspaceResolver(NewWorkspaceManager())
	srv.SetGHRunner(f.gh)
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetNotifier(func(_ context.Context, _, _ string, text string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.notes = append(f.notes, text)
		return nil
	})
	srv.tasks.Upsert(f.repo, f.branch, "root-1", "thread-1", TaskDispatched)
	f.srv = srv
	f.task = srv.dispatchTasks.Create(DispatchRequest{
		Repo:     f.repo,
		Branch:   f.branch,
		Brief:    "dispatch title\n\nbody of the dispatch brief",
		SpecPath: "specs/feature.md",
		Verify:   "go test ./...",
		BaseSHA:  start,
	})
	for i := 0; i < dirty; i++ {
		name := filepath.Join(f.wt, "change-"+string(rune('a'+i))+".txt")
		if err := os.WriteFile(name, []byte("work\n"), 0644); err != nil {
			t.Fatalf("write dirty file: %v", err)
		}
	}
	return f
}

func (f *dispatchFixture) finalize(t *testing.T, denied bool) DispatchResult {
	t.Helper()
	result, err := f.srv.finalizeDispatch(context.Background(), DispatchReport{
		Repo:    f.repo,
		Branch:  f.branch,
		TaskID:  f.task.ID,
		Outcome: "complete",
		Denied:  denied,
	}, true)
	if err != nil {
		t.Fatalf("finalizeDispatch: %v", err)
	}
	return result
}

func (f *dispatchFixture) noteText() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.notes, "\n")
}

func TestDispatchPR_AC01_CleanTreeIsNoWork(t *testing.T) {
	f := newDispatchFixture(t, 0)
	result := f.finalize(t, false)
	if result.Status != DispatchNoWork {
		t.Fatalf("status = %q, want %q", result.Status, DispatchNoWork)
	}
	if f.gh.callCount("create") != 0 || f.gh.callCount("list") != 0 {
		t.Fatalf("clean run must not touch gh: %+v", f.gh.calls)
	}
	if rec, ok := f.srv.tasks.Lookup(f.repo, f.branch); !ok || rec.Status != TaskFailed {
		t.Fatalf("task status = %+v, want failed", rec.Status)
	}
}

func TestDispatchPR_AC02_DirtyTreeCommitsPushesAndOpensPR(t *testing.T) {
	f := newDispatchFixture(t, 3)
	result := f.finalize(t, false)
	if result.Status != DispatchCompleted {
		t.Fatalf("status = %q (%s), want completed", result.Status, result.Detail)
	}
	if !result.Committed || !result.Pushed || result.Files != 3 {
		t.Fatalf("result = %+v", result)
	}
	if count := runGit(t, f.wt, "rev-list", "--count", f.task.BaseSHA+"..HEAD"); count != "1" {
		t.Fatalf("commit count = %s, want 1", count)
	}
	head := runGit(t, f.wt, "rev-parse", "HEAD")
	if pushed := runGit(t, f.remote, "rev-parse", "refs/heads/"+f.branch); pushed != head {
		t.Fatalf("remote branch = %s, want %s", pushed, head)
	}
	if f.gh.callCount("create") != 1 {
		t.Fatalf("pr create calls = %d, want 1", f.gh.callCount("create"))
	}
	call := f.gh.lastCall("create")
	joined := strings.Join(call, " ")
	for _, want := range []string{"--base main", "--head " + f.branch, "--title dispatch title"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("create args = %v, want %q", call, want)
		}
	}
}

func TestDispatchPR_AC03_ExistingOpenPRIsReused(t *testing.T) {
	f := newDispatchFixture(t, 1)
	f.gh.listOut = `[{"number":7,"url":"https://github.com/testowner/myrepo/pull/7"}]`
	result := f.finalize(t, false)
	if result.Status != DispatchCompleted || result.PRNumber != "7" {
		t.Fatalf("result = %+v", result)
	}
	if f.gh.callCount("create") != 0 {
		t.Fatalf("pr create calls = %d, want 0", f.gh.callCount("create"))
	}
}

func TestDispatchPR_AC04_PushFailureIsNamed(t *testing.T) {
	f := newDispatchFixture(t, 1)
	runGit(t, f.wt, "remote", "set-url", "--push", "origin", filepath.Join(t.TempDir(), "missing.git"))
	result := f.finalize(t, false)
	if result.Status != DispatchPushFailed {
		t.Fatalf("status = %q (%s), want %q", result.Status, result.Detail, DispatchPushFailed)
	}
	if f.gh.callCount("create") != 0 {
		t.Fatalf("pr create calls = %d, want 0", f.gh.callCount("create"))
	}
	if max, _ := retryPolicyFor(failurePush); max != 2 {
		t.Fatalf("push failure retry budget = %d, want 2", max)
	}
}

func TestDispatchPR_AC05_PRFailureNonZeroFailureCarriesStderr(t *testing.T) {
	f := newDispatchFixture(t, 1)
	f.gh.createErr = errors.New("exit status 1")
	f.gh.createOut = "boom: repository is not writable\n"
	result := f.finalize(t, false)
	if result.Status != DispatchPRCreateFailed {
		t.Fatalf("status = %q (%s), want %q", result.Status, result.Detail, DispatchPRCreateFailed)
	}
	if !strings.Contains(f.noteText(), "boom: repository is not writable") {
		t.Fatalf("thread summary missing gh stderr: %q", f.noteText())
	}
	if max, _ := retryPolicyFor(failurePRCreate); max != 2 {
		t.Fatalf("pr failure retry budget = %d, want 2", max)
	}
}

func (f *dispatchFixture) dispatchRunSnapshot() dispatchRunState {
	f.srv.dispatchRunsMu.Lock()
	defer f.srv.dispatchRunsMu.Unlock()
	return f.srv.dispatchRuns[TaskThreadKey(f.repo, f.branch)]
}

func (f *dispatchFixture) waitDispatchRetries(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.dispatchRunSnapshot().Retries >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("retries = %d, want at least %d", f.dispatchRunSnapshot().Retries, want)
}

func (f *dispatchFixture) waitDispatchTerminal(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.dispatchRunSnapshot().Terminal {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("dispatch run did not reach a terminal state")
}

func TestDispatchPR_AC04_PushFailureSchedulesRetry(t *testing.T) {
	f := newDispatchFixture(t, 1)
	runGit(t, f.wt, "remote", "set-url", "--push", "origin", filepath.Join(t.TempDir(), "missing.git"))
	f.srv.retryBackoffs = map[failureClass][]time.Duration{failurePush: {5 * time.Millisecond}}
	f.srv.DispatchRunStarted(f.repo, f.branch, f.task.ID, "session-1", "")
	f.srv.DispatchRunFinished(context.Background(), f.repo, f.branch, f.task.ID, "session-1", "complete", false)
	f.waitDispatchRetries(t, 1)
	f.waitDispatchTerminal(t)
	note := f.noteText()
	if !strings.Contains(note, "retry budget exhausted") {
		t.Fatalf("exhaustion finding missing: %q", note)
	}
	if !strings.Contains(note, "push failed") {
		t.Fatalf("exhaustion finding missing class: %q", note)
	}
	if !strings.Contains(note, "does not appear to be a git repository") {
		t.Fatalf("exhaustion finding missing push error detail: %q", note)
	}
}

func TestDispatchPR_AC05_PRFailureSchedulesRetry(t *testing.T) {
	f := newDispatchFixture(t, 1)
	f.gh.createErr = errors.New("exit status 1")
	f.gh.createOut = "boom: repository is not writable\n"
	f.srv.retryBackoffs = map[failureClass][]time.Duration{failurePRCreate: {5 * time.Millisecond}}
	f.srv.DispatchRunStarted(f.repo, f.branch, f.task.ID, "session-1", "")
	f.srv.DispatchRunFinished(context.Background(), f.repo, f.branch, f.task.ID, "session-1", "complete", false)
	f.waitDispatchRetries(t, 1)
	f.waitDispatchTerminal(t)
	if got := f.gh.callCount("create"); got < 3 {
		t.Fatalf("pr create calls = %d, want initial attempt plus two retries", got)
	}
	note := f.noteText()
	if !strings.Contains(note, "retry budget exhausted") {
		t.Fatalf("exhaustion finding missing: %q", note)
	}
	if !strings.Contains(note, "repository is not writable") {
		t.Fatalf("exhaustion finding missing gh stderr: %q", note)
	}
}

func TestDispatchPR_AC06_StatusMapping(t *testing.T) {
	cases := []struct {
		name       string
		dirty      int
		denied     bool
		pushOK     bool
		prErr      bool
		selfCommit bool
		want       DispatchStatus
	}{
		{name: "clean", want: DispatchNoWork},
		{name: "dirty success", dirty: 1, pushOK: true, want: DispatchCompleted},
		{name: "self committed and pushed", dirty: 1, pushOK: true, selfCommit: true, want: DispatchCompleted},
		{name: "denied clean", denied: true, want: DispatchPermissionDeny},
		{name: "denied with artifact", dirty: 1, denied: true, pushOK: true, want: DispatchCompleted},
		{name: "push failed", dirty: 1, want: DispatchPushFailed},
		{name: "pr failed", dirty: 1, pushOK: true, prErr: true, want: DispatchPRCreateFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDispatchFixture(t, tc.dirty)
			if tc.selfCommit {
				runGit(t, f.wt, "add", "-A")
				runGit(t, f.wt, "commit", "-m", "agent commit")
				runGit(t, f.wt, "push", "origin", "HEAD:refs/heads/"+f.branch)
			}
			if !tc.pushOK && tc.dirty > 0 {
				runGit(t, f.wt, "remote", "set-url", "--push", "origin", filepath.Join(t.TempDir(), "missing.git"))
			}
			if tc.prErr {
				f.gh.createErr = errors.New("exit status 1")
				f.gh.createOut = "create failed\n"
			}
			if got := f.finalize(t, tc.denied).Status; got != tc.want {
				t.Fatalf("status = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDispatchPR_AC06_SelfCommittedArtifactCompletes(t *testing.T) {
	f := newDispatchFixture(t, 1)
	runGit(t, f.wt, "add", "-A")
	runGit(t, f.wt, "commit", "-m", "agent commit")
	runGit(t, f.wt, "push", "origin", "HEAD:refs/heads/"+f.branch)
	result := f.finalize(t, false)
	if result.Status != DispatchCompleted {
		t.Fatalf("status = %q (%s), want completed", result.Status, result.Detail)
	}
	if result.Committed || result.Pushed {
		t.Fatalf("self-committed run must complete on the artifact, result = %+v", result)
	}
	if result.PRNumber == "" {
		t.Fatalf("completed run has no PR: %+v", result)
	}
}

func TestDispatchPR_AC07_FixLaneDoesNotOpenPR(t *testing.T) {
	f := newDispatchFixture(t, 1)
	lease, err := NewWorkspaceManager().DispatchLease(context.Background(), f.root, f.branch, f.wt)
	if err != nil {
		t.Fatalf("DispatchLease: %v", err)
	}
	if _, err := lease.Finalize(context.Background(), "fix", f.task.ID); err != nil {
		t.Fatalf("fix Finalize: %v", err)
	}
	if f.gh.callCount("create") != 0 {
		t.Fatalf("fix lane must not create a PR: %+v", f.gh.calls)
	}
}

func TestDispatchPR_AC08_NoForceArguments(t *testing.T) {
	f := newDispatchFixture(t, 1)
	f.finalize(t, false)
	for _, call := range f.gh.calls {
		for _, arg := range call {
			if arg == "--force" || arg == "-f" {
				t.Fatalf("force argument in %v", call)
			}
		}
	}
}

func TestDispatchPR_AC09_SecondDispatchReusesPR(t *testing.T) {
	f := newDispatchFixture(t, 1)
	first := f.finalize(t, false)
	if first.Status != DispatchCompleted {
		t.Fatalf("first status = %q (%s)", first.Status, first.Detail)
	}
	head := runGit(t, f.wt, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(f.wt, "second.txt"), []byte("more\n"), 0644); err != nil {
		t.Fatalf("write second change: %v", err)
	}
	second := f.srv.dispatchTasks.Create(DispatchRequest{
		Repo:     f.repo,
		Branch:   f.branch,
		Brief:    "second dispatch",
		SpecPath: "specs/feature.md",
		BaseSHA:  head,
	})
	f.gh.listOut = `[{"number":42,"url":"https://github.com/testowner/myrepo/pull/42"}]`
	createBefore := f.gh.callCount("create")
	result, err := f.srv.finalizeDispatch(context.Background(), DispatchReport{Repo: f.repo, Branch: f.branch, TaskID: second.ID}, true)
	if err != nil {
		t.Fatalf("second finalizeDispatch: %v", err)
	}
	if result.Status != DispatchCompleted || result.PRNumber != "42" {
		t.Fatalf("second result = %+v", result)
	}
	if f.gh.callCount("create") != createBefore {
		t.Fatalf("second dispatch created a duplicate PR: %+v", f.gh.calls)
	}
}

func TestDispatchPR_AC10_RunAndFinalizeShareWorktree(t *testing.T) {
	f := newDispatchFixture(t, 1)
	result := f.finalize(t, false)
	if filepath.Clean(result.Worktree) != filepath.Clean(f.wt) {
		t.Fatalf("lease path = %q, want worktree %q", result.Worktree, f.wt)
	}
}

func TestDispatchPR_AC11_PermissionDeniedWithoutCommit(t *testing.T) {
	f := newDispatchFixture(t, 0)
	result := f.finalize(t, true)
	if result.Status != DispatchPermissionDeny {
		t.Fatalf("status = %q, want %q", result.Status, DispatchPermissionDeny)
	}
	if result.Retryable() != true {
		t.Fatal("permission denied must be retryable")
	}
}

func TestDispatchPR_AC12_DenialWithArtifactCompletes(t *testing.T) {
	f := newDispatchFixture(t, 1)
	result := f.finalize(t, true)
	if result.Status != DispatchCompleted {
		t.Fatalf("status = %q (%s), want completed", result.Status, result.Detail)
	}
	if !strings.Contains(f.noteText(), "permission was denied") {
		t.Fatalf("denial note missing from thread: %q", f.noteText())
	}
}

func TestDispatchPR_AC13_DenialAloneNeverCompletes(t *testing.T) {
	f := newDispatchFixture(t, 0)
	f.srv.retryBackoffs = map[failureClass][]time.Duration{failurePermission: {time.Millisecond}}
	f.srv.DispatchRunStarted(f.repo, f.branch, f.task.ID, "session-1", "")
	f.srv.DispatchRunFinished(context.Background(), f.repo, f.branch, f.task.ID, "session-1", "complete", true)
	f.waitDispatchTerminal(t)
	if rec, ok := f.srv.tasks.Lookup(f.repo, f.branch); !ok || rec.Status != TaskFailed {
		t.Fatalf("task status = %+v, want failed", rec.Status)
	}
	note := f.noteText()
	if !strings.Contains(note, "permission denied") {
		t.Fatalf("denial finding missing class: %q", note)
	}
	if strings.Contains(note, "PR #") {
		t.Fatalf("denial alone must not report a completed PR: %q", note)
	}
}

func TestDispatchPR_AC14_FindingPostedAtFinalize(t *testing.T) {
	f := newDispatchFixture(t, 0)
	f.srv.retryBackoffs = map[failureClass][]time.Duration{failureNoWork: {time.Millisecond}}
	f.srv.DispatchRunStarted(f.repo, f.branch, f.task.ID, "session-1", "")
	f.srv.DispatchRunFinished(context.Background(), f.repo, f.branch, f.task.ID, "session-1", "complete", false)
	f.waitDispatchTerminal(t)
	note := f.noteText()
	if !strings.Contains(note, "no work in lease") {
		t.Fatalf("terminal finding missing class: %q", note)
	}
	if !strings.Contains(note, "ended without a PR") {
		t.Fatalf("terminal finding missing missing-PR copy: %q", note)
	}
	if rec, ok := f.srv.tasks.Lookup(f.repo, f.branch); !ok || rec.Status != TaskFailed {
		t.Fatalf("task status = %+v, want failed", rec.Status)
	}
}

type dispatchFallbackFixture struct {
	srv         *Server
	mu          sync.Mutex
	posts       []string
	branchCalls int
}

func newDispatchFallbackFixture(t *testing.T, after time.Duration) *dispatchFallbackFixture {
	t.Helper()
	f := &dispatchFallbackFixture{srv: newDispatchServer(t, dispatchEndpoints("testowner/myrepo"))}
	f.srv.SetFallbackPollAfter(after)
	f.srv.SetBranchChecker(func(_ context.Context, _, _ string) (BranchPRResult, error) {
		f.mu.Lock()
		f.branchCalls++
		f.mu.Unlock()
		return BranchPRResult{}, nil
	})
	f.srv.SetNotifier(func(_ context.Context, _, _ string, text string) error {
		f.mu.Lock()
		f.posts = append(f.posts, text)
		f.mu.Unlock()
		return nil
	})
	t.Cleanup(f.srv.shutdownCancel)
	return f
}

func (f *dispatchFallbackFixture) branchCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.branchCalls
}

func (f *dispatchFallbackFixture) postCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.posts)
}

func (f *dispatchFallbackFixture) waitBranchCall(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.branchCallCount() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("fallback poll never checked the branch")
}

func dispatchFallbackTask(id string) DispatchTask {
	return DispatchTask{
		ID:        id,
		Repo:      "testowner/myrepo",
		Branch:    "dispatch/test-branch",
		CreatedAt: time.Date(2026, 10, 25, 23, 41, 0, 0, time.UTC),
	}
}

func TestDispatchPR_AC15_ActiveRunBlocksFallback(t *testing.T) {
	f := newDispatchFallbackFixture(t, 10*time.Millisecond)
	start := time.Date(2026, 10, 25, 23, 41, 0, 0, time.UTC)
	var clockMu sync.Mutex
	now := start
	f.srv.fallbackNow = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	f.srv.SetSessionLiveness(func(context.Context, string, string) (bool, error) { return true, nil })
	f.srv.DispatchRunStarted("testowner/myrepo", "dispatch/test-branch", "task-1", "session-1", "")
	clockMu.Lock()
	now = now.Add(10 * time.Minute)
	clockMu.Unlock()
	f.srv.scheduleDispatchFallback(dispatchFallbackTask("task-1"), start)
	time.Sleep(60 * time.Millisecond)
	if got := f.branchCallCount(); got != 0 {
		t.Fatalf("fallback branch checks = %d, want 0 while the run is live", got)
	}
	if got := f.postCount(); got != 0 {
		t.Fatalf("fallback posts = %d, want 0 while the run is live", got)
	}
}

func TestDispatchPR_AC16_TerminalFindingDedupesPerRun(t *testing.T) {
	f := newDispatchFallbackFixture(t, 10*time.Millisecond)
	fixed := time.Date(2026, 10, 25, 23, 41, 0, 0, time.UTC)
	f.srv.fallbackNow = func() time.Time { return fixed }
	f.srv.DispatchRunStarted("testowner/myrepo", "dispatch/test-branch", "task-1", "session-1", "")
	f.srv.noteDispatchRun("testowner/myrepo", "dispatch/test-branch", "task-1")
	f.srv.scheduleDispatchFallback(dispatchFallbackTask("task-1"), fixed)
	time.Sleep(60 * time.Millisecond)
	if got := f.branchCallCount(); got != 0 {
		t.Fatalf("terminal run fallback branch checks = %d, want 0", got)
	}
	if got := f.postCount(); got != 0 {
		t.Fatalf("terminal run fallback posts = %d, want 0", got)
	}
	f.srv.DispatchRunStarted("testowner/myrepo", "dispatch/test-branch", "task-2", "", "")
	f.srv.scheduleDispatchFallback(dispatchFallbackTask("task-2"), fixed)
	f.waitBranchCall(t)
}

func TestScheduleDispatchFallbackLostRun(t *testing.T) {
	f := newDispatchFallbackFixture(t, 10*time.Millisecond)
	f.srv.fallbackNow = func() time.Time { return time.Date(2026, 10, 25, 23, 41, 0, 0, time.UTC) }
	f.srv.DispatchRunStarted("testowner/myrepo", "dispatch/test-branch", "task-1", "", "")
	f.srv.scheduleDispatchFallback(dispatchFallbackTask("task-1"), time.Now())
	f.waitBranchCall(t)
}

func TestScheduleDispatchFallbackDeadSession(t *testing.T) {
	f := newDispatchFallbackFixture(t, 10*time.Millisecond)
	f.srv.fallbackNow = func() time.Time { return time.Date(2026, 10, 25, 23, 41, 0, 0, time.UTC) }
	f.srv.SetSessionLiveness(func(context.Context, string, string) (bool, error) { return false, nil })
	f.srv.DispatchRunStarted("testowner/myrepo", "dispatch/test-branch", "task-1", "session-1", "")
	f.srv.scheduleDispatchFallback(dispatchFallbackTask("task-1"), time.Now())
	f.waitBranchCall(t)
}

func TestScheduleDispatchFallbackLiveSession(t *testing.T) {
	f := newDispatchFallbackFixture(t, 10*time.Millisecond)
	f.srv.fallbackNow = func() time.Time { return time.Date(2026, 10, 25, 23, 41, 0, 0, time.UTC) }
	f.srv.SetSessionLiveness(func(context.Context, string, string) (bool, error) { return true, nil })
	f.srv.DispatchRunStarted("testowner/myrepo", "dispatch/test-branch", "task-1", "session-1", "")
	f.srv.scheduleDispatchFallback(dispatchFallbackTask("task-1"), time.Now())
	time.Sleep(60 * time.Millisecond)
	if got := f.branchCallCount(); got != 0 {
		t.Fatalf("live session fallback branch checks = %d, want 0", got)
	}
	if got := f.postCount(); got != 0 {
		t.Fatalf("live session fallback posts = %d, want 0", got)
	}
}
