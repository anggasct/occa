package webhook

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func gitIdentity(t *testing.T, dir string) {
	t.Helper()
	runGit(t, dir, "config", "user.name", "occa test")
	runGit(t, dir, "config", "user.email", "occa@example.invalid")
	runGit(t, dir, "config", "commit.gpgsign", "false")
}

func writeAndCommit(t *testing.T, dir, file, content, msg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", file)
	runGit(t, dir, "commit", "-q", "-m", msg)
}

// conflictCheckout builds origin with main and feat/x, then moves main on.
// conflicting=true edits the same line on both sides.
func conflictCheckout(t *testing.T, conflicting bool) (string, *WorkspaceLease) {
	t.Helper()
	remote := t.TempDir()
	runGit(t, remote, "init", "-q", "--bare", "-b", "main")
	seed := t.TempDir()
	runGit(t, seed, "init", "-q", "-b", "main")
	gitIdentity(t, seed)
	writeAndCommit(t, seed, "a.txt", "base\n", "base")
	runGit(t, seed, "remote", "add", "origin", remote)
	runGit(t, seed, "push", "-q", "origin", "main")
	runGit(t, seed, "checkout", "-q", "-b", "feat/x")
	if conflicting {
		writeAndCommit(t, seed, "a.txt", "feature\n", "feature")
	} else {
		writeAndCommit(t, seed, "b.txt", "feature\n", "feature")
	}
	runGit(t, seed, "push", "-q", "origin", "feat/x")
	runGit(t, seed, "checkout", "-q", "main")
	writeAndCommit(t, seed, "a.txt", "main moved\n", "main moved")
	runGit(t, seed, "push", "-q", "origin", "main")

	clone := t.TempDir()
	runGit(t, clone, "clone", "-q", "-b", "feat/x", remote, ".")
	gitIdentity(t, clone)
	head := runGit(t, clone, "rev-parse", "HEAD")
	return clone, &WorkspaceLease{manager: NewWorkspaceManager(), Path: clone, Mode: config.WorkspaceModeMutable, Branch: "feat/x", StartRevision: head}
}

func conflictPrep(t *testing.T, lease *WorkspaceLease, base string) (bool, WebhookEnvelope, *WebhookWorkContext) {
	t.Helper()
	srv, _, _ := operatorTestServer(t, nil)
	envelope := WebhookEnvelope{"base_branch": base, "conflict_files": ""}
	work := &WebhookWorkContext{PRKey: "acme/widget#7"}
	ok := srv.prepareConflict(context.Background(), srv.endpointsByName["github-widget-fix"], envelope, work, lease)
	return ok, envelope, work
}

// IMP-113 AC-09: a clean base merge needs no agent and finalizes as a push.
func TestConflictPrepCleanMergeSkipsAgent(t *testing.T) {
	clone, lease := conflictCheckout(t, false)
	skip, envelope, work := conflictPrep(t, lease, "main")
	if !skip || envelope["conflict_files"] != "" || work.AgentReply != "merged origin/main cleanly — no conflicts left" {
		t.Fatalf("clean prep = %v %+v %q", skip, envelope, work.AgentReply)
	}
	if parents := strings.Fields(runGit(t, clone, "log", "-1", "--format=%P")); len(parents) != 2 {
		t.Fatalf("HEAD must be the merge commit, parents = %v", parents)
	}
	if _, err := lease.Finalize(context.Background(), "fix", "del-clean"); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if local, remote := runGit(t, clone, "rev-parse", "HEAD"), runGit(t, clone, "rev-parse", "origin/feat/x"); local != remote {
		t.Fatalf("the merge commit must be pushed: local %s, origin %s", local, remote)
	}
}

// IMP-113 AC-09, AC-10: conflicts are listed for the agent, and a run that
// leaves them unresolved is not committed.
func TestConflictPrepListsConflictsAndFinalizeRefuses(t *testing.T) {
	_, lease := conflictCheckout(t, true)
	skip, envelope, _ := conflictPrep(t, lease, "main")
	if skip || envelope["conflict_files"] != "a.txt" {
		t.Fatalf("conflict prep = %v %+v", skip, envelope)
	}
	_, err := lease.Finalize(context.Background(), "fix", "del-conflict")
	if err == nil || !strings.Contains(err.Error(), "unresolved merge conflicts in a.txt") {
		t.Fatalf("Finalize err = %v, want unresolved conflicts", err)
	}
}

// IMP-113 AC-09: a failed fetch leaves no half merge and lets the agent run.
func TestConflictPrepFetchFailureLeavesTreeAlone(t *testing.T) {
	clone, lease := conflictCheckout(t, true)
	skip, envelope, _ := conflictPrep(t, lease, "no-such-base")
	if skip || envelope["conflict_files"] != "" {
		t.Fatalf("failed prep = %v %+v", skip, envelope)
	}
	cmd := exec.Command("git", "rev-parse", "-q", "--verify", "MERGE_HEAD")
	cmd.Dir = clone
	if err := cmd.Run(); err == nil {
		t.Fatal("a failed prep must not leave a merge in progress")
	}
}

// IMP-113 AC-10: added conflict markers are not committed either.
func TestFinalizeRefusesConflictMarkers(t *testing.T) {
	clone, lease := conflictCheckout(t, false)
	if err := os.WriteFile(filepath.Join(clone, "b.txt"), []byte("<<<<<<< HEAD\nours\n=======\ntheirs\n>>>>>>> main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := lease.Finalize(context.Background(), "fix", "del-markers")
	if err == nil || !strings.Contains(err.Error(), "unresolved merge conflicts in b.txt") {
		t.Fatalf("Finalize err = %v, want the marker file named", err)
	}
	if got := conflictMarkerFiles("+++ b/doc.md\n+=======\n+Title\n"); len(got) != 0 {
		t.Fatalf("a setext rule alone is not a conflict marker: %v", got)
	}
}
