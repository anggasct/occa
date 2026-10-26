package webhook

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureDocsLinkMirrorsSymlink(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "repo")
	lease := filepath.Join(base, "lease")
	docs := filepath.Join(base, "vault", "1-projects", "demo")
	for _, dir := range []string{root, lease, docs} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.Symlink(docs, filepath.Join(root, "project-docs")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	manager := &WorkspaceManager{}
	manager.ensureDocsLink(root, lease)

	got, err := os.Readlink(filepath.Join(lease, "project-docs"))
	if err != nil {
		t.Fatalf("lease link missing: %v", err)
	}
	if got != docs {
		t.Fatalf("link target = %q, want %q", got, docs)
	}

	// Idempotent: a second call must not fail or replace the link.
	manager.ensureDocsLink(root, lease)
	if again, err := os.Readlink(filepath.Join(lease, "project-docs")); err != nil || again != docs {
		t.Fatalf("second call changed link: %q err=%v", again, err)
	}
}

func TestEnsureDocsLinkSkipsWithoutSource(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "repo")
	lease := filepath.Join(base, "lease")
	for _, dir := range []string{root, lease} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	(&WorkspaceManager{}).ensureDocsLink(root, lease)

	if _, err := os.Lstat(filepath.Join(lease, "project-docs")); !os.IsNotExist(err) {
		t.Fatalf("expected no link, got err=%v", err)
	}
}
