package webhook

import (
	"testing"
)

func TestExtractExecutionKeyGitHubPR(t *testing.T) {
	body := []byte(`{
		"repository": {
			"full_name": "anggasct/occa",
			"name": "occa"
		},
		"pull_request": {
			"base": {
				"repo": {
					"full_name": "anggasct/occa"
				},
				"ref": "main"
			},
			"head": {
				"repo": {
					"full_name": "contributor/occa"
				},
				"ref": "fix/project-aware-orchestration"
			}
		}
	}`)

	key := ExtractExecutionKey(body)
	if key.IsZero() {
		t.Fatal("expected non-zero execution key")
	}
	if key.Repository != "anggasct/occa" {
		t.Errorf("Repository = %q, want anggasct/occa", key.Repository)
	}
	if key.HeadRepository != "contributor/occa" {
		t.Errorf("HeadRepository = %q, want contributor/occa", key.HeadRepository)
	}
	if key.Branch != "fix/project-aware-orchestration" {
		t.Errorf("Branch = %q, want fix/project-aware-orchestration", key.Branch)
	}
	expectedStr := "anggasct/occa:contributor/occa:fix/project-aware-orchestration"
	if key.String() != expectedStr {
		t.Errorf("String() = %q, want %q", key.String(), expectedStr)
	}
}

func TestExtractExecutionKeySameRepoPR(t *testing.T) {
	body := []byte(`{
		"repository": {
			"full_name": "anggasct/occa"
		},
		"pull_request": {
			"base": {
				"repo": {
					"full_name": "anggasct/occa"
				},
				"ref": "main"
			},
			"head": {
				"repo": {
					"full_name": "anggasct/occa"
				},
				"ref": "feat/something"
			}
		}
	}`)

	key := ExtractExecutionKey(body)
	if key.IsZero() {
		t.Fatal("expected non-zero execution key")
	}
	if key.Repository != "anggasct/occa" || key.HeadRepository != "anggasct/occa" || key.Branch != "feat/something" {
		t.Errorf("unexpected key: %+v", key)
	}
	if key.String() != "anggasct/occa:anggasct/occa:feat/something" {
		t.Errorf("String() = %q", key.String())
	}
}

func TestExtractExecutionKeyGitHubPush(t *testing.T) {
	body := []byte(`{
		"ref": "refs/heads/feature-xyz",
		"repository": {
			"full_name": "anggasct/dispatch"
		}
	}`)

	key := ExtractExecutionKey(body)
	if key.IsZero() {
		t.Fatal("expected non-zero execution key")
	}
	if key.Repository != "anggasct/dispatch" || key.HeadRepository != "anggasct/dispatch" || key.Branch != "feature-xyz" {
		t.Errorf("unexpected key: %+v", key)
	}
	if key.String() != "anggasct/dispatch:anggasct/dispatch:feature-xyz" {
		t.Errorf("String() = %q", key.String())
	}
}

func TestExtractExecutionKeyGenericPayload(t *testing.T) {
	body := []byte(`{
		"repository": "myorg/myrepo",
		"head_repository": "fork/myrepo",
		"branch": "fix/bug"
	}`)

	key := ExtractExecutionKey(body)
	if key.IsZero() {
		t.Fatal("expected non-zero execution key")
	}
	if key.Repository != "myorg/myrepo" || key.HeadRepository != "fork/myrepo" || key.Branch != "fix/bug" {
		t.Errorf("unexpected key: %+v", key)
	}
}

func TestExtractExecutionKeyMissingOrAmbiguousFallback(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"malformed json", "not a json"},
		{"empty json object", "{}"},
		{"ping event", `{"zen":"keep it simple"}`},
		{"missing branch", `{"repository":{"full_name":"org/repo"}}`},
		{"missing repo", `{"branch":"fix/test"}`},
		{"empty repo string", `{"repository":"","branch":"main"}`},
		{"name-only repository object without owner", `{"repository":{"name":"occa"},"ref":"refs/heads/main"}`},
		{"PR with head.ref but missing head.repo", `{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"ref":"fix/branch"}}}`},
		{"PR with head.ref and empty head.repo object", `{"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{},"ref":"fix/branch"}}}`},
		{"PR with head.ref and name-only head.repo", `{"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"name":"fork-repo"},"ref":"fix/branch"}}}`},
		{"PR with three path components in repo", `{"repository":{"full_name":"org/repo/extra"},"pull_request":{"base":{"repo":{"full_name":"org/repo/extra"}},"head":{"repo":{"full_name":"org/repo/extra"},"ref":"fix/test"}}}`},
		{"PR with invalid chars in repo", `{"pull_request":{"base":{"repo":{"full_name":"org/repo;evil"}},"head":{"repo":{"full_name":"org/repo;evil"},"ref":"fix/test"}}}`},
		{"PR with spaces in repo", `{"pull_request":{"base":{"repo":{"full_name":"org/repo with space"}},"head":{"repo":{"full_name":"org/repo with space"},"ref":"fix/test"}}}`},
		{"PR with traversal in repo", `{"pull_request":{"base":{"repo":{"full_name":"org/../repo"}},"head":{"repo":{"full_name":"org/../repo"},"ref":"fix/test"}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := ExtractExecutionKey([]byte(tt.body))
			if !key.IsZero() {
				t.Fatalf("expected zero key for %s, got %+v", tt.name, key)
			}
			if key.String() != "" {
				t.Fatalf("expected empty string for zero key, got %q", key.String())
			}
		})
	}
}

func TestExtractExecutionKeyHeadRevision(t *testing.T) {
	prBody := []byte(`{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/x","sha":"ABCDEF0123456789abcdef0123456789abcdef01"}}}`)
	key := ExtractExecutionKey(prBody)
	if key.HeadRevision != "abcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("PR head revision = %q", key.HeadRevision)
	}

	pushBody := []byte(`{"repository":{"full_name":"anggasct/occa"},"ref":"refs/heads/main","after":"1234567890abcdef1234567890abcdef12345678"}`)
	key = ExtractExecutionKey(pushBody)
	if key.HeadRevision != "1234567890abcdef1234567890abcdef12345678" {
		t.Fatalf("push after revision = %q", key.HeadRevision)
	}

	headCommitBody := []byte(`{"repository":{"full_name":"anggasct/occa"},"ref":"refs/heads/main","head_commit":{"id":"fedcba9876543210fedcba9876543210fedcba98"}}`)
	key = ExtractExecutionKey(headCommitBody)
	if key.HeadRevision != "fedcba9876543210fedcba9876543210fedcba98" {
		t.Fatalf("head_commit revision = %q", key.HeadRevision)
	}

	genericBody := []byte(`{"repo":{"full_name":"anggasct/occa"},"branch":"main","head_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
	key = ExtractExecutionKey(genericBody)
	if key.HeadRevision != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("generic head_revision = %q", key.HeadRevision)
	}

	invalidBody := []byte(`{"repo":{"full_name":"anggasct/occa"},"branch":"main","head_revision":"not-a-sha!"}`)
	key = ExtractExecutionKey(invalidBody)
	if key.HeadRevision != "" {
		t.Fatalf("invalid revision must be dropped, got %q", key.HeadRevision)
	}

	shortBody := []byte(`{"repo":{"full_name":"anggasct/occa"},"branch":"main","head_revision":"abc12"}`)
	key = ExtractExecutionKey(shortBody)
	if key.HeadRevision != "" {
		t.Fatalf("too-short revision must be dropped, got %q", key.HeadRevision)
	}
}
