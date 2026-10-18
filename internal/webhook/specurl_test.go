package webhook

import (
	"strings"
	"testing"
)

func TestIsSpecURL(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"https://github.com/o/r/blob/main/specs/feat-x.md", true},
		{"https://jira.example.com/browse/ABC-1", true},
		{"HTTPS://github.com/o/r", true},
		{"  https://github.com/o/r  ", true},
		{"specs/feat-x.md", false},
		{"", false},
		{"https://", false},
		{"https://exa mple.com", false},
		{"http://internal/x", false},
		{"/abs/path.md", false},
	}
	for _, tc := range cases {
		if got := isSpecURL(tc.input); got != tc.want {
			t.Errorf("isSpecURL(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestDispatchValidateSpecURL(t *testing.T) {
	base := DispatchRequest{Repo: "acme/widget", Branch: "feat/x", Scope: "", Verify: ""}
	if err := func() error { r := base; r.SpecPath = "specs/feat-x.md"; return r.Validate() }(); err != nil {
		t.Fatalf("path spec should validate: %v", err)
	}
	if err := func() error { r := base; r.SpecPath = "https://jira.example.com/browse/ABC-1"; return r.Validate() }(); err != nil {
		t.Fatalf("URL spec should validate: %v", err)
	}
	for _, bad := range []string{"https://", "http://internal/x", "https://exa mple.com"} {
		r := base
		r.SpecPath = bad
		err := r.Validate()
		if err == nil || !strings.Contains(err.Error(), "spec looks like a URL") {
			t.Fatalf("spec %q error = %v, want URL shape rejection", bad, err)
		}
	}
	r := base
	r.SpecPath = "../escape.md"
	if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "must not escape") {
		t.Fatalf("traversal error = %v, want escape rejection", err)
	}
}
