package webhook

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/anggasct/occa/internal/relay"
)

type fakePermissionReplier struct {
	replies []recordedReply
	err     error
}

type recordedReply struct {
	requestID string
	reply     relay.PermissionReply
}

func (f *fakePermissionReplier) ReplyPermission(_ context.Context, requestID string, reply relay.PermissionReply) error {
	if f.err != nil {
		return f.err
	}
	f.replies = append(f.replies, recordedReply{requestID: requestID, reply: reply})
	return nil
}

func TestPermissionResponderAllowVaultDocs(t *testing.T) {
	const docsRoot = "/home/ubuntu/Documents/obsidian-vault"

	cases := []struct {
		name     string
		patterns []string
	}{
		{name: "exact root", patterns: []string{"/home/ubuntu/Documents/obsidian-vault"}},
		{name: "project docs subpath", patterns: []string{"/home/ubuntu/Documents/obsidian-vault/1-projects/occa/development-plan.md"}},
		{name: "glob under root", patterns: []string{"/home/ubuntu/Documents/obsidian-vault/**"}},
		{name: "glob subdir", patterns: []string{"/home/ubuntu/Documents/obsidian-vault/1-projects/**"}},
		{name: "sibling mix", patterns: []string{"/etc/passwd", "/home/ubuntu/Documents/obsidian-vault/skills"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakePermissionReplier{}
			h := NewPermissionResponder(fake, docsRoot)
			err := h.Prompt(context.Background(), relay.PermissionRequest{
				ID:         "per_allow",
				Permission: "external_directory",
				Patterns:   tc.patterns,
			})
			if err != nil {
				t.Fatalf("Prompt returned error: %v", err)
			}
			want := []recordedReply{{requestID: "per_allow", reply: relay.PermissionAlways}}
			if !reflect.DeepEqual(fake.replies, want) {
				t.Fatalf("replies = %+v, want %+v", fake.replies, want)
			}
		})
	}
}

func TestPermissionResponderRejectsOutsideScope(t *testing.T) {
	const docsRoot = "/home/ubuntu/Documents/obsidian-vault"

	cases := []struct {
		name    string
		request relay.PermissionRequest
	}{
		{
			name: "non-docs permission kind",
			request: relay.PermissionRequest{
				ID: "per_1", Permission: "bash", Patterns: []string{"git push origin main"},
			},
		},
		{
			name: "path outside docs root",
			request: relay.PermissionRequest{
				ID: "per_2", Permission: "external_directory", Patterns: []string{"/home/ubuntu/projects/occa"},
			},
		},
		{
			name: "system path",
			request: relay.PermissionRequest{
				ID: "per_3", Permission: "external_directory", Patterns: []string{"/etc/ssh/sshd_config"},
			},
		},
		{
			name: "sibling of docs root",
			request: relay.PermissionRequest{
				ID: "per_4", Permission: "external_directory", Patterns: []string{"/home/ubuntu/Documents/obsidian-vault-other"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakePermissionReplier{}
			h := NewPermissionResponder(fake, docsRoot)
			if err := h.Prompt(context.Background(), tc.request); err != nil {
				t.Fatalf("Prompt returned error: %v", err)
			}
			want := []recordedReply{{requestID: tc.request.ID, reply: relay.PermissionReject}}
			if !reflect.DeepEqual(fake.replies, want) {
				t.Fatalf("replies = %+v, want %+v", fake.replies, want)
			}
		})
	}
}

func TestPermissionResponderEmptyDocsRootDeniesEverything(t *testing.T) {
	fake := &fakePermissionReplier{}
	h := NewPermissionResponder(fake, "")
	if err := h.Prompt(context.Background(), relay.PermissionRequest{
		ID:         "per_empty",
		Permission: "external_directory",
		Patterns:   []string{"/home/ubuntu/Documents/obsidian-vault/1-projects/occa"},
	}); err != nil {
		t.Fatalf("Prompt returned error: %v", err)
	}
	want := []recordedReply{{requestID: "per_empty", reply: relay.PermissionReject}}
	if !reflect.DeepEqual(fake.replies, want) {
		t.Fatalf("replies = %+v, want %+v", fake.replies, want)
	}
}

func TestPermissionResponderDuplicateRequestAnsweredOnce(t *testing.T) {
	fake := &fakePermissionReplier{}
	h := NewPermissionResponder(fake, "/vault")
	req := relay.PermissionRequest{
		ID:         "per_dup",
		Permission: "external_directory",
		Patterns:   []string{"/vault/1-projects"},
	}
	if err := h.Prompt(context.Background(), req); err != nil {
		t.Fatalf("first Prompt returned error: %v", err)
	}
	if err := h.Prompt(context.Background(), req); err != nil {
		t.Fatalf("second Prompt returned error: %v", err)
	}
	if len(fake.replies) != 1 {
		t.Fatalf("replies = %+v, want exactly one reply for duplicate request IDs", fake.replies)
	}
}

func TestPermissionResponderPropagatesReplyError(t *testing.T) {
	wantErr := errors.New("agent unreachable")
	fake := &fakePermissionReplier{err: wantErr}
	h := NewPermissionResponder(fake, "/vault")
	err := h.Prompt(context.Background(), relay.PermissionRequest{
		ID:         "per_err",
		Permission: "external_directory",
		Patterns:   []string{"/vault/1-projects"},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Prompt error = %v, want %v", err, wantErr)
	}
}

func TestUnderRoot(t *testing.T) {
	cases := []struct {
		pattern string
		root    string
		want    bool
	}{
		{pattern: "/vault", root: "/vault", want: true},
		{pattern: "/vault/1-projects/occa", root: "/vault", want: true},
		{pattern: "/vault/**", root: "/vault", want: true},
		{pattern: "/vault/1-projects/**", root: "/vault", want: true},
		{pattern: "/vault-other/x", root: "/vault", want: false},
		{pattern: "/vaultx", root: "/vault", want: false},
		{pattern: "/proc/1", root: "/vault", want: false},
		{pattern: "/vault", root: "/vault/", want: true},
	}
	for _, tc := range cases {
		if got := underRoot(tc.pattern, tc.root); got != tc.want {
			t.Errorf("underRoot(%q, %q) = %v, want %v", tc.pattern, tc.root, got, tc.want)
		}
	}
}
