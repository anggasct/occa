package router

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

// The router's own fakes never exercise the SQL driver, so authorization is
// also asserted against a real store: a role written without a model is the
// shape every /allow and admin bootstrap produces.
func newSQLiteBackedRouter(t *testing.T, adminID string) (*Router, *fakeRelayClient, *fakeReplyCtx, store.Store) {
	t.Helper()
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "router.db"), "")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	client := &fakeRelayClient{sessionID: "sess-new"}
	r := New(&fakeInstanceProvider{client: client}, st, "/default-workdir", adminID)
	return r, client, &fakeReplyCtx{}, st
}

func assertNotRefused(t *testing.T, sends []string) {
	t.Helper()
	for _, s := range sends {
		if s == accessDeniedMessage || s == accessVerifyMessage {
			t.Fatalf("authorized request was refused: %q", s)
		}
	}
}

func TestAllowedUserAuthorizedAgainstRealStore(t *testing.T) {
	r, client, reply, st := newSQLiteBackedRouter(t, "")
	ctx := context.Background()

	if err := st.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "user1", "allow"); err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}

	if err := r.Route(ctx, msg("hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	waitForDispatch(t, client)
	waitForResponse(t, r)

	assertNotRefused(t, reply.sends)
	if client.lastMsg != "hello" {
		t.Fatalf("message did not reach the agent: %q", client.lastMsg)
	}
}

func TestBootstrapAdminStaysAuthorizedAcrossMessagesAgainstRealStore(t *testing.T) {
	r, _, reply, _ := newSQLiteBackedRouter(t, "admin1")
	ctx := context.Background()

	for i := range 3 {
		reply.sends = nil
		if err := r.Route(ctx, msgFrom("admin1", "/help", reply)); err != nil {
			t.Fatalf("Route message %d: %v", i+1, err)
		}
		assertNotRefused(t, reply.sends)
		if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "/status") {
			t.Fatalf("message %d: unexpected reply %v", i+1, reply.sends)
		}
	}
}

func TestLastAdminGuardWithRoleOnlyRowAgainstRealStore(t *testing.T) {
	r, _, reply, st := newSQLiteBackedRouter(t, "")
	ctx := context.Background()

	if err := st.OverrideRepo().UpsertRole(ctx, "telegram", "chat1", "user1", "admin"); err != nil {
		t.Fatalf("UpsertRole: %v", err)
	}

	if err := r.Route(ctx, msg("/deny user1", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || !strings.Contains(reply.sends[0], "Cannot deny the last admin") {
		t.Fatalf("expected last-admin guard, got %v", reply.sends)
	}
}
