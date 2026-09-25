package router

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

// The router's own fakes never exercise the SQL driver, so the allowlist
// gate is also asserted against a real store: preferences written to the
// store must not affect authorization either way.
func newSQLiteBackedRouter(t *testing.T, adminID string, discordSenders, telegramSenders []string) (*Router, *fakeRelayClient, *fakeReplyCtx, store.Store) {
	t.Helper()
	st, err := store.OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "router.db"), "")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	client := &fakeRelayClient{sessionID: "sess-new"}
	r := NewWithAllowlists(&fakeInstanceProvider{client: client}, st, "/default-workdir", adminID, discordSenders, telegramSenders, testRouterConfig())
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

func TestAllowlistedUserAuthorizedAgainstRealStore(t *testing.T) {
	r, client, reply, st := newSQLiteBackedRouter(t, "", nil, []string{"user1"})
	ctx := context.Background()

	if err := st.OverrideRepo().UpsertAgent(ctx, "telegram", "chat1", "user1", "planner"); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
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

func TestEnvAliasStaysAuthorizedAcrossMessagesAgainstRealStore(t *testing.T) {
	r, _, reply, _ := newSQLiteBackedRouter(t, "admin1", nil, nil)
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

func TestUnknownSenderDeniedAgainstRealStore(t *testing.T) {
	r, _, reply, _ := newSQLiteBackedRouter(t, "", nil, []string{"user1"})
	ctx := context.Background()

	if err := r.Route(ctx, msgFrom("stranger", "hello", reply)); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(reply.sends) != 1 || reply.sends[0] != accessDeniedMessage {
		t.Fatalf("expected deny, got %v", reply.sends)
	}
}
