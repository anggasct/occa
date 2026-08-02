package relay

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

type mockSessionRepo struct {
	activeID string
	ownerPID int
	byKey    map[[4]string]string
	setCalls []setActiveCall
}

type setActiveCall struct {
	platform, channelID, threadID, userID, sessionID string
	agentPID                                         int
}

func (m *mockSessionRepo) Active(_ context.Context, platform, channelID, threadID, userID string) (string, int, error) {
	key := [4]string{platform, channelID, threadID, userID}
	if id, ok := m.byKey[key]; ok {
		return id, 0, nil
	}
	return m.activeID, m.ownerPID, nil
}

func (m *mockSessionRepo) SetActive(_ context.Context, platform, channelID, threadID, userID, sessionID string, agentPID int) error {
	m.setCalls = append(m.setCalls, setActiveCall{platform, channelID, threadID, userID, sessionID, agentPID})
	m.activeID = sessionID
	m.ownerPID = agentPID
	return nil
}

func (m *mockSessionRepo) Deactivate(_ context.Context, platform, channelID, threadID, userID string) error {
	m.activeID = ""
	return nil
}

func (m *mockSessionRepo) List(_ context.Context, platform, channelID string) ([]store.Session, error) {
	return nil, nil
}

func (m *mockSessionRepo) ThreadChannel(_ context.Context, platform, threadID string) (string, error) {
	return "", nil
}

func (m *mockSessionRepo) Delete(_ context.Context, id int64) error { return nil }

type mockClient struct {
	sessionID string
}

func (m *mockClient) CreateSession(_ context.Context) (string, error) {
	return m.sessionID, nil
}

func (m *mockClient) SendMessage(_ context.Context, _, _ string, _ *ModelRef, _ []Attachment) error {
	return nil
}
func (m *mockClient) Providers(_ context.Context) (Providers, error)  { return Providers{}, nil }
func (m *mockClient) RunCommand(_ context.Context, _, _ string) error { return nil }
func (m *mockClient) ReplyPermission(_ context.Context, _ string, _ PermissionReply) error {
	return nil
}
func (m *mockClient) ListCommands(_ context.Context) ([]CommandInfo, error) { return nil, nil }
func (m *mockClient) Events(_ context.Context, _ string) (<-chan Event, error) {
	return nil, nil
}

func TestResolveExisting(t *testing.T) {
	repo := &mockSessionRepo{activeID: "existing", ownerPID: 100}
	client := &mockClient{sessionID: "new-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != "existing" {
		t.Fatalf("got %q, want %q", id, "existing")
	}
	if len(repo.setCalls) != 0 {
		t.Fatal("should not call SetActive when session exists")
	}
}

func TestResolveCreatesNew(t *testing.T) {
	repo := &mockSessionRepo{}
	client := &mockClient{sessionID: "new-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "456", "", "user-1", 100)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != "new-session" {
		t.Fatalf("got %q, want %q", id, "new-session")
	}
	if len(repo.setCalls) != 1 {
		t.Fatalf("expected 1 SetActive call, got %d", len(repo.setCalls))
	}
	call := repo.setCalls[0]
	if call.platform != "telegram" || call.channelID != "456" || call.threadID != "" || call.userID != "user-1" || call.sessionID != "new-session" || call.agentPID != 100 {
		t.Fatalf("unexpected SetActive call: %+v", call)
	}
}

// TestResolveKeysByConversation checks the session-key policy: two users in
// the same channel resolve different sessions, the same user resolves the
// same session, and thread participants share one session.
func TestResolveKeysByConversation(t *testing.T) {
	keyed := map[[4]string]string{
		{"telegram", "chat", "", "alice"}:   "sess-alice",
		{"telegram", "chat", "", "bob"}:     "sess-bob",
		{"discord", "chat", "thread-1", ""}: "sess-thread",
	}
	repo := &mockSessionRepo{byKey: keyed}
	client := &mockClient{sessionID: "sess-new"}
	resolver := NewSessionResolver(repo, client)

	for key, want := range keyed {
		got, err := resolver.Resolve(context.Background(), key[0], key[1], key[2], key[3], 100)
		if err != nil {
			t.Fatalf("Resolve(%v): %v", key, err)
		}
		if got != want {
			t.Fatalf("Resolve(%v) = %q, want %q", key, got, want)
		}
	}
	if len(repo.setCalls) != 0 {
		t.Fatalf("expected no session creation for existing keys, got %d SetActive calls", len(repo.setCalls))
	}
}

// TestResolveStaleSessionRecreates: a session owned by a dead agent process
// (PID mismatch) is replaced with a fresh one instead of being reused.
func TestResolveStaleSessionRecreates(t *testing.T) {
	repo := &mockSessionRepo{activeID: "stale-session", ownerPID: 999}
	client := &mockClient{sessionID: "fresh-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != "fresh-session" {
		t.Fatalf("got %q, want %q", id, "fresh-session")
	}
	if len(repo.setCalls) != 1 || repo.setCalls[0].agentPID != 100 {
		t.Fatalf("expected SetActive with new PID, got %+v", repo.setCalls)
	}
}

// TestResolveUnknownOwnerReuses: legacy rows without an owner PID are reused.
func TestResolveUnknownOwnerReuses(t *testing.T) {
	repo := &mockSessionRepo{activeID: "legacy-session", ownerPID: 0}
	client := &mockClient{sessionID: "fresh-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != "legacy-session" {
		t.Fatalf("got %q, want %q", id, "legacy-session")
	}
}
