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
	takeover *store.TakeoverCandidate
}

type setActiveCall struct {
	platform, channelID, threadID, userID, sessionID string
	agentPID                                         int
}

func (m *mockSessionRepo) Active(_ context.Context, platform, channelID, threadID, userID string) (string, int, error) {
	key := [4]string{platform, channelID, threadID, userID}
	if id, ok := m.byKey[key]; ok {
		return id, m.ownerPID, nil
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

func (m *mockSessionRepo) ListConversation(_ context.Context, platform, channelID, threadID, userID string) ([]store.Session, error) {
	return nil, nil
}

func (m *mockSessionRepo) ThreadChannel(_ context.Context, platform, threadID string) (string, error) {
	return "", nil
}

func (m *mockSessionRepo) SetTitle(_ context.Context, _ int64, _ string) error { return nil }

func (m *mockSessionRepo) SetModel(_ context.Context, _, _, _, _, _ string) error { return nil }

func (m *mockSessionRepo) ActiveModel(_ context.Context, _, _, _, _ string) (string, error) {
	return "", nil
}

func (m *mockSessionRepo) Delete(_ context.Context, id int64) error { return nil }

func (m *mockSessionRepo) MarkTakeoverEligible(_ context.Context, _, _, _, sessionID string, agentPID int, seed string) error {
	m.takeover = &store.TakeoverCandidate{SessionID: sessionID, AgentPID: agentPID, Seed: seed}
	return nil
}

func (m *mockSessionRepo) ClearTakeoverEligible(_ context.Context, _, _, _ string) error {
	m.takeover = nil
	return nil
}

func (m *mockSessionRepo) TakeoverCandidate(_ context.Context, _, _, _ string) (*store.TakeoverCandidate, error) {
	return m.takeover, nil
}

func (m *mockSessionRepo) LinkThreadRoot(_ context.Context, _, _, _, _, _, _ string) error {
	return nil
}

func (m *mockSessionRepo) ThreadRoot(_ context.Context, _, _, _ string) (*store.ThreadRootCard, error) {
	return nil, nil
}

type mockClient struct {
	sessionID     string
	sessionExists bool
	existsErr     error
	existsCalls   int
}

func (m *mockClient) CreateSession(_ context.Context) (string, error) {
	return m.sessionID, nil
}

func (m *mockClient) GetSession(_ context.Context, _ string) (*SessionInfo, error) {
	return &SessionInfo{}, nil
}

func (m *mockClient) SessionExists(_ context.Context, _ string) (bool, error) {
	m.existsCalls++
	if m.existsErr != nil {
		return false, m.existsErr
	}
	return m.sessionExists, nil
}

func (m *mockClient) SendMessage(_ context.Context, _, _ string, _ *ModelRef, _ []Attachment) error {
	return nil
}
func (m *mockClient) Providers(_ context.Context) (Providers, error)  { return Providers{}, nil }
func (m *mockClient) RunCommand(_ context.Context, _, _ string) error { return nil }
func (m *mockClient) ReplyPermission(_ context.Context, _ string, _ PermissionReply) error {
	return nil
}
func (m *mockClient) ListCommands(_ context.Context) ([]CommandInfo, error)          { return nil, nil }
func (m *mockClient) AnswerQuestion(_ context.Context, _ string, _ [][]string) error { return nil }
func (m *mockClient) RejectQuestion(_ context.Context, _ string) error               { return nil }
func (m *mockClient) Events(_ context.Context, _ string) (<-chan Event, error) {
	return nil, nil
}
func (m *mockClient) AbortSession(_ context.Context, _ string) error { return nil }
func (m *mockClient) SummarizeSession(_ context.Context, _, _, _ string) error {
	return nil
}
func (m *mockClient) RevertMessage(_ context.Context, _, _ string) error { return nil }
func (m *mockClient) UnrevertSession(_ context.Context, _ string) error  { return nil }
func (m *mockClient) ListMessages(_ context.Context, _ string) ([]MessageInfo, error) {
	return nil, nil
}
func (m *mockClient) ListAgents(_ context.Context) ([]AgentInfo, error) { return nil, nil }
func (m *mockClient) SwitchAgent(_ context.Context, _, _ string) error  { return nil }

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
	if client.existsCalls != 0 {
		t.Fatalf("same-PID fast path must not call SessionExists, got %d calls", client.existsCalls)
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

func TestResolveKeysByConversation(t *testing.T) {
	keyed := map[[4]string]string{
		{"telegram", "chat", "", "alice"}:   "sess-alice",
		{"telegram", "chat", "", "bob"}:     "sess-bob",
		{"discord", "chat", "thread-1", ""}: "sess-thread",
	}
	repo := &mockSessionRepo{byKey: keyed, ownerPID: 100}
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
	if client.existsCalls != 0 {
		t.Fatalf("expected no validation for owned sessions, got %d SessionExists calls", client.existsCalls)
	}
}

func TestResolveStaleSessionRecreates(t *testing.T) {
	t.Run("stale session exists on new agent", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "stale-session", ownerPID: 999}
		client := &mockClient{sessionID: "fresh-session", sessionExists: true}
		resolver := NewSessionResolver(repo, client)

		id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if id != "stale-session" {
			t.Fatalf("got %q, want %q", id, "stale-session")
		}
		if len(repo.setCalls) != 1 {
			t.Fatalf("expected 1 SetActive call, got %d", len(repo.setCalls))
		}
		call := repo.setCalls[0]
		if call.sessionID != "stale-session" || call.agentPID != 100 {
			t.Fatalf("unexpected SetActive call: %+v", call)
		}
	})

	t.Run("stale session gone on new agent", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "stale-session", ownerPID: 999}
		client := &mockClient{sessionID: "fresh-session", sessionExists: false}
		resolver := NewSessionResolver(repo, client)

		id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if id != "fresh-session" {
			t.Fatalf("got %q, want %q", id, "fresh-session")
		}
		if len(repo.setCalls) != 1 {
			t.Fatalf("expected 1 SetActive call, got %d", len(repo.setCalls))
		}
		call := repo.setCalls[0]
		if call.sessionID != "fresh-session" || call.agentPID != 100 {
			t.Fatalf("unexpected SetActive call: %+v", call)
		}
	})
}

func TestResolveLegacyRowValidates(t *testing.T) {
	t.Run("legacy session exists", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "legacy-session", ownerPID: 0}
		client := &mockClient{sessionID: "fresh-session", sessionExists: true}
		resolver := NewSessionResolver(repo, client)

		id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if id != "legacy-session" {
			t.Fatalf("got %q, want %q", id, "legacy-session")
		}
		if client.existsCalls != 1 {
			t.Fatalf("expected 1 SessionExists call, got %d", client.existsCalls)
		}
		if len(repo.setCalls) != 1 {
			t.Fatalf("expected 1 SetActive call, got %d", len(repo.setCalls))
		}
		call := repo.setCalls[0]
		if call.sessionID != "legacy-session" || call.agentPID != 100 {
			t.Fatalf("expected adoption stamping the current PID, got %+v", call)
		}
	})

	t.Run("legacy session gone", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "legacy-session", ownerPID: 0}
		client := &mockClient{sessionID: "fresh-session", sessionExists: false}
		resolver := NewSessionResolver(repo, client)

		id, err := resolver.Resolve(context.Background(), "telegram", "123", "", "user-1", 100)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if id != "fresh-session" {
			t.Fatalf("got %q, want %q", id, "fresh-session")
		}
		if client.existsCalls != 1 {
			t.Fatalf("expected 1 SessionExists call, got %d", client.existsCalls)
		}
		if len(repo.setCalls) != 1 {
			t.Fatalf("expected 1 SetActive call, got %d", len(repo.setCalls))
		}
		call := repo.setCalls[0]
		if call.sessionID != "fresh-session" || call.agentPID != 100 {
			t.Fatalf("unexpected SetActive call: %+v", call)
		}
	})
}

func TestResolveDetailedOutcomes(t *testing.T) {
	t.Run("owner match resumes without backend check", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "stored", ownerPID: 100}
		client := &mockClient{sessionID: "fresh"}
		res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "telegram", "1", "", "u", 100)
		if err != nil {
			t.Fatalf("ResolveDetailed: %v", err)
		}
		if !res.Resumed || !res.HadStored || res.SessionID != "stored" {
			t.Fatalf("resolution = %+v, want resumed stored session", res)
		}
		if client.existsCalls != 0 {
			t.Fatalf("SessionExists calls = %d, want 0", client.existsCalls)
		}
	})

	t.Run("verified session resumes across agent restart", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "stored", ownerPID: 100}
		client := &mockClient{sessionID: "fresh", sessionExists: true}
		res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "telegram", "1", "", "u", 200)
		if err != nil {
			t.Fatalf("ResolveDetailed: %v", err)
		}
		if !res.Resumed || !res.HadStored || res.SessionID != "stored" {
			t.Fatalf("resolution = %+v, want resumed stored session", res)
		}
	})

	t.Run("missing session recreates with HadStored", func(t *testing.T) {
		repo := &mockSessionRepo{activeID: "stored", ownerPID: 100}
		client := &mockClient{sessionID: "fresh", sessionExists: false}
		res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "telegram", "1", "", "u", 200)
		if err != nil {
			t.Fatalf("ResolveDetailed: %v", err)
		}
		if res.Resumed || !res.HadStored || res.SessionID != "fresh" {
			t.Fatalf("resolution = %+v, want recreated session with HadStored", res)
		}
	})

	t.Run("no stored session creates fresh without HadStored", func(t *testing.T) {
		repo := &mockSessionRepo{}
		client := &mockClient{sessionID: "fresh"}
		res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "telegram", "1", "", "u", 100)
		if err != nil {
			t.Fatalf("ResolveDetailed: %v", err)
		}
		if res.Resumed || res.HadStored || res.SessionID != "fresh" {
			t.Fatalf("resolution = %+v, want fresh session without HadStored", res)
		}
	})
}

func TestResolveTakeoverAdoptsFailedSession(t *testing.T) {
	repo := &mockSessionRepo{takeover: &store.TakeoverCandidate{SessionID: "failed-sess", AgentPID: 999, Seed: "envelope-seed"}}
	client := &mockClient{sessionID: "fresh", sessionExists: true}
	res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "discord", "chan-1", "thread-1", "op-1", 200)
	if err != nil {
		t.Fatalf("ResolveDetailed: %v", err)
	}
	if !res.Resumed || !res.HadStored || res.SessionID != "failed-sess" || res.TakeoverSeed != "envelope-seed" {
		t.Fatalf("resolution = %+v, want adopted failed session with seed", res)
	}
	if client.existsCalls != 1 {
		t.Fatalf("SessionExists calls = %d, want 1", client.existsCalls)
	}
	if len(repo.setCalls) != 1 {
		t.Fatalf("expected 1 SetActive call, got %d", len(repo.setCalls))
	}
	call := repo.setCalls[0]
	if call.channelID != "chan-1" || call.threadID != "thread-1" || call.userID != "op-1" || call.sessionID != "failed-sess" || call.agentPID != 200 {
		t.Fatalf("expected adoption re-keyed to operator with current PID, got %+v", call)
	}
}

func TestResolveTakeoverAdoptsCompletedSession(t *testing.T) {
	repo := &mockSessionRepo{takeover: &store.TakeoverCandidate{SessionID: "completed-sess", AgentPID: 999, Seed: "envelope-seed"}}
	client := &mockClient{sessionID: "fresh", sessionExists: true}
	res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "discord", "chan-1", "thread-9", "op-1", 200)
	if err != nil {
		t.Fatalf("ResolveDetailed: %v", err)
	}
	if !res.Resumed || !res.HadStored || res.SessionID != "completed-sess" || res.TakeoverSeed != "envelope-seed" {
		t.Fatalf("resolution = %+v, want adopted completed session with seed", res)
	}
	if client.existsCalls != 1 {
		t.Fatalf("SessionExists calls = %d, want 1", client.existsCalls)
	}
	if len(repo.setCalls) != 1 {
		t.Fatalf("expected 1 SetActive call, got %d SetActive calls", len(repo.setCalls))
	}
	call := repo.setCalls[0]
	if call.channelID != "chan-1" || call.threadID != "thread-9" || call.userID != "op-1" || call.sessionID != "completed-sess" || call.agentPID != 200 {
		t.Fatalf("expected adoption re-keyed to operator with current PID, got %+v", call)
	}
}

func TestResolveTakeoverDeadCandidateCreatesFresh(t *testing.T) {
	repo := &mockSessionRepo{takeover: &store.TakeoverCandidate{SessionID: "gone-sess", AgentPID: 999, Seed: "envelope-seed"}}
	client := &mockClient{sessionID: "fresh", sessionExists: false}
	res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "discord", "chan-1", "thread-1", "op-1", 200)
	if err != nil {
		t.Fatalf("ResolveDetailed: %v", err)
	}
	if res.Resumed || !res.HadStored || res.SessionID != "fresh" || res.TakeoverSeed != "envelope-seed" {
		t.Fatalf("resolution = %+v, want fresh session with HadStored and seed", res)
	}
	if len(repo.setCalls) != 1 || repo.setCalls[0].sessionID != "fresh" {
		t.Fatalf("expected 1 SetActive for fresh session, got %+v", repo.setCalls)
	}
}

func TestResolveTakeoverSkippedWithoutThread(t *testing.T) {
	repo := &mockSessionRepo{takeover: &store.TakeoverCandidate{SessionID: "failed-sess", AgentPID: 999, Seed: "envelope-seed"}}
	client := &mockClient{sessionID: "fresh", sessionExists: true}
	res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "telegram", "chat1", "", "user1", 100)
	if err != nil {
		t.Fatalf("ResolveDetailed: %v", err)
	}
	if res.Resumed || res.HadStored || res.SessionID != "fresh" || res.TakeoverSeed != "" {
		t.Fatalf("resolution = %+v, want plain fresh session outside threads", res)
	}
	if client.existsCalls != 0 {
		t.Fatalf("SessionExists calls = %d, want 0", client.existsCalls)
	}
}

func TestResolveDirectKeyResumeWithoutTakeover(t *testing.T) {
	repo := &mockSessionRepo{activeID: "own-sess", ownerPID: 999}
	client := &mockClient{sessionID: "fresh", sessionExists: true}
	res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "discord", "chan-1", "thread-9", "", 200)
	if err != nil {
		t.Fatalf("ResolveDetailed: %v", err)
	}
	if !res.Resumed || !res.HadStored || res.SessionID != "own-sess" || res.TakeoverSeed != "" {
		t.Fatalf("resolution = %+v, want own session resumed without takeover", res)
	}
}

func TestResolveTakeoverSeedOnlyCandidateCreatesFresh(t *testing.T) {
	repo := &mockSessionRepo{takeover: &store.TakeoverCandidate{SessionID: "", Seed: "envelope-seed"}}
	client := &mockClient{sessionID: "fresh"}
	res, err := NewSessionResolver(repo, client).ResolveDetailed(context.Background(), "discord", "chan-1", "thread-1", "op-1", 200)
	if err != nil {
		t.Fatalf("ResolveDetailed: %v", err)
	}
	if res.Resumed || !res.HadStored || res.SessionID != "fresh" || res.TakeoverSeed != "envelope-seed" {
		t.Fatalf("resolution = %+v, want fresh session carrying the seed", res)
	}
	if client.existsCalls != 0 {
		t.Fatalf("SessionExists calls = %d, want 0 for a session-less mark", client.existsCalls)
	}
	if len(repo.setCalls) != 1 || repo.setCalls[0].sessionID != "fresh" {
		t.Fatalf("expected 1 SetActive for fresh session, got %+v", repo.setCalls)
	}
}
