package relay

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

type mockSessionRepo struct {
	sessions []*store.Session
	created  *store.Session
}

func (m *mockSessionRepo) FindActive(_ context.Context, platform, channelID string) (*store.Session, error) {
	for _, s := range m.sessions {
		if s.Platform == platform && s.ChannelID == channelID && s.Active {
			return s, nil
		}
	}
	return nil, nil
}

func (m *mockSessionRepo) Create(_ context.Context, s *store.Session) error {
	m.created = s
	m.sessions = append(m.sessions, s)
	return nil
}

func (m *mockSessionRepo) Deactivate(_ context.Context, platform, channelID string) error {
	for _, s := range m.sessions {
		if s.Platform == platform && s.ChannelID == channelID {
			s.Active = false
		}
	}
	return nil
}

type mockClient struct {
	sessionID string
}

func (m *mockClient) CreateSession(_ context.Context) (string, error) {
	return m.sessionID, nil
}

func (m *mockClient) SendMessage(_ context.Context, _, _ string) error { return nil }
func (m *mockClient) RunCommand(_ context.Context, _, _ string) error  { return nil }
func (m *mockClient) Events(_ context.Context, _ string) (<-chan Event, error) {
	return nil, nil
}

func TestResolveExisting(t *testing.T) {
	repo := &mockSessionRepo{
		sessions: []*store.Session{
			{Platform: "telegram", ChannelID: "123", OpenCodeSessionID: "existing", Active: true},
		},
	}
	client := &mockClient{sessionID: "new-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "123")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != "existing" {
		t.Fatalf("got %q, want %q", id, "existing")
	}
	if repo.created != nil {
		t.Fatal("should not create when session exists")
	}
}

func TestResolveCreatesNew(t *testing.T) {
	repo := &mockSessionRepo{}
	client := &mockClient{sessionID: "new-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "456")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != "new-session" {
		t.Fatalf("got %q, want %q", id, "new-session")
	}
	if repo.created == nil {
		t.Fatal("expected session to be created")
	}
	if repo.created.Platform != "telegram" || repo.created.ChannelID != "456" {
		t.Fatalf("unexpected created session: %+v", repo.created)
	}
	if !repo.created.Active {
		t.Fatal("new session should be active")
	}
}
