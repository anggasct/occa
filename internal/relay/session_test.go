package relay

import (
	"context"
	"testing"

	"github.com/anggasct/occa/internal/store"
)

type mockSessionRepo struct {
	activeID string
	setCalls []setActiveCall
}

type setActiveCall struct {
	platform, channelID, sessionID string
}

func (m *mockSessionRepo) Active(_ context.Context, platform, channelID string) (string, error) {
	return m.activeID, nil
}

func (m *mockSessionRepo) SetActive(_ context.Context, platform, channelID, sessionID string) error {
	m.setCalls = append(m.setCalls, setActiveCall{platform, channelID, sessionID})
	m.activeID = sessionID
	return nil
}

func (m *mockSessionRepo) Deactivate(_ context.Context, platform, channelID string) error {
	m.activeID = ""
	return nil
}

func (m *mockSessionRepo) List(_ context.Context, platform, channelID string) ([]store.Session, error) {
	return nil, nil
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
func (m *mockClient) Events(_ context.Context, _ string) (<-chan Event, error) {
	return nil, nil
}

func TestResolveExisting(t *testing.T) {
	repo := &mockSessionRepo{activeID: "existing"}
	client := &mockClient{sessionID: "new-session"}
	resolver := NewSessionResolver(repo, client)

	id, err := resolver.Resolve(context.Background(), "telegram", "123")
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

	id, err := resolver.Resolve(context.Background(), "telegram", "456")
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
	if call.platform != "telegram" || call.channelID != "456" || call.sessionID != "new-session" {
		t.Fatalf("unexpected SetActive call: %+v", call)
	}
}
