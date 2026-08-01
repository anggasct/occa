package mcpserver

import (
	"errors"
	"testing"
)

func TestGenerateFailsOnRandomSourceError(t *testing.T) {
	store := NewTokenStore()
	store.random = errReader{}

	token, err := store.Generate("telegram", "chat1")
	if err == nil {
		t.Fatalf("expected error, got token %q", token)
	}
	if token != "" {
		t.Fatalf("failed generation must not hand out a token: %q", token)
	}
	if _, _, ok := store.Lookup(token); ok {
		t.Fatal("failed generation must not store a token")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("csp rng failure") }

func TestGenerateTokensUniqueAndResolvable(t *testing.T) {
	store := NewTokenStore()

	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		token, err := store.Generate("telegram", "chat1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if seen[token] {
			t.Fatalf("duplicate token: %q", token)
		}
		seen[token] = true
	}

	platform, channelID, ok := store.Lookup("does-not-exist")
	if ok {
		t.Fatalf("unknown token resolved: %s/%s", platform, channelID)
	}

	token, _ := store.Generate("discord", "guild-42")
	platform, channelID, ok = store.Lookup(token)
	if !ok || platform != "discord" || channelID != "guild-42" {
		t.Fatalf("token resolved to %s/%s (ok=%v)", platform, channelID, ok)
	}
}
