package mcpserver

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type TokenStore struct {
	mu     sync.Mutex
	tokens map[string]tokenEntry
}

type tokenEntry struct {
	platform  string
	channelID string
	expires   time.Time
}

func NewTokenStore() *TokenStore {
	return &TokenStore{tokens: make(map[string]tokenEntry)}
}

func (t *TokenStore) Generate(platform, channelID string) string {
	b := make([]byte, 16)
	rand.Read(b)
	token := hex.EncodeToString(b)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.tokens[token] = tokenEntry{
		platform:  platform,
		channelID: channelID,
		expires:   time.Now().Add(5 * time.Minute),
	}
	return token
}

func (t *TokenStore) Lookup(token string) (platform, channelID string, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, exists := t.tokens[token]
	if !exists || time.Now().After(entry.expires) {
		if exists {
			delete(t.tokens, token)
		}
		return "", "", false
	}
	return entry.platform, entry.channelID, true
}
