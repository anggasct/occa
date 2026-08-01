package mcpserver

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"
)

type TokenStore struct {
	mu     sync.Mutex
	tokens map[string]tokenEntry
	random io.Reader
}

type tokenEntry struct {
	platform  string
	channelID string
	expires   time.Time
}

func NewTokenStore() *TokenStore {
	return &TokenStore{tokens: make(map[string]tokenEntry), random: rand.Reader}
}

func (t *TokenStore) Generate(platform, channelID string) (string, error) {
	b := make([]byte, 16)
	if _, err := io.ReadFull(t.random, b); err != nil {
		return "", fmt.Errorf("mcpserver: token randomness: %w", err)
	}
	token := hex.EncodeToString(b)

	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, v := range t.tokens {
		if now.After(v.expires) {
			delete(t.tokens, k)
		}
	}
	t.tokens[token] = tokenEntry{
		platform:  platform,
		channelID: channelID,
		expires:   now.Add(5 * time.Minute),
	}
	return token, nil
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
