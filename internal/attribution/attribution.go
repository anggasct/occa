// Package attribution correlates a schedule_task tool call observed on the
// relay event stream with the originating conversation, so the MCP handler
// can attribute a schedule without any control-plane metadata crossing into
// the agent's context.
//
// The relay observes the tool call (fingerprint + conversation) as the tool
// starts executing and appends the conversation to a per-fingerprint FIFO.
// The MCP handler — which runs inside that same tool execution — pops the
// oldest entry and stamps its own pending row with the conversation. Because
// the FIFO is consumed exactly once per entry and each handler stamps the
// row it created (by id), identical concurrent calls from different
// conversations pair one-to-one regardless of row-insert order.
package attribution

import (
	"crypto/sha256"
	"encoding/json"
	"sync"
	"time"
)

const ttl = 30 * time.Second

type entry struct {
	platform  string
	channelID string
	expires   time.Time
}

// Store holds, per schedule fingerprint, the conversations that issued a
// schedule_task call, in the order the relay observed them.
type Store struct {
	mu    sync.Mutex
	items map[string][]entry
}

func NewStore() *Store {
	return &Store{items: make(map[string][]entry)}
}

// Fingerprint returns the correlation key for a schedule request: the SHA-256
// of the canonical JSON of its identifying fields. Computed identically by
// the router (from the observed tool args) and by the MCP handler (from its
// input). No randomness, no secrets.
func Fingerprint(cronExpression, prompt, humanSchedule string) string {
	canon, _ := json.Marshal([]string{cronExpression, prompt, humanSchedule})
	sum := sha256.Sum256(canon)
	return string(sum[:])
}

// Put appends the conversation to the fingerprint's FIFO.
func (s *Store) Put(fingerprint, platform, channelID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	items := s.items[fingerprint]
	// Drop expired entries lazily, same pattern as the deleted TokenStore.
	kept := items[:0]
	for _, it := range items {
		if it.expires.After(now) {
			kept = append(kept, it)
		}
	}
	s.items[fingerprint] = append(kept, entry{platform: platform, channelID: channelID, expires: now.Add(ttl)})
}

// Pop removes and returns the oldest unconsumed conversation for the
// fingerprint, if one exists.
func (s *Store) Pop(fingerprint string) (platform, channelID string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	items := s.items[fingerprint]
	for len(items) > 0 {
		head := items[0]
		items = items[1:]
		if head.expires.After(now) {
			s.items[fingerprint] = items
			return head.platform, head.channelID, true
		}
		// Expired head: drop and try the next.
	}
	delete(s.items, fingerprint)
	return "", "", false
}
