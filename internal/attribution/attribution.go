package attribution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"
)

type entry struct {
	platform  string
	channelID string
	expires   time.Time
}

type Store struct {
	mu    sync.Mutex
	items map[string]entry
	ttl   time.Duration
}

func NewStore() *Store {
	return &Store{
		items: make(map[string]entry),
		ttl:   30 * time.Second,
	}
}

func (s *Store) Put(fingerprint, platform, channelID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[fingerprint] = entry{
		platform:  platform,
		channelID: channelID,
		expires:   time.Now().Add(s.ttl),
	}
}

func (s *Store) Get(fingerprint string) (platform, channelID string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, found := s.items[fingerprint]
	if !found {
		return "", "", false
	}
	if time.Now().After(item.expires) {
		delete(s.items, fingerprint)
		return "", "", false
	}
	return item.platform, item.channelID, true
}

type payload struct {
	CronExpression string `json:"cron_expression"`
	Prompt         string `json:"prompt"`
	HumanSchedule  string `json:"human_schedule"`
}

func Fingerprint(cronExpression, prompt, humanSchedule string) string {
	data, _ := json.Marshal(payload{
		CronExpression: cronExpression,
		Prompt:         prompt,
		HumanSchedule:  humanSchedule,
	})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
