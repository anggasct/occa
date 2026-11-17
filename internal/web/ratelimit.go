package web

import (
	"sync"
	"time"
)

const (
	actionLimitPerItem  = 10
	actionLimitPerActor = 60
	rateWindow          = time.Minute
)

type rateLimiter struct {
	mu   sync.Mutex
	hits map[string][]time.Time
	now  func() time.Time
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{hits: make(map[string][]time.Time), now: time.Now}
}

func (l *rateLimiter) check(workItemID, actor string) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-rateWindow)
	retryAfter := 0
	limited := false
	for key, limit := range map[string]int{
		"item:" + workItemID: actionLimitPerItem,
		"actor:" + actor:     actionLimitPerActor,
	} {
		l.hits[key] = pruneHits(l.hits[key], cutoff)
		if len(l.hits[key]) == 0 {
			delete(l.hits, key)
			continue
		}
		if len(l.hits[key]) >= limit {
			limited = true
			wait := int(l.hits[key][0].Add(rateWindow).Sub(now).Seconds()) + 1
			if wait > retryAfter {
				retryAfter = wait
			}
		}
	}
	if limited {
		return false, retryAfter
	}
	l.hits["item:"+workItemID] = append(l.hits["item:"+workItemID], now)
	l.hits["actor:"+actor] = append(l.hits["actor:"+actor], now)
	return true, 0
}

func pruneHits(hits []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(hits) && !hits[i].After(cutoff) {
		i++
	}
	return hits[i:]
}
