package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type SessionLiveness func(ctx context.Context, sessionID, workdir string) (bool, error)

type PRStateResolver func(ctx context.Context, repo, number string) (string, error)

const (
	PRStateOpen   = "open"
	PRStateMerged = "merged"
	PRStateClosed = "closed"
)

const prStateCacheMaxEntries = 256

type prStateCacheEntry struct {
	state string
	err   error
	at    time.Time
}
type activeRun struct {
	mu         sync.Mutex
	key        string
	prKey      string
	endpoint   string
	deliveryID string
	sessionID  string
	workdir    string
	private    bool
	aborted    bool
	abortOK    bool
}

func (r *activeRun) setSession(sessionID string) {
	r.mu.Lock()
	r.sessionID = strings.TrimSpace(sessionID)
	r.mu.Unlock()
}

func (r *activeRun) identity() (string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessionID, r.workdir, r.private
}

func (r *activeRun) markAborted(ok bool) {
	r.mu.Lock()
	r.aborted = true
	r.abortOK = ok
	r.mu.Unlock()
}

func (r *activeRun) abortedState() (bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.aborted, r.abortOK
}

func activeRunKey(prKey, endpoint, deliveryID string) string {
	if key := strings.TrimSpace(prKey); key != "" {
		return "pr:" + key
	}
	return "ep:" + strings.TrimSpace(endpoint) + "/" + strings.TrimSpace(deliveryID)
}

func (s *Server) beginActiveRun(prKey, endpoint, deliveryID, workdir string, privateWorkspace bool) *activeRun {
	run := &activeRun{
		key:        activeRunKey(prKey, endpoint, deliveryID),
		prKey:      strings.TrimSpace(prKey),
		endpoint:   strings.TrimSpace(endpoint),
		deliveryID: strings.TrimSpace(deliveryID),
		workdir:    strings.TrimSpace(workdir),
		private:    privateWorkspace,
	}
	s.activeRunsMu.Lock()
	if s.activeRuns == nil {
		s.activeRuns = make(map[string]*activeRun)
	}
	s.activeRuns[run.key] = run
	s.activeRunsMu.Unlock()
	return run
}

func (s *Server) activeRunFor(prKey, endpoint, deliveryID string) *activeRun {
	s.activeRunsMu.Lock()
	defer s.activeRunsMu.Unlock()
	return s.activeRuns[activeRunKey(prKey, endpoint, deliveryID)]
}

func (s *Server) endActiveRun(run *activeRun) {
	if run == nil {
		return
	}
	s.activeRunsMu.Lock()
	if current, ok := s.activeRuns[run.key]; ok && current == run {
		delete(s.activeRuns, run.key)
	}
	s.activeRunsMu.Unlock()
}

func (s *Server) SetSessionLiveness(fn SessionLiveness) {
	s.sessionLiveness = fn
}

func (s *Server) SetPRStateResolver(fn PRStateResolver) {
	s.prStateResolver = fn
}

// sessionAlive reports whether the agent behind the run is still working. A
// live agent that is silent is slow, not stuck: declaring it stuck would kill
// a healthy run and release its key to a second writer.
func (s *Server) sessionAlive(prKey, endpoint, deliveryID string) bool {
	if s.sessionLiveness == nil {
		return false
	}
	run := s.activeRunFor(prKey, endpoint, deliveryID)
	if run == nil {
		return false
	}
	sessionID, workdir, private := run.identity()
	if sessionID == "" || workdir == "" || !private {
		return false
	}
	alive, err := s.sessionLiveness(context.Background(), sessionID, workdir)
	if err != nil {
		return false
	}
	return alive
}

// livePRState resolves the live state of a PR. The answer is reused only for
// the horizon the caller passes: a retry decision reuses the reading until
// the next attempt would fire, so the attempt itself always reads a fresh
// state while a burst of decisions on one PR costs a single lookup.
func (s *Server) livePRState(ctx context.Context, prKey string, horizon time.Duration) (string, error) {
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return "", fmt.Errorf("webhook: live PR state: malformed pr key %q", prKey)
	}
	s.prStateMu.Lock()
	entry, cached := s.prStateCache[prKey]
	s.prStateMu.Unlock()
	if cached && horizon > 0 && time.Since(entry.at) < horizon {
		return entry.state, entry.err
	}
	resolver := s.prStateResolver
	if resolver == nil {
		resolver = GHPRStateResolver
	}
	state, err := resolver(ctx, repo, number)
	s.prStateMu.Lock()
	if s.prStateCache == nil {
		s.prStateCache = make(map[string]prStateCacheEntry)
	}
	s.prStateCache[prKey] = prStateCacheEntry{state: state, err: err, at: time.Now()}
	s.prunePRStateCache()
	s.prStateMu.Unlock()
	return state, err
}

func (s *Server) prunePRStateCache() {
	if len(s.prStateCache) <= prStateCacheMaxEntries {
		return
	}
	oldestKey := ""
	var oldest time.Time
	for key, entry := range s.prStateCache {
		if oldestKey == "" || entry.at.Before(oldest) {
			oldestKey = key
			oldest = entry.at
		}
	}
	if oldestKey != "" {
		delete(s.prStateCache, oldestKey)
	}
}

func GHPRStateResolver(ctx context.Context, repo, number string) (string, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(lookupCtx, "gh", "pr", "view", strings.TrimSpace(number), "--repo", strings.TrimSpace(repo), "--json", "state,mergedAt")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return "", fmt.Errorf("webhook: live PR state %s#%s: %s", repo, number, detail)
	}
	state, err := parsePRState(stdout.Bytes())
	if err != nil {
		return "", fmt.Errorf("webhook: live PR state %s#%s: %w", repo, number, err)
	}
	return state, nil
}

func parsePRState(out []byte) (string, error) {
	var raw struct {
		State    string `json:"state"`
		MergedAt string `json:"mergedAt"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if strings.TrimSpace(raw.MergedAt) != "" {
		return PRStateMerged, nil
	}
	switch strings.ToUpper(strings.TrimSpace(raw.State)) {
	case strings.ToUpper(PRStateMerged):
		return PRStateMerged, nil
	case strings.ToUpper(PRStateClosed):
		return PRStateClosed, nil
	case strings.ToUpper(PRStateOpen):
		return PRStateOpen, nil
	}
	return "", fmt.Errorf("unknown state %q", raw.State)
}
