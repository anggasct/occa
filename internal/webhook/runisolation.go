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

const prStateLookupTimeout = 20 * time.Second

type activeRun struct {
	mu         sync.Mutex
	key        string
	prKey      string
	endpoint   string
	deliveryID string
	sessionID  string
	workdir    string
	aborted    bool
	abortOK    bool
	takenOver  bool
}

func (r *activeRun) setSession(sessionID string) {
	r.mu.Lock()
	r.sessionID = strings.TrimSpace(sessionID)
	r.mu.Unlock()
}

func (r *activeRun) identity() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessionID, r.workdir
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

func (r *activeRun) markTakenOver() {
	r.mu.Lock()
	r.takenOver = true
	r.mu.Unlock()
}

func (r *activeRun) watchdogTookOver() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.takenOver
}

func activeRunKey(prKey, endpoint, deliveryID string) string {
	if key := strings.TrimSpace(prKey); key != "" {
		return "pr:" + key
	}
	return "ep:" + strings.TrimSpace(endpoint) + "/" + strings.TrimSpace(deliveryID)
}

func (s *Server) beginActiveRun(prKey, endpoint, deliveryID, workdir string) *activeRun {
	run := &activeRun{
		key:        activeRunKey(prKey, endpoint, deliveryID),
		prKey:      strings.TrimSpace(prKey),
		endpoint:   strings.TrimSpace(endpoint),
		deliveryID: strings.TrimSpace(deliveryID),
		workdir:    strings.TrimSpace(workdir),
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

// sessionLivenessState is the verdict of a liveness probe on the agent
// session behind a run.
type sessionLivenessState int

const (
	// livenessUnknown means liveness could not be established at all.
	livenessUnknown sessionLivenessState = iota
	// livenessDead means the probe ran and the agent session is gone.
	livenessDead
	// livenessAlive means the probe ran and the agent session is still working.
	livenessAlive
)

// probeSessionLiveness reports whether the agent behind the run is still
// working. A live agent that is silent is slow, not stuck: declaring it stuck
// would kill a healthy run and release its key to a second writer. The probe
// runs for every run that carries a session and a workdir, so the rule holds
// independently of the workspace mode. An unknown verdict must never be
// treated as a dead session.
func (s *Server) probeSessionLiveness(prKey, endpoint, deliveryID string) sessionLivenessState {
	if s.sessionLiveness == nil {
		return livenessUnknown
	}
	run := s.activeRunFor(prKey, endpoint, deliveryID)
	if run == nil {
		return livenessUnknown
	}
	sessionID, workdir := run.identity()
	if sessionID == "" || workdir == "" {
		return livenessUnknown
	}
	alive, err := s.sessionLiveness(context.Background(), sessionID, workdir)
	if err != nil {
		return livenessUnknown
	}
	if alive {
		return livenessAlive
	}
	return livenessDead
}

// prStateDecision is one live PR-state decision. The reading is memoized for
// the decision that asked for it, so a later decision resolves the state again
// instead of inheriting an earlier reading.
type prStateDecision struct {
	srv      *Server
	prKey    string
	state    string
	err      error
	resolved bool
}

func (s *Server) newPRStateDecision(prKey string) *prStateDecision {
	return &prStateDecision{srv: s, prKey: prKey}
}

func (d *prStateDecision) resolve(ctx context.Context) (string, error) {
	if d.resolved {
		return d.state, d.err
	}
	d.state, d.err = d.srv.resolveLivePRState(ctx, d.prKey)
	d.resolved = true
	return d.state, d.err
}

func (s *Server) resolveLivePRState(ctx context.Context, prKey string) (string, error) {
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return "", fmt.Errorf("webhook: live PR state: malformed pr key %q", prKey)
	}
	resolver := s.prStateResolver
	if resolver == nil {
		resolver = GHPRStateResolver
	}
	return resolver(ctx, repo, number)
}

func GHPRStateResolver(ctx context.Context, repo, number string) (string, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, prStateLookupTimeout)
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
