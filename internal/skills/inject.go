package skills

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
)

// ChangedNotice is the one line an operator sees when a live session's
// skills no longer match the directories. The running session is left as is.
const ChangedNotice = "skills changed — /reset to apply"

// Record is one skill injected into one agent session.
type Record struct {
	Name    string
	Scope   string
	Trigger string
	Digest  string
	Runes   int
}

// Ledger persists which skills a session received. Its rows are the
// once-per-session guard: a session with rows never gets another block.
type Ledger interface {
	SessionSkills(ctx context.Context, sessionID string) ([]Record, error)
	RecordSessionSkills(ctx context.Context, sessionID string, records []Record) error
}

// Plan is what to do with one prompt.
type Plan struct {
	// Block goes at the top of the prompt; "" leaves the prompt unchanged.
	Block string
	// Notice is a one-line operator message; "" sends nothing.
	Notice  string
	records []Record
	session string
}

// Prefix returns the prompt to send: the block, a blank line, the prompt.
// Without a block the prompt is returned byte for byte.
func (p Plan) Prefix(prompt string) string {
	if p.Block == "" {
		return prompt
	}
	return p.Block + "\n\n" + prompt
}

// Injector resolves skills at run start and injects them once per session.
// A nil *Injector is valid and never changes a prompt.
type Injector struct {
	enabled bool
	options func(workdir string) Options
	ledger  Ledger

	mu      sync.Mutex
	started map[string]bool
	noticed map[string]string
	// warnedGlobal keeps the unreadable-global WARN to once per process.
	warnedGlobal bool
}

// NewInjector builds an injector. options maps a run's workdir to resolver
// options; enabled=false short-circuits before any filesystem read.
func NewInjector(enabled bool, options func(workdir string) Options, ledger Ledger) *Injector {
	return &Injector{
		enabled: enabled,
		options: options,
		ledger:  ledger,
		started: map[string]bool{},
		noticed: map[string]string{},
	}
}

// Enabled reports whether resolution runs at all.
func (in *Injector) Enabled() bool { return in != nil && in.enabled }

// Options returns the resolver options for a workdir.
func (in *Injector) Options(workdir string) Options {
	if in == nil || in.options == nil {
		return DefaultOptions(workdir)
	}
	return in.options(workdir)
}

// ForNewSession plans the first prompt of a session the caller is about to
// create (a webhook lane run). Commit records it once the session exists.
func (in *Injector) ForNewSession(workdir string, input Input) Plan {
	if !in.Enabled() || strings.TrimSpace(workdir) == "" {
		return Plan{}
	}
	set := in.load(workdir)
	if set.empty() {
		return Plan{}
	}
	return in.plan(workdir, set, input)
}

// ForSession plans a prompt into an existing session. isFirst reports whether
// the session has had no prompt yet; it is asked only when the session has no
// recorded skills, so a session that already received its block costs one
// ledger read and nothing else.
func (in *Injector) ForSession(ctx context.Context, sessionID, workdir string, input Input, isFirst func() bool) Plan {
	if !in.Enabled() || strings.TrimSpace(workdir) == "" || strings.TrimSpace(sessionID) == "" {
		return Plan{}
	}
	if in.isStarted(sessionID) && !in.hasRecords(sessionID) {
		return Plan{}
	}
	var recorded []Record
	if in.ledger != nil {
		rows, err := in.ledger.SessionSkills(ctx, sessionID)
		if err != nil {
			slog.Warn("skills: session record lookup failed; injecting nothing", "session_id", sessionID, "error", err)
			return Plan{}
		}
		recorded = rows
	}
	set := in.load(workdir)
	if len(recorded) > 0 {
		in.markStarted(sessionID, true)
		return in.changeNotice(sessionID, set, recorded)
	}
	if in.isStarted(sessionID) {
		return Plan{}
	}
	if set.empty() {
		return Plan{}
	}
	if isFirst != nil && !isFirst() {
		in.markStarted(sessionID, false)
		return Plan{}
	}
	p := in.plan(workdir, set, input)
	p.session = sessionID
	return p
}

// Commit records a sent plan against its session. Call it only after the
// prompt was accepted by the backend; a failed send leaves the session
// unmarked so the next first prompt still carries the block.
func (in *Injector) Commit(ctx context.Context, sessionID string, p Plan) {
	if in == nil || strings.TrimSpace(sessionID) == "" {
		return
	}
	if p.session != "" {
		// Only sessions that take more prompts are remembered; a webhook
		// run's session is guarded by its rows alone.
		in.markStarted(sessionID, len(p.records) > 0)
	}
	if len(p.records) == 0 || in.ledger == nil {
		return
	}
	if err := in.ledger.RecordSessionSkills(ctx, sessionID, p.records); err != nil {
		slog.Error("skills: session record failed", "session_id", sessionID, "error", err)
	}
}

func (in *Injector) load(workdir string) Set {
	return Load(in.Options(workdir))
}

func (in *Injector) plan(workdir string, set Set, input Input) Plan {
	// Skip and shadow lines are logged when a run actually resolves — also
	// when every skill was skipped — not on every later message of a session
	// that is only checked for changes.
	for _, e := range set.Errors {
		slog.Error("skill skipped", "path", e.Path, "line", e.Line, "reason", e.Reason)
	}
	for _, sk := range set.Shadowed {
		slog.Info("skill shadowed", "name", sk.Name, "by", ScopeRepo)
	}
	if set.GlobalUnreadable != nil && in.warnGlobalOnce() {
		slog.Warn("skills: global dir unreadable", "error", set.GlobalUnreadable)
	}
	opts := in.Options(workdir)
	sel := Select(set, input, opts.ContextMaxRunes)
	slog.Info("skills resolved", "workdir", workdir, "applied", len(sel.Applied), "dropped", len(sel.Dropped), "runes", sel.Runes)
	records := make([]Record, 0, len(sel.Applied))
	for _, a := range sel.Applied {
		records = append(records, Record{Name: a.Skill.Name, Scope: a.Skill.Scope, Trigger: a.Skill.Trigger, Digest: a.Skill.Digest, Runes: a.Skill.Runes})
	}
	return Plan{Block: Render(sel.Applied), records: records}
}

// changeNotice compares a live session's record with the directories: a
// recorded skill that changed or vanished, or an always-on skill the session
// never got, is a change. Keyword and paths skills a later message would
// trigger are not: the session's set was fixed by its first prompt.
func (in *Injector) changeNotice(sessionID string, set Set, recorded []Record) Plan {
	current := map[string]Skill{}
	for _, sk := range set.Skills {
		current[sk.Name] = sk
	}
	have := map[string]bool{}
	var diffs []string
	for _, r := range recorded {
		have[r.Name] = true
		sk, ok := current[r.Name]
		switch {
		case !ok:
			diffs = append(diffs, "-"+r.Name)
		case sk.Digest != r.Digest || sk.Scope != r.Scope:
			diffs = append(diffs, "~"+r.Name+"@"+sk.Digest)
		}
	}
	for _, sk := range set.Skills {
		if sk.Trigger == TriggerAlways && !have[sk.Name] {
			diffs = append(diffs, "+"+sk.Name+"@"+sk.Digest)
		}
	}
	if len(diffs) == 0 {
		return Plan{}
	}
	sort.Strings(diffs)
	fingerprint := strings.Join(diffs, ",")
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.noticed[sessionID] == fingerprint {
		return Plan{}
	}
	in.noticed[sessionID] = fingerprint
	slog.Info("skills changed in a live session; not injecting", "session_id", sessionID, "changes", fingerprint)
	return Plan{Notice: ChangedNotice}
}

// markStarted notes that a session has had its first prompt; recorded
// notes that it holds skill rows. Presence in the map is "started".
func (in *Injector) markStarted(sessionID string, recorded bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.started[sessionID] = in.started[sessionID] || recorded
}

func (in *Injector) isStarted(sessionID string) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	_, ok := in.started[sessionID]
	return ok
}

func (in *Injector) hasRecords(sessionID string) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.started[sessionID]
}

func (in *Injector) warnGlobalOnce() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.warnedGlobal {
		return false
	}
	in.warnedGlobal = true
	return true
}

// empty reports a load with nothing to inject and nothing to report: no
// valid skill, no skipped file, no unreadable global dir. A load with only
// skipped files still resolves, so its ERROR lines are logged.
func (s Set) empty() bool {
	return len(s.Skills) == 0 && len(s.Errors) == 0 && s.GlobalUnreadable == nil
}
