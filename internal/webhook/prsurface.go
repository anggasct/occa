package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const threadStatusPrefix = "📌 Status — "

const escalationActionLine = "Action: /retry · /skip · /cancel"

const blockedQueueCommand = "run /retry, /skip or /cancel"

type ThreadStatusUpdate struct {
	Phase   Phase
	Waiting string
	Next    string
	Action  string
	Queued  string
}

func FormatThreadStatusLine(phase Phase, queued, waiting, next, action string, icons map[string]string) string {
	parts := []string{PhaseIcon(phase, icons) + " " + string(phase)}
	if value := strings.TrimSpace(queued); value != "" {
		parts = append(parts, "queued: "+value)
	}
	if value := strings.TrimSpace(waiting); value != "" {
		parts = append(parts, "waiting: "+value)
	}
	if value := strings.TrimSpace(next); value != "" {
		parts = append(parts, "next: "+value)
	}
	if value := strings.TrimSpace(action); value != "" {
		parts = append(parts, "ACTION: "+value)
	}
	return threadStatusPrefix + strings.Join(parts, " · ")
}

func FormatQueueDepth(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%s (%d)", kind, counts[kind]))
	}
	return strings.Join(parts, ", ")
}

func FormatEscalationCopy(repo, prNumber string, phase Phase, what, why string, icons map[string]string) string {
	head := strings.TrimSpace(repo)
	if number := strings.TrimSpace(prNumber); number != "" {
		if head != "" {
			head += "#"
		}
		head += number
	}
	lines := []string{PhaseIcon(phase, icons) + " " + head + " — " + string(phase) + " needs you"}
	detail := strings.TrimSpace(what)
	if reason := strings.TrimSpace(why); reason != "" {
		if detail != "" {
			detail += " — " + reason
		} else {
			detail = reason
		}
	}
	if detail != "" {
		lines = append(lines, detail)
	}
	lines = append(lines, escalationActionLine)
	return strings.Join(lines, "\n")
}

// blockedQueueText renders the blocked-queue text of a PR: the PR key, the
// block reason stored on the queue row and the operator ask.
func blockedQueueText(prKey, reason, ask string) string {
	head := "queue blocked"
	if key := strings.TrimSpace(prKey); key != "" {
		head = key + " — " + head
	}
	if trimmed := strings.TrimSpace(reason); trimmed != "" {
		head += ": " + trimmed
	}
	return head + " — " + ask
}

// blockedQueueSkipReason renders the reason a delivery of a blocked PR queue
// is skipped with: the PR key, the block reason and the kind /retry re-runs.
func blockedQueueSkipReason(prKey, reason, kind string) string {
	ask := "/retry re-runs the failed run"
	if trimmed := strings.TrimSpace(kind); trimmed != "" {
		ask = "/retry re-runs " + trimmed
	}
	return blockedQueueText(prKey, reason, ask)
}

func FormatPRThreadOpener(envelope WebhookEnvelope) string {
	number := strings.TrimSpace(stringValue(envelope["pr_number"]))
	title := auditField(stringValue(envelope["title"]))
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	if number == "" && title == "" && branch == "" {
		return ""
	}
	head := "PR"
	if number != "" {
		head += " #" + number
	}
	if title != "" {
		head += " — " + clipRunes(title, 120)
	}
	if branch != "" {
		head += " · " + branch
	}
	return head
}

func FormatRunOutcome(envelope WebhookEnvelope, workflow, status, reason, next string) string {
	marker, kind := outcomeMarker(workflow)
	upper := strings.ToUpper(strings.TrimSpace(status))
	head := marker + " " + kind
	switch upper {
	case "SKIP":
		head += " skipped"
	case "FAILED":
		head += " failed"
	case "COMPLETED":
		head += " done"
	}
	if upper == "SKIP" || upper == "FAILED" {
		if detail := strings.TrimSpace(reason); detail != "" {
			head += " — " + clipRunes(detail, 160)
		}
	}
	parts := []string{head}
	if verdict := outcomeVerdict(envelope); verdict != "" {
		parts = append(parts, verdict)
	}
	if step := strings.TrimSpace(next); step != "" {
		parts = append(parts, "next: "+step)
	}
	return strings.Join(parts, " · ")
}

func outcomeVerdict(envelope WebhookEnvelope) string {
	verdict := strings.ToUpper(strings.TrimSpace(stringValue(envelope["gate_verdict"])))
	if verdict == "" {
		verdict = strings.ToUpper(strings.TrimSpace(stringValue(envelope["review_verdict"])))
	}
	if verdict == "" {
		return ""
	}
	return "verdict: " + verdict
}

// gateVerdict returns the review verdict the merge gate holds for a PR. The
// gate is the one place the verdict is computed, so every lane that renders a
// surface shows the same one.
func (s *Server) gateVerdict(prKey string) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	return s.mergeGateVerdict(prKey)
}

// withGateVerdict returns the envelope with the verdict the merge gate holds
// added to it. A lane that renders a surface reads the verdict from there, so
// the run outcome, the thread card and the merge button cannot disagree.
func withGateVerdict(envelope WebhookEnvelope, verdict string) WebhookEnvelope {
	verdict = gateVerdictText(verdict)
	if verdict == "" {
		return envelope
	}
	out := make(WebhookEnvelope, len(envelope)+1)
	for key, value := range envelope {
		out[key] = value
	}
	out["gate_verdict"] = verdict
	return out
}

// gateVerdictText renders the gate verdict the way a review verdict is
// rendered, so a lane that fills the verdict in shows the same token as a
// lane that carries one.
func gateVerdictText(verdict string) string {
	switch strings.ToLower(strings.TrimSpace(verdict)) {
	case "changes_requested", "request_changes", "requested_changes":
		return "REQUEST_CHANGES"
	case "approved":
		return "APPROVED"
	}
	return strings.TrimSpace(verdict)
}

// gateEnvelope returns the envelope with the verdict the merge gate holds for
// its PR added to it.
func (s *Server) gateEnvelope(ep config.EndpointConfig, envelope WebhookEnvelope) WebhookEnvelope {
	if s == nil || ep.Automation == nil {
		return envelope
	}
	prKey := s.prKeyForEnvelope(ep, envelope)
	if strings.TrimSpace(prKey) == "" {
		return envelope
	}
	return withGateVerdict(envelope, s.gateVerdict(prKey))
}

func outcomeMarker(workflow string) (string, string) {
	switch strings.TrimSpace(strings.ToLower(workflow)) {
	case "review", "reviewer":
		return "🔍", "REVIEW"
	case "fix", "pr-fix":
		return "🔨", "FIX"
	case "merge":
		return "🔀", "MERGE"
	case "merged":
		return "✅", "DONE"
	default:
		return "📨", "WEBHOOK"
	}
}

func (s *Server) queueKind(endpoint string) string {
	name := strings.TrimSpace(endpoint)
	if name == "" {
		return "run"
	}
	if ep, ok := s.endpointsByName[name]; ok {
		switch kind := strings.TrimSpace(strings.ToLower(ep.Workflow)); kind {
		case "review", "fix", "merge", "merged", "dispatch":
			return kind
		}
	}
	return "run"
}

func (s *Server) queueSnapshot(ctx context.Context, prKey string) string {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return ""
	}
	items, err := s.prQueues.ListItems(ctx, prKey)
	if err != nil || len(items) == 0 {
		return ""
	}
	counts := make(map[string]int, len(items))
	for _, item := range items {
		counts[s.queueKind(item.Endpoint)]++
	}
	return FormatQueueDepth(counts)
}

func (s *Server) mergeGateAllows(ctx context.Context, prKey string) bool {
	if strings.TrimSpace(prKey) == "" {
		return true
	}
	if s.mergeGate != nil {
		return s.mergeGate(ctx, prKey)
	}
	if !s.defaultMergeGate(ctx, prKey) {
		return false
	}
	return s.tasks.GateAllows(prKey)
}

func (s *Server) defaultMergeGate(ctx context.Context, prKey string) bool {
	if s.prQueues == nil {
		return true
	}
	return mergeGateAllows(ctx, s.prQueues, s.queueKind, prKey)
}

func mergeGateAllows(ctx context.Context, queues store.PRQueueRepo, kindOf func(string) string, prKey string) bool {
	row, err := queues.Get(ctx, prKey)
	if err != nil || row == nil {
		return true
	}
	if row.Status == store.PRStatusBlocked {
		return false
	}
	if kind := strings.TrimSpace(strings.ToLower(row.ActiveKind)); kind != "" && kind != "merge" {
		return false
	}
	items, err := queues.ListItems(ctx, prKey)
	if err != nil {
		return true
	}
	for _, item := range items {
		if kindOf(item.Endpoint) != "merge" {
			return false
		}
	}
	return true
}

// MergeGateEnabled reports whether the merge gate would pass for a PR right
// now. The merge button is disabled while another item waits or runs.
func MergeGateEnabled(queues store.PRQueueRepo, endpoints []config.EndpointConfig, prKey string) bool {
	if queues == nil || strings.TrimSpace(prKey) == "" {
		return true
	}
	workflows := make(map[string]string, len(endpoints))
	for _, ep := range endpoints {
		workflows[strings.TrimSpace(ep.Name)] = strings.TrimSpace(strings.ToLower(ep.Workflow))
	}
	kindOf := func(endpoint string) string {
		switch kind := workflows[strings.TrimSpace(endpoint)]; kind {
		case "review", "fix", "merge", "merged", "dispatch":
			return kind
		}
		return "run"
	}
	return mergeGateAllows(context.Background(), queues, kindOf, prKey)
}

// MergeGateAllows reports whether the merge gate would pass for a PR right
// now: no other queued or active item waits for it, the last observed review
// approves it and the last observed checks are green.
func MergeGateAllows(queues store.PRQueueRepo, endpoints []config.EndpointConfig, registry *TaskThreadRegistry, prKey string) bool {
	if !MergeGateEnabled(queues, endpoints, prKey) {
		return false
	}
	if registry == nil {
		return true
	}
	return registry.GateAllows(prKey)
}

func (s *Server) phaseParentButtons(ep config.EndpointConfig, prKey, repo, branch, prURL, threadID string) []channel.Button {
	mergeEnabled := s.mergeGateAllows(context.Background(), prKey)
	return PhaseParentButtons(ep.Platform, s.guildIDFor(ep.Platform, ep.ChannelID, threadID), prURL, threadID, repo, branch, mergeEnabled)
}

func PhaseParentButtons(platform, guildID, prURL, threadID, repo, branch string, mergeEnabled bool) []channel.Button {
	buttons := TaskLinkButtons(platform, guildID, prURL, threadID)
	buttons = append(buttons, TaskActionButtonsWithMerge(repo, branch, mergeEnabled)...)
	return buttons
}

func (s *Server) prSurfaceState(prKey string) (string, Phase, string, bool) {
	if s == nil || s.tasks == nil {
		return "", "", "", false
	}
	return s.tasks.SurfaceState(prKey)
}

func (s *Server) syncPhaseParent(ctx context.Context, ep config.EndpointConfig, prKey string, envelope WebhookEnvelope, phase Phase) (string, bool) {
	if s == nil || strings.TrimSpace(prKey) == "" {
		return "", false
	}
	rootID, previous, _, known := s.prSurfaceState(prKey)
	persistedThreadID := ""
	if strings.TrimSpace(rootID) == "" {
		// The task that opened this PR already has a channel parent: the
		// dispatch parent. Reusing it keeps one parent per task in the
		// channel instead of a second one for the same work.
		rootID = s.reusableTaskParent(ep, prKey, envelope)
	}
	if strings.TrimSpace(rootID) == "" {
		// The registry is in memory only. After a restart the queue row is
		// the only surviving home, so reuse it instead of posting a card the
		// PR already owns.
		rootID, persistedThreadID = s.reusablePersistedParent(ctx, ep, prKey, envelope)
	}
	if known {
		if previous == phase {
			return rootID, false
		}
		// A blocked or failed parent stays frozen until a retry is admitted.
		if IsTerminalPhase(previous) && !surfaceReopenedForRetry(s.tasks, prKey, previous) {
			slog.Info("webhook: write refused, PR terminal",
				"pr_key", prKey,
				"surface", "parent",
				"run", string(phase),
				"phase", string(previous),
			)
			return rootID, false
		}
	}
	repo, prNumber := repoAndNumber(prKey, envelope)
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	prURL := strings.TrimSpace(stringValue(envelope["pr_url"]))
	threadID := ""
	if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
		threadID = strings.TrimSpace(rec.ThreadID)
		if branch == "" {
			branch = strings.TrimSpace(rec.Branch)
		}
	}
	if detail, ok := s.tasks.SurfaceDetail(prKey); ok {
		if prURL == "" {
			prURL = detail.PRURL
		}
		if branch == "" {
			branch = detail.Branch
		}
		if threadID == "" {
			threadID = detail.ThreadID
		}
	}
	if threadID == "" {
		threadID = persistedThreadID
	}
	text := FormatPhaseParent(repo, prNumber, phase, s.statusIcons)
	buttons := s.phaseParentButtons(ep, prKey, repo, branch, prURL, threadID)
	if rootID == "" {
		id, err := s.sendPhaseParent(ctx, ep.Platform, ep.ChannelID, text, buttons)
		if err != nil {
			slog.Warn("webhook: phase parent send failed", "pr_key", prKey, "phase", string(phase), "error", err)
			return "", false
		}
		rootID = strings.TrimSpace(id)
	} else if err := s.editPhaseParent(ctx, ep.Platform, ep.ChannelID, rootID, text, buttons); err != nil {
		slog.Warn("webhook: phase parent edit failed", "pr_key", prKey, "phase", string(phase), "error", err)
		return rootID, false
	}
	s.tasks.SetSurface(prKey, SurfaceUpdate{
		RootMessageID: rootID,
		Repo:          repo,
		PRNumber:      prNumber,
		PRURL:         prURL,
		Branch:        branch,
		ThreadID:      threadID,
		Phase:         string(phase),
	})
	s.tasks.ClearReopen(prKey)
	slog.Info("webhook: phase parent updated", "pr_key", prKey, "from_phase", string(previous), "to_phase", string(phase))
	return rootID, true
}

// reusableTaskParent returns the channel parent of the task that owns a PR,
// when that parent lives in the channel this endpoint writes to. A parent in
// another channel cannot be edited from here, so a new one is posted instead.
func (s *Server) reusableTaskParent(ep config.EndpointConfig, prKey string, envelope WebhookEnvelope) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	repo, _ := repoAndNumber(prKey, envelope)
	repo = normalizeRepoKey(repo)
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	prRec, hasPR := s.tasks.ResolvePRThread(prKey)
	if branch == "" && hasPR {
		branch = strings.TrimSpace(prRec.Branch)
	}
	if branch == "" {
		if detail, ok := s.tasks.SurfaceDetail(prKey); ok {
			branch = strings.TrimSpace(detail.Branch)
		}
	}
	rootID := ""
	if hasPR {
		rootID = strings.TrimSpace(prRec.RootMessageID)
	}
	if rootID == "" && repo != "" && branch != "" {
		if rec, ok := s.tasks.Lookup(repo, branch); ok {
			rootID = strings.TrimSpace(rec.RootMessageID)
		}
	}
	if rootID == "" {
		return ""
	}
	if owner, taken := s.tasks.SurfaceRootOwner(rootID); taken && owner != prKey {
		return ""
	}
	platform, channelID := s.dispatchNotifyTarget(repo)
	if platform == "" || channelID == "" {
		return ""
	}
	if platform != ep.Platform || channelID != ep.ChannelID {
		return ""
	}
	return rootID
}

// reusablePersistedParent returns the PR parent stored on the queue row when
// the message lives in the channel this endpoint writes to.
func (s *Server) reusablePersistedParent(ctx context.Context, ep config.EndpointConfig, prKey string, envelope WebhookEnvelope) (string, string) {
	if s == nil || s.tasks == nil || s.prQueues == nil {
		return "", ""
	}
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil || row == nil {
		return "", ""
	}
	rootID := strings.TrimSpace(row.RootMsgID)
	if rootID == "" {
		return "", ""
	}
	repo, _ := repoAndNumber(prKey, envelope)
	repo = normalizeRepoKey(repo)
	platform, channelID := s.dispatchNotifyTarget(repo)
	if platform == "" || channelID == "" || platform != ep.Platform || channelID != ep.ChannelID {
		return "", ""
	}
	if owner, taken := s.tasks.SurfaceRootOwner(rootID); taken && owner != prKey {
		return "", ""
	}
	return rootID, strings.TrimSpace(row.ThreadID)
}

// refreshPhaseParentButtons re-renders the channel parent once the thread of
// a PR is known: the button set changes, the phase line does not.
func (s *Server) refreshPhaseParentButtons(ctx context.Context, ep config.EndpointConfig, prKey string, envelope WebhookEnvelope) {
	if s == nil || s.tasks == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	rootID, phase, _, known := s.prSurfaceState(prKey)
	if !known || rootID == "" || phase == "" {
		return
	}
	detail, ok := s.tasks.SurfaceDetail(prKey)
	if !ok {
		return
	}
	threadID := strings.TrimSpace(detail.ThreadID)
	if threadID == "" {
		if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
			threadID = strings.TrimSpace(rec.ThreadID)
		}
	}
	if threadID == "" || threadID == strings.TrimSpace(detail.ThreadID) {
		return
	}
	repo, prNumber := repoAndNumber(prKey, envelope)
	branch := strings.TrimSpace(stringValue(envelope["head_branch"]))
	if branch == "" {
		branch = strings.TrimSpace(detail.Branch)
	}
	prURL := strings.TrimSpace(stringValue(envelope["pr_url"]))
	if prURL == "" {
		prURL = strings.TrimSpace(detail.PRURL)
	}
	text := FormatPhaseParent(repo, prNumber, phase, s.statusIcons)
	buttons := s.phaseParentButtons(ep, prKey, repo, branch, prURL, threadID)
	if err := s.editPhaseParent(ctx, ep.Platform, ep.ChannelID, rootID, text, buttons); err != nil {
		slog.Warn("webhook: phase parent button refresh failed", "pr_key", prKey, "phase", string(phase), "error", err)
		return
	}
	s.tasks.SetSurface(prKey, SurfaceUpdate{
		RootMessageID: rootID,
		Repo:          repo,
		PRNumber:      prNumber,
		PRURL:         prURL,
		Branch:        branch,
		ThreadID:      threadID,
		Phase:         string(phase),
	})
	slog.Info("webhook: phase parent buttons refreshed", "pr_key", prKey, "phase", string(phase))
}

func (s *Server) ensureThreadStatus(ctx context.Context, platform, channelID, threadID, repo, branch, prKey string, update ThreadStatusUpdate) string {
	if s == nil || s.tasks == nil || strings.TrimSpace(threadID) == "" {
		return ""
	}
	queued := strings.TrimSpace(update.Queued)
	if queued == "" && strings.TrimSpace(prKey) != "" {
		queued = s.queueSnapshot(ctx, prKey)
	}
	text := FormatThreadStatusLine(update.Phase, queued, update.Waiting, update.Next, update.Action, s.statusIcons)
	target := threadTargetFor(platform, channelID, threadID)
	if strings.TrimSpace(target) == "" {
		return ""
	}
	writer := s.threadStatusWriter(ctx, platform, target, update.Phase)
	return writer.Write(target, threadID, repo, branch, prKey, text)
}

// threadStatusWriter builds the writer of a thread's state line. The
// dispatch writer in the executor uses the same writer, so both edit one
// message.
func (s *Server) threadStatusWriter(ctx context.Context, platform, target string, phase Phase) *ThreadStatusWriter {
	return &ThreadStatusWriter{
		Registry: s.tasks,
		Phase:    phase,
		Send: func(target, text string) (string, error) {
			return s.sendThread(ctx, platform, target, text)
		},
		Edit: func(target, messageID, text string) error {
			return s.editThread(ctx, platform, target, messageID, text)
		},
		Pin: func(target, messageID string) {
			s.pinThreadStatus(ctx, platform, target, messageID)
		},
	}
}

// pinThreadStatus pins the thread state line so the operator finds the
// current phase without hunting through the thread. Pinning is best-effort:
// a platform without pin support must not fail the write.
func (s *Server) pinThreadStatus(ctx context.Context, platform, target, msgID string) {
	if s == nil || s.pinner == nil {
		return
	}
	if strings.TrimSpace(target) == "" || strings.TrimSpace(msgID) == "" {
		return
	}
	if err := s.pinner(ctx, platform, target, msgID); err != nil {
		slog.Warn("webhook: thread status pin failed", "platform", platform, "message_id", msgID, "error", err)
	}
}

// SurfaceFrozen reports whether a write to the PR surface must be refused:
// the PR reached a terminal phase and the write asks for a different one.
// Every writer of a PR surface asks through here, so a terminal PR keeps its
// state line.
func SurfaceFrozen(tasks *TaskThreadRegistry, prKey string, phase Phase) bool {
	if tasks == nil || strings.TrimSpace(prKey) == "" {
		return false
	}
	_, current, _, known := tasks.SurfaceState(prKey)
	if !known || current == "" || current == phase {
		return false
	}
	if !IsTerminalPhase(current) {
		return false
	}
	if surfaceReopenedForRetry(tasks, prKey, current) {
		return false
	}
	return true
}

func surfaceReopenedForRetry(tasks *TaskThreadRegistry, prKey string, phase Phase) bool {
	if phase != PhaseBlocked && phase != PhaseFailed {
		return false
	}
	return tasks.SurfaceReopened(prKey)
}

func (s *Server) escalateToChannel(ctx context.Context, ep config.EndpointConfig, repo, prNumber string, phase Phase, what, why string) {
	text := FormatEscalationCopy(repo, prNumber, phase, what, why, s.statusIcons)
	mention := strings.TrimSpace(s.escalationMention)
	userID := ""
	if mention != "" {
		resolved, ok := s.resolveMention(ctx, ep.Platform, mention)
		if ok && strings.TrimSpace(resolved) != "" {
			userID = strings.TrimSpace(resolved)
		} else {
			slog.Warn("webhook: escalation mention unresolved, posting without mention", "endpoint", ep.Name, "mention", mention)
		}
	}
	if userID != "" && s.mentionNotifier != nil {
		// The adapter renders the mention token for the resolved id: a bare
		// name in the body is inert text and never pings anyone.
		if err := s.mentionNotifier(ctx, ep.Platform, ep.ChannelID, text, userID); err != nil {
			slog.Warn("webhook: escalation channel notification failed", "endpoint", ep.Name, "channel_id", ep.ChannelID, "error", err)
		}
		slog.Info("webhook: escalation posted to channel", "endpoint", ep.Name, "mention_resolved", true)
		return
	}
	if userID != "" {
		slog.Warn("webhook: escalation mention notifier unavailable, posting without mention", "endpoint", ep.Name)
	}
	if s.rawNotifier != nil {
		if err := s.rawNotifier(ctx, ep.Platform, ep.ChannelID, text); err != nil {
			slog.Warn("webhook: escalation channel notification failed", "endpoint", ep.Name, "error", err)
			return
		}
		slog.Info("webhook: escalation posted to channel", "endpoint", ep.Name, "mention_resolved", userID != "")
		return
	}
	if s.notifier == nil {
		slog.Info("webhook: escalation notification skipped", "endpoint", ep.Name)
		return
	}
	if err := s.notifier(ctx, ep.Platform, ep.ChannelID, text); err != nil {
		slog.Warn("webhook: escalation channel notification failed", "endpoint", ep.Name, "error", err)
		return
	}
	slog.Info("webhook: escalation posted to channel", "endpoint", ep.Name, "mention_resolved", userID != "")
}

func (s *Server) resolveMention(ctx context.Context, platform, name string) (string, bool) {
	if s.mentionResolver == nil {
		return "", false
	}
	return s.mentionResolver(ctx, platform, name)
}

// markEscalated claims the escalation of a PR. The claim is held while the
// escalation is posted, so two concurrent failures post one channel message.
// The blocking episode itself is carried by the queue row: a PR that blocks
// again after the operator acted posts a fresh escalation.
func (s *Server) markEscalated(prKey string) bool {
	if s == nil {
		return false
	}
	s.escalationMu.Lock()
	defer s.escalationMu.Unlock()
	if s.escalated == nil {
		s.escalated = make(map[string]bool)
	}
	if s.escalated[prKey] {
		return false
	}
	s.escalated[prKey] = true
	return true
}

// clearEscalated releases the escalation claim of a PR.
func (s *Server) clearEscalated(prKey string) {
	if s == nil {
		return
	}
	s.escalationMu.Lock()
	defer s.escalationMu.Unlock()
	delete(s.escalated, prKey)
}

// recordMergeGate remembers the approval and CI state a delivery reported so
// the merge button is disabled while the merge gate would fail. The gate is
// the single place the verdict is computed: a REQUEST_CHANGES verdict blocks
// the merge until a strictly later approval supersedes it.
func (s *Server) recordMergeGate(prKey string, envelope WebhookEnvelope) string {
	if s == nil || s.tasks == nil || strings.TrimSpace(prKey) == "" {
		return ""
	}
	incoming := SurfaceGate{
		Approval: approvalState(envelope),
		CI:       ciState(envelope),
		Commit:   strings.TrimSpace(stringValue(envelope["review_commit"])),
		Head:     strings.TrimSpace(stringValue(envelope["head_sha"])),
		ReviewID: reviewIDValue(envelope),
		ReviewAt: reviewSubmittedAt(envelope),
	}
	current, _ := s.tasks.Gate(prKey)
	head := s.gateHead(context.Background(), prKey, current, incoming)
	current.Head = head

	if incoming.CI != "" {
		current.CI = incoming.CI
	}
	if incoming.Approval != "" && gateVerdictApplies(head, incoming) {
		if incoming.Approval != "approved" ||
			strings.TrimSpace(current.Approval) != "changes_requested" ||
			s.approvalSupersedesBlock(prKey, current, incoming) {
			current.Approval = incoming.Approval
			current.Commit = incoming.Commit
			current.ReviewID = incoming.ReviewID
			current.ReviewAt = incoming.ReviewAt
		}
	}
	s.tasks.SetGate(prKey, current)
	return s.mergeGateVerdict(prKey)
}

// gateHead returns the head a record may move the gate to. A delivery never
// moves the known head backwards: when two deliveries disagree, the live PR
// head decides, and without it the first observed head stands.
func (s *Server) gateHead(ctx context.Context, prKey string, current, incoming SurfaceGate) string {
	currentHead := strings.TrimSpace(current.Head)
	incomingHead := strings.TrimSpace(incoming.Head)
	if incomingHead == "" {
		return currentHead
	}
	if currentHead == "" || currentHead == incomingHead {
		return incomingHead
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if live := s.livePRHeadSHA(lookupCtx, prKey); live != "" {
		return live
	}
	return currentHead
}

// gateVerdictApplies reports whether a verdict was observed on the head the
// gate now holds. A verdict with no head recorded is not judged stale.
func gateVerdictApplies(head string, verdict SurfaceGate) bool {
	commit := strings.TrimSpace(verdict.Commit)
	if commit == "" || head == "" {
		return true
	}
	return commit == head
}

// approvalSupersedesBlock reports whether an approval clears a recorded
// REQUEST_CHANGES verdict. A same-commit approval must be strictly later than
// the blocking review; an approval on another commit wins only when it is the
// current head.
func (s *Server) approvalSupersedesBlock(prKey string, current, approval SurfaceGate) bool {
	blockedCommit := strings.TrimSpace(current.Commit)
	approvedCommit := strings.TrimSpace(approval.Commit)
	if approvedCommit == "" {
		return blockedCommit == ""
	}
	if blockedCommit == "" {
		return true
	}
	if blockedCommit == approvedCommit {
		if later, known := reviewOrdering(current, approval); known {
			return later
		}
		return strings.EqualFold(s.livePRReviewDecision(context.Background(), prKey), "APPROVED")
	}
	if head := strings.TrimSpace(current.Head); head != "" && head == approvedCommit {
		return true
	}
	head := s.livePRHeadSHA(context.Background(), prKey)
	return head != "" && head == approvedCommit
}

// reviewOrdering reports whether approval came after blocked on the same
// commit. known is false when neither review ids nor timestamps can order them.
func reviewOrdering(blocked, approval SurfaceGate) (later, known bool) {
	if blocked.ReviewID > 0 && approval.ReviewID > 0 {
		return approval.ReviewID > blocked.ReviewID, true
	}
	if blocked.ReviewAt > 0 && approval.ReviewAt > 0 && blocked.ReviewAt != approval.ReviewAt {
		return approval.ReviewAt > blocked.ReviewAt, true
	}
	return false, false
}

func reviewIDValue(envelope WebhookEnvelope) int64 {
	id, err := strconv.ParseInt(strings.TrimSpace(stringValue(envelope["review_id"])), 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func reviewSubmittedAt(envelope WebhookEnvelope) int64 {
	submitted := strings.TrimSpace(stringValue(envelope["review_submitted_at"]))
	if submitted == "" {
		return 0
	}
	at, err := time.Parse(time.RFC3339, submitted)
	if err != nil {
		return 0
	}
	return at.Unix()
}

// livePRReviewDecision returns the latest actionable review verdict on the live
// PR, so an unordered approval never clears a block on its own.
func (s *Server) livePRReviewDecision(ctx context.Context, prKey string) string {
	repo, number, ok := splitPRKey(prKey)
	if !ok {
		return ""
	}
	prNumber, err := strconv.Atoi(strings.TrimSpace(number))
	if err != nil || prNumber <= 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	reviews, err := ghReviews(ctx, repo, prNumber)
	if err != nil {
		return ""
	}
	return ghReviewDecision(reviews)
}

// mergeGateVerdict returns the review verdict the merge gate holds for a PR
// and whether it blocks the merge. A verdict recorded for another head is not
// the current decision, so it renders as unknown.
func (s *Server) mergeGateVerdict(prKey string) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	gate, ok := s.tasks.Gate(prKey)
	if !ok {
		return ""
	}
	if gateVerdictStale(gate) {
		return ""
	}
	return strings.TrimSpace(gate.Approval)
}

func gateVerdictStale(gate SurfaceGate) bool {
	commit := strings.TrimSpace(gate.Commit)
	head := strings.TrimSpace(gate.Head)
	return commit != "" && head != "" && commit != head
}

// mergeGateBlocked reports whether the recorded review verdict blocks the
// merge. The verdict is head-scoped: a blocking review recorded for an older
// head is stale once a newer head has been observed, and a REQUEST_CHANGES
// verdict blocks also when the review is a self-review by the PR author.
func (s *Server) mergeGateBlocked(prKey string) (string, bool) {
	if s == nil || s.tasks == nil || strings.TrimSpace(prKey) == "" {
		return "", false
	}
	gate, ok := s.tasks.Gate(prKey)
	if !ok {
		return "", false
	}
	verdict := strings.TrimSpace(gate.Approval)
	if verdict == "" || verdict == "approved" {
		return verdict, false
	}
	if gateVerdictStale(gate) {
		return verdict, false
	}
	return verdict, true
}

func (s *Server) mergeGateCommit(prKey string) string {
	if s == nil || s.tasks == nil {
		return ""
	}
	gate, ok := s.tasks.Gate(prKey)
	if !ok {
		return ""
	}
	return strings.TrimSpace(gate.Commit)
}

// approvalState reads the review verdict a delivery carries.
func approvalState(envelope WebhookEnvelope) string {
	switch strings.ToLower(strings.TrimSpace(stringValue(envelope["review_verdict"]))) {
	case "approved":
		return "approved"
	case "request_changes", "requested_changes", "changes_requested":
		return "changes_requested"
	}
	switch strings.ToLower(strings.TrimSpace(stringValue(envelope["review_state"]))) {
	case "approved":
		return "approved"
	case "changes_requested":
		return "changes_requested"
	}
	return ""
}

// ciState reads the check-suite state a delivery carries.
func ciState(envelope WebhookEnvelope) string {
	switch strings.ToLower(strings.TrimSpace(stringValue(envelope["conclusion"]))) {
	case "success":
		return "success"
	case "failure", "timed_out", "cancelled", "canceled", "action_required", "startup_failure", "stale":
		return "failure"
	}
	switch strings.ToLower(strings.TrimSpace(stringValue(envelope["status"]))) {
	case "queued", "in_progress", "pending", "waiting", "requested":
		return "pending"
	}
	return ""
}

func repoAndNumber(prKey string, envelope WebhookEnvelope) (string, string) {
	repo, number, ok := splitPRKey(prKey)
	if ok {
		return repo, number
	}
	return strings.TrimSpace(stringValue(envelope["repository"])), strings.TrimSpace(stringValue(envelope["pr_number"]))
}

func (s *Server) prKeyForEnvelope(ep config.EndpointConfig, envelope WebhookEnvelope) string {
	key := WebhookExecutionKey{
		Repository: stringValue(envelope["repository"]),
		Branch:     stringValue(envelope["head_branch"]),
	}
	if prKey, ok := prKeyForEndpoint(ep, key, envelope); ok {
		return prKey
	}
	return ""
}

func (s *Server) currentSurfacePhase(prKey string) Phase {
	_, phase, _, ok := s.prSurfaceState(prKey)
	if !ok {
		return ""
	}
	return phase
}

// reopenPRSurface unfreezes the channel parent of a PR: the operator acted on
// a blocked PR, so the next phase change edits the parent again.
func (s *Server) reopenPRSurface(prKey string) {
	if s == nil || s.tasks == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	s.tasks.ReopenSurface(prKey)
}

// queueState renders the queue-driven parts of the thread state line: the
// reason the PR waits on another run, and the operator ask of a blocked
// queue. A blocked queue waits on nothing — its run is over — so it carries
// the block reason and the ask instead of a waiting reason.
func (s *Server) queueState(ctx context.Context, prKey string, phase Phase) (string, string) {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return "", ""
	}
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil || row == nil {
		return "", ""
	}
	if row.Status == store.PRStatusBlocked {
		return "", blockedQueueText(prKey, row.BlockedReason, blockedQueueCommand)
	}
	kind := strings.TrimSpace(strings.ToLower(row.ActiveKind))
	if kind == "" || kind == string(phase) {
		return "", ""
	}
	return kind + " run active", ""
}

func waitingReasonForGate(envelope WebhookEnvelope) string {
	conclusion := strings.ToLower(strings.TrimSpace(stringValue(envelope["conclusion"])))
	if conclusion == "failure" || conclusion == "failed" {
		return "CI (check_suite)"
	}
	verdict := strings.ToUpper(strings.TrimSpace(stringValue(envelope["gate_verdict"])))
	if verdict == "" {
		verdict = strings.ToUpper(strings.TrimSpace(stringValue(envelope["review_verdict"])))
	}
	switch verdict {
	case "REQUEST_CHANGES", "REQUESTED_CHANGES", "CHANGES_REQUESTED":
		return "approval"
	}
	return ""
}

// QueueSnapshot renders the queue depth of a PR: "fix (2), merge (1)".
func QueueSnapshot(queues store.PRQueueRepo, endpoints []config.EndpointConfig, prKey string) string {
	if queues == nil || strings.TrimSpace(prKey) == "" {
		return ""
	}
	items, err := queues.ListItems(context.Background(), prKey)
	if err != nil || len(items) == 0 {
		return ""
	}
	workflows := make(map[string]string, len(endpoints))
	for _, ep := range endpoints {
		workflows[strings.TrimSpace(ep.Name)] = strings.TrimSpace(strings.ToLower(ep.Workflow))
	}
	counts := make(map[string]int, len(items))
	for _, item := range items {
		kind := "run"
		switch workflow := workflows[strings.TrimSpace(item.Endpoint)]; workflow {
		case "review", "fix", "merge", "merged", "dispatch":
			kind = workflow
		}
		counts[kind]++
	}
	return FormatQueueDepth(counts)
}

// EnvelopeString reads a string field of a webhook envelope.
func EnvelopeString(envelope WebhookEnvelope, key string) string {
	return stringValue(envelope[key])
}

// markPRQueued shows a PR in the channel as soon as its first delivery is
// accepted. Later deliveries keep the phase they already have.
func (s *Server) markPRQueued(prKey string, ep config.EndpointConfig, envelope WebhookEnvelope) {
	if s == nil || s.tasks == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	if _, phase, _, ok := s.tasks.SurfaceState(prKey); ok && phase != "" {
		return
	}
	s.syncPhaseParent(context.Background(), ep, prKey, envelope, PhaseQueued)
}

// sendPhaseParent posts the channel parent. The parent carries no card
// separator: it is a single index line.
func (s *Server) sendPhaseParent(ctx context.Context, platform, channelID, text string, buttons []channel.Button) (string, error) {
	if len(buttons) > 0 && s.rawSenderButtons != nil {
		return s.rawSenderButtons(ctx, platform, channelID, text, buttons)
	}
	if s.rawSender != nil {
		return s.rawSender(ctx, platform, channelID, text)
	}
	if s.rawNotifier != nil {
		return "", s.rawNotifier(ctx, platform, channelID, text)
	}
	return s.sendParent(ctx, platform, channelID, text, buttons)
}

// editPhaseParent rewrites the channel parent in place.
func (s *Server) editPhaseParent(ctx context.Context, platform, channelID, messageID, text string, buttons []channel.Button) error {
	if len(buttons) > 0 && s.rawEditorButtons != nil {
		return s.rawEditorButtons(ctx, platform, channelID, messageID, text, buttons)
	}
	if s.rawEditor != nil {
		return s.rawEditor(ctx, platform, channelID, messageID, text)
	}
	return s.editParent(ctx, platform, channelID, messageID, text, buttons)
}
