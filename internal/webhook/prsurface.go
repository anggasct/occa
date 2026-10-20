package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const threadStatusPrefix = "📌 Status — "

const escalationActionLine = "Action: /retry · /skip · /cancel"

const mergeActionAsk = "retry exhausted — run /retry, /skip or /cancel"

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
	verdict := strings.ToUpper(strings.TrimSpace(stringValue(envelope["review_verdict"])))
	if verdict == "" {
		return ""
	}
	return "verdict: " + verdict
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
	return s.defaultMergeGate(ctx, prKey)
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
	if known {
		if previous == phase {
			return rootID, false
		}
		if IsTerminalPhase(previous) {
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
	slog.Info("webhook: phase parent updated", "pr_key", prKey, "from_phase", string(previous), "to_phase", string(phase))
	return rootID, true
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
	existing := s.tasks.StatusMessageIDKeyed(repo, branch, prKey)
	if existing != "" {
		if err := s.editThread(ctx, platform, target, existing, text); err == nil {
			return existing
		}
	}
	msgID, err := s.sendThread(ctx, platform, target, text)
	if err != nil || strings.TrimSpace(msgID) == "" {
		return strings.TrimSpace(msgID)
	}
	s.tasks.SetStatusMessageKeyed(repo, branch, prKey, msgID)
	return msgID
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
	if userID != "" {
		text += "\n@" + mention
		if s.mentionNotifier != nil {
			if err := s.mentionNotifier(ctx, ep.Platform, ep.ChannelID, text, userID); err != nil {
				slog.Warn("webhook: escalation channel notification failed", "endpoint", ep.Name, "channel_id", ep.ChannelID, "error", err)
			}
			slog.Info("webhook: escalation posted to channel", "endpoint", ep.Name, "mention_resolved", true)
			return
		}
		slog.Warn("webhook: escalation mention notifier unavailable, posting without mention", "endpoint", ep.Name)
		text = strings.TrimSuffix(text, "\n@"+mention)
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

func (s *Server) queueWaiting(ctx context.Context, prKey string, phase Phase) string {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return ""
	}
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil || row == nil {
		return ""
	}
	kind := strings.TrimSpace(strings.ToLower(row.ActiveKind))
	if kind == "" || kind == string(phase) {
		return ""
	}
	return kind + " run active"
}

func waitingReasonForGate(envelope WebhookEnvelope) string {
	conclusion := strings.ToLower(strings.TrimSpace(stringValue(envelope["conclusion"])))
	if conclusion == "failure" || conclusion == "failed" {
		return "CI (check_suite)"
	}
	switch strings.ToUpper(strings.TrimSpace(stringValue(envelope["review_verdict"]))) {
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
