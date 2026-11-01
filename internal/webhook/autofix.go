package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const (
	fixKindReview   = "review"
	fixKindCI       = "ci"
	fixKindConflict = "conflict"
	fixKindMixed    = "mixed"

	autoFixStormMax      = 3
	autoFixStormWindow   = 24 * time.Hour
	autoFixReserveSlots  = 1
	autoFixCommentUser   = "operator"
	conflictMarkerPrefix = "occa:conflict:"
)

func isRedSuiteConclusion(conclusion string) bool {
	switch strings.ToLower(strings.TrimSpace(conclusion)) {
	case "failure", "timed_out", "action_required":
		return true
	}
	return false
}

func classifyCommentTrigger(text string) []string {
	lower := strings.ToLower(strings.TrimSpace(text))
	if lower == "" {
		return nil
	}
	var reasons []string
	if strings.Contains(lower, "ci") {
		reasons = append(reasons, fixKindCI)
	}
	if strings.Contains(lower, "review") {
		reasons = append(reasons, fixKindReview)
	}
	if len(reasons) == 0 {
		return []string{fixKindMixed}
	}
	return reasons
}

func fixKindOf(reasons []string) string {
	set := map[string]bool{}
	for _, r := range reasons {
		r = strings.TrimSpace(strings.ToLower(r))
		if r == fixKindReview || r == fixKindCI || r == fixKindConflict {
			set[r] = true
		}
	}
	if len(set) == 0 {
		return fixKindMixed
	}
	if len(set) == 1 {
		for k := range set {
			return k
		}
	}
	return fixKindMixed
}

func mergeFixReasons(existing, added []string) []string {
	set := map[string]bool{}
	for _, r := range existing {
		r = strings.TrimSpace(strings.ToLower(r))
		if r == fixKindReview || r == fixKindCI || r == fixKindConflict {
			set[r] = true
		}
	}
	for _, r := range added {
		r = strings.TrimSpace(strings.ToLower(r))
		if r == fixKindReview || r == fixKindCI || r == fixKindConflict {
			set[r] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func encodeFixReasons(reasons []string) string {
	if len(reasons) == 0 {
		return "[]"
	}
	raw, err := json.Marshal(mergeFixReasons(nil, reasons))
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func decodeFixReasons(s string) []string {
	var out []string
	if strings.TrimSpace(s) == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return mergeFixReasons(nil, out)
}

func injectFixContext(envelope WebhookEnvelope, fixKind, fixReasons string) {
	if envelope == nil {
		return
	}
	kind := strings.TrimSpace(strings.ToLower(fixKind))
	if kind != fixKindReview && kind != fixKindCI && kind != fixKindConflict && kind != fixKindMixed {
		kind = ""
	}
	reasons := strings.TrimSpace(fixReasons)
	if reasons == "" {
		reasons = "[]"
	}
	envelope["fix_kind"] = kind
	envelope["fix_reasons"] = reasons
}

func fixReasonsFromEnvelope(envelope WebhookEnvelope) []string {
	var reasons []string
	if verdict := strings.TrimSpace(strings.ToLower(stringValue(envelope["review_verdict"]))); verdict == "request_changes" {
		reasons = append(reasons, fixKindReview)
	}
	reasons = append(reasons, classifyCommentTrigger(stringValue(envelope["comment_trigger"]))...)
	body := stringValue(envelope["comment_body"])
	if strings.Contains(body, conflictMarkerPrefix) {
		reasons = append(reasons, fixKindConflict)
	}
	if event := strings.TrimSpace(strings.ToLower(stringValue(envelope["event_type"]))); event == "check_suite" {
		if isRedSuiteConclusion(stringValue(envelope["conclusion"])) {
			reasons = append(reasons, fixKindCI)
		}
	}
	return mergeFixReasons(nil, reasons)
}

type prLiveState struct {
	mergeable  bool
	dirty      bool
	draft      bool
	headSHA    string
	checksRed  bool
	checksDone bool
}

func ghPrLiveState(ctx context.Context, repo, number string) (prLiveState, error) {
	var st prLiveState
	timeoutCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(timeoutCtx, "gh", "pr", "view", number, "--repo", repo,
		"--json", "mergeable,mergeStateStatus,isDraft,headRefOid,statusCheckRollup")
	raw, err := cmd.Output()
	if err != nil {
		return st, fmt.Errorf("lookup PR live state %s#%s: %w", repo, number, err)
	}
	var decoded struct {
		Mergeable         string `json:"mergeable"`
		MergeStateStatus  string `json:"mergeStateStatus"`
		IsDraft           bool   `json:"isDraft"`
		HeadRefOid        string `json:"headRefOid"`
		StatusCheckRollup []struct {
			Conclusion string `json:"conclusion"`
			State      string `json:"state"`
		} `json:"statusCheckRollup"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return st, fmt.Errorf("decode PR live state %s#%s: %w", repo, number, err)
	}
	st.mergeable = strings.EqualFold(strings.TrimSpace(decoded.MergeStateStatus), "CLEAN") ||
		strings.EqualFold(strings.TrimSpace(decoded.Mergeable), "MERGEABLE")
	st.dirty = strings.EqualFold(strings.TrimSpace(decoded.MergeStateStatus), "DIRTY") ||
		strings.EqualFold(strings.TrimSpace(decoded.Mergeable), "CONFLICTING")
	st.draft = decoded.IsDraft
	st.headSHA = strings.TrimSpace(decoded.HeadRefOid)
	sawPending := false
	for _, check := range decoded.StatusCheckRollup {
		state := strings.ToUpper(strings.TrimSpace(check.State))
		if state == "EXPECTED" || state == "PENDING" {
			sawPending = true
			continue
		}
		if state == "ERROR" || state == "FAILURE" || isRedSuiteConclusion(check.Conclusion) {
			st.checksRed = true
		}
	}
	st.checksDone = !sawPending
	return st, nil
}

func autoFixEnqueueAllowed(rowAutoFixCount int, windowStart int64, now time.Time) (count int, start int64, allowed bool) {
	if windowStart == 0 || now.Unix()-windowStart >= int64((autoFixStormWindow/time.Second)) {
		return 0, now.Unix(), true
	}
	if rowAutoFixCount >= autoFixStormMax {
		return rowAutoFixCount, windowStart, false
	}
	return rowAutoFixCount, windowStart, true
}

func (s *Server) fixEndpointForRepo(repo string) (config.EndpointConfig, bool) {
	want := normalizeRepoKey(repo)
	for _, ep := range s.endpoints {
		if normalizeRepoKey(ep.Repository) != want || want == "" {
			continue
		}
		if strings.TrimSpace(strings.ToLower(ep.Workflow)) == "fix" {
			return ep, true
		}
	}
	return config.EndpointConfig{}, false
}

func (s *Server) maybeAutoFixCI(ctx context.Context, ep config.EndpointConfig, envelope WebhookEnvelope, key WebhookExecutionKey, prKey string) {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return
	}
	if !ep.AutoFix.CI {
		return
	}
	if !isRedSuiteConclusion(stringValue(envelope["conclusion"])) {
		return
	}
	if state := strings.TrimSpace(strings.ToLower(stringValue(envelope["pr_state"]))); state == "closed" || state == "merged" {
		return
	}
	number := strings.TrimSpace(stringValue(envelope["pr_number"]))
	repo := normalizeRepoKey(stringValue(envelope["repository"]))
	if number == "" || repo == "" {
		slog.Debug("webhook: auto-fix skipped, unresolvable PR", "endpoint", ep.Name)
		return
	}
	headSHA := strings.TrimSpace(stringValue(envelope["head_sha"]))
	if err := s.enqueueAutoFixCI(ctx, repo, number, headSHA, []string{fixKindCI}); err != nil {
		slog.Debug("webhook: auto-fix skipped", "endpoint", ep.Name, "pr_key", prKey, "error", err)
	}
}

func (s *Server) enqueueAutoFixCI(ctx context.Context, repo, number, headSHA string, reasons []string) error {
	prKey := normalizeRepoKey(repo) + "#" + strings.TrimSpace(number)
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil {
		return err
	}
	if row != nil && row.Status == store.PRStatusBlocked {
		return fmt.Errorf("pr queue blocked")
	}
	body, eventType, author, err := s.buildAutoFixBody(ctx, repo, number)
	if err != nil {
		return err
	}
	fixEp, found := s.fixEndpointForRepo(repo)
	if !found {
		return fmt.Errorf("no fix endpoint for repo %s", repo)
	}
	author = strings.TrimSpace(author)
	if author == "" || automationMode(fixEp, author) != config.AutomationFull {
		return fmt.Errorf("author policy forbids auto-fix")
	}
	var rowCount int
	var rowWindow int64
	var rowSHA string
	if row != nil {
		rowCount, rowWindow, rowSHA = row.AutoFixCount, row.AutoFixWindowStart, row.AutoFixSHA
	}
	if strings.TrimSpace(rowSHA) != "" && strings.TrimSpace(headSHA) != "" && rowSHA == headSHA {
		return fmt.Errorf("duplicate auto-fix for head SHA")
	}
	count, windowStart, allowed := autoFixEnqueueAllowed(rowCount, rowWindow, time.Now())
	if !allowed {
		fullRow, err := s.prQueues.Get(ctx, prKey)
		if err != nil {
			return err
		}
		s.escalateAutoFixBlocked(fixEp, prKey, fullRow)
		return fmt.Errorf("auto-fix storm cap reached")
	}
	inserted, err := s.enqueueAutoFix(ctx, fixEp, body, eventType, prKey, reasons)
	if err != nil {
		return err
	}
	if inserted {
		_ = s.prQueues.SetAutoFixState(ctx, prKey, count+1, windowStart, headSHA)
	} else {
		_ = s.prQueues.SetAutoFixState(ctx, prKey, count, windowStart, headSHA)
	}
	slog.Info("webhook: auto-fix enqueued", "pr_key", prKey, "sha", headSHA, "reasons", encodeFixReasons(reasons))
	return nil
}

func (s *Server) enqueueAutoFix(ctx context.Context, fixEp config.EndpointConfig, body []byte, eventType, prKey string, reasons []string) (bool, error) {
	kind := fixKindOf(reasons)
	encoded := encodeFixReasons(reasons)
	if existing, err := s.prQueues.ListItems(ctx, prKey); err == nil {
		for _, item := range existing {
			if item.Priority == lanePriority(laneOfWorkflow("fix")) {
				merged := mergeFixReasons(decodeFixReasons(item.FixReasons), reasons)
				if err := s.prQueues.SetItemFixReasons(ctx, item.ID, fixKindOf(merged), encodeFixReasons(merged)); err != nil {
					return false, err
				}
				slog.Info("webhook: auto-fix coalesced", "endpoint", fixEp.Name, "pr_key", prKey, "reasons", encodeFixReasons(merged))
				return false, nil
			}
		}
	}
	lockKey := "pr:" + prKey
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	d, ok := s.dispatchers[lockKey]
	if !ok || d.exiting {
		d = newDispatcher(s, lockKey)
		s.dispatchers[lockKey] = d
		s.dispatchWG.Add(1)
		go d.run(s.shutdownCtx)
	}
	if s.maxQueuedPerKey > autoFixReserveSlots && d.queueLen() >= s.maxQueuedPerKey-autoFixReserveSlots {
		return false, fmt.Errorf("pr queue full for auto-fix")
	}
	if s.maxQueuedPerKey <= autoFixReserveSlots && d.queueLen() >= s.maxQueuedPerKey {
		return false, fmt.Errorf("pr queue full for auto-fix")
	}
	item := dispatchItem{
		ep:         fixEp,
		body:       body,
		deliveryID: fmt.Sprintf("op-autofix-%d", time.Now().UnixNano()),
		eventType:  eventType,
		prKey:      prKey,
		lane:       laneOfWorkflow("fix"),
		fixKind:    kind,
		fixReasons: mergeFixReasons(nil, reasons),
	}
	item.queueID = s.mirrorEnqueueFix(prKey, fixEp.Name, eventType, item.fixKind, encoded)
	if item.queueID == 0 {
		return false, fmt.Errorf("pr queue mirror failed")
	}
	if !d.push(item) {
		s.mirrorDelete(item.queueID)
		return false, fmt.Errorf("pr queue full for auto-fix")
	}
	return true, nil
}

func (s *Server) buildAutoFixBody(ctx context.Context, repo, number string) ([]byte, string, string, error) {
	if s.autoFixBodyBuilder != nil {
		return s.autoFixBodyBuilder(ctx, repo, number)
	}
	return defaultAutoFixBody(ctx, repo, number, s.endpoints)
}

func pickAutoFixTrigger(endpoints map[string]config.EndpointConfig, repo string) string {
	trigger := ""
	for _, ep := range endpoints {
		if normalizeRepoKey(ep.Repository) != normalizeRepoKey(repo) {
			continue
		}
		if strings.TrimSpace(strings.ToLower(ep.Workflow)) != "fix" {
			continue
		}
		for _, t := range ep.CommentTrigger {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if strings.Contains(strings.ToLower(t), "ci") {
				return t
			}
			if trigger == "" {
				trigger = t
			}
		}
	}
	return trigger
}

func defaultAutoFixBody(ctx context.Context, repo, number string, endpoints map[string]config.EndpointConfig) ([]byte, string, string, error) {
	pr, err := ghLookupPR(ctx, repo, number)
	if err != nil {
		return nil, "", "", err
	}
	trigger := pickAutoFixTrigger(endpoints, repo)
	if trigger == "" {
		return nil, "", "", fmt.Errorf("no fix trigger configured for repo %s", repo)
	}
	body := map[string]any{
		"action": "created",
		"issue": map[string]any{
			"number":       pr.Number,
			"state":        "open",
			"title":        pr.Title,
			"user":         map[string]any{"login": pr.Author},
			"pull_request": map[string]any{"html_url": pr.URL},
		},
		"comment":         map[string]any{"body": trigger, "user": map[string]any{"login": autoFixCommentUser}},
		"repository":      map[string]any{"full_name": repo},
		"branch":          pr.HeadRef,
		"head_revision":   pr.HeadSHA,
		"head_repository": pr.HeadRepo,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, "", "", err
	}
	return raw, "issue_comment", pr.Author, nil
}

func (s *Server) mirrorEnqueueFix(prKey, endpoint, eventType, fixKind, fixReasons string) int64 {
	if s.prQueues == nil || strings.TrimSpace(prKey) == "" {
		return 0
	}
	if _, err := s.prQueues.EnsurePR(context.Background(), prKey, "", ""); err != nil {
		return 0
	}
	id, err := s.prQueues.EnqueueFix(context.Background(), prKey, endpoint, eventType, 0, lanePriority(laneOfWorkflow("fix")), fixKind, fixReasons)
	if err != nil {
		return 0
	}
	return id
}

func (s *Server) escalateAutoFixBlocked(ep config.EndpointConfig, prKey string, row *store.PRRun) {
	if row == nil || s.prQueues == nil {
		return
	}
	reason := fmt.Sprintf("auto-fix storm cap — manual action (%d auto-fix runs in 24h)", autoFixStormMax)
	if !s.escalateBlock(ep, prKey, row, reason, "auto-fix stopped", fmt.Sprintf("%d auto-fix runs in 24h", autoFixStormMax)) {
		return
	}
	slog.Error("webhook: PR blocked on auto-fix storm cap",
		"endpoint", ep.Name,
		"pr_key", prKey,
	)
}

func containsReason(reasons []string, kind string) bool {
	for _, r := range reasons {
		if strings.TrimSpace(strings.ToLower(r)) == kind {
			return true
		}
	}
	return false
}

func (s *Server) revalidateFixItem(item *dispatchItem, envelope WebhookEnvelope) (skip bool, reason string) {
	stored := mergeFixReasons(nil, item.fixReasons)
	derived := fixReasonsFromEnvelope(envelope)
	merged := mergeFixReasons(stored, derived)
	item.fixKind = fixKindOf(merged)
	item.fixReasons = merged
	autoInvolved := len(stored) > 0
	// A manual fix on a repo without conflict auto-fix uses none of the live
	// state below, so it costs no lookup.
	if !autoInvolved && !item.ep.AutoFix.Conflict {
		return false, ""
	}
	repo, number, found := splitPRKey(item.prKey)
	if !found || repo == "" || number == "" {
		return false, ""
	}
	live, err := s.checkMergeable(context.Background(), repo, number)
	if err != nil {
		slog.Warn("webhook: fix live check failed, proceeding", "pr_key", item.prKey, "error", err)
		return false, ""
	}
	if autoInvolved && live.draft {
		return true, "auto-fix skipped on draft PR"
	}
	if live.dirty {
		author := strings.TrimSpace(stringValue(envelope["pr_author"]))
		if item.ep.AutoFix.Conflict && author != "" && automationMode(item.ep, author) == config.AutomationFull {
			merged = mergeFixReasons(merged, []string{fixKindConflict})
			item.fixKind = fixKindOf(merged)
			item.fixReasons = merged
			slog.Info("webhook: fix conflict reason added", "pr_key", item.prKey)
		}
	}
	if autoInvolved && containsReason(merged, fixKindCI) && !containsReason(merged, fixKindConflict) && live.checksDone && !live.checksRed {
		return true, "CI no longer red; skipping auto-fix"
	}
	return false, ""
}
