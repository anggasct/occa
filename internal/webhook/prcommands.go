package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

var operatorCommandNames = map[string]bool{
	"queue":  true,
	"retry":  true,
	"skip":   true,
	"cancel": true,
	"status": true,
}

func parseOperatorCommand(text string) (name, arg string, ok bool) {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "/") {
		return "", "", false
	}
	parts := strings.Fields(strings.TrimSpace(strings.TrimPrefix(trimmed, "/")))
	if len(parts) == 0 {
		return "", "", false
	}
	name = strings.ToLower(parts[0])
	if !operatorCommandNames[name] {
		return "", "", false
	}
	if len(parts) > 1 {
		arg = strings.TrimPrefix(strings.TrimSpace(parts[1]), "#")
	}
	return name, arg, true
}

func (s *Server) operatorAllowed(platform, userID string) bool {
	if s.operatorAuthorizer == nil {
		return false
	}
	return s.operatorAuthorizer(platform, userID)
}

func (s *Server) HandleOperatorCommand(ctx context.Context, msg channel.IncomingMessage, name, arg string) (string, bool, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if !operatorCommandNames[name] {
		return "", false, nil
	}
	prKey, inThread := s.tasks.PRKeyForThread(msg.ThreadID)
	if name == "status" && !inThread {
		return "", false, nil
	}
	if !s.operatorAllowed(msg.Platform, msg.UserID) {
		return "⛔ Not allowed.", true, nil
	}
	if !inThread {
		return "Run this inside the PR thread — I can't tell which PR you mean from here.", true, nil
	}
	if arg != "" {
		number := strings.TrimSpace(arg)
		if _, want, found := splitPRKey(prKey); !found || number != want {
			return fmt.Sprintf("This thread tracks %s — run the command there without arguments, or from that PR's thread.", prKey), true, nil
		}
	}
	switch name {
	case "queue":
		return s.operatorQueue(ctx, prKey), true, nil
	case "status":
		return s.operatorStatus(ctx, prKey), true, nil
	case "retry":
		return s.operatorRetry(ctx, msg, prKey)
	case "skip":
		return s.operatorSkip(ctx, msg, prKey)
	case "cancel":
		return s.operatorCancel(ctx, msg, prKey)
	}
	return "", false, nil
}

func (s *Server) operatorRow(ctx context.Context, prKey string) (*store.PRRun, error) {
	if s.prQueues == nil {
		return nil, fmt.Errorf("pr queue unavailable")
	}
	return s.prQueues.Get(ctx, prKey)
}

type QueueStats struct {
	QueueLen        int
	QueueBudget     int
	RejectedPending int
	ReplayExpired   int
}

// QueueStats reports queue depth against the per-PR budget. A non-empty prKey
// scopes the length to that PR's dispatcher; an empty one sums every dispatcher.
func (s *Server) QueueStats(ctx context.Context, prKey string) QueueStats {
	stats := QueueStats{QueueBudget: s.maxQueuedPerKey}
	want := ""
	if key := strings.TrimSpace(prKey); key != "" {
		want = "pr:" + key
	}
	s.dispatchMu.Lock()
	for key, d := range s.dispatchers {
		if want != "" && key != want {
			continue
		}
		stats.QueueLen += d.queueLen()
	}
	s.dispatchMu.Unlock()
	if s.replays != nil {
		if n, err := s.replays.Count(ctx, store.WebhookReplayPending); err == nil {
			stats.RejectedPending = n
		}
		if n, err := s.replays.Count(ctx, store.WebhookReplayExpired); err == nil {
			stats.ReplayExpired = n
		}
	}
	return stats
}

func (s *Server) operatorQueue(ctx context.Context, prKey string) string {
	row, err := s.operatorRow(ctx, prKey)
	if err != nil || row == nil {
		return "No queue state for " + prKey + " yet."
	}
	var b strings.Builder
	fmt.Fprintf(&b, "📦 %s — %s", prKey, strings.ToLower(string(row.Status)))
	if row.ActiveKind != "" {
		fmt.Fprintf(&b, " (%s)", row.ActiveKind)
	}
	if row.ActiveSession != "" {
		elapsed := time.Since(time.Unix(row.ActiveSince, 0))
		fmt.Fprintf(&b, "\n   session %s · running %s · retry %d", row.ActiveSession, formatDuration(elapsed), row.RetryCount)
		if row.NextRetryAt > 0 {
			wait := time.Until(time.Unix(row.NextRetryAt, 0))
			if wait > 0 {
				fmt.Fprintf(&b, " · next retry in %s", formatDuration(wait))
			}
		}
	}
	items, err := s.prQueues.ListItems(ctx, prKey)
	if err == nil && len(items) > 0 {
		fmt.Fprintf(&b, "\n   queued (%d):", len(items))
		for i, it := range items {
			fmt.Fprintf(&b, " [%d %s p%d #%d]", i+1, it.Endpoint, it.Priority, it.DeliveryID)
			if i >= 7 {
				fmt.Fprintf(&b, " …+%d more", len(items)-i-1)
				break
			}
		}
	} else {
		b.WriteString("\n   queue empty")
	}
	return b.String()
}

func (s *Server) operatorStatus(ctx context.Context, prKey string) string {
	row, err := s.operatorRow(ctx, prKey)
	if err != nil || row == nil {
		return "No queue state for " + prKey + " yet."
	}
	var b strings.Builder
	fmt.Fprintf(&b, "PR %s — %s", prKey, strings.ToLower(string(row.Status)))
	kind := strings.TrimSpace(row.ActiveKind)
	if kind == "" {
		kind = "-"
	}
	session := strings.TrimSpace(row.ActiveSession)
	if session == "" {
		session = "-"
	}
	fmt.Fprintf(&b, "\nkind: %s · session: %s", kind, session)
	if row.ActiveSince > 0 {
		fmt.Fprintf(&b, " · running %s", formatDuration(time.Since(time.Unix(row.ActiveSince, 0))))
	}
	fmt.Fprintf(&b, "\nretries: %d", row.RetryCount)
	if row.NextRetryAt > 0 {
		if wait := time.Until(time.Unix(row.NextRetryAt, 0)); wait > 0 {
			fmt.Fprintf(&b, " · next retry in %s", formatDuration(wait))
		}
	}
	if reason := strings.TrimSpace(row.BlockedReason); reason != "" {
		fmt.Fprintf(&b, "\nblocked: %s", reason)
	}
	if row.ThreadID != "" {
		fmt.Fprintf(&b, "\nthread: %s", row.ThreadID)
	}
	stats := s.QueueStats(ctx, prKey)
	fmt.Fprintf(&b, "\nqueue: %d/%d · rejected pending: %d · replay expired: %d", stats.QueueLen, stats.QueueBudget, stats.RejectedPending, stats.ReplayExpired)
	return b.String()
}

func (s *Server) liveDispatcher(prKey string) *dispatcher {
	return s.dispatcherFor(prKey)
}

func (s *Server) abortActiveSession(ctx context.Context, prKey, repo string) {
	if s.sessionAborter == nil || s.prQueues == nil {
		return
	}
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil || row == nil || strings.TrimSpace(row.ActiveSession) == "" {
		return
	}
	workdir := s.checkoutForRepo(repo)
	if workdir == "" {
		return
	}
	if err := s.sessionAborter(ctx, strings.TrimSpace(row.ActiveSession), workdir); err != nil {
		slog.Warn("webhook: operator abort failed", "pr_key", prKey, "error", err)
	}
}

func (s *Server) operatorRetry(ctx context.Context, msg channel.IncomingMessage, prKey string) (string, bool, error) {
	row, err := s.operatorRow(ctx, prKey)
	if err != nil || row == nil {
		return "Nothing to retry for " + prKey + " — no queue state.", true, nil
	}
	kind := s.inferRetryKind(ctx, row)
	if kind == "" || kind == "dispatch" || kind == "merged" {
		return "Nothing to retry for " + prKey + ".", true, nil
	}
	if err := s.prQueues.SetRetryState(ctx, prKey, 0, 0); err != nil {
		return "", true, err
	}
	if d := s.liveDispatcher(prKey); d != nil {
		if d.isBackingOff() {
			d.requestRetryNow()
			return "Retry count reset — firing now.", true, nil
		}
		if d.isActive() || d.queueLen() > 0 {
			return "Retry count reset — a run is already queued or running.", true, nil
		}
	}
	if row.Status == store.PRStatusBlocked {
		if err := s.prQueues.Unblock(ctx, prKey); err != nil {
			return "", true, err
		}
		s.clearEscalated(prKey)
	}
	s.reopenPRSurface(prKey)
	repo, number, _ := splitPRKey(prKey)
	if err := s.enqueueSyntheticRetry(ctx, repo, number, kind, prKey); err != nil {
		return "Retry reset, but re-run failed: " + err.Error(), true, nil
	}
	return "Re-running " + kind + " for " + prKey + ".", true, nil
}

func (s *Server) inferRetryKind(ctx context.Context, row *store.PRRun) string {
	if row == nil {
		return ""
	}
	kind := strings.TrimSpace(strings.ToLower(row.ActiveKind))
	if kind != "" {
		return kind
	}
	if s.deliveries == nil {
		return ""
	}
	var rec *store.WebhookDelivery
	if row.ActiveDelivery != 0 {
		if got, err := s.deliveries.GetByID(ctx, row.ActiveDelivery); err == nil {
			rec = got
		}
	}
	if rec == nil {
		if got, err := s.deliveries.LatestForPR(ctx, row.PRKey); err == nil {
			rec = got
		}
	}
	return s.retryKindForDelivery(rec)
}

func (s *Server) retryKindForDelivery(rec *store.WebhookDelivery) string {
	if rec == nil {
		return ""
	}
	if ep, ok := s.endpointsByName[strings.TrimSpace(rec.Endpoint)]; ok {
		switch kind := strings.TrimSpace(strings.ToLower(ep.Workflow)); kind {
		case "review", "reviewer":
			return "review"
		case "fix", "pr-fix":
			return "fix"
		case "merge", "merged":
			return kind
		}
	}
	summary := strings.ToUpper(rec.ErrorSummary)
	switch strings.ToLower(strings.TrimSpace(rec.EventType)) {
	case "pull_request_review":
		if strings.Contains(summary, "APPROVED") {
			return "merge"
		}
		return "fix"
	case "issue_comment":
		return "fix"
	case "check_suite":
		return "merge"
	case "pull_request":
		if strings.Contains(summary, "MERGED") {
			return "merged"
		}
		return "review"
	}
	return ""
}

func (s *Server) operatorSkip(ctx context.Context, msg channel.IncomingMessage, prKey string) (string, bool, error) {
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			return "Queue is blocked — use /retry to re-run or /cancel to clear it.", true, nil
		}
	}
	d := s.liveDispatcher(prKey)
	if d == nil {
		return "No live run for " + prKey + " — nothing to skip.", true, nil
	}
	repo, _, _ := splitPRKey(prKey)
	s.abortActiveSession(ctx, prKey, repo)
	d.requestSkip()
	return "Skipping after the current step — dequeuing next.", true, nil
}

func (s *Server) operatorCancel(ctx context.Context, msg channel.IncomingMessage, prKey string) (string, bool, error) {
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			if uerr := s.prQueues.Unblock(ctx, prKey); uerr != nil {
				return "", true, uerr
			}
			s.reopenPRSurface(prKey)
			s.clearEscalated(prKey)
		}
	}
	d := s.liveDispatcher(prKey)
	repo, _, _ := splitPRKey(prKey)
	s.abortActiveSession(ctx, prKey, repo)
	dropped := 0
	if d != nil {
		d.requestCancel()
	} else if s.prQueues != nil {
		if n, err := s.prQueues.ClearQueue(ctx, prKey); err == nil {
			dropped = n
		}
		if err := s.prQueues.ClearActive(ctx, prKey); err != nil {
			return "", true, err
		}
	}
	rest := ""
	if dropped > 0 {
		rest = fmt.Sprintf(" (%d queued item(s) cleared)", dropped)
	}
	return "Cancelled active work for " + prKey + rest + ".", true, nil
}

func (s *Server) enqueueSyntheticRetry(ctx context.Context, repo, number, kind, prKey string) error {
	body, eventType, err := s.buildRetryBody(ctx, repo, number, kind)
	if err != nil {
		return err
	}
	var ep config.EndpointConfig
	found := false
	for _, candidate := range s.endpoints {
		if normalizeRepoKey(candidate.Repository) != normalizeRepoKey(repo) {
			continue
		}
		if strings.TrimSpace(strings.ToLower(candidate.Workflow)) == strings.TrimSpace(strings.ToLower(kind)) {
			ep = candidate
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("no %s endpoint for repo %s", kind, repo)
	}
	deliveryID := fmt.Sprintf("%s%d", operatorRetryDeliveryPrefix, time.Now().UnixNano())
	item := dispatchItem{
		ep:         ep,
		body:       body,
		deliveryID: deliveryID,
		eventType:  eventType,
		prKey:      prKey,
		lane:       laneOfWorkflow(kind),
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	lockKey := "pr:" + prKey
	d, ok := s.dispatchers[lockKey]
	if !ok || d.exiting {
		d = newDispatcher(s, lockKey)
		s.dispatchers[lockKey] = d
		s.dispatchWG.Add(1)
		go d.run(s.shutdownCtx)
	}
	if d.queueLen() >= s.maxQueuedPerKey {
		return fmt.Errorf("pr queue full")
	}
	item.queueID = s.mirrorEnqueue(prKey, ep.Name, eventType, 0, lanePriority(item.lane))
	if !d.push(item) {
		s.mirrorDelete(item.queueID)
		return fmt.Errorf("pr queue full")
	}
	return nil
}

func (s *Server) buildRetryBody(ctx context.Context, repo, number, kind string) ([]byte, string, error) {
	if s.retryBodyBuilder != nil {
		return s.retryBodyBuilder(ctx, repo, number, kind)
	}
	return defaultRetryBody(ctx, repo, number, kind, s.endpoints)
}

type ghPR struct {
	Number   int    `json:"number"`
	URL      string `json:"url"`
	Author   string `json:"author"`
	HeadRef  string `json:"headRef"`
	HeadSHA  string `json:"headSHA"`
	HeadRepo string `json:"headRepo"`
	BaseRef  string `json:"baseRef"`
	Title    string `json:"title"`
}

func ghLookupPR(ctx context.Context, repo, number string) (ghPR, error) {
	var out ghPR
	timeoutCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(timeoutCtx, "gh", "pr", "view", number, "--repo", repo,
		"--json", "number,url,author,headRefName,headRefOid,headRepository,baseRefName,title")
	raw, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("lookup PR %s#%s: %w", repo, number, err)
	}
	var decoded struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
		Author struct {
			Login string `json:"login"`
		} `json:"author"`
		HeadRef  string `json:"headRefName"`
		HeadSHA  string `json:"headRefOid"`
		HeadRepo *struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"headRepository"`
		BaseRef string `json:"baseRefName"`
		Title   string `json:"title"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return out, fmt.Errorf("lookup PR %s#%s: decode: %w", repo, number, err)
	}
	headRepo := ""
	if decoded.HeadRepo != nil {
		headRepo = strings.TrimSpace(decoded.HeadRepo.NameWithOwner)
	}
	out = ghPR{
		Number:   decoded.Number,
		URL:      strings.TrimSpace(decoded.URL),
		Author:   strings.TrimSpace(decoded.Author.Login),
		HeadRef:  strings.TrimSpace(decoded.HeadRef),
		HeadSHA:  normalizeRevision(decoded.HeadSHA),
		HeadRepo: headRepo,
		BaseRef:  strings.TrimSpace(decoded.BaseRef),
		Title:    strings.TrimSpace(decoded.Title),
	}
	if out.Number <= 0 || out.HeadRef == "" || out.HeadSHA == "" {
		return out, fmt.Errorf("lookup PR %s#%s: incomplete head identity", repo, number)
	}
	return out, nil
}

func defaultRetryBody(ctx context.Context, repo, number, kind string, endpoints map[string]config.EndpointConfig) ([]byte, string, error) {
	n, err := strconv.Atoi(strings.TrimSpace(number))
	if err != nil || n <= 0 || strings.TrimSpace(repo) == "" {
		return nil, "", fmt.Errorf("invalid pr reference %s#%s", repo, number)
	}
	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind != "review" && kind != "fix" && kind != "merge" {
		return nil, "", fmt.Errorf("cannot re-run kind %q", kind)
	}
	pr, err := ghLookupPR(ctx, repo, number)
	if err != nil {
		return nil, "", err
	}
	var trigger string
	for _, ep := range endpoints {
		if normalizeRepoKey(ep.Repository) != normalizeRepoKey(repo) {
			continue
		}
		if strings.TrimSpace(strings.ToLower(ep.Workflow)) != kind {
			continue
		}
		for _, t := range ep.CommentTrigger {
			if strings.TrimSpace(t) != "" {
				trigger = strings.TrimSpace(t)
				break
			}
		}
	}
	switch kind {
	case "review", "fix":
		if trigger == "" {
			return nil, "", fmt.Errorf("no comment trigger configured for %s", kind)
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
			"comment":         map[string]any{"body": trigger, "user": map[string]any{"login": "operator"}},
			"repository":      map[string]any{"full_name": repo},
			"branch":          pr.HeadRef,
			"head_revision":   pr.HeadSHA,
			"head_repository": pr.HeadRepo,
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, "", err
		}
		return raw, "issue_comment", nil
	default:
		body := map[string]any{
			"action": "completed",
			"check_suite": map[string]any{
				"status":      "completed",
				"conclusion":  "success",
				"app":         map[string]any{"name": "GitHub Actions"},
				"head_branch": pr.HeadRef,
				"head_sha":    pr.HeadSHA,
				"pull_requests": []any{map[string]any{
					"number":   pr.Number,
					"html_url": pr.URL,
					"title":    pr.Title,
					"state":    "open",
					"head":     map[string]any{"ref": pr.HeadRef},
					"base":     map[string]any{"ref": pr.BaseRef},
				}},
			},
			"repository": map[string]any{"full_name": repo},
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, "", err
		}
		return raw, "check_suite", nil
	}
}
