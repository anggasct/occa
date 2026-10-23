package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type ReconcileFacts struct {
	State         string
	HeadSHA       string
	HeadRef       string
	BaseRef       string
	Title         string
	URL           string
	Author        string
	HeadRepo      string
	CIGreen       bool
	ReviewVerdict string
}

type ReconcileFactsResolver func(ctx context.Context, repo, number string) (ReconcileFacts, error)

func (s *Server) SetReconcileFacts(fn ReconcileFactsResolver) {
	s.reconcileFacts = fn
}

func (s *Server) reconcileLoop(ctx context.Context) {
	defer s.dispatchWG.Done()
	if s.prQueues == nil {
		return
	}
	s.reconcileOnce(ctx)
	ticker := time.NewTicker(s.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileOnce(ctx)
		}
	}
}

func (s *Server) reconcileOnce(ctx context.Context) {
	if !s.reconcileRunning.CompareAndSwap(false, true) {
		return
	}
	defer s.reconcileRunning.Store(false)
	s.drainSpool(ctx)
	if s.prQueues == nil {
		return
	}
	rows, err := s.prQueues.ListUnresolved(ctx)
	if err != nil {
		slog.Warn("webhook: reconcile list failed", "error", err)
		return
	}
	bound := make([]store.PRRun, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.PRKey) == "" {
			continue
		}
		if strings.TrimSpace(row.ThreadID) == "" {
			if rec, ok := s.tasks.ResolvePRThread(row.PRKey); ok {
				row.ThreadID = strings.TrimSpace(rec.ThreadID)
			}
		}
		if strings.TrimSpace(row.ThreadID) == "" {
			continue
		}
		bound = append(bound, row)
		if len(bound) >= s.reconcileMaxPRs {
			break
		}
	}
	for _, row := range bound {
		if ctx.Err() != nil {
			return
		}
		s.reconcilePR(ctx, row)
	}
}

func (s *Server) reconcilePR(ctx context.Context, row store.PRRun) {
	repo, number, ok := splitPRKey(row.PRKey)
	if !ok {
		return
	}
	facts, err := s.reconcileFactsFor(ctx, repo, number)
	if err != nil {
		slog.Warn("webhook: reconcile facts failed", "pr_key", row.PRKey, "error", err)
		return
	}
	if !strings.EqualFold(strings.TrimSpace(facts.State), "OPEN") {
		return
	}
	if strings.TrimSpace(facts.HeadSHA) == "" {
		return
	}
	verdict := strings.ToLower(strings.TrimSpace(facts.ReviewVerdict))
	if verdict == "approved" && facts.CIGreen {
		if s.reconcileLanePending(ctx, row.PRKey, "merge") {
			return
		}
		s.reconcileEnqueue(ctx, row, "merge", facts, "approved and ci green")
		return
	}
	if verdict == "" {
		if s.reconcileLanePending(ctx, row.PRKey, "review") {
			return
		}
		s.reconcileEnqueue(ctx, row, "review", facts, "no verdict on head")
	}
}

func (s *Server) reconcileFactsFor(ctx context.Context, repo, number string) (ReconcileFacts, error) {
	resolver := s.reconcileFacts
	if resolver == nil {
		resolver = ghReconcileFacts
	}
	return resolver(ctx, repo, number)
}

func (s *Server) reconcileLanePending(ctx context.Context, prKey, lane string) bool {
	if s.prQueues == nil {
		return false
	}
	if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil {
		if strings.EqualFold(strings.TrimSpace(row.ActiveKind), lane) && row.ActiveDelivery != 0 {
			return true
		}
	}
	items, err := s.prQueues.ListItems(ctx, prKey)
	if err != nil {
		return false
	}
	for _, item := range items {
		ep, ok := s.endpointsByName[strings.TrimSpace(item.Endpoint)]
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(ep.Workflow), lane) {
			return true
		}
	}
	return false
}

func (s *Server) reconcileFloorKey(prKey, lane, head string) string {
	return strings.TrimSpace(prKey) + "|" + lane + "|" + normalizeRevision(head)
}

func (s *Server) reconcileEnqueue(ctx context.Context, row store.PRRun, lane string, facts ReconcileFacts, reason string) {
	repo, _, _ := splitPRKey(row.PRKey)
	ep, ok := s.endpointForWorkflow(repo, lane)
	if !ok {
		slog.Warn("webhook: reconcile endpoint missing", "pr_key", row.PRKey, "lane", lane)
		return
	}
	key := s.reconcileFloorKey(row.PRKey, lane, facts.HeadSHA)
	now := time.Now()
	s.reconcileMu.Lock()
	if last, seen := s.reconcileLast[key]; seen && now.Sub(last) < s.reconcileFloor {
		s.reconcileMu.Unlock()
		return
	}
	s.reconcileMu.Unlock()

	body, eventType, err := reconcileBody(ep, lane, repo, facts)
	if err != nil {
		slog.Warn("webhook: reconcile body failed", "pr_key", row.PRKey, "lane", lane, "error", err)
		return
	}
	bucket := now.Unix() / int64(s.reconcileFloor/time.Second)
	deliveryID := fmt.Sprintf("reconcile-%s-%s-%d", lane, normalizeRevision(facts.HeadSHA), bucket)
	if receipt, getErr := s.deliveries.Get(ctx, ep.Name, deliveryID); getErr == nil && receipt != nil {
		s.reconcileMu.Lock()
		s.reconcileLast[key] = now
		s.reconcileMu.Unlock()
		return
	}
	item := dispatchItem{
		ep:          ep,
		body:        body,
		deliveryID:  deliveryID,
		eventType:   eventType,
		payloadHash: sha256Hex(body),
	}
	if _, admitted, why := s.enqueue(ep, ExtractExecutionKey(body), item); !admitted {
		slog.Warn("webhook: reconcile deferred", "pr_key", row.PRKey, "lane", lane, "reason", why)
		return
	}
	s.reconcileMu.Lock()
	s.reconcileLast[key] = now
	s.reconcileMu.Unlock()
	slog.Info("webhook: reconcile enqueued",
		"lane", lane,
		"reason", reason,
		"head", normalizeRevision(facts.HeadSHA),
		"pr_key", row.PRKey,
		"delivery_id", deliveryID,
	)
}

func (s *Server) endpointForWorkflow(repo, workflow string) (config.EndpointConfig, bool) {
	want := normalizeRepoKey(repo)
	for _, ep := range s.endpoints {
		if normalizeRepoKey(ep.Repository) != want {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(ep.Workflow), workflow) {
			return ep, true
		}
	}
	return config.EndpointConfig{}, false
}

func reconcileBody(ep config.EndpointConfig, lane, repo string, facts ReconcileFacts) ([]byte, string, error) {
	number := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(facts.URL), ""))
	_ = number
	prNumbers := prNumberFromFacts(facts)
	switch lane {
	case "review":
		trigger := ""
		for _, candidate := range ep.CommentTrigger {
			if strings.TrimSpace(candidate) != "" {
				trigger = strings.TrimSpace(candidate)
				break
			}
		}
		if trigger == "" {
			return nil, "", fmt.Errorf("no comment trigger configured for review")
		}
		body := map[string]any{
			"action": "created",
			"issue": map[string]any{
				"number":       prNumbers,
				"state":        "open",
				"title":        facts.Title,
				"user":         map[string]any{"login": facts.Author},
				"pull_request": map[string]any{"html_url": facts.URL},
			},
			"comment":         map[string]any{"body": trigger, "user": map[string]any{"login": "occa-reconcile"}},
			"repository":      map[string]any{"full_name": repo},
			"branch":          facts.HeadRef,
			"head_revision":   facts.HeadSHA,
			"head_repository": facts.HeadRepo,
		}
		raw, err := json.Marshal(body)
		return raw, "issue_comment", err
	case "merge":
		body := map[string]any{
			"action": "completed",
			"check_suite": map[string]any{
				"status":      "completed",
				"conclusion":  "success",
				"app":         map[string]any{"name": "GitHub Actions"},
				"head_branch": facts.HeadRef,
				"head_sha":    facts.HeadSHA,
				"pull_requests": []any{map[string]any{
					"number":   prNumbers,
					"html_url": facts.URL,
					"title":    facts.Title,
					"state":    "open",
					"head":     map[string]any{"ref": facts.HeadRef},
					"base":     map[string]any{"ref": facts.BaseRef},
				}},
			},
			"repository": map[string]any{"full_name": repo},
		}
		raw, err := json.Marshal(body)
		return raw, "check_suite", err
	}
	return nil, "", fmt.Errorf("unsupported reconcile lane %q", lane)
}

func prNumberFromFacts(facts ReconcileFacts) any {
	url := strings.TrimSpace(facts.URL)
	if idx := strings.LastIndex(url, "/"); idx >= 0 && idx+1 < len(url) {
		if n := strings.TrimSpace(url[idx+1:]); n != "" {
			if number, err := parsePRNumber(n); err == nil {
				return number
			}
		}
	}
	return 0
}

func parsePRNumber(raw string) (int, error) {
	var n int
	_, err := fmt.Sscanf(strings.TrimSpace(raw), "%d", &n)
	return n, err
}

type ghCheckContext struct {
	TypeName   string `json:"__typename"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	State      string `json:"state"`
}

func ghReconcileFacts(ctx context.Context, repo, number string) (ReconcileFacts, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(lookupCtx, "gh", "pr", "view", strings.TrimSpace(number), "--repo", strings.TrimSpace(repo),
		"--json", "state,headRefOid,headRefName,baseRefName,title,url,author,headRepository,reviewDecision,statusCheckRollup")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return ReconcileFacts{}, fmt.Errorf("gh pr view: %s", detail)
	}
	var raw struct {
		State             string           `json:"state"`
		HeadRefOid        string           `json:"headRefOid"`
		HeadRefName       string           `json:"headRefName"`
		BaseRefName       string           `json:"baseRefName"`
		Title             string           `json:"title"`
		URL               string           `json:"url"`
		ReviewDecision    string           `json:"reviewDecision"`
		StatusCheckRollup []ghCheckContext `json:"statusCheckRollup"`
		Author            struct {
			Login string `json:"login"`
		} `json:"author"`
		HeadRepository struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"headRepository"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return ReconcileFacts{}, fmt.Errorf("gh pr view decode: %w", err)
	}
	facts := ReconcileFacts{
		State:    strings.TrimSpace(raw.State),
		HeadSHA:  normalizeRevision(raw.HeadRefOid),
		HeadRef:  strings.TrimSpace(raw.HeadRefName),
		BaseRef:  strings.TrimSpace(raw.BaseRefName),
		Title:    strings.TrimSpace(raw.Title),
		URL:      strings.TrimSpace(raw.URL),
		Author:   strings.TrimSpace(raw.Author.Login),
		HeadRepo: strings.TrimSpace(raw.HeadRepository.NameWithOwner),
		CIGreen:  checksGreen(raw.StatusCheckRollup),
	}
	switch strings.ToUpper(strings.TrimSpace(raw.ReviewDecision)) {
	case "APPROVED":
		facts.ReviewVerdict = "approved"
	case "CHANGES_REQUESTED":
		facts.ReviewVerdict = "changes_requested"
	}
	return facts, nil
}

func checksGreen(checks []ghCheckContext) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		switch strings.ToUpper(strings.TrimSpace(check.TypeName)) {
		case "STATUSCONTEXT":
			if strings.ToUpper(strings.TrimSpace(check.State)) != "SUCCESS" {
				return false
			}
		default:
			if strings.ToUpper(strings.TrimSpace(check.Status)) != "COMPLETED" {
				return false
			}
			switch strings.ToUpper(strings.TrimSpace(check.Conclusion)) {
			case "SUCCESS", "NEUTRAL", "SKIPPED":
			default:
				return false
			}
		}
	}
	return true
}
