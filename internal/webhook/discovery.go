package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/store"
)

const (
	// Bounded so the first tick after a deploy cannot flood the model; the
	// rest are found on later ticks.
	discoveryMaxPerTick = 3
	discoveryListLimit  = 100
)

type openPR struct {
	Number      int       `json:"number"`
	IsDraft     bool      `json:"isDraft"`
	HeadRefName string    `json:"headRefName"`
	HeadRefOid  string    `json:"headRefOid"`
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	CreatedAt   time.Time `json:"createdAt"`
	Author      struct {
		Login string `json:"login"`
	} `json:"author"`
}

type openPRLister func(ctx context.Context, repo string) ([]openPR, error)

func ghListOpenPRs(ctx context.Context, repo string) ([]openPR, error) {
	ctx, cancel := context.WithTimeout(ctx, prStateLookupTimeout)
	defer cancel()
	out, err := runGH(ctx, "pr", "list", "--repo", repo, "--state", "open",
		"--json", "number,isDraft,headRefName,headRefOid,headRepository,url,title,author,createdAt",
		"--limit", strconv.Itoa(discoveryListLimit))
	if err != nil {
		return nil, err
	}
	var prs []openPR
	if err := json.Unmarshal(out, &prs); err != nil {
		return nil, fmt.Errorf("decode open PRs of %s: %w", repo, err)
	}
	return prs, nil
}

type discoveredPR struct {
	repo  string
	prKey string
	pr    openPR
}

// discoverUnknownPRs queues a review for open PRs occa has no record of, such
// as one whose opened event never arrived. Reconcile only walks PRs it knows.
func (s *Server) discoverUnknownPRs(ctx context.Context) {
	if s.stages == nil || s.prQueues == nil {
		return
	}
	list := s.openPRs
	if list == nil {
		list = ghListOpenPRs
	}
	var cutoff time.Time
	if s.retentionAge > 0 {
		cutoff = time.Now().Add(-s.retentionAge)
	}
	var found []discoveredPR
	for _, repo := range s.reviewRepos() {
		if ctx.Err() != nil {
			return
		}
		prs, err := list(ctx, repo)
		if err != nil {
			slog.Warn("webhook: discovery list failed", "repo", repo, "error", err)
			continue
		}
		for _, pr := range prs {
			if pr.Number <= 0 || pr.IsDraft || pr.CreatedAt.IsZero() || pr.CreatedAt.Before(cutoff) {
				continue
			}
			prKey := repo + "#" + strconv.Itoa(pr.Number)
			if s.prKnown(ctx, prKey) {
				continue
			}
			found = append(found, discoveredPR{repo: repo, prKey: prKey, pr: pr})
		}
	}
	slices.SortStableFunc(found, func(a, b discoveredPR) int { return a.pr.CreatedAt.Compare(b.pr.CreatedAt) })
	queued := 0
	for _, d := range found {
		if queued >= discoveryMaxPerTick || ctx.Err() != nil {
			return
		}
		if s.enqueueDiscovered(ctx, d) {
			queued++
		}
	}
}

func (s *Server) reviewRepos() []string {
	var repos []string
	for _, ep := range s.endpoints {
		if !strings.EqualFold(strings.TrimSpace(ep.Workflow), "review") {
			continue
		}
		if repo := normalizeRepoKey(ep.Repository); repo != "" && !slices.Contains(repos, repo) {
			repos = append(repos, repo)
		}
	}
	slices.Sort(repos)
	return repos
}

// prKnown fails closed: a lookup error counts as known, so discovery never
// queues a PR it could not check.
func (s *Server) prKnown(ctx context.Context, prKey string) bool {
	if _, err := s.stages.repo.GetWorkItem(ctx, prKey); !errors.Is(err, store.ErrNotFound) {
		if err != nil {
			slog.Warn("webhook: discovery lookup failed", "pr_key", prKey, "error", err)
		}
		return true
	}
	row, err := s.prQueues.Get(ctx, prKey)
	if err != nil {
		slog.Warn("webhook: discovery lookup failed", "pr_key", prKey, "error", err)
		return true
	}
	return row != nil
}

// enqueueDiscovered queues the review the Review button would. The PR author
// is checked against the endpoint's ignored authors first: the review body
// is an occa comment, so admission would otherwise see occa as the actor.
func (s *Server) enqueueDiscovered(ctx context.Context, d discoveredPR) bool {
	ep, ok := s.reviewEndpointForPR(d.prKey)
	if !ok {
		return false
	}
	if reason := ignoredAuthorReason(ep, "pull_request", WebhookEnvelope{"pr_author": d.pr.Author.Login}); reason != "" {
		return false
	}
	trigger := s.reviewTrigger(ep)
	if trigger == "" {
		slog.Warn("webhook: discovery skipped, review endpoint has no comment trigger", "pr_key", d.prKey, "endpoint", ep.Name)
		return false
	}
	body := internalReviewCommentBody(d.repo, strconv.Itoa(d.pr.Number), trigger, d.pr.URL, d.pr.HeadRefName, "")
	item := dispatchItem{
		ep:          ep,
		body:        body,
		deliveryID:  fmt.Sprintf("%s%d", discoverDeliveryPrefix, time.Now().UnixNano()),
		eventType:   "issue_comment",
		payloadHash: payloadHashOf(body),
	}
	key := WebhookExecutionKey{Repository: d.repo, Branch: d.pr.HeadRefName}
	if _, admitted, why := s.enqueue(ep, key, item); !admitted {
		slog.Warn("webhook: discovery deferred", "pr_key", d.prKey, "reason", why)
		return false
	}
	if err := s.stages.ensureWorkItem(ctx, d.prKey); err != nil {
		s.stages.warn("discover", ep.Name, d.prKey, item.deliveryID, err)
	}
	slog.Info("webhook: discovered unknown PR", "pr_key", d.prKey, "head", shortRevision(d.pr.HeadRefOid))
	return true
}
