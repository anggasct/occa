package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
)

// errReviewNotPosted: a review run finished without a review on GitHub, so
// it is not a review of the head (no dedupe hit, retried, then blocked).
var errReviewNotPosted = errors.New("review not posted: the agent finished without a review on GitHub")

// reviewPostedFunc reports whether the account occa runs gh as submitted a
// review on the PR at or after since.
type reviewPostedFunc func(ctx context.Context, repo, number string, since time.Time) (bool, error)

// skipReplyRe matches an intentional skip: a line that is SKIP, or a status
// line reading SKIP (e.g. "✅ Status: SKIP — no actionable findings").
var skipReplyRe = regexp.MustCompile(`(?m)^[^\pL\pN]*(?:(?i:status):\s*)?SKIP\b`)

func replyIsSkip(reply string) bool {
	return skipReplyRe.MatchString(reply)
}

// verifyReviewPosted checks a successful review run against GitHub. A SKIP
// reply is not checked; a lookup error keeps the run's result.
func (s *Server) verifyReviewPosted(ctx context.Context, ep config.EndpointConfig, workCtx *WebhookWorkContext, runStart time.Time) error {
	if workCtx == nil || replyIsSkip(workCtx.AgentReply) {
		return nil
	}
	repo, number, ok := splitPRKey(workCtx.PRKey)
	if !ok {
		return nil
	}
	check := s.reviewPosted
	if check == nil {
		check = s.ghReviewPostedSince
	}
	// One minute of slack for clock skew between this host and GitHub.
	since := runStart.Add(-time.Minute)
	posted, err := check(ctx, repo, number, since)
	if err != nil {
		slog.Warn("webhook: review posted check failed, keeping the run result", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "error", err)
		return nil
	}
	if posted {
		return nil
	}
	slog.Warn("webhook: review run posted no review", "endpoint", ep.Name, "pr_key", workCtx.PRKey, "delivery_id", workCtx.DeliveryID, "since", since.UTC().Format(time.RFC3339))
	return errReviewNotPosted
}

func (s *Server) ghReviewPostedSince(ctx context.Context, repo, number string, since time.Time) (bool, error) {
	login, err := s.ghLogin(ctx)
	if err != nil {
		return false, err
	}
	out, err := runGH(ctx, "api", "--paginate", "repos/"+strings.TrimSpace(repo)+"/pulls/"+strings.TrimSpace(number)+"/reviews",
		"--jq", `.[] | [.user.login, .submitted_at] | @tsv`)
	if err != nil {
		return false, err
	}
	return reviewedSince(string(out), login, since), nil
}

// reviewedSince scans "login<TAB>submitted_at" lines for a review by login at
// or after since.
func reviewedSince(lines, login string, since time.Time) bool {
	for _, line := range strings.Split(lines, "\n") {
		who, at, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || !strings.EqualFold(strings.TrimSpace(who), login) {
			continue
		}
		submitted, err := time.Parse(time.RFC3339, strings.TrimSpace(at))
		if err == nil && !submitted.Before(since) {
			return true
		}
	}
	return false
}

// ghLogin is the account occa runs gh as, looked up once.
func (s *Server) ghLogin(ctx context.Context) (string, error) {
	s.ghLoginMu.Lock()
	defer s.ghLoginMu.Unlock()
	if s.ghLoginCached != "" {
		return s.ghLoginCached, nil
	}
	out, err := runGH(ctx, "api", "user", "--jq", ".login")
	if err != nil {
		return "", err
	}
	login := strings.TrimSpace(string(out))
	if login == "" {
		return "", fmt.Errorf("gh api user: empty login")
	}
	s.ghLoginCached = login
	return login, nil
}
