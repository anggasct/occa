package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anggasct/occa/internal/config"
)

// mergedSummary is what the merged lane reports about a merged PR.
type mergedSummary struct {
	Title        string
	Commits      int
	ChangedFiles int
	Additions    int
	Deletions    int
	MergedBy     string
	MergeCommit  string
}

type mergedSummaryFunc func(ctx context.Context, repo, number string) (mergedSummary, error)

// postMergedSummary replaces the built-in merged prompt's agent turn with one
// thread line built from the merged PR. A lookup failure still posts the
// title the event carried.
func (s *Server) postMergedSummary(ctx context.Context, ep config.EndpointConfig, envelope WebhookEnvelope, workCtx *WebhookWorkContext) {
	repo := strings.TrimSpace(stringValue(envelope["repository"]))
	number := strings.TrimSpace(stringValue(envelope["pr_number"]))
	lookup := s.mergedSummaryFacts
	if lookup == nil {
		lookup = ghMergedSummary
	}
	summary, err := lookup(ctx, repo, number)
	if err != nil {
		slog.Warn("webhook: merged summary lookup failed", "endpoint", ep.Name, "repo", repo, "pr_number", number, "error", err)
		summary = mergedSummary{}
	}
	if summary.Title == "" {
		summary.Title = strings.TrimSpace(stringValue(envelope["title"]))
	}
	text := FormatMergedSummary(repo, number, summary, err == nil)
	target := threadTargetFor(ep.Platform, ep.ChannelID, workCtx.ThreadID)
	if err := s.notifyThread(ctx, ep.Platform, target, text); err != nil {
		slog.Warn("webhook: merged summary post failed", "endpoint", ep.Name, "repo", repo, "pr_number", number, "error", err)
		return
	}
	slog.Info("webhook: merged summary posted", "endpoint", ep.Name, "repo", repo, "pr_number", number)
}

// FormatMergedSummary renders the merged line; without facts only the title.
func FormatMergedSummary(repo, number string, m mergedSummary, withFacts bool) string {
	head := "✅ Merged " + repo + "#" + number
	if title := strings.TrimSpace(m.Title); title != "" {
		head += " — " + title
	}
	if !withFacts {
		return head
	}
	parts := []string{
		fmt.Sprintf("%d commit%s", m.Commits, plural(m.Commits)),
		fmt.Sprintf("%d file%s (+%d −%d)", m.ChangedFiles, plural(m.ChangedFiles), m.Additions, m.Deletions),
	}
	if by := strings.TrimSpace(m.MergedBy); by != "" {
		parts = append(parts, "merged by "+by)
	}
	if sha := shortRevision(m.MergeCommit); sha != "" {
		parts = append(parts, sha)
	}
	return head + "\n" + strings.Join(parts, " · ")
}

func ghMergedSummary(ctx context.Context, repo, number string) (mergedSummary, error) {
	out, err := runGH(ctx, "pr", "view", number, "--repo", repo,
		"--json", "title,commits,changedFiles,additions,deletions,mergedBy,mergeCommit")
	if err != nil {
		return mergedSummary{}, err
	}
	var raw struct {
		Title        string            `json:"title"`
		Commits      []json.RawMessage `json:"commits"`
		ChangedFiles int               `json:"changedFiles"`
		Additions    int               `json:"additions"`
		Deletions    int               `json:"deletions"`
		MergedBy     struct {
			Login string `json:"login"`
		} `json:"mergedBy"`
		MergeCommit struct {
			OID string `json:"oid"`
		} `json:"mergeCommit"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return mergedSummary{}, fmt.Errorf("gh pr view decode: %w", err)
	}
	return mergedSummary{
		Title:        strings.TrimSpace(raw.Title),
		Commits:      len(raw.Commits),
		ChangedFiles: raw.ChangedFiles,
		Additions:    raw.Additions,
		Deletions:    raw.Deletions,
		MergedBy:     strings.TrimSpace(raw.MergedBy.Login),
		MergeCommit:  normalizeRevision(raw.MergeCommit.OID),
	}, nil
}
