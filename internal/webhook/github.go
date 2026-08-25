package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	githubAPIBaseURL       = "https://api.github.com"
	githubRequestTimeout   = 5 * time.Second
	githubResponseBodySize = 1 << 20
)

type GitHubPullRequestRef struct {
	Repository string
	Number     string
	HTMLURL    string
}

type PullRequestResolver interface {
	ResolvePullRequest(ctx context.Context, ref GitHubPullRequestRef) (WebhookExecutionKey, error)
}

type GitHubPullRequestResolver struct {
	client  *http.Client
	apiBase string
}

func NewGitHubPullRequestResolver(client *http.Client) *GitHubPullRequestResolver {
	if client == nil {
		client = &http.Client{Timeout: githubRequestTimeout}
	}
	return &GitHubPullRequestResolver{client: client, apiBase: githubAPIBaseURL}
}

func (r *GitHubPullRequestResolver) ResolvePullRequest(ctx context.Context, ref GitHubPullRequestRef) (WebhookExecutionKey, error) {
	if err := validateGitHubPullRequestRef(ref); err != nil {
		return WebhookExecutionKey{}, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, githubRequestTimeout)
	defer cancel()
	endpoint := strings.TrimRight(r.apiBase, "/") + "/repos/" + ref.Repository + "/pulls/" + ref.Number
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return WebhookExecutionKey{}, fmt.Errorf("build pull request inspection request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "occa-webhook")

	resp, err := r.client.Do(req)
	if err != nil {
		return WebhookExecutionKey{}, fmt.Errorf("inspect pull request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return WebhookExecutionKey{}, fmt.Errorf("inspect pull request: unexpected status %d", resp.StatusCode)
	}

	var payload struct {
		Number int `json:"number"`
		Base   struct {
			Repo struct {
				FullName string `json:"full_name"`
			} `json:"repo"`
		} `json:"base"`
		Head struct {
			Ref  string `json:"ref"`
			Repo *struct {
				FullName string `json:"full_name"`
			} `json:"repo"`
		} `json:"head"`
	}
	limited := io.LimitReader(resp.Body, githubResponseBodySize)
	if err := json.NewDecoder(limited).Decode(&payload); err != nil {
		return WebhookExecutionKey{}, fmt.Errorf("decode pull request inspection: %w", err)
	}

	if strconv.Itoa(payload.Number) != ref.Number {
		return WebhookExecutionKey{}, errors.New("pull request inspection number mismatch")
	}
	baseRepo := strings.TrimSpace(payload.Base.Repo.FullName)
	if !isValidRepoFullName(baseRepo) || !strings.EqualFold(baseRepo, ref.Repository) {
		return WebhookExecutionKey{}, errors.New("pull request inspection repository mismatch")
	}
	if payload.Head.Repo == nil {
		return WebhookExecutionKey{}, errors.New("pull request inspection has no head repository")
	}
	headRepo := strings.TrimSpace(payload.Head.Repo.FullName)
	branch := strings.TrimSpace(payload.Head.Ref)
	if !isValidRepoFullName(headRepo) || !isSafeBranch(branch) {
		return WebhookExecutionKey{}, errors.New("pull request inspection has an invalid head scope")
	}
	return WebhookExecutionKey{Repository: ref.Repository, HeadRepository: headRepo, Branch: branch}, nil
}

func extractIssueCommentPullRequestRef(body []byte) (GitHubPullRequestRef, error) {
	var payload struct {
		Repository any `json:"repository"`
		Issue      struct {
			Number      int `json:"number"`
			PullRequest struct {
				HTMLURL string `json:"html_url"`
			} `json:"pull_request"`
		} `json:"issue"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return GitHubPullRequestRef{}, fmt.Errorf("decode issue comment payload: %w", err)
	}
	ref := GitHubPullRequestRef{
		Repository: extractRepoFullName(payload.Repository),
		Number:     strconv.Itoa(payload.Issue.Number),
		HTMLURL:    strings.TrimSpace(payload.Issue.PullRequest.HTMLURL),
	}
	if err := validateGitHubPullRequestRef(ref); err != nil {
		return GitHubPullRequestRef{}, err
	}
	return ref, nil
}

func validateGitHubPullRequestRef(ref GitHubPullRequestRef) error {
	if !isValidRepoFullName(ref.Repository) || !isDigits(ref.Number) || ref.Number == "0" {
		return errors.New("invalid pull request reference")
	}
	parsed, err := url.Parse(ref.HTMLURL)
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, "github.com") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("invalid pull request URL")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 4 || !strings.EqualFold(parts[2], "pull") || !strings.EqualFold(parts[0]+"/"+parts[1], ref.Repository) || parts[3] != ref.Number {
		return errors.New("pull request URL does not match reference")
	}
	return nil
}
