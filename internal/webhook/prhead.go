package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type PRHead struct {
	Branch     string
	SHA        string
	HeadRepo   string
	BaseBranch string
}

type PRHeadResolver interface {
	ResolvePRHead(ctx context.Context, repo string, number int) (PRHead, error)
}

type ghPRHeadResolver struct {
	bin     string
	timeout time.Duration
}

func (r *ghPRHeadResolver) ResolvePRHead(ctx context.Context, repo string, number int) (PRHead, error) {
	bin := r.bin
	if bin == "" {
		bin = "gh"
	}
	timeout := r.timeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, bin, "pr", "view", strconv.Itoa(number), "--repo", repo, "--json", "headRefName,headRefOid,headRepository,baseRefName,isCrossRepository")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return PRHead{}, fmt.Errorf("lookup PR %s#%d: %s", repo, number, detail)
	}
	var raw struct {
		HeadRefName    string `json:"headRefName"`
		HeadRefOid     string `json:"headRefOid"`
		HeadRepository *struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"headRepository"`
		BaseRefName       string `json:"baseRefName"`
		IsCrossRepository bool   `json:"isCrossRepository"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return PRHead{}, fmt.Errorf("lookup PR %s#%d: decode response: %v", repo, number, err)
	}
	branch := strings.TrimSpace(raw.HeadRefName)
	sha := normalizeRevision(raw.HeadRefOid)
	headRepo := ""
	if raw.HeadRepository != nil {
		headRepo = strings.TrimSpace(raw.HeadRepository.NameWithOwner)
	}
	if branch == "" || sha == "" || !isValidRepoFullName(headRepo) {
		return PRHead{}, fmt.Errorf("lookup PR %s#%d: incomplete head identity", repo, number)
	}
	if raw.IsCrossRepository || !strings.EqualFold(headRepo, strings.TrimSpace(repo)) {
		return PRHead{}, fmt.Errorf("fork PR %s#%d (head %s) is not supported", repo, number, headRepo)
	}
	return PRHead{
		Branch:     branch,
		SHA:        sha,
		HeadRepo:   headRepo,
		BaseBranch: strings.TrimSpace(raw.BaseRefName),
	}, nil
}

func parseIssueCommentPR(body []byte) (string, int, bool) {
	if len(body) == 0 {
		return "", 0, false
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, false
	}
	issue, _ := payload["issue"].(map[string]any)
	if issue == nil {
		return "", 0, false
	}
	link, _ := issue["pull_request"].(map[string]any)
	if link == nil {
		return "", 0, false
	}
	repo := extractRepoFullName(payload["repository"])
	if repo == "" {
		return "", 0, false
	}
	var number int
	switch v := issue["number"].(type) {
	case float64:
		number = int(v)
	case int:
		number = v
	case int64:
		number = int(v)
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return "", 0, false
		}
		number = int(n)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return "", 0, false
		}
		number = n
	default:
		return "", 0, false
	}
	if number <= 0 {
		return "", 0, false
	}
	return repo, number, true
}

func seedEnrichedBody(body []byte, head PRHead, baseRepo string) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	payload["branch"] = head.Branch
	payload["head_revision"] = head.SHA
	payload["head_repository"] = head.HeadRepo
	if strings.TrimSpace(baseRepo) != "" {
		if _, ok := payload["repository"]; !ok {
			payload["repository"] = baseRepo
		}
	}
	if strings.TrimSpace(head.BaseBranch) != "" {
		payload["base_branch"] = head.BaseBranch
	}
	return json.Marshal(payload)
}
