package webhook

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/anggasct/occa/internal/relay"
)

type PermissionPolicyInput struct {
	Workflow        string
	Repository      string
	PRNumber        string
	Worktree        string
	ProjectDocsRoot string
	Branch          string
}

func BuildPermissionPolicy(input PermissionPolicyInput) (relay.PermissionRuleset, error) {
	workflow := strings.TrimSpace(strings.ToLower(input.Workflow))
	repository := strings.TrimSpace(input.Repository)
	if !isWebhookWorkflow(workflow) {
		return nil, fmt.Errorf("unsupported webhook workflow %q", input.Workflow)
	}
	if !isSafeRepository(repository) || strings.TrimSpace(input.PRNumber) == "" {
		return nil, fmt.Errorf("repository and pull request are required")
	}
	if !isDigits(input.PRNumber) {
		return nil, fmt.Errorf("invalid pull request number")
	}
	if workflow == "github_fix" && !isSafeBranch(input.Branch) {
		return nil, fmt.Errorf("invalid branch scope")
	}
	if !isSafeAbsolutePath(input.Worktree) {
		return nil, fmt.Errorf("safe absolute worktree is required")
	}
	if !isSafeAbsolutePath(input.ProjectDocsRoot) {
		return nil, fmt.Errorf("safe absolute project-doc root is required")
	}

	worktree := canonicalPath(strings.TrimSpace(input.Worktree))
	docs := canonicalPath(strings.TrimSpace(input.ProjectDocsRoot))
	policy := relay.PermissionRuleset{{Permission: "*", Pattern: "*", Action: "deny"}}
	allowPath := func(permission, root string) {
		for _, pattern := range []string{root, root + string(filepath.Separator) + "**"} {
			policy = append(policy, relay.PermissionRule{Permission: permission, Pattern: filepath.ToSlash(pattern), Action: "allow"})
		}
	}
	allowCurrentWorkspace := func() {
		for _, permission := range []string{"read", "glob", "grep", "lsp"} {
			policy = append(policy, relay.PermissionRule{Permission: permission, Pattern: "*", Action: "allow"})
		}
	}
	allowExternalDocs := func() {
		policy = append(policy,
			relay.PermissionRule{Permission: "external_directory", Pattern: filepath.ToSlash(docs), Action: "allow"},
			relay.PermissionRule{Permission: "external_directory", Pattern: filepath.ToSlash(docs) + "/**", Action: "allow"},
		)
		for _, permission := range []string{"read", "glob", "grep"} {
			allowPath(permission, docs)
		}
	}
	allowExternalWorktree := func() {
		policy = append(policy,
			relay.PermissionRule{Permission: "external_directory", Pattern: filepath.ToSlash(worktree), Action: "allow"},
			relay.PermissionRule{Permission: "external_directory", Pattern: filepath.ToSlash(worktree) + "/**", Action: "allow"},
		)
	}
	deny := func(permission, pattern string) {
		policy = append(policy, relay.PermissionRule{Permission: permission, Pattern: pattern, Action: "deny"})
	}
	allowCommand := func(command string) {
		policy = append(policy, relay.PermissionRule{Permission: "bash", Pattern: command, Action: "allow"})
	}
	allowSafeQuotedArgument := func(prefix, suffix string) {
		allowCommand(prefix + "*" + suffix)
		for _, character := range []string{";", "|", "&", "$", "`", "(", ")", "<", ">", "\\", "\"", "\n", "\r"} {
			deny("bash", prefix+"*"+character+"*"+suffix)
		}
	}
	deny("external_directory", "*")

	switch workflow {
	case "github_reviewer":
		deny("edit", "*")
		allowCurrentWorkspace()
		allowExternalWorktree()
		allowExternalDocs()
		for _, command := range []string{
			"git status --short",
			"git diff --check",
			"git diff --stat",
			"git diff origin/main...HEAD",
			"git log --oneline -5",
			"gh pr view " + input.PRNumber + " --repo " + repository,
			"gh pr view " + input.PRNumber + " --repo " + repository + " --json title,state",
			"gh pr view " + input.PRNumber + " --repo " + repository + " --json title,state,headRefName,baseRefName",
			"gh pr view " + input.PRNumber + " --repo " + repository + " --json files,commits,headRefName,baseRefName,title",
			"gh pr view " + input.PRNumber + " --repo " + repository + " --json mergedAt,mergeCommit,files,additions,deletions,changedFiles,labels,headRefName,baseRefName,title",
			"gh pr diff " + input.PRNumber + " --repo " + repository,
			"gh pr checks " + input.PRNumber + " --repo " + repository,
			"gh pr review " + input.PRNumber + " --repo " + repository + " --approve --body-file /tmp/review.md",
			"gh pr review " + input.PRNumber + " --repo " + repository + " --request-changes --body-file /tmp/review.md",
			"gh pr review " + input.PRNumber + " --repo " + repository + " --comment --body-file /tmp/review.md",
		} {
			allowCommand(command)
		}
	case "github_fix":
		allowCurrentWorkspace()
		allowExternalWorktree()
		allowExternalDocs()
		allowPath("edit", worktree)
		for _, command := range []string{
			"git status --short",
			"git diff --check",
			"git diff --stat",
			"git diff origin/main...HEAD",
			"git log --oneline -5",
			"git show --stat --oneline HEAD",
			"git add --all",
			"git push origin " + input.Branch,
			"go test ./... -count=1",
			"go test -race ./internal/webhook/... ./internal/relay/... ./internal/process/...",
			"go vet ./...",
			"gofmt -l .",
			"make test",
			"make lint",
			"make check",
			"make build",
			"make fmt",
			"gh pr view " + input.PRNumber + " --repo " + repository,
			"gh pr checks " + input.PRNumber + " --repo " + repository,
			"gh pr comment " + input.PRNumber + " --repo " + repository + " --body-file /tmp/review-fix.md",
		} {
			allowCommand(command)
		}
		allowSafeQuotedArgument("git commit -m \"", "\"")
		allowSafeQuotedArgument("gh pr comment "+input.PRNumber+" --repo "+repository+" --body \"", "\"")
		for _, command := range []string{
			"git push --force",
			"git push -f",
			"git checkout",
			"git switch",
			"git worktree",
			"git reset",
			"git clean",
			"git stash",
			"gh pr merge",
			"gh pr review",
		} {
			deny("bash", command)
		}
	case "github_merge":
		deny("edit", "*")
		for _, command := range []string{
			"gh pr view " + input.PRNumber + " --repo " + repository,
			"gh pr checks " + input.PRNumber + " --repo " + repository,
			"gh pr merge " + input.PRNumber + " --repo " + repository + " --squash --delete-branch",
			"gh pr merge " + input.PRNumber + " --repo " + repository + " --auto --squash --delete-branch",
		} {
			allowCommand(command)
		}
	case "github_merged":
		allowPath("read", docs)
		allowPath("glob", docs)
		allowPath("grep", docs)
		allowPath("edit", docs)
		allowExternalDocs()
		for _, command := range []string{
			"gh pr view " + input.PRNumber + " --repo " + repository,
			"git -C " + filepath.ToSlash(docs) + " status --short",
			"git -C " + filepath.ToSlash(docs) + " diff --check",
			"git -C " + filepath.ToSlash(docs) + " diff --stat",
			"git -C " + filepath.ToSlash(docs) + " add --all",
			"git -C " + filepath.ToSlash(docs) + " commit -m \"docs: update project documentation\"",
			"git -C " + filepath.ToSlash(docs) + " push origin main",
		} {
			allowCommand(command)
		}
		for _, command := range []string{"gh pr review", "gh pr merge"} {
			deny("bash", command)
		}
	}

	return policy, nil
}

func isWebhookWorkflow(workflow string) bool {
	switch workflow {
	case "github_reviewer", "github_fix", "github_merge", "github_merged":
		return true
	default:
		return false
	}
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isSafeRepository(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, part := range parts {
		for _, r := range part {
			if !isSafeScopeChar(r) {
				return false
			}
		}
	}
	return true
}

func isSafeBranch(value string) bool {
	if value == "" || strings.HasPrefix(value, "-") || strings.Contains(value, "..") {
		return false
	}
	for _, r := range value {
		if r != '/' && !isSafeScopeChar(r) {
			return false
		}
	}
	return true
}

func isSafeScopeChar(r rune) bool {
	return r == '-' || r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func isSafeAbsolutePath(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || !filepath.IsAbs(value) {
		return false
	}
	for _, r := range value {
		if r == ';' || r == '|' || r == '&' || r == '$' || r == '`' || r == '(' || r == ')' || r == '<' || r == '>' || r == '*' || r == '?' || r == '\n' || r == '\r' {
			return false
		}
	}
	return filepath.Clean(value) != string(filepath.Separator)
}

func canonicalPath(value string) string {
	clean := filepath.Clean(value)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return resolved
	}
	return clean
}

func projectDocsRoot(worktree string) string {
	repoRoot := filepath.Dir(filepath.Dir(filepath.Clean(worktree)))
	candidate := filepath.Join(repoRoot, "project-docs")
	if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
		return resolved
	}
	return candidate
}
