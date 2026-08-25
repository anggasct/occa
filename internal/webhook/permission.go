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
	if !isWebhookWorkflow(workflow) {
		return nil, fmt.Errorf("unsupported webhook workflow %q", input.Workflow)
	}
	if !isSafeRepository(input.Repository) || strings.TrimSpace(input.PRNumber) == "" {
		return nil, fmt.Errorf("repository and pull request are required")
	}
	if !isDigits(input.PRNumber) {
		return nil, fmt.Errorf("invalid pull request number")
	}
	if workflow == "github_fix" && !isSafeBranch(input.Branch) {
		return nil, fmt.Errorf("invalid branch scope")
	}
	if strings.TrimSpace(input.Worktree) == "" || !filepath.IsAbs(input.Worktree) {
		return nil, fmt.Errorf("absolute worktree is required")
	}
	if strings.TrimSpace(input.ProjectDocsRoot) == "" || !filepath.IsAbs(input.ProjectDocsRoot) {
		return nil, fmt.Errorf("absolute project-doc root is required")
	}

	worktree := canonicalPath(input.Worktree)
	docs := canonicalPath(input.ProjectDocsRoot)
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
			policy = append(policy, relay.PermissionRule{Permission: permission, Pattern: "project-docs/**", Action: "allow"})
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
	deny("external_directory", "*")

	switch workflow {
	case "github_reviewer":
		deny("edit", "*")
		allowCurrentWorkspace()
		allowExternalWorktree()
		allowExternalDocs()
		for _, command := range []string{
			"git status*",
			"git diff*",
			"git log*",
			"gh pr view " + input.PRNumber + " --repo " + input.Repository + "*",
			"gh pr checks " + input.PRNumber + " --repo " + input.Repository + "*",
			"gh pr review " + input.PRNumber + " --repo " + input.Repository + "*",
		} {
			allowCommand(command)
		}
	case "github_fix":
		allowCurrentWorkspace()
		allowExternalWorktree()
		allowExternalDocs()
		allowPath("edit", worktree)
		policy = append(policy, relay.PermissionRule{Permission: "edit", Pattern: "*", Action: "allow"})
		for _, command := range []string{
			"git status*",
			"git diff*",
			"git log*",
			"git show*",
			"git add*",
			"git commit*",
			"git push origin " + input.Branch,
			"go test*",
			"go vet*",
			"gofmt*",
			"make test*",
			"make lint*",
			"make check*",
			"make build*",
			"gh pr view " + input.PRNumber + " --repo " + input.Repository + "*",
			"gh pr checks " + input.PRNumber + " --repo " + input.Repository + "*",
			"gh pr comment " + input.PRNumber + " --repo " + input.Repository + "*",
		} {
			allowCommand(command)
		}
		for _, command := range []string{
			"git push --force*",
			"git push -f*",
			"git checkout*",
			"git switch*",
			"git worktree*",
			"git reset*",
			"git clean*",
			"git stash*",
			"gh pr merge*",
			"gh pr review*",
		} {
			deny("bash", command)
		}
	case "github_merge":
		deny("edit", "*")
		for _, command := range []string{
			"gh pr view " + input.PRNumber + " --repo " + input.Repository + "*",
			"gh pr checks " + input.PRNumber + " --repo " + input.Repository + "*",
			"gh pr merge " + input.PRNumber + " --repo " + input.Repository + "*",
		} {
			allowCommand(command)
		}
	case "github_merged":
		allowPath("read", docs)
		allowPath("glob", docs)
		allowPath("grep", docs)
		allowPath("edit", docs)
		policy = append(policy, relay.PermissionRule{Permission: "edit", Pattern: "project-docs/**", Action: "allow"})
		allowExternalDocs()
		for _, command := range []string{
			"gh pr view " + input.PRNumber + " --repo " + input.Repository + "*",
			"git -C " + filepath.ToSlash(docs) + " status*",
			"git -C " + filepath.ToSlash(docs) + " diff*",
			"git -C " + filepath.ToSlash(docs) + " add*",
			"git -C " + filepath.ToSlash(docs) + " commit*",
			"git -C " + filepath.ToSlash(docs) + " push*",
		} {
			allowCommand(command)
		}
		for _, command := range []string{"gh pr review*", "gh pr merge*"} {
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
