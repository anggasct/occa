package webhook

import (
	"regexp"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/relay"
)

func TestBuildPermissionPolicyCoversEveryWebhookWorkflow(t *testing.T) {
	for _, workflow := range []string{"github_reviewer", "github_fix", "github_merge", "github_merged"} {
		t.Run(workflow, func(t *testing.T) {
			policy, err := BuildPermissionPolicy(PermissionPolicyInput{
				Workflow:        workflow,
				Repository:      "anggasct/occa",
				PRNumber:        "42",
				Branch:          "fix/webhook-permissions",
				Worktree:        "/srv/projects/occa/.worktree/pr-42",
				ProjectDocsRoot: "/srv/vault/1-projects/occa",
			})
			if err != nil {
				t.Fatalf("BuildPermissionPolicy: %v", err)
			}
			if len(policy) == 0 {
				t.Fatal("expected non-empty policy")
			}
			if policy[0] != (relay.PermissionRule{Permission: "*", Pattern: "*", Action: "deny"}) {
				t.Fatalf("policy must start with a fail-closed default, got %+v", policy[0])
			}
			for _, rule := range policy {
				if rule.Permission == "*" && rule.Pattern == "*" && rule.Action == "allow" {
					t.Fatal("policy contains a global allow rule")
				}
				if rule.Permission == "bash" && strings.ContainsAny(rule.Pattern, "*?") {
					t.Fatalf("bash rule is not an exact command scope: %+v", rule)
				}
			}
		})
	}
}

func TestBuildPermissionPolicyExactPathIsolation(t *testing.T) {
	policy, err := BuildPermissionPolicy(PermissionPolicyInput{
		Workflow:        "github_fix",
		Repository:      "anggasct/occa",
		PRNumber:        "42",
		Branch:          "fix/webhook-permissions",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: "/srv/vault/1-projects/occa",
	})
	if err != nil {
		t.Fatalf("BuildPermissionPolicy: %v", err)
	}

	if permissionAction(policy, "edit", "/srv/projects/occa/.worktree/pr-42/internal/webhook/permission.go") != "allow" {
		t.Fatal("fixer must allow edits below the exact PR worktree")
	}
	for _, path := range []string{
		"/srv/projects/occa/.worktree/other/internal/webhook/permission.go",
		"/srv/projects/occa/internal/webhook/permission.go",
		"/srv/vault/1-projects/occa/development-plan.md",
	} {
		if got := permissionAction(policy, "edit", path); got != "deny" {
			t.Fatalf("fixer edit %q = %q, want deny", path, got)
		}
	}
	if hasRule(policy, "edit", "*", "allow") {
		t.Fatal("fixer must not contain a wildcard edit allow")
	}
	if !hasRule(policy, "external_directory", "/srv/vault/1-projects/occa/**", "allow") {
		t.Fatal("fixer must be able to read the matching project docs")
	}
}

func TestBuildPermissionPolicyPostMergeEditsOnlyCanonicalDocsRoot(t *testing.T) {
	const docsRoot = "/srv/vault/1-projects/occa"
	policy, err := BuildPermissionPolicy(PermissionPolicyInput{
		Workflow:        "github_merged",
		Repository:      "anggasct/occa",
		PRNumber:        "42",
		Branch:          "fix/webhook-permissions",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: docsRoot,
	})
	if err != nil {
		t.Fatalf("BuildPermissionPolicy: %v", err)
	}

	if got := permissionAction(policy, "edit", docsRoot+"/development-plan.md"); got != "allow" {
		t.Fatalf("canonical docs edit = %q, want allow", got)
	}
	for _, path := range []string{
		"/srv/projects/occa/.worktree/pr-42/project-docs/development-plan.md",
		"/srv/projects/occa/project-docs/development-plan.md",
		"/srv/projects/other/development-plan.md",
	} {
		if got := permissionAction(policy, "edit", path); got != "deny" {
			t.Fatalf("post-merge edit %q = %q, want deny", path, got)
		}
	}
}

func TestBuildPermissionPolicyCommandMatrix(t *testing.T) {
	base := PermissionPolicyInput{
		Repository:      "anggasct/occa",
		PRNumber:        "42",
		Branch:          "fix/webhook-permissions",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: "/srv/vault/1-projects/occa",
	}
	tests := []struct {
		workflow string
		allowed  []string
		denied   []string
	}{
		{
			workflow: "github_reviewer",
			allowed: []string{
				"git status --short",
				"git diff --check",
				"gh pr view 42 --repo anggasct/occa",
				"gh pr checks 42 --repo anggasct/occa",
				"gh pr review 42 --repo anggasct/occa --approve",
			},
			denied: []string{
				"git status --short --untracked-files=all",
				"gh pr view 42 --repo anggasct/other",
				"gh pr view 42 --repo anggasct/occa --json files",
				"gh pr review 42 --repo anggasct/occa --approve --body hacked",
				"git status --short; touch /tmp/pwned",
			},
		},
		{
			workflow: "github_fix",
			allowed: []string{
				"git status --short",
				"git diff --check",
				"go test ./... -count=1",
				"make test",
				"gofmt -l .",
				"git push origin fix/webhook-permissions",
				"gh pr comment 42 --repo anggasct/occa --body \"please re-review @kumasct\"",
			},
			denied: []string{
				"git add --all --no-verify",
				"make test ./internal/webhook",
				"gofmt -l internal/webhook",
				"git push origin fix/webhook-permissions --force",
				"git push origin other-branch",
				"gh pr comment 42 --repo other/occa --body \"please re-review @kumasct\"",
				"git commit -m \"fix\"; git push origin main",
			},
		},
		{
			workflow: "github_merge",
			allowed: []string{
				"gh pr view 42 --repo anggasct/occa",
				"gh pr checks 42 --repo anggasct/occa",
				"gh pr merge 42 --repo anggasct/occa --squash --delete-branch",
			},
			denied: []string{
				"gh pr merge 42 --repo anggasct/occa --squash --delete-branch --admin",
				"gh pr merge 42 --repo evil/occa --squash --delete-branch",
				"gh pr merge 42 --repo anggasct/occa --squash --delete-branch && rm -rf /",
			},
		},
		{
			workflow: "github_merged",
			allowed: []string{
				"gh pr view 42 --repo anggasct/occa",
				"git -C /srv/vault/1-projects/occa status --short",
				"git -C /srv/vault/1-projects/occa diff --check",
				"git -C /srv/vault/1-projects/occa push origin main",
			},
			denied: []string{
				"git -C /srv/vault/other status --short",
				"git -C /srv/vault/1-projects/occa push origin main --force",
				"git -C /srv/vault/1-projects/occa status --short; git push origin main",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.workflow, func(t *testing.T) {
			base.Workflow = tt.workflow
			policy, err := BuildPermissionPolicy(base)
			if err != nil {
				t.Fatalf("BuildPermissionPolicy: %v", err)
			}
			for _, command := range tt.allowed {
				if got := permissionAction(policy, "bash", command); got != "allow" {
					t.Errorf("allowed command %q resolved to %q", command, got)
				}
			}
			for _, command := range tt.denied {
				if got := permissionAction(policy, "bash", command); got == "allow" {
					t.Errorf("denied command %q was allowed", command)
				}
			}
		})
	}
}

func TestBuildPermissionPolicyFailsClosedForInvalidScope(t *testing.T) {
	if _, err := BuildPermissionPolicy(PermissionPolicyInput{Workflow: "github_unknown"}); err == nil {
		t.Fatal("unknown workflow must fail closed")
	}
	if _, err := BuildPermissionPolicy(PermissionPolicyInput{
		Workflow:        "github_merge",
		Repository:      "anggasct/occa",
		PRNumber:        "not-a-number",
		Worktree:        "/srv/projects/occa/.worktree/pr-42",
		ProjectDocsRoot: "/srv/vault/1-projects/occa",
	}); err == nil {
		t.Fatal("invalid pull request scope must fail closed")
	}
	for name, input := range map[string]PermissionPolicyInput{
		"repository shell metacharacter": {
			Workflow: "github_fix", Repository: "anggasct/occa;touch", PRNumber: "42", Branch: "fix/webhook-permissions", Worktree: "/srv/projects/occa/.worktree/pr-42", ProjectDocsRoot: "/srv/vault/1-projects/occa",
		},
		"branch shell metacharacter": {
			Workflow: "github_fix", Repository: "anggasct/occa", PRNumber: "42", Branch: "fix/webhook-permissions;git push origin main", Worktree: "/srv/projects/occa/.worktree/pr-42", ProjectDocsRoot: "/srv/vault/1-projects/occa",
		},
		"worktree shell metacharacter": {
			Workflow: "github_fix", Repository: "anggasct/occa", PRNumber: "42", Branch: "fix/webhook-permissions", Worktree: "/srv/projects/occa/.worktree/pr-42;touch", ProjectDocsRoot: "/srv/vault/1-projects/occa",
		},
		"relative docs root": {
			Workflow: "github_merged", Repository: "anggasct/occa", PRNumber: "42", Worktree: "/srv/projects/occa/.worktree/pr-42", ProjectDocsRoot: "project-docs",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildPermissionPolicy(input); err == nil {
				t.Fatal("malformed authorization scope must fail closed")
			}
		})
	}
}

func hasRule(policy relay.PermissionRuleset, permission, pattern, action string) bool {
	for _, rule := range policy {
		if rule.Permission == permission && rule.Pattern == pattern && rule.Action == action {
			return true
		}
	}
	return false
}

func permissionAction(policy relay.PermissionRuleset, permission, resource string) string {
	for i := len(policy) - 1; i >= 0; i-- {
		rule := policy[i]
		if rule.Permission != permission && rule.Permission != "*" {
			continue
		}
		if wildcardMatch(resource, rule.Pattern) {
			return rule.Action
		}
	}
	return "ask"
}

func wildcardMatch(value, pattern string) bool {
	var expression strings.Builder
	expression.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			expression.WriteString(".*")
		case '?':
			expression.WriteByte('.')
		default:
			expression.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	expression.WriteString("$")
	return regexp.MustCompile(expression.String()).MatchString(value)
}
