package config

import (
	"fmt"
	"path"
	"strings"
)

// RepoMerge is the optional repos[].merge block. occa always merges in code;
// the block only shapes how.
type RepoMerge struct {
	// Method is merge, squash or rebase; empty follows the repository's
	// allowed methods.
	Method       string `yaml:"method,omitempty"`
	DeleteBranch bool   `yaml:"delete_branch,omitempty"`
	// StaleApprovalPaths lets an approval on an older commit carry over when
	// every file changed since matches one of these patterns.
	StaleApprovalPaths []string `yaml:"stale_approval_paths,omitempty"`
}

// MergePolicy is the resolved merge behaviour of a merge endpoint.
type MergePolicy struct {
	Method             string
	DeleteBranch       bool
	StaleApprovalPaths []string
}

// PromptSourceCode marks an endpoint occa runs without a prompt (merge).
const PromptSourceCode = "code"

var mergeMethods = map[string]bool{"merge": true, "squash": true, "rebase": true}

// ResolveMergePolicy validates a repo's merge block.
func ResolveMergePolicy(m *RepoMerge, context string) (MergePolicy, error) {
	var in RepoMerge
	if m != nil {
		in = *m
	}
	method := strings.ToLower(strings.TrimSpace(in.Method))
	if method != "" && !mergeMethods[method] {
		return MergePolicy{}, fmt.Errorf("config: %smerge.method must be merge, squash or rebase, got %q", context, in.Method)
	}
	var patterns []string
	for j, raw := range in.StaleApprovalPaths {
		pattern := strings.TrimSpace(raw)
		if pattern == "" {
			return MergePolicy{}, fmt.Errorf("config: %smerge.stale_approval_paths[%d] must not be empty", context, j)
		}
		if _, err := path.Match(strings.TrimSuffix(pattern, "/**"), ""); err != nil {
			return MergePolicy{}, fmt.Errorf("config: %smerge.stale_approval_paths[%d] is not a valid pattern %q: %v", context, j, raw, err)
		}
		patterns = append(patterns, pattern)
	}
	return MergePolicy{Method: method, DeleteBranch: in.DeleteBranch, StaleApprovalPaths: patterns}, nil
}

// MatchesApprovalPath reports whether a changed file matches a
// stale_approval_paths pattern: "dir/**" is everything under dir/, a pattern
// without "/" matches the base name, anything else matches the full path.
func MatchesApprovalPath(pattern, file string) bool {
	pattern = strings.TrimSpace(pattern)
	file = strings.TrimPrefix(strings.TrimSpace(file), "./")
	if pattern == "" || file == "" {
		return false
	}
	if dir, ok := strings.CutSuffix(pattern, "/**"); ok {
		return strings.HasPrefix(file, dir+"/")
	}
	if !strings.Contains(pattern, "/") {
		ok, _ := path.Match(pattern, path.Base(file))
		return ok
	}
	ok, _ := path.Match(pattern, file)
	return ok
}
