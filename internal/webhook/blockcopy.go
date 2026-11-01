package webhook

import (
	"regexp"
	"strconv"
	"strings"
)

// blockCopy is what the operator reads about a blocked PR: what happened in
// plain words, the detail the run left (agent reply, refused scope, path),
// and the next step. It is derived from the stored block reason, so the
// escalation, the 📌 line and /inbox say the same thing.
type blockCopy struct {
	Headline string
	Detail   string
	Next     string
}

var (
	reasonKindRe    = regexp.MustCompile(`^(\S+) (?:made no change|stopped on a denied permission|stopped on a provider limit|could not get its worktree|exhausted after)`)
	reasonRetriesRe = regexp.MustCompile(`exhausted after (\d+)/\d+ retries \(([^)]*)\)`)
	reasonStormRe   = regexp.MustCompile(`\((\d+) auto-fix runs in 24h\)`)
)

// plainFailureClass words a failure class label for the operator.
func plainFailureClass(label string) string {
	switch strings.TrimSpace(label) {
	case "provider stall":
		return "the model stopped responding"
	case "build/test failure":
		return "build or tests failed"
	case "no work in lease":
		return "it changed nothing"
	case "permission denied":
		return "a permission was refused"
	case "push failed":
		return "git push failed"
	case "pr create failed":
		return "opening the PR failed"
	case "model error":
		return "the model returned an error"
	case "review not posted":
		return "the review was not posted on GitHub"
	case "provider limit":
		return "the model provider refused the run (limit reached)"
	}
	return strings.TrimSpace(label)
}

// reasonDetail is the text after the first ": " of a block reason — the
// agent reply, refused scope or workspace error the hand-off appended.
func reasonDetail(reason string) string {
	if _, after, ok := strings.Cut(reason, "operator action required: "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// describeBlock turns a stored block reason into operator copy. An unknown
// reason is shown as it is, with the generic next step.
func describeBlock(reason string) blockCopy {
	reason = strings.TrimSpace(reason)
	kind := "PR"
	if m := reasonKindRe.FindStringSubmatch(reason); m != nil {
		kind = m[1]
	}
	detail := reasonDetail(reason)
	switch {
	case strings.Contains(reason, " made no change"):
		c := blockCopy{
			Headline: "The " + kind + " run finished without changing anything.",
			Next:     "If no code change is needed, press Merge. Otherwise say what to change here, then press Retry.",
		}
		if detail != "" {
			c.Detail = "Agent said: " + detail
		}
		return c
	case strings.Contains(reason, " stopped on a provider limit"):
		c := blockCopy{
			Headline: "The " + kind + " run was refused by the model provider (limit reached).",
			Next:     "Restore the provider quota or switch the model, then press Retry.",
		}
		if detail != "" {
			c.Detail = "Provider said: " + detail
		}
		return c
	case strings.Contains(reason, " stopped on a denied permission"):
		c := blockCopy{
			Headline: "The " + kind + " run was refused access it asked for.",
			Next:     "If that path should be readable, allow it in the config, then press Retry.",
		}
		if detail != "" {
			c.Detail = "Refused: " + detail
		}
		return c
	case strings.Contains(reason, " could not get its worktree"):
		return blockCopy{
			Headline: "The " + kind + " run could not get a clean checkout.",
			Detail:   detail,
			Next:     "Clean or remove the worktree named above, then press Retry.",
		}
	case strings.HasPrefix(reason, "live PR state unresolvable"):
		return blockCopy{
			Headline: "occa could not read this PR's state from GitHub.",
			Next:     "Check that gh is logged in and GitHub is reachable, then press Retry.",
		}
	case strings.HasPrefix(reason, "auto-fix storm cap"):
		n := "3"
		if m := reasonStormRe.FindStringSubmatch(reason); m != nil {
			n = m[1]
		}
		return blockCopy{
			Headline: "Auto-fix ran " + n + " times in 24h and stopped.",
			Next:     "Fix it by hand, or press Retry to allow one more run.",
		}
	}
	if m := reasonRetriesRe.FindStringSubmatch(reason); m != nil {
		return blockCopy{
			Headline: "The " + kind + " run failed " + m[1] + " time" + pluralOf(m[1]) + " in a row: " + plainFailureClass(m[2]) + ".",
			Next:     "Read the last run above, then press Retry.",
		}
	}
	headline := reason
	if headline == "" {
		headline = "This PR is blocked."
	}
	return blockCopy{Headline: headline, Next: "Press Retry to run it again, or Cancel to drop the queue."}
}

// statusAction is the 📌 ACTION segment of a blocked PR.
func (c blockCopy) statusAction() string {
	return c.Headline + " — use the buttons below"
}

// formatBlockedCopy renders the blocked notice. The thread copy sits above the
// Retry · Merge · Cancel buttons; the channel copy points at the thread.
func formatBlockedCopy(repo, prNumber string, c blockCopy, icons map[string]string, channelCopy bool) string {
	head := strings.TrimSpace(repo)
	if number := strings.TrimSpace(prNumber); number != "" {
		if head != "" {
			head += "#"
		}
		head += number
	}
	lines := []string{PhaseIcon(PhaseBlocked, icons) + " " + head + " needs you — blocked", c.Headline}
	if detail := strings.TrimSpace(c.Detail); detail != "" {
		lines = append(lines, detail)
	}
	if next := strings.TrimSpace(c.Next); next != "" {
		lines = append(lines, "Next: "+next)
	}
	if channelCopy {
		lines = append(lines, "Open the PR thread to act.")
	}
	return strings.Join(lines, "\n")
}

// pluralOf is plural for a count written as text.
func pluralOf(count string) string {
	n, err := strconv.Atoi(strings.TrimSpace(count))
	if err != nil {
		return "s"
	}
	return plural(n)
}
