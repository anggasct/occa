package webhook

import (
	"fmt"
	"strings"
)

func legacyWorkflowAllows(workflow string, envelope WebhookEnvelope) (bool, string) {
	if workflow == "" {
		return true, ""
	}

	eventType := strings.ToLower(strings.TrimSpace(stringValue(envelope["event_type"])))
	action := strings.ToLower(stringValue(envelope["action"]))
	reviewState := strings.ToLower(stringValue(envelope["review_state"]))
	prNumber := stringValue(envelope["pr_number"])
	reviewUser := stringValue(envelope["review_user"])
	prAuthor := stringValue(envelope["pr_author"])
	reviewVerdict := strings.ToLower(stringValue(envelope["review_verdict"]))
	selfReview := reviewUser == "kumasct" && prAuthor == "kumasct" && reviewState == "commented"

	switch workflow {
	case "github_reviewer":
		if eventType == "pull_request" && action == "synchronize" {
			return false, "skipped: pull_request synchronize (push while PR open spawns no execution)"
		}
		if eventType == "pull_request" && containsString([]string{"opened", "reopened", "ready_for_review"}, action) {
			return true, ""
		}
		if eventType == "issue_comment" {
			if !boolValue(envelope["comment_trigger_configured"]) {
				return false, "skipped: comment trigger not configured"
			}
			if action == "created" && prNumber != "" && stringValue(envelope["comment_trigger"]) != "" {
				if state := prState(envelope); state == "closed" || state == "merged" {
					return false, fmt.Sprintf("skipped: PR #%s is %s; no re-review execution", prNumber, state)
				}
				return true, ""
			}
		}
	case "github_fix":
		if eventType == "pull_request_review" && action == "submitted" && reviewState == "changes_requested" {
			return true, ""
		}
		if eventType == "pull_request_review" && action == "submitted" && selfReview && reviewVerdict == "request_changes" {
			return true, ""
		}
	case "github_merge":
		if eventType == "pull_request_review" && action == "submitted" && reviewState == "approved" {
			return true, ""
		}
		if eventType == "pull_request_review" && action == "submitted" && selfReview && reviewVerdict == "approved" && !boolValue(envelope["has_findings"]) {
			return true, ""
		}
		if eventType == "check_suite" {
			status := strings.ToLower(stringValue(envelope["status"]))
			appName := stringValue(envelope["app_name"])
			if action != "completed" || (status != "" && status != "completed") {
				return false, fmt.Sprintf("skipped: check_suite status is %s.%s (only completed is admitted)", action, status)
			}
			if !strings.EqualFold(appName, "github actions") {
				return false, fmt.Sprintf("skipped: check_suite app %q is not GitHub Actions", appName)
			}
			if state := prState(envelope); state == "closed" || state == "merged" {
				if prNumber != "" {
					return false, fmt.Sprintf("skipped: PR #%s is %s; no merge execution", prNumber, state)
				}
				return false, fmt.Sprintf("skipped: PR is %s; no merge execution", state)
			}
			if !hasResolvablePR(envelope) {
				return false, "skipped: check_suite has no resolvable open PR"
			}
			return true, ""
		}
	case "github_merged":
		if eventType == "pull_request" && action == "closed" && boolValue(envelope["merged"]) {
			return true, ""
		}
	}
	return false, fmt.Sprintf("workflow %s rejected %s.%s", workflow, eventType, action)
}
