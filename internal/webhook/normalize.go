package webhook

import (
	"encoding/json"
	"fmt"
	"strings"
)

type WebhookEnvelope map[string]any

func normalizeWebhook(body []byte, eventType, deliveryID string, skipped bool, skipReason string) WebhookEnvelope {
	payload := make(map[string]any)
	var decoded any
	if json.Unmarshal(body, &decoded) == nil {
		if object, ok := decoded.(map[string]any); ok {
			payload = object
		}
	}

	envelope := WebhookEnvelope{
		"source":          "github",
		"event_type":      strings.TrimSpace(eventType),
		"action":          stringValue(payload["action"]),
		"delivery_id":     deliveryID,
		"repository":      repoName(payload["repository"]),
		"project":         "",
		"pr_number":       "",
		"pr_url":          "",
		"title":           "",
		"head_branch":     "",
		"base_branch":     "",
		"pr_author":       "",
		"review_state":    "",
		"review_user":     "",
		"review_verdict":  "",
		"has_findings":    false,
		"comment_body":    "",
		"comment_trigger": "",
		"merged":          false,
		"merge_commit":    "",
		"skip":            skipped,
		"skip_reason":     skipReason,
	}

	if envelope["event_type"] == "" {
		envelope["event_type"] = stringValue(payload["event_type"])
	}

	pullRequest, _ := payload["pull_request"].(map[string]any)
	switch envelope["event_type"] {
	case "pull_request", "pull_request_review":
		fillPullRequest(envelope, pullRequest)
		if envelope["event_type"] == "pull_request_review" {
			if review, ok := payload["review"].(map[string]any); ok {
				envelope["review_state"] = stringValue(review["state"])
				envelope["review_user"] = userName(review["user"])
				envelope["comment_body"] = stringValue(review["body"])
				envelope["review_verdict"] = reviewVerdict(stringValue(review["body"]))
				envelope["has_findings"] = hasActionableFindings(stringValue(review["body"]))
			}
		}
	case "issue_comment":
		issue, _ := payload["issue"].(map[string]any)
		comment, _ := payload["comment"].(map[string]any)
		if issuePR, ok := issue["pull_request"].(map[string]any); ok {
			envelope["pr_number"] = numberValue(issue["number"])
			envelope["title"] = stringValue(issue["title"])
			envelope["pr_url"] = stringValue(issuePR["html_url"])
			envelope["comment_trigger"] = commentTrigger(stringValue(comment["body"]))
		}
		envelope["comment_body"] = stringValue(comment["body"])
		envelope["review_user"] = userName(comment["user"])
	}

	if envelope["repository"] == "" {
		envelope["repository"] = repoName(mapValue(pullRequest, "base")["repo"])
	}
	return envelope
}

func fillPullRequest(envelope WebhookEnvelope, pullRequest map[string]any) {
	if pullRequest == nil {
		return
	}
	envelope["pr_number"] = numberValue(pullRequest["number"])
	envelope["pr_url"] = stringValue(pullRequest["html_url"])
	envelope["title"] = stringValue(pullRequest["title"])
	envelope["head_branch"] = stringValue(mapValue(pullRequest, "head")["ref"])
	envelope["base_branch"] = stringValue(mapValue(pullRequest, "base")["ref"])
	envelope["pr_author"] = userName(pullRequest["user"])
	envelope["merged"] = boolValue(pullRequest["merged"])
	envelope["merge_commit"] = stringValue(pullRequest["merge_commit_sha"])
	if envelope["repository"] == "" {
		envelope["repository"] = repoName(mapValue(pullRequest, "base")["repo"])
	}
}

func mapValue(object map[string]any, key string) map[string]any {
	if object == nil {
		return nil
	}
	value, _ := object[key].(map[string]any)
	return value
}

func repoName(value any) string {
	if object, ok := value.(map[string]any); ok {
		return stringValue(object["full_name"])
	}
	return stringValue(value)
}

func userName(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if login := stringValue(object["login"]); login != "" {
		return login
	}
	return stringValue(object["name"])
}

func stringValue(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case string:
		return value
	case json.Number:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

func numberValue(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case json.Number:
		return value.String()
	case float64:
		return fmt.Sprintf("%.0f", value)
	case float32:
		return fmt.Sprintf("%.0f", value)
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	default:
		return stringValue(value)
	}
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func commentTrigger(body string) string {
	const trigger = "please re-review"
	if strings.Contains(strings.ToLower(body), trigger) {
		return trigger
	}
	return ""
}

func reviewVerdict(body string) string {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
		line = strings.TrimSpace(strings.Trim(line, "*_` "))
		line = strings.ReplaceAll(line, "**", "")
		lower := strings.ToLower(strings.TrimSpace(line))
		for _, verdict := range []string{"approved", "request_changes"} {
			if lower == verdict || strings.HasPrefix(lower, "verdict: "+verdict) || strings.HasPrefix(lower, "verdict:"+verdict) {
				return verdict
			}
		}
	}
	return ""
}

func hasActionableFindings(body string) bool {
	lower := strings.ToLower(body)
	if declaresNoFindings(lower) {
		return false
	}

	lines := strings.Split(body, "\n")
	for i, line := range lines {
		heading := strings.TrimSpace(line)
		if !strings.HasPrefix(heading, "#") {
			continue
		}
		heading = strings.TrimSpace(strings.TrimLeft(heading, "#"))
		heading = strings.Trim(heading, " *_`")
		if !strings.EqualFold(heading, "findings") {
			continue
		}

		sectionEnd := len(lines)
		for j := i + 1; j < len(lines); j++ {
			next := strings.TrimSpace(lines[j])
			if strings.HasPrefix(next, "#") {
				sectionEnd = j
				break
			}
		}
		section := strings.TrimSpace(strings.Join(lines[i+1:sectionEnd], "\n"))
		return section != "" && !declaresNoFindings(section)
	}

	if strings.Contains(lower, "severity:") &&
		strings.Contains(lower, "type:") &&
		strings.Contains(lower, "problem:") {
		return true
	}
	if strings.Contains(lower, "actionable finding") || strings.Contains(lower, "finding #") {
		return true
	}

	return true
}

func declaresNoFindings(value string) bool {
	return strings.Contains(value, "no actionable finding") ||
		strings.Contains(value, "actionable findings: 0") ||
		strings.Contains(value, "actionable findings: none") ||
		strings.Contains(value, "findings: none") ||
		strings.Contains(value, "findings: 0") ||
		strings.Contains(value, "no findings") ||
		strings.Contains(value, "findings\nnone")
}
