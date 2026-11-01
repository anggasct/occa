package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
)

const dispatchHTTPTimeout = time.Second * 10

func parseTaskCallback(data string) (string, string, string, bool) {
	trimmed := strings.TrimSpace(data)
	if !strings.HasPrefix(trimmed, "task:") {
		return "", "", "", false
	}
	rest := strings.TrimPrefix(trimmed, "task:")
	action := ""
	if strings.HasPrefix(rest, "merge:") {
		action = "merge"
		rest = strings.TrimPrefix(rest, "merge:")
	} else if strings.HasPrefix(rest, "redispatch:") {
		action = "redispatch"
		rest = strings.TrimPrefix(rest, "redispatch:")
	} else if strings.HasPrefix(rest, "review:") {
		action = "review"
		rest = strings.TrimPrefix(rest, "review:")
	} else {
		return "", "", "", false
	}
	rest = strings.TrimSpace(rest)
	repo, branch, ok := strings.Cut(rest, "#")
	repo = strings.TrimSpace(repo)
	branch = strings.TrimSpace(branch)
	if !ok || repo == "" || branch == "" {
		return "", "", "", false
	}
	return action, repo, branch, true
}

func (r *Router) handleTaskCallback(ctx context.Context, msg channel.IncomingMessage) error {
	action, repo, branch, ok := parseTaskCallback(msg.CallbackData)
	if !ok {
		return nil
	}
	return r.dispatchTaskAction(ctx, msg, action, repo, branch)
}

// dispatchTaskAction posts a Merge or Re-dispatch press to the dispatch API.
func (r *Router) dispatchTaskAction(ctx context.Context, msg channel.IncomingMessage, action, repo, branch string) error {
	if r.dispatchToken == "" || r.dispatchURL == "" {
		r.editCallback(msg, "⚠️ Dispatch is not configured (missing token or URL).")
		return nil
	}
	var scope string
	if action == "redispatch" {
		scope = "redispatch " + branch
	} else {
		scope = "merge " + branch
	}
	payload := map[string]string{
		"repo":      repo,
		"branch":    branch,
		"spec_path": "specs/task.md",
		"scope":     scope,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		r.editCallback(msg, "⚠️ Dispatch failed: request error.")
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, "POST", r.dispatchURL, bytes.NewReader(body))
	if err != nil {
		r.editCallback(msg, "⚠️ Dispatch failed: request error.")
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.dispatchToken)
	client := r.dispatchClient
	if client == nil {
		client = &http.Client{Timeout: dispatchHTTPTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		r.editCallback(msg, "⚠️ Dispatch failed: "+shortDispatchError(err))
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != 201 {
		errText := dispatchErrorText(respBody)
		if action == "redispatch" && resp.StatusCode == http.StatusConflict {
			r.editCallback(msg, "⚠️ Re-dispatch refused: "+errText)
			return nil
		}
		r.editCallback(msg, fmt.Sprintf("⚠️ Dispatch failed (%d): %s", resp.StatusCode, errText))
		return nil
	}
	var decoded struct {
		TaskID string `json:"task_id"`
	}
	_ = json.Unmarshal(respBody, &decoded)
	taskID := strings.TrimSpace(decoded.TaskID)
	if taskID == "" {
		taskID = strings.TrimSpace(string(respBody))
	}
	text := "⏳ Dispatched " + repo + "#" + branch
	if taskID != "" {
		text += " · " + taskID
	}
	r.editCallback(msg, text)
	return nil
}

// handleTaskReviewCallback hands a Review press to the webhook server. The
// card is left as it is: the adapter already acknowledged the press, and the
// outcome is posted in the PR thread.
func (r *Router) handleTaskReviewCallback(ctx context.Context, msg channel.IncomingMessage) error {
	action, repo, branch, ok := parseTaskCallback(msg.CallbackData)
	if !ok || action != "review" {
		return nil
	}
	return r.runTaskAction(ctx, msg, action, repo, branch)
}

// handleShortTaskCallback runs a compact ta:<action>:<id> press. Merge keeps
// the dispatch path of the card's Merge button; the other actions go to the
// webhook server.
func (r *Router) handleShortTaskCallback(ctx context.Context, msg channel.IncomingMessage) error {
	action, id, ok := parseShortTaskCallback(msg.CallbackData)
	if !ok {
		return nil
	}
	repo, branch, found := "", "", false
	if r.taskRefResolver != nil {
		repo, branch, found = r.taskRefResolver(id)
	}
	if !found {
		r.editCallback(msg, "⚠️ This button is out of date — open the PR thread.")
		return nil
	}
	if action == "merge" {
		return r.dispatchTaskAction(ctx, msg, action, repo, branch)
	}
	return r.runTaskAction(ctx, msg, action, repo, branch)
}

func (r *Router) runTaskAction(ctx context.Context, msg channel.IncomingMessage, action, repo, branch string) error {
	if r.taskActionHook == nil {
		slog.Warn("router: task action pressed with no action hook", "action", action, "repo", repo, "branch", branch)
		return nil
	}
	if err := r.taskActionHook(ctx, msg, action, repo, branch); err != nil {
		slog.Warn("router: task action failed", "action", action, "repo", repo, "branch", branch, "error", err)
	}
	return nil
}

// parseShortTaskCallback splits ta:<action>:<id>.
func parseShortTaskCallback(data string) (string, string, bool) {
	trimmed := strings.TrimSpace(data)
	if !strings.HasPrefix(trimmed, "ta:") {
		return "", "", false
	}
	action, id, ok := strings.Cut(strings.TrimPrefix(trimmed, "ta:"), ":")
	if !ok {
		return "", "", false
	}
	switch action {
	case "review", "retry", "cancel", "merge":
	default:
		return "", "", false
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return "", "", false
	}
	return action, id, true
}

func (r *Router) editCallback(msg channel.IncomingMessage, text string) {
	if msg.ReplyCtx == nil || msg.CallbackRef == nil {
		r.reply(msg, text)
		return
	}
	if err := msg.ReplyCtx.Edit(msg.CallbackRef, text); err != nil {
		r.reply(msg, text)
	}
}

func dispatchErrorText(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "unknown error"
	}
	var decoded struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil && strings.TrimSpace(decoded.Error) != "" {
		trimmed = strings.TrimSpace(decoded.Error)
	}
	if len([]rune(trimmed)) > 200 {
		trimmed = string([]rune(trimmed)[:200])
	}
	return trimmed
}

func shortDispatchError(err error) string {
	msg := strings.TrimSpace(err.Error())
	if len([]rune(msg)) > 120 {
		msg = string([]rune(msg)[:120])
	}
	return msg
}
