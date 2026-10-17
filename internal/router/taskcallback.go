package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		r.editCallback(msg, fmt.Sprintf("⚠️ Dispatch failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(respBody))))
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

func (r *Router) editCallback(msg channel.IncomingMessage, text string) {
	if msg.ReplyCtx == nil || msg.CallbackRef == nil {
		r.reply(msg, text)
		return
	}
	if err := msg.ReplyCtx.Edit(msg.CallbackRef, text); err != nil {
		r.reply(msg, text)
	}
}

func shortDispatchError(err error) string {
	msg := strings.TrimSpace(err.Error())
	if len([]rune(msg)) > 120 {
		msg = string([]rune(msg)[:120])
	}
	return msg
}
