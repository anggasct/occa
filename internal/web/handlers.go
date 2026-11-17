package web

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/anggasct/occa/internal/webhook"
)

const (
	maxBodyBytes      = 65536
	maxReasonRunes    = 500
	defaultInboxLimit = 50
	maxInboxLimit     = 200
)

var (
	errBodyTooLarge  = errors.New("web: request body too large")
	errBodyMalformed = errors.New("web: request body malformed")
)

var legalActions = map[string]struct{}{
	webhook.ActionRetry:          {},
	webhook.ActionRetryWith:      {},
	webhook.ActionCleanWorkspace: {},
	webhook.ActionUnblock:        {},
	webhook.ActionApprove:        {},
	webhook.ActionReject:         {},
	webhook.ActionSkipStage:      {},
	webhook.ActionStop:           {},
	webhook.ActionRecheck:        {},
	webhook.ActionReReview:       {},
	webhook.ActionFixCI:          {},
	webhook.ActionFixReview:      {},
	webhook.ActionRebind:         {},
	webhook.ActionMergeNow:       {},
}

func (s *Server) handleInbox(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.resolveAuth(r); !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication required.", "")
		return
	}
	q := r.URL.Query()
	limit := defaultInboxLimit
	if raw := q.Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxInboxLimit {
		limit = maxInboxLimit
	}
	rows, next, err := s.control.ListInbox(r.Context(), q.Get("repo"), limit, q.Get("cursor"))
	if err != nil {
		slog.Error("web: list inbox", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "Inbox could not be loaded.", "")
		return
	}
	resp := inboxResponse{Rows: make([]inboxRowDTO, 0, len(rows)), NextCursor: next}
	for _, row := range rows {
		resp.Rows = append(resp.Rows, toInboxRow(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetWorkItem(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.resolveAuth(r); !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication required.", "")
		return
	}
	detail, err := s.control.GetWorkItemDetail(r.Context(), r.PathValue("id"))
	if err != nil {
		slog.Error("web: work item detail", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "Work item could not be loaded.", "")
		return
	}
	if detail == nil {
		writeError(w, http.StatusNotFound, "item_unknown", "Work item not found.", "")
		return
	}
	writeJSON(w, http.StatusOK, toDetailResponse(detail))
}

func (s *Server) handleGetAction(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.resolveAuth(r); !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication required.", "")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusNotFound, "action_unknown", "Action not found.", "")
		return
	}
	outcome, err := s.control.GetActionOutcome(r.Context(), id)
	if err != nil {
		slog.Error("web: action outcome", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "Action could not be loaded.", "")
		return
	}
	if outcome == nil {
		writeError(w, http.StatusNotFound, "action_unknown", "Action not found.", "")
		return
	}
	result := outcome.Result
	if result == "" {
		if outcome.Accepted {
			result = "accepted"
		} else {
			result = "refused"
		}
	}
	writeJSON(w, http.StatusOK, actionResultResponse{
		ActionID: outcome.ActionID,
		Action:   outcome.Action,
		Actor:    outcome.Actor,
		Result:   result,
		Code:     outcome.Code,
		Sentence: outcome.Sentence,
	})
}

func (s *Server) handleFireAction(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.resolveAuth(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication required.", "")
		return
	}
	if sess != nil && !csrfMatches(sess, r) {
		writeError(w, http.StatusForbidden, "csrf_failed", "CSRF token missing or did not match.", "")
		return
	}
	action := r.PathValue("action")
	if _, legal := legalActions[action]; !legal {
		writeError(w, http.StatusNotFound, "action_unknown", "Action not found.", "")
		return
	}
	workItemID := r.PathValue("id")
	if allowed, retryAfter := s.limiter.check(workItemID, "owner"); !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		writeError(w, http.StatusTooManyRequests, "rate_limited", "Too many actions — wait a moment.", "")
		return
	}
	var body actionRequestBody
	if err := decodeJSONBody(w, r, &body); err != nil {
		writeBodyError(w, err)
		return
	}
	if utf8.RuneCountInString(body.Reason) > maxReasonRunes {
		writeError(w, http.StatusUnprocessableEntity, "params_invalid", "Reason must be 500 characters or fewer.", "")
		return
	}
	outcome, err := s.control.FireAction(r.Context(), "owner", webhook.FireActionRequest{
		WorkItemID:     workItemID,
		Action:         action,
		Reason:         body.Reason,
		Confirm:        body.Confirm,
		Params:         body.Params,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		slog.Error("web: fire action", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "Action could not be evaluated.", "")
		return
	}
	if !outcome.Accepted {
		writeError(w, refusalStatus(outcome.Code), outcome.Code, outcome.Sentence, outcome.Detail)
		return
	}
	writeJSON(w, http.StatusAccepted, actionAcceptedResponse{
		ActionID:   outcome.ActionID,
		StageRunID: outcome.StageRunID,
		State:      "accepted",
		ThreadLine: outcome.ThreadLine,
	})
}

func (s *Server) handlePair(w http.ResponseWriter, r *http.Request) {
	var body pairRequest
	if err := decodeJSONBody(w, r, &body); err != nil {
		writeBodyError(w, err)
		return
	}
	if s.token == "" || subtle.ConstantTimeCompare([]byte(body.Code), []byte(s.token)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Pairing code did not match.", "")
		return
	}
	sess, err := s.createSession()
	if err != nil {
		slog.Error("web: create session", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "Session could not be created.", "")
		return
	}
	http.SetCookie(w, sessionCookie(sess))
	writeJSON(w, http.StatusOK, pairResponse{CSRFToken: sess.csrf})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.resolveAuth(r)
	if !ok || sess == nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication required.", "")
		return
	}
	if !csrfMatches(sess, r) {
		writeError(w, http.StatusForbidden, "csrf_failed", "CSRF token missing or did not match.", "")
		return
	}
	s.deleteSession(sess.id)
	http.SetCookie(w, expiredSessionCookie())
	writeJSON(w, http.StatusOK, struct {
		OK bool `json:"ok"`
	}{OK: true})
}

func refusalStatus(code string) int {
	switch code {
	case "reason_required", "confirm_mismatch", "params_invalid":
		return http.StatusUnprocessableEntity
	case "action_unknown", "item_unknown":
		return http.StatusNotFound
	default:
		return http.StatusConflict
	}
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any) error {
	if r.ContentLength > maxBodyBytes {
		return errBodyTooLarge
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errBodyMalformed
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errBodyMalformed
	}
	return nil
}

func writeBodyError(w http.ResponseWriter, err error) {
	if errors.Is(err, errBodyTooLarge) {
		writeError(w, http.StatusUnprocessableEntity, "params_invalid", "Request body too large.", "")
		return
	}
	writeError(w, http.StatusUnprocessableEntity, "params_invalid", "Request body is not valid JSON.", "")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, sentence, detail string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Sentence: sentence, Detail: detail}})
}
