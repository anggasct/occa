package web

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/webhook"
)

func TestInbox(t *testing.T) {
	fc := &fakeControl{
		inboxRows: []webhook.InboxRow{{
			WorkItemID:      "anggasct/occa#211",
			Repo:            "anggasct/occa",
			Number:          211,
			Title:           "feat(webhook): live run age",
			Stage:           "fix",
			Attempt:         2,
			State:           "blocked",
			BlockerCode:     "ci_red",
			BlockerSentence: "CI red at a9e3d95",
			PrimaryAction:   "fix_ci",
			AgeSeconds:      843,
			WaitingInputs:   2,
		}},
		inboxCursor: "cur-2",
	}
	s := newTestServer(fc)
	rec := doReq(t, s, "GET", "/api/v1/inbox?repo=anggasct/occa&cursor=cur-1", "", bearerHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if fc.listRepo != "anggasct/occa" || fc.listLimit != 50 || fc.listCursor != "cur-1" {
		t.Fatalf("list args: repo=%q limit=%d cursor=%q", fc.listRepo, fc.listLimit, fc.listCursor)
	}
	var resp inboxResponse
	decodeBody(t, rec, &resp)
	if len(resp.Rows) != 1 || resp.Rows[0].WorkItemID != "anggasct/occa#211" || resp.Rows[0].Number != 211 ||
		resp.Rows[0].BlockerCode != "ci_red" || resp.Rows[0].AgeSeconds != 843 || resp.Rows[0].WaitingInputs != 2 {
		t.Fatalf("rows: %+v", resp.Rows)
	}
	if resp.NextCursor != "cur-2" {
		t.Fatalf("next_cursor %q", resp.NextCursor)
	}
}

func TestInboxEmptyRowsIsArray(t *testing.T) {
	s := newTestServer(&fakeControl{})
	rec := doReq(t, s, "GET", "/api/v1/inbox", "", bearerHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"rows":[]`) {
		t.Fatalf("rows not empty array: %s", rec.Body.String())
	}
}

func TestInboxLimitClamp(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", 50},
		{"?limit=10", 10},
		{"?limit=500", 200},
		{"?limit=abc", 50},
		{"?limit=0", 50},
		{"?limit=-3", 50},
	}
	for _, tc := range cases {
		fc := &fakeControl{}
		s := newTestServer(fc)
		rec := doReq(t, s, "GET", "/api/v1/inbox"+tc.query, "", bearerHeaders())
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", tc.query, rec.Code)
		}
		if fc.listLimit != tc.want {
			t.Fatalf("%s: limit %d want %d", tc.query, fc.listLimit, tc.want)
		}
	}
}

func TestInboxControlError(t *testing.T) {
	s := newTestServer(&fakeControl{inboxErr: errors.New("db exploded")})
	rec := doReq(t, s, "GET", "/api/v1/inbox", "", bearerHeaders())
	if rec.Code != http.StatusInternalServerError || errorCode(t, rec) != "internal" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "db exploded") {
		t.Fatal("internal error leaked")
	}
}

func TestGetWorkItem(t *testing.T) {
	fc := &fakeControl{detail: &webhook.WorkItemDetailView{
		Item:   webhook.WorkItemSummary{ID: "anggasct/occa#211", Kind: "pull_request", SpecRef: "issue:anggasct/occa#204", State: "open"},
		Phases: webhook.PhaseSummary{Phase: "fix", Attempt: 2, State: "blocked", BlockerCode: "ci_red"},
		Stages: []webhook.StageRunView{{
			ID: 41, Stage: "review", Attempt: 1, State: "done",
			StartedAt: 1792861273, EndedAt: 1792861801, LastHeartbeatAt: 1792861799,
			ArtifactRef: "review:5637993106@a9e3d95", GateResult: "[]",
			WorkspacePath: "/tmp/ws", WorkspaceKeptUntil: 1792868473,
			TriggeredBy: "event:83a458e0",
		}},
		WaitingInputs: []webhook.WaitingInputView{{DeliveryID: "9f21c0aa", Endpoint: "github-occa-review", ReceivedAt: 1792864000}},
		Actions:       []string{"retry", "unblock"},
	}}
	s := newTestServer(fc)
	rec := doReq(t, s, "GET", "/api/v1/work-items/anggasct%2Focca%23211", "", bearerHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if fc.detailID != "anggasct/occa#211" {
		t.Fatalf("decoded id %q", fc.detailID)
	}
	var resp workItemDetailResponse
	decodeBody(t, rec, &resp)
	if resp.Item.ID != "anggasct/occa#211" || resp.Item.Kind != "pull_request" || resp.Phases.BlockerCode != "ci_red" {
		t.Fatalf("resp: %+v", resp)
	}
	if len(resp.Stages) != 1 || resp.Stages[0].ID != 41 || resp.Stages[0].WorkspaceKeptUntil != 1792868473 {
		t.Fatalf("stages: %+v", resp.Stages)
	}
	if len(resp.WaitingInputs) != 1 || resp.WaitingInputs[0].DeliveryID != "9f21c0aa" {
		t.Fatalf("waiting: %+v", resp.WaitingInputs)
	}
	if len(resp.Actions) != 2 || resp.Actions[0] != "retry" {
		t.Fatalf("actions: %v", resp.Actions)
	}
}

func TestGetWorkItemUnknown(t *testing.T) {
	s := newTestServer(&fakeControl{detail: nil})
	rec := doReq(t, s, "GET", "/api/v1/work-items/o%2Fr%23999", "", bearerHeaders())
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "item_unknown" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestGetActionOutcome(t *testing.T) {
	fc := &fakeControl{outcome: &webhook.ActionOutcome{
		ActionID: 917, Action: "merge_now", Actor: "owner", Result: "done", Sentence: "Merged a9e3d95",
	}}
	s := newTestServer(fc)
	rec := doReq(t, s, "GET", "/api/v1/actions/917", "", bearerHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if fc.outcomeID != 917 {
		t.Fatalf("outcome id %d", fc.outcomeID)
	}
	var resp actionResultResponse
	decodeBody(t, rec, &resp)
	if resp.ActionID != 917 || resp.Action != "merge_now" || resp.Actor != "owner" || resp.Result != "done" || resp.Sentence != "Merged a9e3d95" {
		t.Fatalf("resp: %+v", resp)
	}
}

func TestGetActionOutcomeUnknownAndBadID(t *testing.T) {
	s := newTestServer(&fakeControl{outcome: nil})
	rec := doReq(t, s, "GET", "/api/v1/actions/42", "", bearerHeaders())
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "action_unknown" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	rec = doReq(t, s, "GET", "/api/v1/actions/abc", "", bearerHeaders())
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "action_unknown" {
		t.Fatalf("bad id: status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestGetActionOutcomeDerivesResult(t *testing.T) {
	fc := &fakeControl{outcome: &webhook.ActionOutcome{ActionID: 1, Accepted: true}}
	s := newTestServer(fc)
	rec := doReq(t, s, "GET", "/api/v1/actions/1", "", bearerHeaders())
	var resp actionResultResponse
	decodeBody(t, rec, &resp)
	if resp.Result != "accepted" {
		t.Fatalf("result %q", resp.Result)
	}
	fc2 := &fakeControl{outcome: &webhook.ActionOutcome{ActionID: 2, Accepted: false, Code: "verdict_stale"}}
	s2 := newTestServer(fc2)
	rec = doReq(t, s2, "GET", "/api/v1/actions/2", "", bearerHeaders())
	decodeBody(t, rec, &resp)
	if resp.Result != "refused" {
		t.Fatalf("result %q", resp.Result)
	}
}

func TestFireActionAccepted(t *testing.T) {
	fc := &fakeControl{fireOutcome: webhook.ActionOutcome{
		Accepted: true, ActionID: 917, StageRunID: 42, ThreadLine: "⚙️ merge_now · @owner",
	}}
	s := newTestServer(fc)
	target := "/api/v1/work-items/anggasct%2Focca%23211/actions/merge_now"
	hdrs := bearerHeaders()
	hdrs["Idempotency-Key"] = "idem-1"
	rec := doReq(t, s, "POST", target, `{"reason":"CI green","confirm":"anggasct/occa#211","params":{"mode":"snapshot"}}`, hdrs)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var resp actionAcceptedResponse
	decodeBody(t, rec, &resp)
	if resp.ActionID != 917 || resp.StageRunID != 42 || resp.State != "accepted" || resp.ThreadLine == "" {
		t.Fatalf("resp: %+v", resp)
	}
	if len(fc.fireCalls) != 1 {
		t.Fatalf("fire calls %d", len(fc.fireCalls))
	}
	got := fc.fireCalls[0]
	if got.WorkItemID != "anggasct/occa#211" || got.Action != "merge_now" || got.Reason != "CI green" ||
		got.Confirm != "anggasct/occa#211" || got.Params["mode"] != "snapshot" || got.IdempotencyKey != "idem-1" {
		t.Fatalf("req: %+v", got)
	}
	if fc.fireActor != "owner" {
		t.Fatalf("actor %q", fc.fireActor)
	}
}

func TestFireActionUnknownAction(t *testing.T) {
	fc := &fakeControl{}
	s := newTestServer(fc)
	rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%231/actions/nuke_it", `{}`, bearerHeaders())
	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "action_unknown" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if len(fc.fireCalls) != 0 {
		t.Fatal("control called for unknown action")
	}
	for _, action := range []string{"retry", "retry_with", "clean_workspace", "unblock", "approve", "reject",
		"skip_stage", "stop", "recheck", "re_review", "fix_ci", "fix_review", "rebind", "merge_now"} {
		fc := &fakeControl{fireOutcome: webhook.ActionOutcome{Accepted: true}}
		s := newTestServer(fc)
		rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%231/actions/"+action, `{}`, bearerHeaders())
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s: status %d body %s", action, rec.Code, rec.Body.String())
		}
	}
}

func TestFireActionRefusalMapping(t *testing.T) {
	cases := []struct {
		code string
		want int
	}{
		{"reason_required", 422},
		{"confirm_mismatch", 422},
		{"params_invalid", 422},
		{"action_unknown", 404},
		{"item_unknown", 404},
		{"verdict_stale", 409},
		{"action_not_legal", 409},
		{"ci_red", 409},
	}
	for _, tc := range cases {
		fc := &fakeControl{fireOutcome: webhook.ActionOutcome{
			Accepted: false, Code: tc.code, Sentence: "sent-" + tc.code, Detail: "det-" + tc.code,
		}}
		s := newTestServer(fc)
		rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%231/actions/retry", `{}`, bearerHeaders())
		if rec.Code != tc.want {
			t.Fatalf("%s: status %d want %d", tc.code, rec.Code, tc.want)
		}
		var env errorEnvelope
		decodeBody(t, rec, &env)
		if env.Error.Code != tc.code || env.Error.Sentence != "sent-"+tc.code || env.Error.Detail != "det-"+tc.code {
			t.Fatalf("%s: body %+v", tc.code, env.Error)
		}
	}
}

func TestFireActionControlError(t *testing.T) {
	s := newTestServer(&fakeControl{fireErr: errors.New("sqlite locked")})
	rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%231/actions/retry", `{}`, bearerHeaders())
	if rec.Code != http.StatusInternalServerError || errorCode(t, rec) != "internal" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var env errorEnvelope
	decodeBody(t, rec, &env)
	if env.Error.Sentence != "Action could not be evaluated." || strings.Contains(rec.Body.String(), "sqlite") {
		t.Fatalf("leaked: %s", rec.Body.String())
	}
}

func TestFireActionBodyValidation(t *testing.T) {
	newSrv := func() *Server { return newTestServer(&fakeControl{fireOutcome: webhook.ActionOutcome{Accepted: true}}) }
	target := "/api/v1/work-items/o%2Fr%231/actions/retry"

	for name, body := range map[string]string{
		"unknown field": `{"wat":1}`,
		"bad type":      `{"reason":5}`,
		"not json":      `nope`,
		"trailing json": `{}{}`,
		"params bad":    `{"params":{"mode":3}}`,
	} {
		rec := doReq(t, newSrv(), "POST", target, body, bearerHeaders())
		if rec.Code != http.StatusUnprocessableEntity || errorCode(t, rec) != "params_invalid" {
			t.Fatalf("%s: status %d body %s", name, rec.Code, rec.Body.String())
		}
	}

	longReason := `{"reason":"` + strings.Repeat("x", 501) + `"}`
	rec := doReq(t, newSrv(), "POST", target, longReason, bearerHeaders())
	if rec.Code != http.StatusUnprocessableEntity || errorCode(t, rec) != "params_invalid" {
		t.Fatalf("long reason: status %d body %s", rec.Code, rec.Body.String())
	}

	fc := &fakeControl{fireOutcome: webhook.ActionOutcome{Accepted: true}}
	s := newTestServer(fc)
	req := httptest.NewRequest("POST", target, strings.NewReader(`{}`))
	req.ContentLength = maxBodyBytes + 1
	req.Header.Set("Authorization", "Bearer secret-token")
	rec2 := httptest.NewRecorder()
	s.httpSrv.Handler.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusUnprocessableEntity || errorCode(t, rec2) != "params_invalid" {
		t.Fatalf("oversize: status %d body %s", rec2.Code, rec2.Body.String())
	}
}

func TestFireActionEmptyBodyAllowed(t *testing.T) {
	fc := &fakeControl{fireOutcome: webhook.ActionOutcome{Accepted: true}}
	s := newTestServer(fc)
	rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%231/actions/recheck", "", bearerHeaders())
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}
