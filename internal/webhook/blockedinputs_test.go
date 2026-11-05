package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

const noChangeReason = "fix made no change — operator action required: nothing to do"

type blockedHarness struct {
	srv   *Server
	st    *store.SQLiteStore
	url   string
	rec   *prRecorder
	lines *threadLines
}

func blockedInputEndpoints() []config.EndpointConfig {
	review := prTestEndpoint("review", "/review", config.AutomationFull)
	review.CommentTrigger = []string{"please re-review"}
	fix := prTestEndpoint("fix", "/fix", config.AutomationFull)
	fix.Admit = []config.AdmitRule{{Event: "pull_request_review", ReviewState: []string{"changes_requested"}}}
	merged := prTestEndpoint("merged", "/merged", config.AutomationFull)
	return []config.EndpointConfig{review, fix, merged}
}

func openBlockedStore(t *testing.T, path string) *store.SQLiteStore {
	t.Helper()
	st, err := store.OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func serveBlocked(t *testing.T, st *store.SQLiteStore) *blockedHarness {
	t.Helper()
	rec := newPRRecorder("thread-pr", nil)
	cfg := config.WebhookConfig{Bind: "127.0.0.1:0", Policy: testPolicy(), Runtime: testRuntime(), Endpoints: blockedInputEndpoints()}
	srv := New(cfg, rec.exec, st.WebhookDeliveryRepo())
	srv.SetChannelStore(st.ChannelRepo())
	srv.SetPRQueueRepo(st.PRQueueRepo())
	srv.SetStageRepo(st.StageRepo())
	srv.openPRs = func(context.Context, string) ([]openPR, error) { return nil, nil }
	setOpenPRState(srv)
	mergeThroughExecutor(srv)
	lines := &threadLines{}
	srv.SetNotifier(lines.add)
	srv.retryBodyBuilder = func(_ context.Context, _, _, kind string) ([]byte, string, error) {
		state := "commented"
		if kind == "fix" {
			state = "changes_requested"
		}
		return []byte(prReviewBody(7, "feat/x", "owner", state, "c0ffee-retry")), "pull_request_review", nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return &blockedHarness{srv: srv, st: st, url: ts.URL, rec: rec, lines: lines}
}

func newBlockedHarness(t *testing.T, activeKind, reason string) *blockedHarness {
	t.Helper()
	h := serveBlocked(t, openBlockedStore(t, filepath.Join(t.TempDir(), "webhook.db")))
	h.block(t, activeKind, reason)
	return h
}

func (h *blockedHarness) block(t *testing.T, activeKind, reason string) {
	t.Helper()
	ctx := context.Background()
	q := h.st.PRQueueRepo()
	if _, err := q.EnsurePR(ctx, stagePR, "thread-pr", "root-1"); err != nil {
		t.Fatal(err)
	}
	if activeKind != "" {
		if err := q.SetActive(ctx, stagePR, activeKind, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.SetBlocked(ctx, stagePR, reason); err != nil {
		t.Fatal(err)
	}
}

func (h *blockedHarness) post(t *testing.T, path, deliveryID, eventType, body string) {
	t.Helper()
	if resp := post(t, h.url+path+"?secret=s3cret", deliveryID, eventType, body); resp.StatusCode != http.StatusOK {
		t.Fatalf("post %s: status %d", deliveryID, resp.StatusCode)
	}
}

func (h *blockedHarness) review(t *testing.T, deliveryID string) {
	t.Helper()
	h.post(t, "/review", deliveryID, "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee-"+deliveryID))
}

func (h *blockedHarness) fix(t *testing.T, deliveryID string) {
	t.Helper()
	h.post(t, "/fix", deliveryID, "pull_request_review", prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee-"+deliveryID))
}

func (h *blockedHarness) inputs(t *testing.T) []store.StageInput {
	t.Helper()
	inputs, err := h.st.StageRepo().ListInputs(context.Background(), stagePR)
	if err != nil {
		t.Fatalf("ListInputs: %v", err)
	}
	return inputs
}

func (h *blockedHarness) waitInputs(t *testing.T, n int) []store.StageInput {
	t.Helper()
	var got []store.StageInput
	waitForCondition(t, fmt.Sprintf("%d stage inputs", n), func() bool {
		got = h.inputs(t)
		return len(got) >= n
	})
	return got
}

func (h *blockedHarness) receipt(t *testing.T, endpoint, deliveryID string) *store.WebhookDelivery {
	t.Helper()
	d, err := h.st.WebhookDeliveryRepo().Get(context.Background(), endpoint, deliveryID)
	if err != nil || d == nil {
		t.Fatalf("receipt %s: %+v, %v", deliveryID, d, err)
	}
	return d
}

func (h *blockedHarness) calls() []string {
	h.rec.mu.Lock()
	defer h.rec.mu.Unlock()
	return slices.Clone(h.rec.order)
}

func inputStates(inputs []store.StageInput) map[string]store.StageInputState {
	out := map[string]store.StageInputState{}
	for _, in := range inputs {
		out[in.DeliveryID] = in.State
	}
	return out
}

const reviewEP = "github-widget-review"
const fixEP = "github-widget-fix"

func TestBlockedPRKeepsAdmittedRequest(t *testing.T) {
	h := newBlockedHarness(t, "fix", noChangeReason)
	body := string(issueCommentEventBodyForRepo(t, "acme/widget", 7, "please re-review", ""))
	h.post(t, "/review", "del-ask", "issue_comment", body)

	inputs := h.waitInputs(t, 1)
	waitForDeliveryStatus(t, h.st, reviewEP, "del-ask", store.WebhookStatusReceived)
	in := inputs[0]
	if in.State != store.StageInputWaiting || in.Endpoint != reviewEP || in.EventType != "issue_comment" || string(in.Body) != body {
		t.Fatalf("input = %+v", in)
	}
	kept := waitForStageRun(t, h.st, reviewEP, "del-ask", store.StageStateSkipped)
	if kept.Stage != store.StageReview || kept.Attempt != 1 || kept.BlockerDetail != keptWhileBlockedDetail || kept.StartedAt != 0 {
		t.Fatalf("kept request run = %+v, want review attempt 1 skipped as kept while blocked", kept)
	}
	blocked, err := h.st.StageRepo().LatestBlockedRun(context.Background(), stagePR)
	if err != nil || blocked.Stage != store.StageFix || blocked.Attempt != 1 || blocked.BlockerCode != store.BlockerAgentNoCommit {
		t.Fatalf("blocked run = %+v, %v", blocked, err)
	}
	if runs := stageRunsOf(t, h.st, stagePR); len(runs) != 2 || in.StageRunID != blocked.ID || blocked.BlockerDetail != noChangeReason {
		t.Fatalf("input run %d, runs %+v", in.StageRunID, runs)
	}
	if got := h.calls(); len(got) != 0 {
		t.Fatalf("executor ran %v while the PR is blocked", got)
	}
	got := waitForLines(t, h.lines, 1)
	if len(got) != 1 || got[0] != "⏸ acme/widget#7 is blocked — The fix run finished without changing anything. Your request is kept and runs when you press Retry; Cancel drops it." {
		t.Fatalf("thread lines = %q", got)
	}
}

func TestBlockedPRSkipsRequestsItWouldNotRun(t *testing.T) {
	logs := captureServerLogs(t)
	cases := []struct {
		name     string
		endpoint string
		send     func(t *testing.T, h *blockedHarness)
	}{
		{
			name:     "fan-out copy the endpoint does not admit",
			endpoint: fixEP,
			send: func(t *testing.T, h *blockedHarness) {
				h.post(t, "/fix", "del-skip", "pull_request_review", prReviewBody(7, "feat/x", "owner", "commented", "c0ffee1"))
			},
		},
		{
			name:     "merged hook",
			endpoint: "github-widget-merged",
			send: func(t *testing.T, h *blockedHarness) {
				setPRState(h.srv, PRStateOpen, nil)
				h.post(t, "/merged", "del-skip", "pull_request", prClosedBody(7, false))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBlockedHarness(t, "fix", noChangeReason)
			tc.send(t, h)
			waitForDeliveryStatus(t, h.st, tc.endpoint, "del-skip", store.WebhookStatusSkipped)
			if got := h.inputs(t); len(got) != 0 {
				t.Fatalf("a request the endpoint would not run was kept: %+v", got)
			}
		})
	}
	if got := logs.all("webhook: delivery kept while PR blocked"); len(got) != 0 {
		t.Fatalf("kept logs = %+v", got)
	}
}

func TestBlockedPRRedeliveryKeepsOneInput(t *testing.T) {
	logs := captureServerLogs(t)
	h := newBlockedHarness(t, "fix", noChangeReason)
	h.review(t, "del-again")
	h.waitInputs(t, 1)
	waitForDeliveryStatus(t, h.st, reviewEP, "del-again", store.WebhookStatusReceived)
	h.review(t, "del-again")
	waitForCondition(t, "the redelivery kept", func() bool {
		return len(logs.all("webhook: delivery kept while PR blocked")) >= 2
	})
	kept := logs.all("webhook: delivery kept while PR blocked")
	if len(kept) != 2 || kept[1].attrs["redelivery"] != true {
		t.Fatalf("kept logs = %+v, want the redelivery recognised", kept)
	}
	if got := h.inputs(t); len(got) != 1 {
		t.Fatalf("inputs = %+v, want one", got)
	}
	if d := h.receipt(t, reviewEP, "del-again"); d.Status != store.WebhookStatusReceived {
		t.Fatalf("receipt = %s, want received", d.Status)
	}
}

type attachFailingRepo struct{ store.StageRepo }

func (attachFailingRepo) AttachInput(context.Context, store.StageRunBlock, store.StageInput) (store.StageInput, bool, error) {
	return store.StageInput{}, false, errStageDown
}

func TestBlockedPRAttachFailureSkipsAsBefore(t *testing.T) {
	logs := captureServerLogs(t)
	h := newBlockedHarness(t, "fix", noChangeReason)
	h.srv.SetStageRepo(attachFailingRepo{h.st.StageRepo()})
	h.post(t, "/review", "del-down", "issue_comment", string(issueCommentEventBodyForRepo(t, "acme/widget", 7, "please re-review", "")))
	waitForDeliveryStatus(t, h.st, reviewEP, "del-down", store.WebhookStatusSkipped)
	if d := h.receipt(t, reviewEP, "del-down"); !strings.Contains(d.ErrorSummary, noChangeReason) {
		t.Fatalf("skip summary = %q, want the block reason", d.ErrorSummary)
	}
	got := waitForLines(t, h.lines, 1)
	if len(got) != 1 || !strings.Contains(got[0], "Use Retry or Cancel") {
		t.Fatalf("thread lines = %q", got)
	}
	if len(logs.all("webhook: blocked input attach failed")) != 1 {
		t.Fatal("the attach failure was not logged once")
	}
}

func TestRetryRunsNewestWaitingInputPerStage(t *testing.T) {
	cases := []struct {
		name       string
		activeKind string
		send       func(t *testing.T, h *blockedHarness)
		inputs     int
		wantRuns   []string
		wantStates map[string]store.StageInputState
		superseded []string
		reply      string
	}{
		{
			name: "newer request of the blocked stage replaces the retry", activeKind: "review",
			send: func(t *testing.T, h *blockedHarness) {
				h.review(t, "del-old")
				h.waitInputs(t, 1)
				h.review(t, "del-new")
			},
			inputs:     2,
			wantRuns:   []string{"del-new"},
			wantStates: map[string]store.StageInputState{"del-old": store.StageInputSuperseded, "del-new": store.StageInputConsumed},
			superseded: []string{"del-old"},
			reply:      "Re-running review for acme/widget#7.",
		},
		{
			name: "newest of each stage runs", activeKind: "fix",
			send: func(t *testing.T, h *blockedHarness) {
				h.review(t, "del-review")
				h.waitInputs(t, 1)
				h.fix(t, "del-fix")
			},
			inputs:     2,
			wantRuns:   []string{"del-fix", "del-review"},
			wantStates: map[string]store.StageInputState{"del-review": store.StageInputConsumed, "del-fix": store.StageInputConsumed},
			reply:      "Re-running fix and review for acme/widget#7.",
		},
		{
			name: "blocked stage without a request still retries", activeKind: "fix",
			send: func(t *testing.T, h *blockedHarness) {
				h.review(t, "del-review")
			},
			inputs:     1,
			wantRuns:   []string{operatorRetryDeliveryPrefix, "del-review"},
			wantStates: map[string]store.StageInputState{"del-review": store.StageInputConsumed},
			reply:      "Re-running fix and review for acme/widget#7.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBlockedHarness(t, tc.activeKind, noChangeReason)
			tc.send(t, h)
			h.waitInputs(t, tc.inputs)
			for _, in := range h.waitInputs(t, tc.inputs) {
				waitForDeliveryStatus(t, h.st, in.Endpoint, in.DeliveryID, store.WebhookStatusReceived)
			}
			waitForCondition(t, "dispatcher idle after keeping the request", func() bool {
				d := h.srv.liveDispatcher(stagePR)
				return d == nil || (!d.isActive() && d.queueLen() == 0)
			})

			reply, _, err := h.srv.operatorRetry(context.Background(), operatorMsg("thread-pr", "/retry"), stagePR)
			if err != nil || reply != tc.reply {
				t.Fatalf("retry = %q, %v; want %q", reply, err, tc.reply)
			}
			waitForCalls(t, h.rec, len(tc.wantRuns))
			waitForPRQueueStatus(t, h.st, stagePR, store.PRStatusDrained)
			got := h.calls()
			if len(got) != len(tc.wantRuns) {
				t.Fatalf("executor ran %v, want %v", got, tc.wantRuns)
			}
			for _, want := range tc.wantRuns {
				if !slices.ContainsFunc(got, func(id string) bool { return strings.HasPrefix(id, want) }) {
					t.Fatalf("executor ran %v, want %q among them", got, want)
				}
			}
			if states := inputStates(h.inputs(t)); !mapsEqual(states, tc.wantStates) {
				t.Fatalf("input states = %v, want %v", states, tc.wantStates)
			}
			for _, id := range tc.superseded {
				d := h.receipt(t, reviewEP, id)
				if d.Status != store.WebhookStatusSkipped || !strings.Contains(d.ErrorSummary, supersededWhileBlocked) {
					t.Fatalf("superseded receipt = %s %q", d.Status, d.ErrorSummary)
				}
			}
			triggers := map[string]bool{}
			for _, run := range stageRunsOf(t, h.st, stagePR) {
				triggers[run.TriggeredBy] = true
			}
			for _, in := range h.inputs(t) {
				if in.State != store.StageInputConsumed {
					continue
				}
				if !triggers["delivery:"+in.Endpoint+"/"+in.DeliveryID] {
					t.Fatalf("no attempt names the consumed delivery %s: %v", in.DeliveryID, triggers)
				}
				if d := h.receipt(t, in.Endpoint, in.DeliveryID); d.Status != store.WebhookStatusCompleted {
					t.Fatalf("consumed receipt %s = %s, want completed", in.DeliveryID, d.Status)
				}
			}
		})
	}
}

func mapsEqual(a, b map[string]store.StageInputState) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestUnblockSupersedesWaitingInputs(t *testing.T) {
	cases := []struct {
		name   string
		unlock func(t *testing.T, h *blockedHarness)
		reason string
	}{
		{
			name: "operator cancel",
			unlock: func(t *testing.T, h *blockedHarness) {
				if _, _, err := h.srv.operatorCancel(context.Background(), operatorMsg("thread-pr", "/cancel"), stagePR); err != nil {
					t.Fatal(err)
				}
			},
			reason: "cancelled by operator",
		},
		{
			name: "new commits",
			unlock: func(t *testing.T, h *blockedHarness) {
				h.post(t, "/review", "del-sync", "pull_request", syncBody)
			},
			reason: "superseded: new commits",
		},
		{
			name: "PR merged",
			unlock: func(t *testing.T, h *blockedHarness) {
				setPRState(h.srv, PRStateMerged, nil)
				h.post(t, "/merged", "del-merged", "pull_request", prClosedBody(7, true))
			},
			reason: "PR merged",
		},
		{
			name: "PR closed",
			unlock: func(t *testing.T, h *blockedHarness) {
				setPRState(h.srv, PRStateClosed, nil)
				h.post(t, "/merged", "del-closed", "pull_request", prClosedBody(7, false))
			},
			reason: "PR closed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBlockedHarness(t, "fix", noChangeReason)
			h.review(t, "del-a")
			h.waitInputs(t, 1)
			h.fix(t, "del-b")
			for _, in := range h.waitInputs(t, 2) {
				waitForDeliveryStatus(t, h.st, in.Endpoint, in.DeliveryID, store.WebhookStatusReceived)
			}
			tc.unlock(t, h)
			for _, in := range h.inputs(t) {
				waitForDeliveryStatus(t, h.st, in.Endpoint, in.DeliveryID, store.WebhookStatusSkipped)
				if d := h.receipt(t, in.Endpoint, in.DeliveryID); !strings.Contains(d.ErrorSummary, tc.reason) {
					t.Fatalf("receipt %s summary = %q, want %q", in.DeliveryID, d.ErrorSummary, tc.reason)
				}
			}
			for id, state := range inputStates(h.inputs(t)) {
				if state != store.StageInputSuperseded {
					t.Fatalf("input %s = %s, want superseded", id, state)
				}
			}
			for _, id := range h.calls() {
				if id == "del-a" || id == "del-b" {
					t.Fatalf("a superseded request ran: %v", h.calls())
				}
			}
		})
	}
}

func TestBlockedInputsExpireAfterRetention(t *testing.T) {
	h := newBlockedHarness(t, "fix", noChangeReason)
	h.srv.retentionAge = time.Hour
	h.review(t, "del-stale")
	h.waitInputs(t, 1)
	waitForDeliveryStatus(t, h.st, reviewEP, "del-stale", store.WebhookStatusReceived)
	ctx := context.Background()

	h.srv.expireBlockedInputs(ctx, time.Now())
	if states := inputStates(h.inputs(t)); states["del-stale"] != store.StageInputWaiting {
		t.Fatalf("input inside the window = %v, want waiting", states)
	}

	h.srv.expireBlockedInputs(ctx, time.Now().Add(h.srv.retentionAge+time.Minute))
	if states := inputStates(h.inputs(t)); states["del-stale"] != store.StageInputExpired {
		t.Fatalf("input past the window = %v, want expired", states)
	}
	d := h.receipt(t, reviewEP, "del-stale")
	if d.Status != store.WebhookStatusSkipped || !strings.Contains(d.ErrorSummary, expiredWhileBlocked) {
		t.Fatalf("expired receipt = %s %q", d.Status, d.ErrorSummary)
	}
}

func TestBlockedInputSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook.db")
	first := serveBlocked(t, openBlockedStore(t, path))
	first.block(t, "review", noChangeReason)
	first.review(t, "del-kept")
	first.waitInputs(t, 1)
	waitForDeliveryStatus(t, first.st, reviewEP, "del-kept", store.WebhookStatusReceived)
	_ = first.st.Close()

	second := serveBlocked(t, openBlockedStore(t, path))
	if reply, _, err := second.srv.operatorRetry(context.Background(), operatorMsg("thread-pr", "/retry"), stagePR); err != nil || reply != "Re-running review for acme/widget#7." {
		t.Fatalf("retry = %q, %v", reply, err)
	}
	waitForDeliveryStatus(t, second.st, reviewEP, "del-kept", store.WebhookStatusCompleted)
	if got := second.calls(); len(got) != 1 || got[0] != "del-kept" {
		t.Fatalf("executor ran %v, want the kept request", got)
	}
	if got := first.calls(); len(got) != 0 {
		t.Fatalf("the first server ran %v", got)
	}
}

func TestBlockBeforeAnyRunRecordsBlockedAttempt(t *testing.T) {
	storm := "auto-fix storm cap — manual action (3 auto-fix runs in 24h)"
	cases := []struct {
		name    string
		block   func(t *testing.T, h *blockedHarness)
		want    []stageRunWant
		running bool
	}{
		{
			name: "merge starved",
			block: func(t *testing.T, h *blockedHarness) {
				h.srv.prMirrorBlocked(stagePR, "merge", "merge starved — manual action")
			},
			want: []stageRunWant{{store.StageMerge, 1, store.StageStateBlocked, store.BlockerMergeStarved}},
		},
		{
			name: "storm cap at enqueue",
			block: func(t *testing.T, h *blockedHarness) {
				row, err := h.st.PRQueueRepo().Get(context.Background(), stagePR)
				if err != nil || row == nil {
					t.Fatalf("row = %+v, %v", row, err)
				}
				h.srv.escalateBlock(h.srv.endpointsByName[fixEP], stagePR, row, storm, "auto-fix stopped", "3 auto-fix runs in 24h")
			},
			want: []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerRetriesExhausted}},
		},
		{
			name: "block during a running attempt waits for its end",
			block: func(t *testing.T, h *blockedHarness) {
				h.srv.stages.start(context.Background(), h.srv.endpointsByName[fixEP], &WebhookWorkContext{PRKey: stagePR, DeliveryID: "del-run"})
				h.srv.prMirrorBlocked(stagePR, "fix", "fix made no change — operator action required")
			},
			want:    []stageRunWant{{store.StageFix, 1, store.StageStateRunning, ""}},
			running: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := serveBlocked(t, openBlockedStore(t, filepath.Join(t.TempDir(), "webhook.db")))
			if _, err := h.st.PRQueueRepo().EnsurePR(context.Background(), stagePR, "thread-pr", "root-1"); err != nil {
				t.Fatal(err)
			}
			tc.block(t, h)
			runs := assertStageRuns(t, h.st, stagePR, tc.want)
			if !tc.running && runs[0].StartedAt != 0 {
				t.Fatalf("a run that never started has started_at %d", runs[0].StartedAt)
			}
			if tc.running {
				h.srv.stages.finish(context.Background(), fixEP, "del-run", store.StageStateFailed, "failed")
				assertStageRuns(t, h.st, stagePR, []stageRunWant{{store.StageFix, 1, store.StageStateBlocked, store.BlockerAgentNoCommit}})
			}
		})
	}
}

func TestStageInputErrorsAreSentinels(t *testing.T) {
	h := newBlockedHarness(t, "fix", noChangeReason)
	h.review(t, "del-x")
	in := h.waitInputs(t, 1)[0]
	repo := h.st.StageRepo()
	if err := repo.SetInputState(context.Background(), in.ID, store.StageInputConsumed); err != nil {
		t.Fatal(err)
	}
	if h.srv.settleInput(context.Background(), in, store.StageInputSuperseded, "late") {
		t.Fatal("a consumed input was settled again")
	}
	if err := repo.SetInputState(context.Background(), in.ID, store.StageInputExpired); !errors.Is(err, store.ErrStageInputNotWaiting) {
		t.Fatalf("err = %v", err)
	}
}

func TestBlockedPRKeepsEachEndpointCopy(t *testing.T) {
	h := newBlockedHarness(t, "fix", noChangeReason)
	body := prReviewBody(7, "feat/x", "owner", "changes_requested", "c0ffee-shared")
	h.post(t, "/review", "del-shared", "pull_request_review", body)
	h.waitInputs(t, 1)
	h.post(t, "/fix", "del-shared", "pull_request_review", body)
	inputs := h.waitInputs(t, 2)
	for _, in := range inputs {
		waitForDeliveryStatus(t, h.st, in.Endpoint, in.DeliveryID, store.WebhookStatusReceived)
	}
	if inputs[0].Endpoint != reviewEP || inputs[1].Endpoint != fixEP {
		t.Fatalf("inputs = %+v, want one per endpoint", inputs)
	}

	reply, _, err := h.srv.operatorRetry(context.Background(), operatorMsg("thread-pr", "/retry"), stagePR)
	if err != nil || reply != "Re-running fix and review for acme/widget#7." {
		t.Fatalf("retry = %q, %v", reply, err)
	}
	waitForDeliveryStatus(t, h.st, reviewEP, "del-shared", store.WebhookStatusCompleted)
	waitForDeliveryStatus(t, h.st, fixEP, "del-shared", store.WebhookStatusCompleted)
	if got := h.calls(); len(got) != 2 || got[0] != "del-shared" || got[1] != "del-shared" {
		t.Fatalf("executor ran %v, want both copies", got)
	}
}

func TestBootRecoveryKeepsWaitingInputReceipt(t *testing.T) {
	h := newBlockedHarness(t, "review", noChangeReason)
	h.review(t, "del-kept")
	h.waitInputs(t, 1)
	waitForDeliveryStatus(t, h.st, reviewEP, "del-kept", store.WebhookStatusReceived)
	seedProcessingDelivery(t, h.st, reviewEP, "del-orphan", "pull_request_review")
	// A negative grace puts the cutoff in the future, so every receipt counts
	// as older than claim_grace.
	grace := h.srv.claimGrace
	h.srv.claimGrace = -time.Hour
	h.srv.recoverStale(context.Background())
	if d := h.receipt(t, reviewEP, "del-orphan"); d.Status != store.WebhookStatusFailed {
		t.Fatalf("orphan receipt = %s, want failed by recovery", d.Status)
	}
	if d := h.receipt(t, reviewEP, "del-kept"); d.Status != store.WebhookStatusReceived {
		t.Fatalf("kept receipt = %s, want received", d.Status)
	}
	h.srv.claimGrace = grace
	if reply, _, err := h.srv.operatorRetry(context.Background(), operatorMsg("thread-pr", "/retry"), stagePR); err != nil || reply != "Re-running review for acme/widget#7." {
		t.Fatalf("retry = %q, %v", reply, err)
	}
	waitForDeliveryStatus(t, h.st, reviewEP, "del-kept", store.WebhookStatusCompleted)
	if got := h.calls(); len(got) != 1 || got[0] != "del-kept" {
		t.Fatalf("executor ran %v, want the kept request", got)
	}
}
