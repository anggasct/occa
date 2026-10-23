package webhook

import (
	"context"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

// TestThreadStatusLineIsOnePerThreadAndPinned asserts the thread contract:
// one state line per thread, edited in place by every writer, pinned once.
func TestThreadStatusLineIsOnePerThreadAndPinned(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	var pinned []string
	srv.SetPinner(func(_ context.Context, platform, channelID, messageID string) error {
		pinned = append(pinned, platform+"/"+channelID+"/"+messageID)
		return nil
	})
	ctx := context.Background()
	threadID := "thread-77"

	// The dispatch writer keys the line by repo and branch, the PR-queue
	// writer by the PR: both must land on the same message.
	srv.ensureThreadStatus(ctx, "discord", "chan-1", threadID, "acme/widget", "feat/x", "", ThreadStatusUpdate{
		Phase: PhaseDispatch,
		Next:  NextStepForPhase(PhaseDispatch),
	})
	srv.ensureThreadStatus(ctx, "discord", "chan-1", threadID, "acme/widget", "feat/x", "acme/widget#181", ThreadStatusUpdate{
		Phase: PhaseReview,
		Next:  NextStepForPhase(PhaseReview),
	})

	if len(rec.threadSent) != 1 {
		t.Fatalf("state line writes = %d, want exactly one: %+v", len(rec.threadSent), rec.threadSent)
	}
	if !strings.Contains(rec.threadSent[0].text, "🛠️") || !strings.Contains(rec.threadSent[0].text, string(PhaseDispatch)) {
		t.Fatalf("state line = %q, want the dispatch phase", rec.threadSent[0].text)
	}
	if len(pinned) != 1 {
		t.Fatalf("pins = %d, want exactly one: %+v", len(pinned), pinned)
	}
	_, edited, _, _ := rec.snapshot()
	if len(edited) != 1 {
		t.Fatalf("state line edits = %d, want the second phase to edit in place: %+v", len(edited), edited)
	}
	if edited[0].messageID != "status-1" {
		t.Fatalf("edited message = %q, want the first state line", edited[0].messageID)
	}
	if !strings.Contains(edited[0].text, string(PhaseReview)) {
		t.Fatalf("edited state line = %q, want the review phase", edited[0].text)
	}
	if strings.Contains(edited[0].text, string(PhaseDispatch)) {
		t.Fatalf("edited state line = %q, must show the current phase only", edited[0].text)
	}
}

// TestDispatchParentCarriesNoBrief asserts the channel parent stays one line
// and keeps the task brief out of the channel.
func TestDispatchParentCarriesNoBrief(t *testing.T) {
	task := DispatchTask{ID: "task-1", Repo: "acme/widget", Branch: "feat/x", SpecPath: "specs/feat-x.md", Verify: "go test ./..."}
	summary := FormatDispatchSummary(task, nil)
	if strings.Contains(summary, "\n") {
		t.Fatalf("dispatch parent must be one line: %q", summary)
	}
	if strings.Contains(summary, "specs/feat-x.md") || strings.Contains(summary, "go test") {
		t.Fatalf("dispatch parent must not carry the brief: %q", summary)
	}
	brief := FormatDispatchBrief(task)
	if !strings.Contains(brief, "specs/feat-x.md") || !strings.Contains(brief, "go test") {
		t.Fatalf("dispatch brief = %q, want the spec and the verify command", brief)
	}
}

// TestThreadCardsAreSingleLine asserts the v1 multi-line cards are gone from
// the thread.
func TestThreadCardsAreSingleLine(t *testing.T) {
	envelope := WebhookEnvelope{
		"repository":     "acme/widget",
		"head_branch":    "feat/x",
		"pr_number":      "181",
		"pr_url":         "https://github.com/acme/widget/pull/181",
		"review_verdict": "REQUEST_CHANGES",
	}
	card := FormatThreadSlimCard(envelope, "fix", TaskPROpened, nil)
	if strings.Contains(card, "\n") {
		t.Fatalf("thread card must be one line: %q", card)
	}
	if !strings.Contains(card, "REQUEST_CHANGES") {
		t.Fatalf("thread card must carry the verdict: %q", card)
	}
}

// TestBuiltinPromptsRenderWithoutMissingKey asserts the built-in prompts
// render for a delivery whose envelope carries no task context.
func TestBuiltinPromptsRenderWithoutMissingKey(t *testing.T) {
	payloads := map[string][]byte{
		"issue_comment": []byte(`{"action":"created","repository":{"full_name":"acme/widget"},"issue":{"number":181,"title":"t","state":"open","pull_request":{"html_url":"https://github.com/acme/widget/pull/181"}},"comment":{"body":"please re-review","user":{"login":"k"}}}`),
		"pull_request":  []byte(`{"action":"opened","repository":{"full_name":"acme/widget"},"pull_request":{"number":181,"title":"t","html_url":"https://github.com/acme/widget/pull/181","head":{"ref":"feat/x"},"base":{"ref":"main","repo":{"full_name":"acme/widget"}},"user":{"login":"a"},"state":"open"}}`),
	}
	for event, body := range payloads {
		envelope := normalizeWebhook(body, event, "d1", nil, false, "")
		for _, workflow := range []string{"review", "fix", "merge", "merged"} {
			prompt := config.BuiltinPrompt(workflow)
			if prompt == "" {
				continue
			}
			rendered, err := renderTemplate(prompt, map[string]any{
				"webhook":       envelope,
				"payload":       map[string]any{},
				"json":          string(body),
				"execution_key": "",
				"worktree":      "",
			})
			if err != nil {
				t.Fatalf("%s/%s prompt render failed: %v", event, workflow, err)
			}
			if strings.Contains(rendered, "<no value>") {
				t.Fatalf("%s/%s prompt rendered an unresolved value", event, workflow)
			}
		}
	}
}

// TestMergeGateBlocksOnRequestChanges asserts a REQUEST_CHANGES verdict on the
// head blocks the merge, also when the review is a self-review.
func TestMergeGateBlocksOnRequestChanges(t *testing.T) {
	srv, _, _ := newSurfaceServer(t)
	ep := srv.endpoints["/merge"]
	prKey := "acme/widget#181"

	envelope := WebhookEnvelope{
		"repository":     "acme/widget",
		"pr_number":      "181",
		"head_branch":    "feat/x",
		"review_state":   "commented",
		"review_verdict": "request_changes",
		"review_user":    "reviewbot",
		"pr_author":      "reviewbot",
		"review_commit":  "c0ffee",
		"review_id":      "10",
	}
	verdict := srv.recordMergeGate(prKey, envelope)
	if verdict != "changes_requested" {
		t.Fatalf("gate verdict = %q, want changes_requested", verdict)
	}
	if _, blocked := srv.mergeGateBlocked(prKey); !blocked {
		t.Fatal("a REQUEST_CHANGES verdict must block the merge")
	}
	if srv.mergeGateAllows(context.Background(), prKey) {
		t.Fatal("the merge gate must not pass while a REQUEST_CHANGES verdict stands")
	}

	// A later approval on the same commit wins: the last review of a commit
	// is the one that counts.
	approval := WebhookEnvelope{
		"repository":     "acme/widget",
		"pr_number":      "181",
		"review_state":   "approved",
		"review_verdict": "approved",
		"review_commit":  "c0ffee",
		"review_id":      "11",
	}
	if verdict := srv.recordMergeGate(prKey, approval); verdict != "approved" {
		t.Fatalf("gate verdict = %q, want approved", verdict)
	}
	if _, blocked := srv.mergeGateBlocked(prKey); blocked {
		t.Fatal("an approval on the same commit must clear the block")
	}
	_ = ep
}

// TestMergeLaneSkipsWhenVerdictBlocks asserts the merge lane reports the
// same verdict the button shows.
func TestMergeLaneSkipsWhenVerdictBlocks(t *testing.T) {
	srv, st, _ := newSurfaceServer(t)
	prKey := "acme/widget#181"
	srv.recordMergeGate(prKey, WebhookEnvelope{
		"repository":     "acme/widget",
		"pr_number":      "181",
		"review_state":   "commented",
		"review_verdict": "request_changes",
		"review_commit":  "c0ffee",
		"review_id":      "10",
		"head_sha":       "c0ffee",
	})
	ep := srv.endpoints["/merge"]
	receipt, err := st.WebhookDeliveryRepo().Create(context.Background(), store.WebhookDelivery{
		Endpoint: ep.Name, DeliveryID: "del-merge-blocked", EventType: "pull_request_review", PayloadHash: "hash-merge-blocked",
	})
	if err != nil || !receipt {
		t.Fatalf("create receipt: %v", err)
	}
	row, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-merge-blocked")
	if err != nil || row == nil {
		t.Fatalf("get receipt: %v", err)
	}
	item := dispatchItem{
		ep:          ep,
		body:        []byte(strings.Replace(prReviewBody(181, "feat/x", "owner", "commented", "c0ffee"), "abc1234def5678", "c0ffee", 1)),
		deliveryID:  "del-merge-blocked",
		eventType:   "pull_request_review",
		payloadHash: "hash-merge-blocked",
		prKey:       prKey,
	}
	d := newDispatcher(srv, "pr:"+prKey)
	srv.dispatchMu.Lock()
	srv.dispatchers[d.key] = d
	srv.dispatchMu.Unlock()
	d.handle(item)
	after, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-merge-blocked")
	if err != nil || after == nil {
		t.Fatalf("get receipt: %v", err)
	}
	if after.Status != store.WebhookStatusSkipped {
		t.Fatalf("merge delivery status = %q, want skipped", after.Status)
	}
	if !strings.Contains(after.ErrorSummary, "changes_requested") {
		t.Fatalf("merge delivery summary = %q, want the blocking verdict", after.ErrorSummary)
	}
}

// TestInternalReviewDeliveryIDIsStablePerHead asserts one fix round queues
// one review: the delivery id carries the head commit.
func TestInternalReviewDeliveryIDIsStablePerHead(t *testing.T) {
	first := internalReviewDeliveryID("acme/widget#181", "aaaaaaa")
	same := internalReviewDeliveryID("acme/widget#181", "aaaaaaa")
	other := internalReviewDeliveryID("acme/widget#181", "bbbbbbb")
	if first != same {
		t.Fatalf("delivery id %q must be stable for one head", first)
	}
	if first == other {
		t.Fatalf("delivery id %q must differ for another head", first)
	}
	if !strings.HasPrefix(first, "internal-review-") {
		t.Fatalf("delivery id = %q, want the internal review prefix", first)
	}
}

// TestInternalReviewBodyCarriesTheTrigger asserts the internal review is a
// comment delivery the review endpoint admits.
func TestInternalReviewBodyCarriesTheTrigger(t *testing.T) {
	body := internalReviewCommentBody("acme/widget", "181", "please re-review", "https://github.com/acme/widget/pull/181", "feat/x", "aaaaaaa")
	envelope := normalizeWebhook(body, "issue_comment", "d1", nil, false, "", []string{"please re-review"})
	if stringValue(envelope["comment_trigger"]) != "please re-review" {
		t.Fatalf("comment trigger = %q", stringValue(envelope["comment_trigger"]))
	}
	if stringValue(envelope["pr_number"]) != "181" {
		t.Fatalf("pr number = %q, want 181", stringValue(envelope["pr_number"]))
	}
	if prState(envelope) != "open" {
		t.Fatalf("pr state = %q, want open", prState(envelope))
	}
	if got := stringValue(envelope["head_sha"]); got != "aaaaaaa" {
		t.Fatalf("head sha = %q, want aaaaaaa", got)
	}
}

// TestMergeGateVerdictRendersIdenticallyInEveryLane asserts the merge gate is
// the one place the verdict is computed: a lane whose delivery carries no
// verdict of its own still reports the blocking one the gate holds, and the
// merge button agrees with it.
func TestMergeGateVerdictRendersIdenticallyInEveryLane(t *testing.T) {
	for _, lane := range []struct {
		path     string
		workflow string
	}{
		{path: "/review", workflow: "review"},
		{path: "/fix", workflow: "fix"},
		{path: "/merge", workflow: "merge"},
	} {
		t.Run(lane.workflow, func(t *testing.T) {
			srv, _, rec := newSurfaceServer(t)
			ep := srv.endpoints[lane.path]
			prKey := "acme/widget#181"
			ctx := context.Background()

			if verdict := srv.recordMergeGate(prKey, WebhookEnvelope{
				"repository":     "acme/widget",
				"pr_number":      "181",
				"review_state":   "commented",
				"review_verdict": "request_changes",
				"review_commit":  "c0ffee",
			}); verdict != "changes_requested" {
				t.Fatalf("gate verdict = %q, want changes_requested", verdict)
			}

			// A lane delivery that carries no review verdict of its own.
			envelope := WebhookEnvelope{
				"repository":  "acme/widget",
				"pr_number":   "181",
				"head_branch": "feat/x",
				"pr_state":    "open",
			}
			srv.emitPRAudit(ctx, ep, envelope, "COMPLETED", "", prKey, "thread-9")

			outcome := ""
			for _, msg := range rec.notified {
				if strings.Contains(msg.text, "verdict:") {
					outcome = msg.text
				}
			}
			if outcome == "" {
				t.Fatalf("the %s lane posted no verdict: %+v", lane.workflow, rec.notified)
			}
			if !strings.Contains(outcome, "REQUEST_CHANGES") {
				t.Fatalf("%s outcome = %q, want the blocking verdict the gate holds", lane.workflow, outcome)
			}

			// The gate outcome that lane renders names the blocking gate.
			// The merge lane reports a merge as ready only when the gate
			// allows it, so this fails when the lane loses the verdict.
			gateOutcome := FormatGateOutcome(withGateVerdict(envelope, srv.gateVerdict(prKey)), lane.workflow, "COMPLETED")
			if gateOutcome != "gate checked, review changes requested, fix running" {
				t.Fatalf("%s gate outcome = %q, want the blocking gate outcome", lane.workflow, gateOutcome)
			}
			if strings.Contains(gateOutcome, "merge ready") {
				t.Fatalf("%s gate outcome = %q, must not report merge ready while the gate blocks", lane.workflow, gateOutcome)
			}

			lines := rec.threadSends()
			if len(lines) == 0 {
				t.Fatalf("the %s lane posted no thread state line", lane.workflow)
			}
			if !strings.Contains(lines[len(lines)-1].text, "waiting: approval") {
				t.Fatalf("%s thread state line = %q, want the blocking verdict as the waiting reason", lane.workflow, lines[len(lines)-1].text)
			}

			mergeDisabled := false
			for _, msg := range append(append([]surfaceMessage(nil), rec.sent...), rec.edited...) {
				for _, button := range msg.buttons {
					if button.Label == "Merge" {
						mergeDisabled = button.Disabled
					}
				}
			}
			if !mergeDisabled {
				t.Fatalf("the merge button must stay disabled while the gate blocks (%s lane)", lane.workflow)
			}
		})
	}
}

// TestStuckSummaryNamesTheAbortOutcome asserts the receipt never claims an
// abort that did not run.
func TestStuckSummaryNamesTheAbortOutcome(t *testing.T) {
	aborted, logMessage := stuckSessionOutcome(stuckAbortDone, livenessAlive)
	if aborted != "stuck — session aborted" || logMessage != "webhook: dispatcher stuck, session aborted" {
		t.Fatalf("summary = %q / %q", aborted, logMessage)
	}
	failed, logMessage := stuckSessionOutcome(stuckAbortFailed, livenessAlive)
	if !strings.Contains(failed, "abort failed") || !strings.Contains(logMessage, "abort failed") {
		t.Fatalf("summary = %q / %q", failed, logMessage)
	}
	gone, logMessage := stuckSessionOutcome(stuckAbortNone, livenessDead)
	if gone != "stuck — no session to abort" || logMessage != "webhook: dispatcher stuck, no session to abort" {
		t.Fatalf("summary = %q / %q", gone, logMessage)
	}
	alive, logMessage := stuckSessionOutcome(stuckAbortNotPossible, livenessUnknown)
	if !strings.Contains(alive, "not aborted") || !strings.Contains(logMessage, "not aborted") {
		t.Fatalf("summary = %q / %q", alive, logMessage)
	}
	dead, logMessage := stuckSessionOutcome(stuckAbortNotPossible, livenessDead)
	if dead != "stuck — no session to abort" || logMessage != "webhook: dispatcher stuck, no session to abort" {
		t.Fatalf("summary = %q / %q", dead, logMessage)
	}
}
