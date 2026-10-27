package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func reviewEventBody(t *testing.T, state, body, commit string) []byte {
	t.Helper()
	return reviewEventBodyForRepo(t, "o/r", state, body, commit)
}

func reviewEventBodyForRepo(t *testing.T, repo, state, body, commit string) []byte {
	t.Helper()
	payload := map[string]any{
		"action":     "submitted",
		"repository": map[string]any{"full_name": repo},
		"pull_request": map[string]any{
			"number":   7,
			"title":    "Fix it",
			"html_url": "https://github.com/" + repo + "/pull/7",
			"user":     map[string]any{"login": "someone"},
			"head":     map[string]any{"ref": "feat/x"},
			"base":     map[string]any{"ref": "main", "repo": map[string]any{"full_name": repo}},
		},
		"review": map[string]any{
			"state":     state,
			"body":      body,
			"commit_id": commit,
			"html_url":  "https://github.com/o/r/pull/7#pullrequestreview-5187604656",
			"user":      map[string]any{"login": "reviewer"},
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal review payload: %v", err)
	}
	return encoded
}

func reviewEnvelope(eventType, state, body, commit string) WebhookEnvelope {
	return WebhookEnvelope{
		"event_type":     eventType,
		"repository":     "o/r",
		"pr_number":      "7",
		"review_state":   state,
		"review_verdict": resolveVerdict(body, testVerdicts()),
		"review_commit":  commit,
		"review_url":     "",
		"comment_body":   body,
	}
}

func seedReviewDelivery(t *testing.T, deliveries store.WebhookDeliveryRepo, endpoint, deliveryID string) int64 {
	t.Helper()
	return seedDeliveryForEvent(t, deliveries, endpoint, deliveryID, "pull_request_review")
}

func seedDeliveryForEvent(t *testing.T, deliveries store.WebhookDeliveryRepo, endpoint, deliveryID, eventType string) int64 {
	t.Helper()
	ctx := context.Background()
	created, err := deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    endpoint,
		DeliveryID:  deliveryID,
		EventType:   eventType,
		PayloadHash: deliveryID,
	})
	if err != nil || !created {
		t.Fatalf("create receipt %s: created=%v err=%v", deliveryID, created, err)
	}
	receipt, err := deliveries.Get(ctx, endpoint, deliveryID)
	if err != nil || receipt == nil {
		t.Fatalf("get receipt %s: %v %v", deliveryID, receipt, err)
	}
	ok, err := deliveries.Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusProcessing, "")
	if err != nil || !ok {
		t.Fatalf("claim receipt %s: ok=%v err=%v", deliveryID, ok, err)
	}
	return receipt.ID
}

func reviewGuardEndpoint() config.EndpointConfig {
	ep := takeoverTestEndpoint()
	ep.Workflow = "review"
	return ep
}

func TestDuplicateReviewDeliverySkipped(t *testing.T) {
	ep := reviewGuardEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	body := reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES\n\n## Findings\n- F-001", "5932515abc123")

	id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
	if err := srv.executeDelivery(ep, body, id1, "del-1", "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("first delivery spawned %d turns, want 1", count)
	}
	first, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-1")
	if err != nil || first == nil || first.Status != store.WebhookStatusCompleted {
		t.Fatalf("first receipt = %+v err=%v, want completed", first, err)
	}

	id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-2")
	if err := srv.executeDelivery(ep, body, id2, "del-2", "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("duplicate executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("duplicate review spawned another turn (total %d, want 1)", count)
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-2")
	if err != nil || second == nil {
		t.Fatalf("get duplicate receipt: %v %v", second, err)
	}
	if second.Status != store.WebhookStatusSkipped {
		t.Fatalf("duplicate receipt status = %s, want skipped", second.Status)
	}
	if !strings.Contains(second.ErrorSummary, "duplicate review event") || !strings.Contains(second.ErrorSummary, "del-1") {
		t.Fatalf("duplicate skip reason = %q, want duplicate review event referencing del-1", second.ErrorSummary)
	}
}

func TestReviewDedupeDifferentHeadAdmitted(t *testing.T) {
	cases := []struct {
		name         string
		firstCommit  string
		secondCommit string
	}{
		{"different head", "5932515abc", "deadbeef000"},
		{"different head with same verdict and body", "5932515abc", "c0ffee12345"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := reviewGuardEndpoint()
			srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})

			id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
			if err := srv.executeDelivery(ep, reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", tc.firstCommit), id1, "del-1", "pull_request_review", 1, nil, nil, nil); err != nil {
				t.Fatalf("first executeDelivery: %v", err)
			}
			id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-2")
			if err := srv.executeDelivery(ep, reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", tc.secondCommit), id2, "del-2", "pull_request_review", 1, nil, nil, nil); err != nil {
				t.Fatalf("second executeDelivery: %v", err)
			}
			if count := exec.callCount(); count != 2 {
				t.Fatalf("distinct heads spawned %d turns, want 2", count)
			}
		})
	}
}

func TestReviewDedupeSameHeadSuppressedAcrossVerdicts(t *testing.T) {
	ep := reviewGuardEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})

	id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
	if err := srv.executeDelivery(ep, reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES\nfix A", "5932515abc"), id1, "del-1", "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}
	id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-2")
	if err := srv.executeDelivery(ep, reviewEventBody(t, "commented", "**Verdict:** REQUEST_CHANGES\nfix B", "5932515abc"), id2, "del-2", "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("second executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("same head spawned %d turns, want 1", count)
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-2")
	if err != nil || second == nil || second.Status != store.WebhookStatusSkipped {
		t.Fatalf("second receipt = %+v, %v; want skipped", second, err)
	}
	if !strings.Contains(second.ErrorSummary, "same head") || !strings.Contains(second.ErrorSummary, "del-1") {
		t.Fatalf("skip reason = %q, want same head handled by del-1", second.ErrorSummary)
	}
}

func TestReviewDedupeWindowExpiry(t *testing.T) {
	ep := reviewGuardEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	body := reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", "5932515abc")

	id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
	if err := srv.executeDelivery(ep, body, id1, "del-1", "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}

	srv.reviewDedupeNow = func() time.Time { return time.Now().Add(2 * time.Hour) }
	id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-2")
	if err := srv.executeDelivery(ep, body, id2, "del-2", "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("expired-window executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 2 {
		t.Fatalf("identical review after window spawned %d turns, want 2", count)
	}
}

func TestReviewDedupeKeyNormalization(t *testing.T) {
	base := reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES", "5932515abc")
	want := reviewDedupeKey(base)
	if want == "" {
		t.Fatal("review envelope produced empty key")
	}

	variants := []WebhookEnvelope{
		reviewEnvelope("pull_request_review", "COMMENTED", "  **Verdict:** REQUEST_CHANGES  ", "5932515abc"),
		reviewEnvelope("pull_request_review", "commented", "**Verdict:**  REQUEST_CHANGES", "5932515abc"),
		reviewEnvelope("pull_request_review", "commented", "\r\n**Verdict:** REQUEST_CHANGES\r\n\r\n", "5932515abc"),
	}
	for i, variant := range variants {
		if got := reviewDedupeKey(variant); got != want {
			t.Fatalf("formatting variant %d changed the key:\nbase:    %s\nvariant: %s", i, want, got)
		}
	}

	distinct := []WebhookEnvelope{
		reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES", "deadbeef000"),
		reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES", "5932515abc"),
	}
	distinct[1]["repository"] = "other/r"
	for i, variant := range distinct {
		if got := reviewDedupeKey(variant); got == want {
			t.Fatalf("distinct variant %d collided with the base key", i)
		}
	}

	sameHead := []WebhookEnvelope{
		reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES — other body", "5932515abc"),
		reviewEnvelope("pull_request_review", "approved", "**Verdict:** APPROVED", "5932515abc"),
		{
			"event_type":      "issue_comment",
			"repository":      "o/r",
			"pr_number":       "7",
			"head_sha":        "5932515abc",
			"comment_trigger": "please re-review",
			"comment_body":    "please re-review",
		},
	}
	for i, variant := range sameHead {
		if got := reviewDedupeKey(variant); got != want {
			t.Fatalf("same-head variant %d key = %q, want %q", i, got, want)
		}
	}

	withoutHead := WebhookEnvelope{
		"event_type":      "issue_comment",
		"repository":      "o/r",
		"pr_number":       "7",
		"comment_trigger": "please re-review",
	}
	if got := reviewDedupeKey(withoutHead); got != "" {
		t.Fatalf("comment without a head produced key %q, want empty", got)
	}
	withoutTrigger := WebhookEnvelope{
		"event_type": "issue_comment",
		"repository": "o/r",
		"pr_number":  "7",
		"head_sha":   "5932515abc",
	}
	if got := reviewDedupeKey(withoutTrigger); got != "" {
		t.Fatalf("comment without a trigger produced key %q, want empty", got)
	}

	if got := reviewDedupeKey(reviewEnvelope("pull_request", "commented", "**Verdict:** REQUEST_CHANGES", "5932515abc")); got != "" {
		t.Fatalf("non-review event produced key %q, want empty", got)
	}
}

func TestReviewVerdictLineOnRootCard(t *testing.T) {
	envelope := reviewEnvelope("pull_request_review", "changes_requested", "**Verdict:** REQUEST_CHANGES", "5932515abcdef")
	envelope["review_url"] = "https://github.com/o/r/pull/7#pullrequestreview-5187604656"

	for _, workflow := range []string{"github_reviewer", "github_fix", "github_merge", "github_merged"} {
		card := FormatRootCard(envelope, workflow, "RUNNING", "", "", "discord")
		if !strings.Contains(card, "Review verdict: REQUEST_CHANGES · commit 5932515 · https://github.com/o/r/pull/7#pullrequestreview-5187604656") {
			t.Fatalf("%s card missing verdict line: %s", workflow, card)
		}
	}

	short := reviewEnvelope("pull_request_review", "changes_requested", "**Verdict:** REQUEST_CHANGES", "5932515")
	if card := FormatRootCard(short, "github_fix", "RUNNING", "", "", "discord"); !strings.Contains(card, "Review verdict: REQUEST_CHANGES · commit 5932515") {
		t.Fatalf("card missing short-commit verdict line: %s", card)
	}

	noVerdict := reviewEnvelope("pull_request_review", "changes_requested", "looks fine", "5932515")
	if card := FormatRootCard(noVerdict, "github_fix", "RUNNING", "", "", "discord"); strings.Contains(card, "Review verdict:") {
		t.Fatalf("card gained verdict line without a verdict: %s", card)
	}
}

func TestReviewDuplicateLookupSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook.db")
	st, err := store.OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	created, err := st.WebhookDeliveryRepo().Create(ctx, store.WebhookDelivery{Endpoint: "ep", DeliveryID: "d1", EventType: "pull_request_review", PayloadHash: "h"})
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	receipt, _ := st.WebhookDeliveryRepo().Get(ctx, "ep", "d1")
	if err := st.WebhookDeliveryRepo().SetReviewKey(ctx, receipt.ID, "key-1"); err != nil {
		t.Fatalf("set review key: %v", err)
	}
	if _, err := st.WebhookDeliveryRepo().Transition(ctx, receipt.ID, []store.WebhookStatus{store.WebhookStatusReceived}, store.WebhookStatusCompleted, ""); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2, err := store.OpenWithDefaultWorkdir(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = st2.Close() }()

	cutoff := time.Now().Add(-time.Hour).Unix()
	prior, err := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "ep", "key-1", 0, cutoff)
	if err != nil {
		t.Fatalf("lookup after restart: %v", err)
	}
	if prior == nil || prior.DeliveryID != "d1" {
		t.Fatalf("prior = %+v, want d1 after restart", prior)
	}
	if prior2, _ := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "ep", "key-1", 0, time.Now().Add(2*time.Second).Unix()); prior2 != nil {
		t.Fatalf("stale cutoff resurrected a duplicate: %+v", prior2)
	}
	if miss, _ := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "ep", "key-2", 0, cutoff); miss != nil {
		t.Fatalf("unknown key matched: %+v", miss)
	}
	if miss, _ := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "other", "key-1", 0, cutoff); miss != nil {
		t.Fatalf("foreign endpoint matched: %+v", miss)
	}
}

func issueCommentEventBody(t *testing.T, prNumber int, commentBody string) []byte {
	t.Helper()
	return issueCommentEventBodyWithHead(t, prNumber, commentBody, "")
}

func issueCommentEventBodyWithHead(t *testing.T, prNumber int, commentBody, headSHA string) []byte {
	t.Helper()
	return issueCommentEventBodyForRepo(t, "o/r", prNumber, commentBody, headSHA)
}

func issueCommentEventBodyForRepo(t *testing.T, repo string, prNumber int, commentBody, headSHA string) []byte {
	t.Helper()
	payload := map[string]any{
		"action":     "created",
		"repository": map[string]any{"full_name": repo},
		"issue": map[string]any{
			"number":       prNumber,
			"title":        "Fix it",
			"state":        "open",
			"pull_request": map[string]any{"html_url": fmt.Sprintf("https://github.com/%s/pull/%d", repo, prNumber)},
		},
		"comment": map[string]any{
			"body": commentBody,
			"user": map[string]any{"login": "someone"},
		},
	}
	if headSHA != "" {
		payload["branch"] = "feat/x"
		payload["head_revision"] = headSHA
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal issue_comment payload: %v", err)
	}
	return encoded
}

func TestDuplicateReviewSuppressedAcrossSyntheticAndCommentPaths(t *testing.T) {
	const head = "5932515abc"
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	ep.CommentTrigger = []string{"please re-review"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})

	syntheticID := internalReviewDeliveryID("o/r#7", head)
	syntheticBody := internalReviewCommentBody("o/r", "7", "please re-review", "https://github.com/o/r/pull/7", "feat/x", head)
	syntheticReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, syntheticID, "issue_comment")
	if err := srv.executeDelivery(ep, syntheticBody, syntheticReceipt, syntheticID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("synthetic executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("synthetic review spawned %d turns, want 1", count)
	}

	commentID := "comment-webhook-echo"
	commentBody := issueCommentEventBodyWithHead(t, 7, "please re-review", head)
	commentReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, commentID, "issue_comment")
	if err := srv.executeDelivery(ep, commentBody, commentReceipt, commentID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("comment executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("same-head webhook echo spawned %d turns, want 1", count)
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, commentID)
	if err != nil || second == nil {
		t.Fatalf("get webhook echo receipt: %v %v", second, err)
	}
	if second.Status != store.WebhookStatusSkipped {
		t.Fatalf("webhook echo status = %s, want skipped", second.Status)
	}
	if !strings.Contains(second.ErrorSummary, "same head") || !strings.Contains(second.ErrorSummary, syntheticID) {
		t.Fatalf("webhook echo skip reason = %q, want same head handled by %s", second.ErrorSummary, syntheticID)
	}

	otherHeadID := "comment-webhook-other-head"
	otherHeadBody := issueCommentEventBodyWithHead(t, 7, "please re-review", "deadbeef000")
	otherHeadReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, otherHeadID, "issue_comment")
	if err := srv.executeDelivery(ep, otherHeadBody, otherHeadReceipt, otherHeadID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("different-head executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 2 {
		t.Fatalf("different head spawned %d turns, want 2", count)
	}
	third, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, otherHeadID)
	if err != nil || third == nil || third.Status != store.WebhookStatusCompleted {
		t.Fatalf("different-head receipt = %+v, %v; want completed", third, err)
	}
}

func TestDuplicateReviewSuppressedForCommentEchoWithoutHead(t *testing.T) {
	const head = "5932515abc"
	ep := reviewGuardEndpoint()
	ep.Name = "review-echo-no-head"
	ep.Workflow = "review"
	ep.CommentTrigger = []string{"please re-review"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	resolver := &fakePRHeadResolver{head: PRHead{SHA: head, Branch: "feat/x", HeadRepo: "o/r"}}
	srv.SetPRHeadResolver(resolver)

	syntheticID := internalReviewDeliveryID("o/r#7", head)
	syntheticBody := internalReviewCommentBody("o/r", "7", "please re-review", "https://github.com/o/r/pull/7", "feat/x", head)
	syntheticReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, syntheticID, "issue_comment")
	if err := srv.executeDelivery(ep, syntheticBody, syntheticReceipt, syntheticID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("synthetic executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("synthetic review spawned %d turns, want 1", count)
	}
	if resolver.calls != 0 {
		t.Fatalf("synthetic review resolved head %d times, want 0", resolver.calls)
	}

	commentID := "comment-webhook-echo-no-head"
	commentBody := issueCommentEventBody(t, 7, "please re-review")
	commentReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, commentID, "issue_comment")
	if err := srv.executeDelivery(ep, commentBody, commentReceipt, commentID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("comment executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("same-head comment echo spawned %d turns, want 1", count)
	}
	if resolver.calls == 0 || resolver.lastRepo != "o/r" || resolver.lastNumber != 7 {
		t.Fatalf("resolver calls = %d repo=%q number=%d, want a lookup for o/r#7", resolver.calls, resolver.lastRepo, resolver.lastNumber)
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, commentID)
	if err != nil || second == nil {
		t.Fatalf("get comment echo receipt: %v %v", second, err)
	}
	if second.Status != store.WebhookStatusSkipped {
		t.Fatalf("comment echo status = %s, want skipped", second.Status)
	}
	if !strings.Contains(second.ErrorSummary, "same head") || !strings.Contains(second.ErrorSummary, syntheticID) {
		t.Fatalf("comment echo skip reason = %q, want same head handled by %s", second.ErrorSummary, syntheticID)
	}
}

func TestDuplicateReviewSuppressedForCommentEchoWithoutHeadHTTP(t *testing.T) {
	const head = "5932515abc"
	ep := reviewGuardEndpoint()
	ep.Name = "review-echo-no-head-http"
	ep.Path = "/review"
	ep.Secret = "s3cret"
	ep.Repository = "o/r"
	ep.Workflow = "review"
	ep.CommentTrigger = []string{"please re-review"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	resolver := &fakePRHeadResolver{head: PRHead{SHA: head, Branch: "feat/x", HeadRepo: "o/r"}}
	srv.SetPRHeadResolver(resolver)

	syntheticID := internalReviewDeliveryID("o/r#7", head)
	syntheticBody := internalReviewCommentBody("o/r", "7", "please re-review", "https://github.com/o/r/pull/7", "feat/x", head)
	syntheticReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, syntheticID, "issue_comment")
	if err := srv.executeDelivery(ep, syntheticBody, syntheticReceipt, syntheticID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("synthetic executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("synthetic review spawned %d turns, want 1", count)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleRequest)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	commentID := "comment-webhook-echo-no-head-http"
	commentBody := issueCommentEventBody(t, 7, "please re-review")
	if resp := post(t, ts.URL+"/review?secret=s3cret", commentID, "issue_comment", string(commentBody)); resp.StatusCode != http.StatusOK {
		t.Fatalf("comment echo post status = %d, want 200", resp.StatusCode)
	}
	second := waitForReceiptDeliveryID(t, st, commentID, store.WebhookStatusSkipped)
	if count := exec.callCount(); count != 1 {
		t.Fatalf("same-head comment echo spawned %d turns, want 1", count)
	}
	if resolver.calls == 0 || resolver.lastRepo != "o/r" || resolver.lastNumber != 7 {
		t.Fatalf("resolver calls = %d repo=%q number=%d, want a lookup for o/r#7", resolver.calls, resolver.lastRepo, resolver.lastNumber)
	}
	if !strings.Contains(second.ErrorSummary, "same head") || !strings.Contains(second.ErrorSummary, syntheticID) {
		t.Fatalf("comment echo skip reason = %q, want same head handled by %s", second.ErrorSummary, syntheticID)
	}
}

func TestInFlightReviewDedupeSkipsSameHead(t *testing.T) {
	const head = "5932515abc"
	ep := reviewGuardEndpoint()
	ep.Name = "review-inflight-dedupe"
	ep.Workflow = "review"
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	exec.block = make(chan struct{})
	exec.started = make(chan struct{})

	body := reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", head)
	firstID := "del-inflight-1"
	firstReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, firstID, "pull_request_review")
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- srv.executeDelivery(ep, body, firstReceipt, firstID, "pull_request_review", 1, nil, nil, nil)
	}()

	select {
	case <-exec.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first review never reached the executor")
	}

	secondID := "del-inflight-2"
	secondReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, secondID, "pull_request_review")
	if err := srv.executeDelivery(ep, body, secondReceipt, secondID, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("second executeDelivery: %v", err)
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, secondID)
	if err != nil || second == nil {
		t.Fatalf("get second receipt: %v %v", second, err)
	}
	if second.Status != store.WebhookStatusSkipped {
		t.Fatalf("second receipt status = %s, want skipped", second.Status)
	}
	if !strings.Contains(second.ErrorSummary, "same head") || !strings.Contains(second.ErrorSummary, firstID) {
		t.Fatalf("second skip reason = %q, want same head handled by %s", second.ErrorSummary, firstID)
	}

	close(exec.block)
	if err := <-firstErr; err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("same-head in-flight duplicate spawned %d turns, want 1", count)
	}
}

func TestFixTriggerNotSuppressedByReviewDedupe(t *testing.T) {
	const head = "5932515abc"
	ep := reviewGuardEndpoint()
	ep.Name = "fix-dedupe-test"
	ep.Workflow = "fix"
	ep.CommentTrigger = []string{"please fix review"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})

	reviewID := "del-review-fix"
	reviewReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, reviewID, "pull_request_review")
	if err := srv.executeDelivery(ep, reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", head), reviewReceipt, reviewID, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("review executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 1 {
		t.Fatalf("review delivery spawned %d turns, want 1", count)
	}

	commentID := "del-fix-comment"
	commentReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, commentID, "issue_comment")
	if err := srv.executeDelivery(ep, issueCommentEventBodyWithHead(t, 7, "please fix review", head), commentReceipt, commentID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("fix comment executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 2 {
		t.Fatalf("fix retrigger spawned %d turns, want 2", count)
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, commentID)
	if err != nil || second == nil || second.Status != store.WebhookStatusCompleted {
		t.Fatalf("fix comment receipt = %+v, %v; want completed", second, err)
	}
}

func TestCommentTriggerRateCap(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	ep.CommentTrigger = []string{"please re-review"}
	ep.Admit = []config.AdmitRule{
		{Event: "issue_comment", Actions: []string{"created"}, Require: []string{"comment_trigger", "pr_open"}},
	}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Hour}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})

	bodyPR7 := issueCommentEventBody(t, 7, "please re-review this PR")

	for i := 1; i <= 3; i++ {
		delID := fmt.Sprintf("del-%d", i)
		id := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, delID)
		if err := srv.executeDelivery(ep, bodyPR7, id, delID, "issue_comment", 1, nil, nil, nil); err != nil {
			t.Fatalf("executeDelivery %d: %v", i, err)
		}
		if count := exec.callCount(); count != i {
			t.Fatalf("delivery %d spawned %d turns, want %d", i, count, i)
		}
		receipt, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, delID)
		if err != nil || receipt == nil || receipt.Status != store.WebhookStatusCompleted {
			t.Fatalf("receipt %d = %+v err=%v, want completed", i, receipt, err)
		}
	}

	id4 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-4")
	if err := srv.executeDelivery(ep, bodyPR7, id4, "del-4", "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("executeDelivery 4: %v", err)
	}
	if count := exec.callCount(); count != 3 {
		t.Fatalf("fourth delivery spawned turn (total %d, want 3)", count)
	}
	fourth, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, "del-4")
	if err != nil || fourth == nil {
		t.Fatalf("get fourth receipt: %v", err)
	}
	if fourth.Status != store.WebhookStatusSkipped {
		t.Fatalf("fourth receipt status = %s, want skipped", fourth.Status)
	}
	if !strings.Contains(fourth.ErrorSummary, "trigger rate cap") {
		t.Fatalf("fourth skip reason = %q, want trigger rate cap", fourth.ErrorSummary)
	}

	bodyPR8 := issueCommentEventBody(t, 8, "please re-review this PR")
	idPR8 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-pr8")
	if err := srv.executeDelivery(ep, bodyPR8, idPR8, "del-pr8", "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("executeDelivery PR 8: %v", err)
	}
	if count := exec.callCount(); count != 4 {
		t.Fatalf("different PR spawned %d turns, want 4", count)
	}

	srv.triggerLimits[ep.Name] = config.EndpointLimits{MaxRunsPerPR: 3, Window: time.Minute}
	for _, delID := range []string{"del-1", "del-2", "del-3"} {
		if _, err := st.DB().Exec(`UPDATE webhook_delivery SET updated_at = ? WHERE endpoint = ? AND delivery_id = ?`, time.Now().Add(-time.Hour).Unix(), ep.Name, delID); err != nil {
			t.Fatalf("backdate trigger rows: %v", err)
		}
	}
	id5 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-5")
	if err := srv.executeDelivery(ep, bodyPR7, id5, "del-5", "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("expired window executeDelivery: %v", err)
	}
	if count := exec.callCount(); count != 5 {
		t.Fatalf("delivery after window spawned %d turns, want 5", count)
	}
}

type fakeReviewObserver struct {
	obs   ReviewObservation
	err   error
	calls int
}

func (f *fakeReviewObserver) observe(context.Context, string, string) (ReviewObservation, error) {
	f.calls++
	return f.obs, f.err
}

func hasReviewKey(t *testing.T, st *store.SQLiteStore, endpoint, key string) bool {
	t.Helper()
	prior, err := st.WebhookDeliveryRepo().FindReviewDuplicate(context.Background(), endpoint, key, 0, time.Now().Add(-time.Hour).Unix())
	if err != nil {
		t.Fatalf("find review duplicate %q: %v", key, err)
	}
	return prior != nil
}

func TestReviewDedupeCompletionKeysVerdictHead(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	ep.CommentTrigger = []string{"please re-review"}
	ep.Limits = &config.EndpointLimits{MaxRunsPerPR: 10, Window: time.Hour}
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	const admitted = "aaaaaaaaaa"
	const judged = "bbbbbbbbbb"
	observer := &fakeReviewObserver{obs: ReviewObservation{LiveHead: judged, VerdictHead: judged}}
	srv.SetReviewObserver(observer.observe)

	firstID := "review-admitted-a"
	firstReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, firstID, "pull_request_review")
	body := reviewEventBody(t, "commented", "**Verdict:** APPROVED", admitted)
	if err := srv.executeDelivery(ep, body, firstReceipt, firstID, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("first review spawned %d turns, want 1", exec.callCount())
	}
	if !hasReviewKey(t, st, ep.Name, "o/r#7@"+judged) {
		t.Fatalf("completed review did not move its key to the judged head %s", judged)
	}
	if hasReviewKey(t, st, ep.Name, "o/r#7@"+admitted) {
		t.Fatalf("completed review still keys the admission head %s", admitted)
	}

	internalID := internalReviewDeliveryID("o/r#7", judged)
	internalBody := internalReviewCommentBody("o/r", "7", "please re-review", "https://github.com/o/r/pull/7", "feat/x", judged)
	internalReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, internalID, "issue_comment")
	if err := srv.executeDelivery(ep, internalBody, internalReceipt, internalID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("internal executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("internal re-review for the judged head spawned a turn (total %d, want 1)", exec.callCount())
	}
	skipped, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, internalID)
	if err != nil || skipped == nil {
		t.Fatalf("get internal receipt: %v %v", skipped, err)
	}
	wantReason := "duplicate review event (same head, already handled by delivery " + firstID + ")"
	if skipped.Status != store.WebhookStatusSkipped || !strings.Contains(skipped.ErrorSummary, wantReason) {
		t.Fatalf("internal receipt = status %s reason %q, want skipped with %q", skipped.Status, skipped.ErrorSummary, wantReason)
	}

	const newer = "cccccccccc"
	observer.obs = ReviewObservation{LiveHead: newer, VerdictHead: newer}
	newerID := "comment-new-head"
	newerBody := issueCommentEventBodyWithHead(t, 7, "please re-review", newer)
	newerReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, newerID, "issue_comment")
	if err := srv.executeDelivery(ep, newerBody, newerReceipt, newerID, "issue_comment", 1, nil, nil, nil); err != nil {
		t.Fatalf("new head executeDelivery: %v", err)
	}
	if exec.callCount() != 2 {
		t.Fatalf("new head spawned %d turns total, want 2", exec.callCount())
	}
	if !hasReviewKey(t, st, ep.Name, "o/r#7@"+newer) {
		t.Fatalf("new head %s was not keyed after its review completed", newer)
	}
}

func TestReviewDedupeMixedCaseRepositoryUsesCanonicalKey(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	const head = "bbbbbbbbbb"
	observer := &fakeReviewObserver{obs: ReviewObservation{LiveHead: head, VerdictHead: head}}
	srv.SetReviewObserver(observer.observe)

	firstID := "review-mixed-case"
	firstReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, firstID, "pull_request_review")
	firstBody := reviewEventBodyForRepo(t, "Owner/Repo", "changes_requested", "**Verdict:** REQUEST_CHANGES", head)
	if err := srv.executeDelivery(ep, firstBody, firstReceipt, firstID, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("first review spawned %d turns, want 1", exec.callCount())
	}
	if !hasReviewKey(t, st, ep.Name, "owner/repo#7@"+head) {
		t.Fatalf("completed review did not key the canonical repository and judged head")
	}

	secondID := "review-mixed-case-duplicate"
	secondReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, secondID, "pull_request_review")
	secondBody := reviewEventBodyForRepo(t, "Owner/Repo", "approved", "**Verdict:** APPROVED", head)
	if err := srv.executeDelivery(ep, secondBody, secondReceipt, secondID, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("duplicate executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("same judged head with mixed-case repository spawned another turn (total %d, want 1)", exec.callCount())
	}
	second, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, secondID)
	if err != nil || second == nil {
		t.Fatalf("get duplicate receipt: %v %v", second, err)
	}
	if second.Status != store.WebhookStatusSkipped {
		t.Fatalf("duplicate receipt status = %s, want skipped", second.Status)
	}
	if !strings.Contains(second.ErrorSummary, "delivery "+firstID) {
		t.Fatalf("duplicate skip reason = %q, want handled by %s", second.ErrorSummary, firstID)
	}
}

func TestReviewDedupeSupersededRunKeepsJudgedHead(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	const judged = "bbbbbbbbbb"
	const newer = "cccccccccc"
	observer := &fakeReviewObserver{obs: ReviewObservation{LiveHead: newer, VerdictHead: judged}}
	srv.SetReviewObserver(observer.observe)

	id := "review-superseded"
	receipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, id, "pull_request_review")
	body := reviewEventBody(t, "commented", "**Verdict:** REQUEST_CHANGES", judged)
	if err := srv.executeDelivery(ep, body, receipt, id, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("superseded review spawned %d turns, want 1", exec.callCount())
	}
	got, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, id)
	if err != nil || got == nil {
		t.Fatalf("get receipt: %v %v", got, err)
	}
	if !strings.Contains(got.ErrorSummary, "superseded by head "+newer[:10]) {
		t.Fatalf("error summary = %q, want superseded by head %s", got.ErrorSummary, newer[:10])
	}
	if !hasReviewKey(t, st, ep.Name, "o/r#7@"+judged) {
		t.Fatalf("superseded review did not keep the judged head %s", judged)
	}
	if hasReviewKey(t, st, ep.Name, "o/r#7@"+newer) {
		t.Fatalf("superseded review keyed the newer head %s it never judged", newer)
	}
}

func TestReviewDedupeTwoPushesKeysVerdictHead(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	const admitted = "d2137f9211"
	const judged = "14320114aa"
	const newer = "9f3c2a1bbb"
	observer := &fakeReviewObserver{obs: ReviewObservation{LiveHead: newer, VerdictHead: judged}}
	srv.SetReviewObserver(observer.observe)

	id := "review-two-pushes"
	receipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, id, "pull_request_review")
	body := reviewEventBody(t, "commented", "**Verdict:** REQUEST_CHANGES", admitted)
	if err := srv.executeDelivery(ep, body, receipt, id, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("two-push review spawned %d turns, want 1", exec.callCount())
	}
	got, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, id)
	if err != nil || got == nil {
		t.Fatalf("get receipt: %v %v", got, err)
	}
	if !strings.Contains(got.ErrorSummary, "superseded by head "+newer[:10]) {
		t.Fatalf("error summary = %q, want superseded by head %s", got.ErrorSummary, newer[:10])
	}
	if !hasReviewKey(t, st, ep.Name, "o/r#7@"+judged) {
		t.Fatalf("two-push review did not key the verdict head %s", judged)
	}
	if hasReviewKey(t, st, ep.Name, "o/r#7@"+admitted) {
		t.Fatalf("two-push review keyed the admission head %s it never judged", admitted)
	}
}

func TestReviewDedupeNoVerdictKeepsAdmissionHead(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	const admitted = "aaaaaaaaaa"
	const moved = "bbbbbbbbbb"
	observer := &fakeReviewObserver{obs: ReviewObservation{LiveHead: moved, VerdictHead: ""}}
	srv.SetReviewObserver(observer.observe)

	id := "review-no-verdict-moved"
	receipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, id, "pull_request_review")
	body := reviewEventBody(t, "commented", "", admitted)
	if err := srv.executeDelivery(ep, body, receipt, id, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("executeDelivery: %v", err)
	}
	if exec.callCount() != 1 {
		t.Fatalf("no-verdict review spawned %d turns, want 1", exec.callCount())
	}
	got, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, id)
	if err != nil || got == nil {
		t.Fatalf("get receipt: %v %v", got, err)
	}
	if !strings.Contains(got.ErrorSummary, "superseded by head "+moved[:10]) {
		t.Fatalf("error summary = %q, want superseded by head %s", got.ErrorSummary, moved[:10])
	}
	if !hasReviewKey(t, st, ep.Name, "o/r#7@"+admitted) {
		t.Fatalf("no-verdict review did not keep the admission head %s", admitted)
	}
	if hasReviewKey(t, st, ep.Name, "o/r#7@"+moved) {
		t.Fatalf("no-verdict review keyed the live head %s it never judged", moved)
	}

	nextID := "review-moved-head"
	nextReceipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, nextID, "pull_request_review")
	nextBody := reviewEventBody(t, "commented", "**Verdict:** APPROVED", moved)
	if err := srv.executeDelivery(ep, nextBody, nextReceipt, nextID, "pull_request_review", 1, nil, nil, nil); err != nil {
		t.Fatalf("moved-head executeDelivery: %v", err)
	}
	if exec.callCount() != 2 {
		t.Fatalf("moved-head review spawned %d turns total, want 2", exec.callCount())
	}
	nextGot, err := st.WebhookDeliveryRepo().Get(context.Background(), ep.Name, nextID)
	if err != nil || nextGot == nil {
		t.Fatalf("get moved-head receipt: %v %v", nextGot, err)
	}
	if nextGot.Status == store.WebhookStatusSkipped {
		t.Fatalf("moved-head review was skipped as duplicate: %q", nextGot.ErrorSummary)
	}
}

func TestReviewDedupePlainOpenedUnchanged(t *testing.T) {
	ep := reviewGuardEndpoint()
	ep.Workflow = "review"
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	body := []byte(`{"action":"opened","repository":{"full_name":"o/r"},"pull_request":{"number":7,"title":"Fix it","html_url":"https://github.com/o/r/pull/7","user":{"login":"someone"},"head":{"ref":"feat/x","sha":"5932515abc"},"base":{"ref":"main","repo":{"full_name":"o/r"}}}}`)
	for _, id := range []string{"opened-1", "opened-2"} {
		receipt := seedDeliveryForEvent(t, st.WebhookDeliveryRepo(), ep.Name, id, "pull_request")
		if err := srv.executeDelivery(ep, body, receipt, id, "pull_request", 1, nil, nil, nil); err != nil {
			t.Fatalf("executeDelivery %s: %v", id, err)
		}
	}
	if exec.callCount() != 2 {
		t.Fatalf("plain pull_request admissions spawned %d turns, want 2 unchanged", exec.callCount())
	}
}
