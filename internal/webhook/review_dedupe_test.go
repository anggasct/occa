package webhook

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

func reviewEventBody(t *testing.T, state, body, commit string) []byte {
	t.Helper()
	payload := map[string]any{
		"action":     "submitted",
		"repository": map[string]any{"full_name": "o/r"},
		"pull_request": map[string]any{
			"number":   7,
			"title":    "Fix it",
			"html_url": "https://github.com/o/r/pull/7",
			"user":     map[string]any{"login": "someone"},
			"head":     map[string]any{"ref": "feat/x"},
			"base":     map[string]any{"ref": "main", "repo": map[string]any{"full_name": "o/r"}},
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
		"review_verdict": reviewVerdict(body),
		"review_commit":  commit,
		"review_url":     "",
		"comment_body":   body,
	}
}

func seedReviewDelivery(t *testing.T, deliveries store.WebhookDeliveryRepo, endpoint, deliveryID string) int64 {
	t.Helper()
	ctx := context.Background()
	created, err := deliveries.Create(ctx, store.WebhookDelivery{
		Endpoint:    endpoint,
		DeliveryID:  deliveryID,
		EventType:   "pull_request_review",
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
	ep.Workflow = ""
	return ep
}

func TestDuplicateReviewDeliverySkipped(t *testing.T) {
	ep := reviewGuardEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	body := reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES\n\n## Findings\n- F-001", "5932515abc123")

	id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
	if err := srv.executeDelivery(ep, body, id1, "del-1", "pull_request_review", 1, nil, nil); err != nil {
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
	if err := srv.executeDelivery(ep, body, id2, "del-2", "pull_request_review", 1, nil, nil); err != nil {
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

func TestReviewDedupeNoFalsePositive(t *testing.T) {
	cases := []struct {
		name         string
		firstBody    string
		secondBody   string
		firstCommit  string
		secondCommit string
		firstState   string
		secondState  string
	}{
		{"different body", "**Verdict:** REQUEST_CHANGES\nfix A", "**Verdict:** REQUEST_CHANGES\nfix B", "5932515abc", "5932515abc", "changes_requested", "changes_requested"},
		{"different commit", "**Verdict:** REQUEST_CHANGES", "**Verdict:** REQUEST_CHANGES", "5932515abc", "deadbeef000", "changes_requested", "changes_requested"},
		{"different state", "**Verdict:** REQUEST_CHANGES", "**Verdict:** REQUEST_CHANGES", "5932515abc", "5932515abc", "changes_requested", "approved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := reviewGuardEndpoint()
			srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})

			id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
			if err := srv.executeDelivery(ep, reviewEventBody(t, tc.firstState, tc.firstBody, tc.firstCommit), id1, "del-1", "pull_request_review", 1, nil, nil); err != nil {
				t.Fatalf("first executeDelivery: %v", err)
			}
			id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-2")
			if err := srv.executeDelivery(ep, reviewEventBody(t, tc.secondState, tc.secondBody, tc.secondCommit), id2, "del-2", "pull_request_review", 1, nil, nil); err != nil {
				t.Fatalf("second executeDelivery: %v", err)
			}
			if count := exec.callCount(); count != 2 {
				t.Fatalf("distinct reviews spawned %d turns, want 2", count)
			}
		})
	}
}

func TestReviewDedupeWindowExpiry(t *testing.T) {
	ep := reviewGuardEndpoint()
	srv, exec, st := newTestServerFull(t, []config.EndpointConfig{ep})
	body := reviewEventBody(t, "changes_requested", "**Verdict:** REQUEST_CHANGES", "5932515abc")

	id1 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-1")
	if err := srv.executeDelivery(ep, body, id1, "del-1", "pull_request_review", 1, nil, nil); err != nil {
		t.Fatalf("first executeDelivery: %v", err)
	}

	srv.reviewDedupeNow = func() time.Time { return time.Now().Add(2 * time.Hour) }
	id2 := seedReviewDelivery(t, st.WebhookDeliveryRepo(), ep.Name, "del-2")
	if err := srv.executeDelivery(ep, body, id2, "del-2", "pull_request_review", 1, nil, nil); err != nil {
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
		reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES — other body", "5932515abc"),
		reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES", "deadbeef000"),
		reviewEnvelope("pull_request_review", "approved", "**Verdict:** REQUEST_CHANGES", "5932515abc"),
		reviewEnvelope("pull_request_review", "commented", "**Verdict:** REQUEST_CHANGES", "5932515abc"),
	}
	distinct[3]["repository"] = "other/r"
	for i, variant := range distinct {
		if got := reviewDedupeKey(variant); got == want {
			t.Fatalf("distinct variant %d collided with the base key", i)
		}
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

func TestReviewerPromptPostContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "webhooks", "github-reviewer.md"))
	if err != nil {
		t.Fatalf("read reviewer prompt: %v", err)
	}
	prompt := string(raw)
	for _, want := range []string{
		"post exactly ONE review command",
		"gh pr view PR_NUMBER --repo REPO --json reviews",
		"Never post a second review to verify",
		"never chain a fallback with `||`",
		"id/url and state into the working narration",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("reviewer prompt missing post-contract phrase %q", want)
		}
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
	prior, err := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "ep", "key-1", cutoff)
	if err != nil {
		t.Fatalf("lookup after restart: %v", err)
	}
	if prior == nil || prior.DeliveryID != "d1" {
		t.Fatalf("prior = %+v, want d1 after restart", prior)
	}
	if prior2, _ := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "ep", "key-1", time.Now().Add(2*time.Second).Unix()); prior2 != nil {
		t.Fatalf("stale cutoff resurrected a duplicate: %+v", prior2)
	}
	if miss, _ := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "ep", "key-2", cutoff); miss != nil {
		t.Fatalf("unknown key matched: %+v", miss)
	}
	if miss, _ := st2.WebhookDeliveryRepo().FindReviewDuplicate(ctx, "other", "key-1", cutoff); miss != nil {
		t.Fatalf("foreign endpoint matched: %+v", miss)
	}
}
