package webhook

import (
	"testing"

	"github.com/anggasct/occa/internal/config"
)

func TestDispatchLockKeyIssueCommentPerPR(t *testing.T) {
	ep := config.EndpointConfig{Platform: "telegram", ChannelID: "chat1"}
	bodyPR44a := []byte(`{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"number":44,"pull_request":{"html_url":"https://github.com/acme/widgets/pull/44"}},"comment":{"body":"please re-review"}}`)
	bodyPR44b := []byte(`{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"number":44,"pull_request":{"html_url":"https://github.com/acme/widgets/pull/44"}},"comment":{"body":"still waiting"}}`)
	bodyPR45 := []byte(`{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"number":45,"pull_request":{"html_url":"https://github.com/acme/widgets/pull/45"}},"comment":{"body":"please re-review"}}`)

	for _, body := range [][]byte{bodyPR44a, bodyPR44b, bodyPR45} {
		if key := ExtractExecutionKey(body); !key.IsZero() {
			t.Fatalf("expected zero execution key for comment payload, got %+v", key)
		}
	}

	key44a := dispatchLockKey(ep, WebhookExecutionKey{}, dispatchItem{ep: ep, body: bodyPR44a, eventType: "issue_comment"})
	key44b := dispatchLockKey(ep, WebhookExecutionKey{}, dispatchItem{ep: ep, body: bodyPR44b, eventType: "issue_comment"})
	key45 := dispatchLockKey(ep, WebhookExecutionKey{}, dispatchItem{ep: ep, body: bodyPR45, eventType: "issue_comment"})

	if key44a == "" || key44b == "" || key45 == "" {
		t.Fatalf("expected non-empty lock keys, got %q %q %q", key44a, key44b, key45)
	}
	if key44a != key44b {
		t.Fatalf("same-PR lock keys differ: %q vs %q", key44a, key44b)
	}
	if key44a == key45 {
		t.Fatalf("different-PR lock keys match: %q", key44a)
	}

	base := "endpoint:telegram|chat1"
	if key44a == base || key45 == base {
		t.Fatalf("comment lock keys must extend the per-channel lock, got %q and %q", key44a, key45)
	}

	missingNumber := []byte(`{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"title":"no number"},"comment":{"body":"hi"}}`)
	fallback := dispatchLockKey(ep, WebhookExecutionKey{}, dispatchItem{ep: ep, body: missingNumber, eventType: "issue_comment"})
	if fallback != base {
		t.Fatalf("missing number must fall back to per-channel lock, got %q want %q", fallback, base)
	}

	nonComment := dispatchLockKey(ep, WebhookExecutionKey{}, dispatchItem{ep: ep, body: bodyPR44a, eventType: "ping"})
	if nonComment != base {
		t.Fatalf("non-comment zero key must stay per-channel, got %q want %q", nonComment, base)
	}
}

func TestDispatchLockKeyKeepsBranchKeying(t *testing.T) {
	ep := config.EndpointConfig{Platform: "telegram", ChannelID: "chat1"}
	body := []byte(`{"repository":{"full_name":"anggasct/occa"},"pull_request":{"base":{"repo":{"full_name":"anggasct/occa"}},"head":{"repo":{"full_name":"anggasct/occa"},"ref":"feat/branch-1"}}}`)
	key := ExtractExecutionKey(body)
	if key.IsZero() {
		t.Fatal("expected non-zero execution key for pull_request payload")
	}
	got := dispatchLockKey(ep, key, dispatchItem{ep: ep, body: body, eventType: "pull_request"})
	want := "key:" + key.String()
	if got != want {
		t.Fatalf("branch lock = %q, want %q", got, want)
	}
}
