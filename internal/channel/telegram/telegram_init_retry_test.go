package telegram

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// initBotTestServer returns an httptest server whose getMe endpoint fails
// with HTTP 500 for the first failCount requests, then succeeds. It counts
// total requests so tests can assert retry actually re-attempts.
func initBotTestServer(t *testing.T, failCount int) (*httptest.Server, *int32) {
	t.Helper()
	var requests int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		if strings.Contains(r.URL.Path, "getMe") {
			if int(atomic.LoadInt32(&requests)) <= failCount {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"test","username":"testbot"}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)
	return ts, &requests
}

func TestInitBotWithRetrySucceedsAfterTransientFailures(t *testing.T) {
	ts, requests := initBotTestServer(t, 2) // fail twice, succeed on 3rd

	bot, err := initBotWithRetry("faketoken", ts.URL+"/bot%s/%s", &http.Client{Timeout: 5 * time.Second}, time.Millisecond)
	if err != nil {
		t.Fatalf("initBotWithRetry: unexpected error: %v", err)
	}
	if bot == nil {
		t.Fatal("initBotWithRetry: nil bot on success")
	}
	if bot.Self.UserName != "testbot" {
		t.Fatalf("bot.Self.UserName = %q, want testbot", bot.Self.UserName)
	}
	// the bounded init client must NOT leak into the long-poll bot.Client
	if hc, ok := bot.Client.(*http.Client); !ok || hc.Timeout != 0 {
		t.Fatalf("bot.Client = %T (timeout %v), want unbounded *http.Client (timeout 0) so getUpdates long-poll is not cut", bot.Client, hcTimeout(bot.Client))
	}
	if got := atomic.LoadInt32(requests); got != 3 {
		t.Fatalf("requests = %d, want 3 (2 failures + 1 success)", got)
	}
}

// hcTimeout reports the timeout of an http.Client, or -1 for other types.
func hcTimeout(c any) time.Duration {
	if hc, ok := c.(*http.Client); ok {
		return hc.Timeout
	}
	return -1
}

func TestInitBotWithRetryFailsAfterAllAttempts(t *testing.T) {
	ts, requests := initBotTestServer(t, 100) // always fail

	bot, err := initBotWithRetry("faketoken", ts.URL+"/bot%s/%s", &http.Client{Timeout: 5 * time.Second}, time.Millisecond)
	if err == nil {
		t.Fatal("initBotWithRetry: expected error after exhausting attempts, got nil")
	}
	if bot != nil {
		t.Fatal("initBotWithRetry: expected nil bot on persistent failure")
	}
	if got := atomic.LoadInt32(requests); got != initBotMaxAttempts {
		t.Fatalf("requests = %d, want %d (bounded, no infinite retry)", got, initBotMaxAttempts)
	}
}

func TestInitBotWithRetrySucceedsFirstTry(t *testing.T) {
	ts, requests := initBotTestServer(t, 0) // succeed immediately

	bot, err := initBotWithRetry("faketoken", ts.URL+"/bot%s/%s", &http.Client{Timeout: 5 * time.Second}, time.Millisecond)
	if err != nil {
		t.Fatalf("initBotWithRetry: unexpected error: %v", err)
	}
	if bot == nil {
		t.Fatal("initBotWithRetry: nil bot on success")
	}
	if got := atomic.LoadInt32(requests); got != 1 {
		t.Fatalf("requests = %d, want 1 (no retry on clean success)", got)
	}
}
