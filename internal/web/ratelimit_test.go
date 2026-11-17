package web

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/anggasct/occa/internal/webhook"
)

func TestRateLimitPerWorkItem(t *testing.T) {
	fc := &fakeControl{fireOutcome: webhook.ActionOutcome{Accepted: true}}
	s := newTestServer(fc)
	target := "/api/v1/work-items/o%2Fr%231/actions/recheck"
	for i := 0; i < 10; i++ {
		rec := doReq(t, s, "POST", target, `{}`, bearerHeaders())
		if rec.Code != http.StatusAccepted {
			t.Fatalf("post %d: status %d", i, rec.Code)
		}
	}
	rec := doReq(t, s, "POST", target, `{}`, bearerHeaders())
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	ra, err := strconv.Atoi(rec.Header().Get("Retry-After"))
	if err != nil || ra < 1 || ra > 60 {
		t.Fatalf("Retry-After %q", rec.Header().Get("Retry-After"))
	}
	if len(fc.fireCalls) != 10 {
		t.Fatalf("fire calls %d", len(fc.fireCalls))
	}

	other := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%232/actions/recheck", `{}`, bearerHeaders())
	if other.Code != http.StatusAccepted {
		t.Fatalf("other item limited: status %d", other.Code)
	}
}

func TestRateLimitPerActor(t *testing.T) {
	fc := &fakeControl{fireOutcome: webhook.ActionOutcome{Accepted: true}}
	s := newTestServer(fc)
	for i := 1; i <= 6; i++ {
		target := "/api/v1/work-items/o%2Fr%23" + strconv.Itoa(i) + "/actions/recheck"
		for j := 0; j < 10; j++ {
			rec := doReq(t, s, "POST", target, `{}`, bearerHeaders())
			if rec.Code != http.StatusAccepted {
				t.Fatalf("item %d post %d: status %d", i, j, rec.Code)
			}
		}
	}
	rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%237/actions/recheck", `{}`, bearerHeaders())
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("actor limit: status %d", rec.Code)
	}
}

func TestRateLimitWindowExpires(t *testing.T) {
	l := newRateLimiter()
	now := time.Now()
	l.now = func() time.Time { return now }
	for i := 0; i < 10; i++ {
		if ok, _ := l.check("w1", "owner"); !ok {
			t.Fatalf("hit %d limited", i)
		}
	}
	if ok, wait := l.check("w1", "owner"); ok || wait < 1 {
		t.Fatalf("expected limited, ok=%v wait=%d", ok, wait)
	}
	now = now.Add(rateWindow + time.Second)
	if ok, _ := l.check("w1", "owner"); !ok {
		t.Fatal("still limited after window")
	}
}

func TestRateLimitReadsUnlimited(t *testing.T) {
	fc := &fakeControl{}
	s := newTestServer(fc)
	for i := 0; i < 20; i++ {
		rec := doReq(t, s, "GET", "/api/v1/inbox", "", bearerHeaders())
		if rec.Code != http.StatusOK {
			t.Fatalf("read %d: status %d", i, rec.Code)
		}
	}
}
