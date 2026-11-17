package web

import (
	"net/http"
	"testing"

	"github.com/anggasct/occa/internal/webhook"
)

func TestUnauthenticatedReturns401(t *testing.T) {
	s := newTestServer(&fakeControl{})
	for _, target := range []string{
		"/api/v1/inbox",
		"/api/v1/work-items/o%2Fr%231",
		"/api/v1/actions/7",
	} {
		rec := doReq(t, s, "GET", target, "", nil)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d", target, rec.Code)
		}
		if code := errorCode(t, rec); code != "unauthenticated" {
			t.Fatalf("%s: code %q", target, code)
		}
	}
	rec := doReq(t, s, "POST", "/api/v1/work-items/o%2Fr%231/actions/retry", `{}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("POST action: status %d", rec.Code)
	}
	rec = doReq(t, s, "GET", "/api/v1/inbox", "", map[string]string{"Authorization": "Bearer wrong"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer: status %d", rec.Code)
	}
}

func TestBearerAuth(t *testing.T) {
	fc := &fakeControl{inboxRows: []webhook.InboxRow{}}
	s := newTestServer(fc)
	rec := doReq(t, s, "GET", "/api/v1/inbox", "", bearerHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestPairWrongCode(t *testing.T) {
	s := newTestServer(&fakeControl{})
	rec := doReq(t, s, "POST", "/api/v1/pair", `{"code":"nope"}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
	if got := errorCode(t, rec); got != "unauthenticated" {
		t.Fatalf("code %q", got)
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Fatal("wrong pair set a cookie")
	}
}

func TestPairEmptyServerToken(t *testing.T) {
	s := New("127.0.0.1:0", "", &fakeControl{})
	rec := doReq(t, s, "POST", "/api/v1/pair", `{"code":""}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestPairCookieCSRFLogoutFlow(t *testing.T) {
	fc := &fakeControl{fireOutcome: webhook.ActionOutcome{
		Accepted:   true,
		ActionID:   9,
		StageRunID: 4,
		ThreadLine: "line",
	}}
	s := newTestServer(fc)
	cookie, csrf := pair(t, s)

	c := cookie
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.MaxAge != 2592000 {
		t.Fatalf("cookie attrs: %+v", c)
	}

	target := "/api/v1/work-items/o%2Fr%231/actions/recheck"
	rec := doReq(t, s, "POST", target, `{}`, map[string]string{"Cookie": c.Name + "=" + c.Value})
	if rec.Code != http.StatusForbidden || errorCode(t, rec) != "csrf_failed" {
		t.Fatalf("no csrf: status %d body %s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, s, "POST", target, `{}`, map[string]string{
		"Cookie":   c.Name + "=" + c.Value,
		csrfHeader: "wrong",
	})
	if rec.Code != http.StatusForbidden || errorCode(t, rec) != "csrf_failed" {
		t.Fatalf("bad csrf: status %d body %s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, s, "POST", target, `{}`, cookieHeaders(cookie, csrf))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("with csrf: status %d body %s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, s, "GET", "/api/v1/inbox", "", map[string]string{"Cookie": c.Name + "=" + c.Value})
	if rec.Code != http.StatusOK {
		t.Fatalf("cookie GET: status %d", rec.Code)
	}

	rec = doReq(t, s, "POST", "/api/v1/logout", "", map[string]string{"Cookie": c.Name + "=" + c.Value})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("logout without csrf: status %d", rec.Code)
	}

	rec = doReq(t, s, "POST", "/api/v1/logout", "", cookieHeaders(cookie, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("logout: status %d body %s", rec.Code, rec.Body.String())
	}
	var expired *http.Cookie
	for _, ec := range rec.Result().Cookies() {
		if ec.Name == sessionCookieName {
			expired = ec
		}
	}
	if expired == nil || expired.MaxAge >= 0 {
		t.Fatalf("logout did not expire cookie: %+v", expired)
	}

	rec = doReq(t, s, "GET", "/api/v1/inbox", "", map[string]string{"Cookie": c.Name + "=" + c.Value})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("after logout: status %d", rec.Code)
	}
}

func TestLogoutRequiresSession(t *testing.T) {
	s := newTestServer(&fakeControl{})
	rec := doReq(t, s, "POST", "/api/v1/logout", "", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rec.Code)
	}
	rec = doReq(t, s, "POST", "/api/v1/logout", "", bearerHeaders())
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bearer logout: status %d", rec.Code)
	}
}

func TestPairBadBody(t *testing.T) {
	s := newTestServer(&fakeControl{})
	rec := doReq(t, s, "POST", "/api/v1/pair", `not json`, nil)
	if rec.Code != http.StatusUnprocessableEntity || errorCode(t, rec) != "params_invalid" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}
