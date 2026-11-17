package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

const (
	sessionCookieName = "occa_session"
	csrfHeader        = "X-OCCA-CSRF"
	sessionMaxAge     = 30 * 24 * time.Hour
)

type session struct {
	id        string
	csrf      string
	createdAt time.Time
}

func (s *Server) resolveAuth(r *http.Request) (*session, bool) {
	if h := r.Header.Get("Authorization"); h != "" {
		if tok, ok := strings.CutPrefix(h, "Bearer "); ok && s.token != "" &&
			subtle.ConstantTimeCompare([]byte(tok), []byte(s.token)) == 1 {
			return nil, true
		}
	}
	if c, err := r.Cookie(sessionCookieName); err == nil {
		if sess := s.lookupSession(c.Value); sess != nil {
			return sess, true
		}
	}
	return nil, false
}

func (s *Server) lookupSession(id string) *session {
	s.sessMu.Lock()
	defer s.sessMu.Unlock()
	sess := s.sessions[id]
	if sess == nil {
		return nil
	}
	if time.Since(sess.createdAt) > sessionMaxAge {
		delete(s.sessions, id)
		return nil
	}
	return sess
}

func (s *Server) createSession() (*session, error) {
	sess := &session{createdAt: time.Now()}
	id, err := randomHex(32)
	if err != nil {
		return nil, err
	}
	csrf, err := randomHex(32)
	if err != nil {
		return nil, err
	}
	sess.id = id
	sess.csrf = csrf
	s.sessMu.Lock()
	s.sessions[id] = sess
	s.sessMu.Unlock()
	return sess, nil
}

func (s *Server) deleteSession(id string) {
	s.sessMu.Lock()
	delete(s.sessions, id)
	s.sessMu.Unlock()
}

func csrfMatches(sess *session, r *http.Request) bool {
	provided := r.Header.Get(csrfHeader)
	return provided != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(sess.csrf)) == 1
}

func sessionCookie(sess *session) *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    sess.id,
		Path:     "/",
		MaxAge:   int(sessionMaxAge.Seconds()),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}
}

func expiredSessionCookie() *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
