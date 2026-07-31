package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDiscoverSuccess(t *testing.T) {
	doc := OpenAPIDoc{
		Info: OpenAPIInfo{Version: "1.2.3"},
		Paths: map[string]any{
			"/session":              nil,
			"/session/{id}/message": nil,
			"/session/{id}/command": nil,
			"/event":                nil,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/doc" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(doc)
	}))
	defer srv.Close()

	got, err := Discover(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if got.Info.Version != "1.2.3" {
		t.Fatalf("version = %q, want 1.2.3", got.Info.Version)
	}
	if missing := got.MissingEndpoints(); len(missing) != 0 {
		t.Fatalf("expected no missing endpoints, got: %v", missing)
	}
}

func TestDiscoverUnreachable(t *testing.T) {
	_, err := Discover(context.Background(), "http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error for unreachable agent")
	}
}

func TestDiscoverMissingEndpoints(t *testing.T) {
	doc := OpenAPIDoc{
		Info: OpenAPIInfo{Version: "0.9.0"},
		Paths: map[string]any{
			"/session": nil,
			"/event":   nil,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(doc)
	}))
	defer srv.Close()

	got, err := Discover(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	missing := got.MissingEndpoints()
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing endpoints, got %d: %v", len(missing), missing)
	}
}

func TestDiscoverNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := Discover(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

func TestDiscoverTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(6 * time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := Discover(ctx, srv.URL)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
