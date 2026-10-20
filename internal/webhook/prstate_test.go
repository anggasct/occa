package webhook

import (
	"context"
	"testing"
)

func setOpenPRState(srv *Server) {
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		return PRStateOpen, nil
	})
}

func setPRState(srv *Server, state string, err error) {
	srv.SetPRStateResolver(func(ctx context.Context, repo, number string) (string, error) {
		return state, err
	})
}

func TestParsePRState(t *testing.T) {
	cases := map[string]struct {
		payload string
		want    string
	}{
		"open":        {payload: `{"state":"OPEN","mergedAt":null}`, want: PRStateOpen},
		"closed":      {payload: `{"state":"CLOSED","mergedAt":null}`, want: PRStateClosed},
		"merged":      {payload: `{"state":"MERGED","mergedAt":"2026-10-20T09:32:55Z"}`, want: PRStateMerged},
		"mergedEmpty": {payload: `{"state":"MERGED","mergedAt":null}`, want: PRStateMerged},
	}
	for name, tc := range cases {
		got, err := parsePRState([]byte(tc.payload))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != tc.want {
			t.Errorf("%s: state = %q, want %q", name, got, tc.want)
		}
	}
	if _, err := parsePRState([]byte(`{"state":"WEIRD"}`)); err == nil {
		t.Error("unknown state must fail closed")
	}
	if _, err := parsePRState([]byte(`not json`)); err == nil {
		t.Error("broken payload must fail closed")
	}
}
