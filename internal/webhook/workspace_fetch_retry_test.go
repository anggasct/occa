package webhook

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestMissingRevisionRetryClass(t *testing.T) {
	err := fmt.Errorf("%w: revision deadbeef not in /repo after fetch", ErrRevisionMissing)
	if got := classifyFailure(err, nil, 2*time.Minute, false); got != failureBuild {
		t.Fatalf("missing revision = %v, want failureBuild", got)
	}
	max, backoffs := retryPolicyFor(failureBuild)
	if max != 2 || len(backoffs) != 2 || backoffs[0] != 60*time.Second || backoffs[1] != 5*time.Minute {
		t.Fatalf("build policy = %d %v, want 2 retries at 60s and 5m", max, backoffs)
	}
}

func TestSurfaceReopensFailedOnRetry(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	envelope := surfaceEnvelope()

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseFailed)
	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseFailed {
		t.Fatalf("surface phase = %q, want failed", phase)
	}
	if _, err := st.PRQueueRepo().EnsurePR(ctx, prKey, "thread-9", "parent-1"); err != nil {
		t.Fatalf("ensure pr: %v", err)
	}
	if !srv.planRetry(ep, prKey, 0, fmt.Errorf("%w: revision deadbeef not in /repo after fetch", ErrRevisionMissing), &WebhookWorkContext{}, false, false) {
		t.Fatal("missing revision must schedule a retry")
	}

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseReview)
	_, edited, _, _ := rec.snapshot()
	if len(edited) != 1 {
		t.Fatalf("a retried failed surface must accept the next phase write: %+v", edited)
	}
	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseReview {
		t.Fatalf("surface phase = %q, want review", phase)
	}
}

func TestSurfaceFailedFrozenWithoutRetry(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	ctx := context.Background()
	prKey := "acme/widget#181"
	envelope := surfaceEnvelope()

	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseFailed)
	srv.syncPhaseParent(ctx, ep, prKey, envelope, PhaseReview)
	_, edited, _, _ := rec.snapshot()
	if len(edited) != 0 {
		t.Fatalf("a failed surface must stay frozen without a retry: %+v", edited)
	}
	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseFailed {
		t.Fatalf("surface phase = %q, want failed", phase)
	}
}
