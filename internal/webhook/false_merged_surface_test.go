package webhook

import (
	"context"
	"strings"
	"testing"
)

func mergeSurfaceEnvelope() WebhookEnvelope {
	envelope := surfaceEnvelope()
	envelope["merged"] = true
	envelope["pr_state"] = "merged"
	return envelope
}

func TestEmitPRAuditSkipPaintsNoPhase(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/merged"]
	prKey := "acme/widget#181"
	envelope := surfaceEnvelope()
	envelope["pr_state"] = "open"
	envelope["merged"] = false

	srv.emitPRAudit(context.Background(), ep, envelope, "SKIP", "no rule for pull_request.opened", prKey, "thread-9")

	if _, phase, _, known := srv.prSurfaceState(prKey); known {
		t.Fatalf("SKIP wrote phase %q, want no phase write", phase)
	}
	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 0 || len(edited) != 0 || len(rec.threadSends()) != 0 {
		t.Fatalf("SKIP wrote surfaces: sent=%d edited=%d thread=%d", len(sent), len(edited), len(rec.threadSends()))
	}
	if _, phase, _, _ := srv.prSurfaceState(prKey); IsTerminalPhase(phase) {
		t.Fatalf("SKIP left a terminal phase %q", phase)
	}
}

func TestEmitPRAuditSkipKeepsExistingPhase(t *testing.T) {
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	ep := srv.endpoints["/merged"]
	prKey := "acme/widget#181"

	srv.syncPhaseParent(ctx, ep, prKey, surfaceEnvelope(), PhaseReview)
	envelope := surfaceEnvelope()
	envelope["pr_state"] = "open"
	srv.emitPRAudit(ctx, ep, envelope, "SKIP", "admitted no rule", prKey, "thread-9")

	if _, phase, _, known := srv.prSurfaceState(prKey); !known || phase != PhaseReview {
		t.Fatalf("phase after SKIP = %q (known=%v), want review", phase, known)
	}
	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 1 || len(edited) != 0 {
		t.Fatalf("SKIP must not repaint the parent: sent=%d edited=%d", len(sent), len(edited))
	}
}

func TestSyncPhaseRefusesMergedWithoutProof(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _, rec := newSurfaceServer(t)
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"
	envelope := surfaceEnvelope()
	envelope["merged"] = false
	envelope["pr_state"] = "open"

	srv.syncPhaseParent(context.Background(), ep, prKey, envelope, PhaseMerged)

	if _, phase, _, known := srv.prSurfaceState(prKey); known {
		t.Fatalf("surface phase = %q, want no write", phase)
	}
	sent, edited, _, _ := rec.snapshot()
	if len(sent) != 0 || len(edited) != 0 {
		t.Fatalf("refused merged write sent surfaces: sent=%d edited=%d", len(sent), len(edited))
	}
	if got := len(logs.all("webhook: merged phase refused (no merge proof)")); got != 1 {
		t.Fatalf("refused log lines = %d, want one", got)
	}
}

func TestSyncPhaseAcceptsMergedWithProof(t *testing.T) {
	cases := map[string]WebhookEnvelope{
		"merged flag": func() WebhookEnvelope {
			envelope := surfaceEnvelope()
			envelope["merged"] = true
			return envelope
		}(),
		"pr state": func() WebhookEnvelope {
			envelope := surfaceEnvelope()
			envelope["pr_state"] = "merged"
			return envelope
		}(),
	}
	for name, envelope := range cases {
		t.Run(name, func(t *testing.T) {
			srv, _, rec := newSurfaceServer(t)
			prKey := "acme/widget#181"
			srv.syncPhaseParent(context.Background(), srv.endpoints["/review"], prKey, envelope, PhaseMerged)
			if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseMerged {
				t.Fatalf("surface phase = %q (known=%v), want merged", phase, ok)
			}
			sent, _, _, _ := rec.snapshot()
			if len(sent) != 1 {
				t.Fatalf("parent sends = %d, want one merged write", len(sent))
			}
		})
	}
}

func TestAdmissionPhaseMergedIsEmpty(t *testing.T) {
	if got := admissionPhase("merged"); got != "" {
		t.Fatalf("admissionPhase(merged) = %q, want empty", got)
	}
	for _, workflow := range []string{"review", "fix", "merge"} {
		if got := admissionPhase(workflow); got != PhaseQueued {
			t.Fatalf("admissionPhase(%s) = %q, want queued", workflow, got)
		}
	}
}

func TestPreRunWriteSkipsMergedLane(t *testing.T) {
	srv, st, rec := newSurfaceServer(t)
	setPRState(srv, PRStateOpen, nil)
	ep := srv.endpoints["/merge"]
	ep.Admit = defaultCatchAllAdmit()
	srv.endpoints["/merge"] = ep
	body := []byte(prReviewBody(181, "feat/x", "owner", "approved", "c0ffee"))
	deliveryID := "del-pre-run-merge"
	receiptID := seedProcessingDelivery(t, st, ep.Name, deliveryID, "pull_request_review")

	if err := srv.executeDelivery(ep, body, receiptID, deliveryID, "pull_request_review", 1, nil, func() bool { return false }, nil); err != nil {
		t.Fatalf("execute merge lane: %v", err)
	}
	if _, phase, _, ok := srv.prSurfaceState("acme/widget#181"); ok && phase == PhaseMerged {
		t.Fatal("merge lane painted merged before confirmation")
	}
	sent, edited, _, _ := rec.snapshot()
	for _, msg := range append(sent, edited...) {
		if strings.HasSuffix(msg.text, " — merged") {
			t.Fatalf("pre-run write painted merged: %q", msg.text)
		}
	}
}

func TestRepairMergedSurfaceOnOpenEvidence(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _, _ := newSurfaceServer(t)
	ctx := context.Background()
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"

	merged := surfaceEnvelope()
	merged["merged"] = true
	merged["pr_state"] = "merged"
	srv.syncPhaseParent(ctx, ep, prKey, merged, PhaseMerged)
	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseMerged {
		t.Fatalf("seeded phase = %q, want merged", phase)
	}

	open := surfaceEnvelope()
	open["event_type"] = "pull_request"
	open["action"] = "opened"
	open["merged"] = false
	open["pr_state"] = "open"
	srv.syncPhaseParent(ctx, ep, prKey, open, admissionPhase(ep.Workflow))
	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase == PhaseMerged {
		t.Fatalf("repaired phase = %q, want a non-terminal phase", phase)
	}
	if got := len(logs.all("webhook: surface merged repaired")); got != 1 {
		t.Fatalf("repair log lines = %d, want one", got)
	}

	srv.syncPhaseParent(ctx, ep, prKey, open, PhaseReview)
	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseReview {
		t.Fatalf("write after repair = %q (known=%v), want review", phase, ok)
	}
}

func TestRepairIgnoresMergeEvidence(t *testing.T) {
	logs := captureServerLogs(t)
	srv, _, rec := newSurfaceServer(t)
	ctx := context.Background()
	ep := srv.endpoints["/review"]
	prKey := "acme/widget#181"

	merged := surfaceEnvelope()
	merged["merged"] = true
	merged["pr_state"] = "merged"
	srv.syncPhaseParent(ctx, ep, prKey, merged, PhaseMerged)
	sentBefore, editedBefore, _, _ := rec.snapshot()

	srv.syncPhaseParent(ctx, ep, prKey, merged, PhaseMerged)

	if _, phase, _, ok := srv.prSurfaceState(prKey); !ok || phase != PhaseMerged {
		t.Fatalf("phase = %q (known=%v), want merged", phase, ok)
	}
	sentAfter, editedAfter, _, _ := rec.snapshot()
	if len(sentAfter) != len(sentBefore) || len(editedAfter) != len(editedBefore) {
		t.Fatalf("merge evidence rewrote the surface: sent=%d edited=%d", len(sentAfter), len(editedAfter))
	}
	if got := len(logs.all("webhook: surface merged repaired")); got != 0 {
		t.Fatalf("merge evidence triggered %d repairs", got)
	}
}
