package webhook

import "testing"

func TestSurfaceFrozenAllowsMergedPhase(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.surfaces["acme/widget#7"] = SurfaceUpdate{
		RootMessageID: "root-1",
		Phase:         string(PhaseBlocked),
	}

	if !SurfaceFrozen(reg, "acme/widget#7", PhaseFix) {
		t.Fatal("a blocked surface must freeze a fix write")
	}
	if SurfaceFrozen(reg, "acme/widget#7", PhaseMerged) {
		t.Fatal("a merge must always reach the surface, even from a blocked phase")
	}
}

func TestThreadStatusWriterAllowsMergedPhase(t *testing.T) {
	reg := NewTaskThreadRegistry()
	reg.surfaces["acme/widget#7"] = SurfaceUpdate{
		RootMessageID:   "root-1",
		ThreadID:        "thread-1",
		Phase:           string(PhaseBlocked),
		StatusMessageID: "status-1",
	}
	reg.RecordStatusMessage("thread-1", "acme/widget", "feat/x", "acme/widget#7", "status-1")
	var edited []string
	writer := &ThreadStatusWriter{
		Registry: reg,
		Phase:    PhaseMerged,
		Edit: func(target, messageID, text string) error {
			edited = append(edited, messageID)
			return nil
		},
		Send: func(target, text string) (string, error) { return "new-1", nil },
	}
	if got := writer.Write("thread-1", "thread-1", "acme/widget", "feat/x", "acme/widget#7", "merged"); got == "" {
		t.Fatal("merged status line was refused on a blocked PR")
	}
	if len(edited) != 1 || edited[0] != "status-1" {
		t.Fatalf("edited = %v, want the existing status message", edited)
	}
}
