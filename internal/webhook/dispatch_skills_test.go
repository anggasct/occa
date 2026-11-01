package webhook

import "testing"

func TestDispatchTaskContextExposesBriefAndScope(t *testing.T) {
	s := &Server{dispatchTasks: NewDispatchStore()}
	task := s.dispatchTasks.Create(DispatchRequest{Repo: "acme/widget", Branch: "feat/x", Brief: "Do it", Scope: "internal/store/"})
	brief, scope, ok := s.DispatchTaskContext(task.ID)
	if !ok || brief != "Do it\n\nScope: internal/store/" || scope != "internal/store/" {
		t.Fatalf("context = %q, %q, %v", brief, scope, ok)
	}
	if _, _, ok := s.DispatchTaskContext("task-missing"); ok {
		t.Fatal("unknown task resolved")
	}
	if _, _, ok := (*Server)(nil).DispatchTaskContext(task.ID); ok {
		t.Fatal("nil server resolved")
	}
}
