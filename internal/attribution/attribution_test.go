package attribution

import (
	"testing"
	"time"
)

func TestStorePutPopFIFO(t *testing.T) {
	s := NewStore()
	fp := Fingerprint("0 9 * * 1-5", "run tests", "weekdays at 9am")

	s.Put(fp, "telegram", "c1")
	s.Put(fp, "discord", "c2")

	p, c, ok := s.Pop(fp)
	if !ok || p != "telegram" || c != "c1" {
		t.Fatalf("first Pop = (%q, %q, %v), want (telegram, c1, true)", p, c, ok)
	}
	p, c, ok = s.Pop(fp)
	if !ok || p != "discord" || c != "c2" {
		t.Fatalf("second Pop = (%q, %q, %v), want (discord, c2, true)", p, c, ok)
	}
	if _, _, ok := s.Pop(fp); ok {
		t.Fatal("expected miss after both entries consumed")
	}
}

func TestStorePopDifferentFingerprintMiss(t *testing.T) {
	s := NewStore()
	s.Put(Fingerprint("0 9 * * 1-5", "a", "b"), "telegram", "c1")
	if _, _, ok := s.Pop(Fingerprint("0 9 * * 1-5", "a", "c")); ok {
		t.Fatal("expected miss for a different fingerprint")
	}
}

func TestStoreTTLExpiry(t *testing.T) {
	s := NewStore()
	fp := Fingerprint("0 9 * * 1-5", "run tests", "weekdays at 9am")
	s.Put(fp, "telegram", "c1")

	s.mu.Lock()
	s.items[fp][0].expires = time.Now().Add(-time.Second)
	s.mu.Unlock()

	if _, _, ok := s.Pop(fp); ok {
		t.Fatal("expected expired entry to be dropped")
	}
}

func TestFingerprintDeterministic(t *testing.T) {
	a := Fingerprint("0 9 * * 1-5", "run tests", "weekdays at 9am")
	b := Fingerprint("0 9 * * 1-5", "run tests", "weekdays at 9am")
	c := Fingerprint("0 9 * * 1-5", "run tests", "weekdays")
	if a != b {
		t.Fatal("fingerprint must be deterministic")
	}
	if a == c {
		t.Fatal("different inputs must differ")
	}
}
