package attribution

import (
	"testing"
	"time"
)

func TestAttributionStoreRoundtrip(t *testing.T) {
	s := NewStore()
	fp := Fingerprint("0 9 * * 1-5", "test prompt", "weekdays at 9am")

	if _, _, ok := s.Get(fp); ok {
		t.Fatal("expected miss for unstored fingerprint")
	}

	s.Put(fp, "telegram", "chat123")

	platform, channelID, ok := s.Get(fp)
	if !ok {
		t.Fatal("expected hit after Put")
	}
	if platform != "telegram" || channelID != "chat123" {
		t.Fatalf("unexpected values: platform=%s channelID=%s", platform, channelID)
	}
}

func TestAttributionStoreTTLExpiry(t *testing.T) {
	s := NewStore()
	s.ttl = 10 * time.Millisecond
	fp := Fingerprint("0 9 * * 1-5", "test prompt", "weekdays at 9am")

	s.Put(fp, "discord", "chan456")
	time.Sleep(20 * time.Millisecond)

	if _, _, ok := s.Get(fp); ok {
		t.Fatal("expected expired entry to return !ok")
	}
}

func TestFingerprintDeterministic(t *testing.T) {
	fp1 := Fingerprint("0 9 * * 1-5", "prompt", "schedule")
	fp2 := Fingerprint("0 9 * * 1-5", "prompt", "schedule")
	if fp1 != fp2 {
		t.Fatalf("expected identical fingerprints, got %s vs %s", fp1, fp2)
	}

	fp3 := Fingerprint("0 10 * * 1-5", "prompt", "schedule")
	if fp1 == fp3 {
		t.Fatal("expected different fingerprints for different cron expressions")
	}
}
