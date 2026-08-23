package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestStorePingAndSchemaVersion(t *testing.T) {
	s, err := OpenWithDefaultWorkdir(filepath.Join(t.TempDir(), "health.db"), "")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	version, err := s.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if version != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", version, SchemaVersion)
	}
}
