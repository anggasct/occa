package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/config"
	"github.com/anggasct/occa/internal/store"
)

type spoolEntry struct {
	EndpointPath string `json:"endpoint_path"`
	EndpointName string `json:"endpoint_name"`
	DeliveryID   string `json:"delivery_id"`
	EventType    string `json:"event_type"`
	PayloadHash  string `json:"payload_hash"`
	Body         []byte `json:"body"`
}

func defaultSpoolDir() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return filepath.Join(".occa", "spool")
	}
	return filepath.Join(home, ".occa", "spool")
}

func (s *Server) effectiveSpoolDir() string {
	if dir := strings.TrimSpace(s.spoolDir); dir != "" {
		return dir
	}
	return defaultSpoolDir()
}

func spoolFileName(deliveryID string) string {
	clean := strings.NewReplacer("/", "_", "\\", "_", "..", "_").Replace(strings.TrimSpace(deliveryID))
	if clean == "" {
		clean = fmt.Sprintf("delivery-%d", time.Now().UnixNano())
	}
	return clean + ".json"
}

func (s *Server) writeSpool(item dispatchItem) (string, error) {
	dir := s.effectiveSpoolDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("webhook: spool mkdir: %w", err)
	}
	entry := spoolEntry{
		EndpointPath: item.ep.Path,
		EndpointName: item.ep.Name,
		DeliveryID:   item.deliveryID,
		EventType:    item.eventType,
		PayloadHash:  item.payloadHash,
		Body:         item.body,
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("webhook: spool encode: %w", err)
	}
	final := filepath.Join(dir, spoolFileName(item.deliveryID))
	tmp, err := os.CreateTemp(dir, ".spool-*")
	if err != nil {
		return "", fmt.Errorf("webhook: spool create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("webhook: spool chmod: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("webhook: spool write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("webhook: spool sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("webhook: spool close: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		return "", fmt.Errorf("webhook: spool rename: %w", err)
	}
	return final, nil
}

func (s *Server) drainSpool(ctx context.Context) {
	dir := s.effectiveSpoolDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("webhook: spool read failed", "dir", dir, "error", err)
		}
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		s.drainSpoolFile(ctx, filepath.Join(dir, entry.Name()))
	}
}

func (s *Server) drainSpoolFile(ctx context.Context, path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("webhook: spool read failed", "path", path, "error", err)
		return
	}
	var entry spoolEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		slog.Error("webhook: spool decode failed", "path", path, "error", err)
		return
	}
	ep, ok := s.spoolEndpoint(entry)
	if !ok {
		slog.Warn("webhook: spool endpoint missing", "path", path, "endpoint", entry.EndpointName)
		return
	}
	if strings.TrimSpace(entry.DeliveryID) == "" {
		slog.Error("webhook: spool entry without delivery id", "path", path)
		return
	}
	if s.deliveries == nil {
		return
	}
	if _, err := s.deliveries.Ensure(ctx, store.WebhookDelivery{
		Endpoint:    ep.Name,
		DeliveryID:  entry.DeliveryID,
		EventType:   entry.EventType,
		PayloadHash: entry.PayloadHash,
		Status:      store.WebhookStatusReceived,
	}); err != nil {
		slog.Warn("webhook: spool insert failed", "path", path, "delivery_id", entry.DeliveryID, "error", err)
		return
	}
	receipt, err := s.deliveries.Get(ctx, ep.Name, entry.DeliveryID)
	if err != nil {
		slog.Warn("webhook: spool receipt lookup failed", "path", path, "delivery_id", entry.DeliveryID, "error", err)
		return
	}
	if receipt != nil && receipt.Status != store.WebhookStatusReceived && receipt.Status != store.WebhookStatusAccepted {
		if err := os.Remove(path); err != nil {
			slog.Warn("webhook: spool remove failed", "path", path, "error", err)
		}
		return
	}
	item := dispatchItem{
		ep:          ep,
		body:        entry.Body,
		deliveryID:  entry.DeliveryID,
		eventType:   entry.EventType,
		payloadHash: entry.PayloadHash,
	}
	if _, admitted, _ := s.enqueue(ep, ExtractExecutionKey(entry.Body), item); !admitted {
		slog.Info("webhook: spool deferred", "path", path, "delivery_id", entry.DeliveryID)
		return
	}
	if err := os.Remove(path); err != nil {
		slog.Warn("webhook: spool remove failed", "path", path, "error", err)
		return
	}
	slog.Info("webhook: spool drained", "path", path, "delivery_id", entry.DeliveryID)
}

func (s *Server) spoolEndpoint(entry spoolEntry) (config.EndpointConfig, bool) {
	if entry.EndpointPath != "" {
		if ep, ok := s.endpoints[entry.EndpointPath]; ok {
			return ep, true
		}
	}
	if entry.EndpointName != "" {
		if ep, ok := s.endpointsByName[entry.EndpointName]; ok {
			return ep, true
		}
	}
	return config.EndpointConfig{}, false
}
