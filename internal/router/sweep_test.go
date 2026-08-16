package router

import (
	"context"
	"errors"
	"testing"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

type fakeDeleter struct {
	channelID string
	messageID string
	err       error
	calls     int
}

func (f *fakeDeleter) DeleteMessage(channelID, messageID string) error {
	f.calls++
	f.channelID = channelID
	f.messageID = messageID
	return f.err
}

var _ channel.MessageDeleter = (*fakeDeleter)(nil)

func TestSweepStaleProgressNoticesDeletesAndClearsRow(t *testing.T) {
	repo := newFakeProgressNoticeRepo()
	repo.mu.Lock()
	repo.notices = []store.ProgressNotice{
		{ID: 1, Platform: "discord", ChannelID: "parent", ThreadID: "thread1", MessageID: "m1"},
	}
	repo.mu.Unlock()
	deleter := &fakeDeleter{}

	swept, err := SweepStaleProgressNotices(context.Background(), repo, func(platform string) channel.MessageDeleter {
		if platform == "discord" {
			return deleter
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1", swept)
	}
	if deleter.calls != 1 || deleter.channelID != "thread1" || deleter.messageID != "m1" {
		t.Fatalf("deleter = (%q, %q, %d calls), want thread1/m1 once", deleter.channelID, deleter.messageID, deleter.calls)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.notices) != 0 {
		t.Fatalf("row not cleared: %+v", repo.notices)
	}
}

func TestSweepStaleProgressNoticesTargetsChannelForTelegram(t *testing.T) {
	repo := newFakeProgressNoticeRepo()
	repo.mu.Lock()
	repo.notices = []store.ProgressNotice{
		{ID: 1, Platform: "telegram", ChannelID: "chat1", ThreadID: "topic9", MessageID: "42"},
	}
	repo.mu.Unlock()
	deleter := &fakeDeleter{}

	swept, err := SweepStaleProgressNotices(context.Background(), repo, func(platform string) channel.MessageDeleter {
		if platform == "telegram" {
			return deleter
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1", swept)
	}
	if deleter.channelID != "chat1" || deleter.messageID != "42" {
		t.Fatalf("deleter = (%q, %q), want chat1/42", deleter.channelID, deleter.messageID)
	}
}

func TestSweepStaleProgressNoticesSkipsMissingDeleter(t *testing.T) {
	repo := newFakeProgressNoticeRepo()
	repo.mu.Lock()
	repo.notices = []store.ProgressNotice{
		{ID: 1, Platform: "unknown", ChannelID: "c1", MessageID: "m1"},
	}
	repo.mu.Unlock()

	swept, err := SweepStaleProgressNotices(context.Background(), repo, func(platform string) channel.MessageDeleter {
		return nil
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("swept = %d, want 0", swept)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.notices) != 1 {
		t.Fatalf("row should be kept when no deleter: %+v", repo.notices)
	}
}

func TestSweepStaleProgressNoticesKeepsRowOnDeleteError(t *testing.T) {
	repo := newFakeProgressNoticeRepo()
	repo.mu.Lock()
	repo.notices = []store.ProgressNotice{
		{ID: 1, Platform: "telegram", ChannelID: "chat1", MessageID: "m1"},
	}
	repo.mu.Unlock()
	deleter := &fakeDeleter{err: errors.New("network")}

	swept, err := SweepStaleProgressNotices(context.Background(), repo, func(platform string) channel.MessageDeleter {
		return deleter
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("swept = %d, want 0", swept)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.notices) != 1 {
		t.Fatalf("row should be kept on delete error: %+v", repo.notices)
	}
}

func TestSweepStaleProgressNoticesRemovesRowOnNotFound(t *testing.T) {
	repo := newFakeProgressNoticeRepo()
	repo.mu.Lock()
	repo.notices = []store.ProgressNotice{
		{ID: 1, Platform: "telegram", ChannelID: "chat1", MessageID: "m1"},
	}
	repo.mu.Unlock()
	deleter := &fakeDeleter{err: channel.ErrMessageNotFound}

	swept, err := SweepStaleProgressNotices(context.Background(), repo, func(platform string) channel.MessageDeleter {
		return deleter
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1", swept)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.notices) != 0 {
		t.Fatalf("row not cleared on not-found: %+v", repo.notices)
	}
}

func TestSweepStaleProgressNoticesListError(t *testing.T) {
	repo := &fakeProgressNoticeRepo{listErr: errors.New("db closed")}

	_, err := SweepStaleProgressNotices(context.Background(), repo, nil)
	if err == nil {
		t.Fatal("expected List error to surface")
	}
}
