package discord

import (
	"errors"
	"net/http"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

func TestParentChannelOfCaches(t *testing.T) {
	calls := 0
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			calls++
			return &discordgo.Channel{ID: channelID, ParentID: "parent-1", Type: discordgo.ChannelTypeGuildPublicThread}, nil
		},
	}
	got, err := a.ParentChannelOf("thread-1")
	if err != nil || got != "parent-1" {
		t.Fatalf("ParentChannelOf=%q err=%v, want parent-1", got, err)
	}
	got, err = a.ParentChannelOf("thread-1")
	if err != nil || got != "parent-1" {
		t.Fatalf("cached ParentChannelOf=%q err=%v", got, err)
	}
	if calls != 1 {
		t.Fatalf("lookup calls=%d, want 1", calls)
	}
}

func TestParentChannelOfNotFound(t *testing.T) {
	calls := 0
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			calls++
			return nil, &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}
		},
	}
	if _, err := a.ParentChannelOf("thread-gone"); !errors.Is(err, channel.ErrThreadNotFound) {
		t.Fatalf("expected ErrThreadNotFound, got %v", err)
	}
	if _, err := a.ParentChannelOf("thread-gone"); !errors.Is(err, channel.ErrThreadNotFound) {
		t.Fatalf("cached 404 must stay not-found, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("404 must be cached, calls=%d want 1", calls)
	}
}

func TestParentChannelOfNonThreadDenied(t *testing.T) {
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			return &discordgo.Channel{ID: channelID, Type: discordgo.ChannelTypeGuildText}, nil
		},
	}
	if _, err := a.ParentChannelOf("channel-1"); !errors.Is(err, channel.ErrThreadNotFound) {
		t.Fatalf("non-thread must be not-found, got %v", err)
	}
}

func TestParentChannelOfErrorNotCached(t *testing.T) {
	calls := 0
	a := &Adapter{
		channelLookup: func(channelID string) (*discordgo.Channel, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("network down")
			}
			return &discordgo.Channel{ID: channelID, ParentID: "parent-1", Type: discordgo.ChannelTypeGuildPublicThread}, nil
		},
	}
	if _, err := a.ParentChannelOf("thread-1"); err == nil || errors.Is(err, channel.ErrThreadNotFound) {
		t.Fatalf("transient error must surface, got %v", err)
	}
	got, err := a.ParentChannelOf("thread-1")
	if err != nil || got != "parent-1" {
		t.Fatalf("retry after transient must succeed, got %q err=%v", got, err)
	}
	if calls != 2 {
		t.Fatalf("transient error must not cache, calls=%d want 2", calls)
	}
}

func TestParentChannelOfEmpty(t *testing.T) {
	a := &Adapter{}
	got, err := a.ParentChannelOf("")
	if err != nil || got != "" {
		t.Fatalf("empty thread must return empty, got %q err=%v", got, err)
	}
}
