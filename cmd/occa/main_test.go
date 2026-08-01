package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anggasct/occa/internal/channel"
)

type stubChannel struct {
	name    string
	start   func(context.Context, func(channel.IncomingMessage)) error
	started chan struct{}
}

func (s *stubChannel) Name() string { return s.name }

func (s *stubChannel) Start(ctx context.Context, handler func(channel.IncomingMessage)) error {
	if s.started != nil {
		close(s.started)
	}
	return s.start(ctx, handler)
}

func (s *stubChannel) Stop() error                     { return nil }
func (s *stubChannel) Notify(_ string, _ string) error { return nil }

type stubRouter struct{ err error }

func (s stubRouter) Route(context.Context, channel.IncomingMessage) error { return s.err }

func TestRunChannelContainsPanic(t *testing.T) {
	panicking := &stubChannel{
		name: "discord",
		start: func(context.Context, func(channel.IncomingMessage)) error {
			var nilUser *struct{ ID string }
			_ = nilUser.ID
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		runChannel(context.Background(), panicking, stubRouter{})
	}()

	<-done
}

func TestRunChannelPanicDoesNotStopOtherChannels(t *testing.T) {
	ctx := context.Background()
	survivor := &stubChannel{
		name:    "telegram",
		started: make(chan struct{}),
		start: func(c context.Context, _ func(channel.IncomingMessage)) error {
			<-c.Done()
			return nil
		},
	}
	go runChannel(ctx, &stubChannel{
		name:  "discord",
		start: func(context.Context, func(channel.IncomingMessage)) error { panic("gateway identity") },
	}, stubRouter{})

	runCtx, cancel := context.WithCancel(ctx)
	go runChannel(runCtx, survivor, stubRouter{})

	<-survivor.started
	cancel()
}

func TestRunChannelReportsStartError(t *testing.T) {
	failing := &stubChannel{
		name: "telegram",
		start: func(context.Context, func(channel.IncomingMessage)) error {
			return errors.New("init failed")
		},
	}

	runChannel(context.Background(), failing, stubRouter{})
}

type captureChannel struct {
	name string
	send func(channelID, text string)
}

func (c *captureChannel) Name() string { return c.name }

func (c *captureChannel) Start(context.Context, func(channel.IncomingMessage)) error { return nil }
func (c *captureChannel) Stop() error                                                { return nil }
func (c *captureChannel) Notify(channelID, text string) error {
	if c.send != nil {
		c.send(channelID, text)
	}
	return nil
}

func TestNotifyEscapesMarkupForTelegram(t *testing.T) {
	var got string
	notify(&captureChannel{
		name: "telegram",
		send: func(_, text string) { got = text },
	}, "chat1", "scheduled run failed: <boom> & more")

	if strings.Contains(got, "<boom>") || !strings.Contains(got, "&lt;boom&gt;") || !strings.Contains(got, "&amp; more") {
		t.Fatalf("notify text not escaped for telegram: %q", got)
	}
}

func TestNotifyPassesThroughDiscord(t *testing.T) {
	var got string
	notify(&captureChannel{
		name: "discord",
		send: func(_, text string) { got = text },
	}, "123", "value <x> & more")

	if got == "" || !strings.Contains(got, "<x>") {
		t.Fatalf("discord content altered: %q", got)
	}
}
