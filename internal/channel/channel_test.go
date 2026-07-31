package channel

import (
	"context"
	"testing"
)

type mockMessageRef struct{ id string }

func (m mockMessageRef) ID() string { return m.id }

type mockReplyContext struct{}

func (m *mockReplyContext) SendTyping() error                { return nil }
func (m *mockReplyContext) Send(text string) (MessageRef, error) { return mockMessageRef{id: "1"}, nil }
func (m *mockReplyContext) SendWithButtons(text string, buttons []Button) (MessageRef, error) {
	return mockMessageRef{id: "1"}, nil
}
func (m *mockReplyContext) Edit(ref MessageRef, text string) error { return nil }

type mockChannel struct{}

func (m *mockChannel) Name() string { return "mock" }
func (m *mockChannel) Start(ctx context.Context, handler func(IncomingMessage)) error {
	return nil
}
func (m *mockChannel) Stop() error { return nil }
func (m *mockChannel) Notify(channelID string, text string) error { return nil }

var (
	_ Channel      = (*mockChannel)(nil)
	_ ReplyContext = (*mockReplyContext)(nil)
	_ MessageRef   = mockMessageRef{}
)

func TestInterfaceCompliance(t *testing.T) {
	var ch Channel = &mockChannel{}
	if ch.Name() != "mock" {
		t.Fatalf("unexpected name: %s", ch.Name())
	}

	var rc ReplyContext = &mockReplyContext{}
	ref, err := rc.Send("hello")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if ref.ID() != "1" {
		t.Fatalf("unexpected ref id: %s", ref.ID())
	}
}
