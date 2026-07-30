package channel

import "context"

type Attachment struct {
	Filename string
	MimeType string
	Data     []byte
}

type IncomingMessage struct {
	Platform    string
	ChannelID   string
	UserID      string
	Text        string
	IsMention   bool
	IsThread    bool
	Attachments []Attachment
	ReplyCtx    ReplyContext
}

type ReplyContext interface {
	SendTyping() error
	Send(text string) (MessageRef, error)
	Edit(ref MessageRef, text string) error
}

type MessageRef interface {
	ID() string
}

type Channel interface {
	Name() string
	Start(ctx context.Context, handler func(IncomingMessage)) error
	Stop() error
}
