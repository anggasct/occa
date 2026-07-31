package channel

import "context"

type Attachment struct {
	Filename string
	MimeType string
	Data     []byte
}

type Button struct {
	Label string
	Value string
}

type IncomingMessage struct {
	Platform     string
	ChannelID    string
	UserID       string
	Text         string
	IsMention    bool
	IsThread     bool
	IsCallback   bool
	CallbackData string
	Attachments  []Attachment
	ReplyCtx     ReplyContext
}

type ReplyContext interface {
	SendTyping() error
	Send(text string) (MessageRef, error)
	SendWithButtons(text string, buttons []Button) (MessageRef, error)
	Edit(ref MessageRef, text string) error
}

type MessageRef interface {
	ID() string
}

type Channel interface {
	Name() string
	Start(ctx context.Context, handler func(IncomingMessage)) error
	Stop() error
	Notify(channelID string, text string) error
}
