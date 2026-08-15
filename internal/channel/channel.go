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
	Row   int
}

type MenuCommand struct {
	Alias       string
	Description string
	HasArgs     bool
}

type IncomingMessage struct {
	Platform               string
	ChannelID              string
	ParentChannelID        string
	ChannelScopeUnresolved bool
	ThreadID               string
	UserID                 string
	Text                   string
	IsMention              bool
	IsThread               bool
	IsCallback             bool
	CallbackData           string
	CallbackRef            MessageRef
	Attachments            []Attachment
	ReplyCtx               ReplyContext
}

type ReplyContext interface {
	SendTyping() error
	Send(text string) (MessageRef, error)
	SendWithButtons(text string, buttons []Button) (MessageRef, error)
	Edit(ref MessageRef, text string) error
	EditWithButtons(ref MessageRef, text string, buttons []Button) error
}

type MessageRemover interface {
	Delete(ref MessageRef) error
}

type ChatCommandSetter interface {
	SetChatCommands(commands []MenuCommand) error
}

type ReactionState int

const (
	ReactionProcessing ReactionState = iota
	ReactionSuccess
	ReactionError
)

type ReactionSetter interface {
	SetReaction(ref MessageRef, state ReactionState) error
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
