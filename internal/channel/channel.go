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

// MenuCommand describes one command for native platform command-menu
// registration (Telegram setMyCommands, Discord slash-command registration).
// Alias is the platform-safe name (letters/digits/underscore only, no ':') —
// adapters register Alias, the router normalizes it back to the canonical
// colon-form command before dispatch.
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

// ChatCommandSetter is implemented by ReplyContexts that support per-chat
// native command-menu registration. Not every ReplyContext does (e.g. a
// Discord DM has no guild to scope to) — callers must type-assert and treat
// a missing implementation as "skip silently."
type ChatCommandSetter interface {
	SetChatCommands(commands []MenuCommand) error
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
