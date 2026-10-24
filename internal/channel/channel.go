package channel

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrMessageNotFound             = errors.New("channel: message not found")
	ErrThreadNotFound              = errors.New("channel: thread not found")
	ErrParentResolutionUnsupported = errors.New("channel: parent resolution unsupported")
	ErrRateLimited                 = errors.New("channel: rate limited")
)

type ThreadParentResolver interface {
	ParentChannelOf(threadID string) (string, error)
}

type GuildResolver interface {
	GuildIDForChannel(channelID string) (string, error)
}

type Attachment struct {
	Filename string
	MimeType string
	Data     []byte
}

type Button struct {
	Label    string
	Value    string
	Row      int
	URL      string
	Disabled bool
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
	IsSelfDispatch         bool
	CallbackData           string
	CallbackRef            MessageRef
	// SourceRef is the original user message that triggered this interaction
	// (nil for callbacks, which use CallbackRef). Reaction setters can target
	// it for read-receipts.
	SourceRef   MessageRef
	Attachments []Attachment
	ReplyCtx    ReplyContext
}

func ParseDispatchSummary(text string) (string, string, string, bool) {
	line := text
	if i := strings.Index(line, "\n"); i >= 0 {
		line = line[:i]
	}
	line = strings.TrimSpace(line)
	idx := strings.Index(line, "Dispatched ")
	if idx < 0 {
		return "", "", "", false
	}
	rest := strings.TrimSpace(line[idx+len("Dispatched "):])
	parts := strings.Split(rest, "·")
	if len(parts) != 2 {
		return "", "", "", false
	}
	head := strings.TrimSpace(parts[0])
	taskID := strings.TrimSpace(parts[1])
	if fields := strings.Fields(taskID); len(fields) > 0 {
		taskID = fields[0]
	}
	if !strings.HasPrefix(taskID, "task-") || len(taskID) <= len("task-") {
		return "", "", "", false
	}
	repo, branch, ok := strings.Cut(head, "#")
	repo = strings.TrimSpace(repo)
	branch = strings.TrimSpace(branch)
	if !ok || repo == "" || branch == "" {
		return "", "", "", false
	}
	if strings.Contains(repo, " ") || strings.Contains(branch, " ") {
		return "", "", "", false
	}
	if !strings.Contains(repo, "/") {
		return "", "", "", false
	}
	return repo, branch, taskID, true
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

type MessageDeleter interface {
	DeleteMessage(channelID, messageID string) error
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

type MessageSender interface {
	SendNotification(channelID string, text string) (string, error)
}

type MessageSenderWithButtons interface {
	SendNotificationWithButtons(channelID string, text string, buttons []Button) (string, error)
}

// MentionResolver resolves a username to a platform user id so an
// escalation can ping the operator.
type MentionResolver interface {
	ResolveMentionID(ctx context.Context, name string) (string, bool)
}

// MessageSenderMention sends a notification that may ping a single user. The
// mention is the escalation channel of the product: every other path stays
// mention-free.
type MessageSenderMention interface {
	SendNotificationMention(channelID string, text string, mentionUserID string) (string, error)
}

type MessageEditor interface {
	EditNotification(channelID, messageID string, text string) error
}

type MessageEditorWithButtons interface {
	EditNotificationWithButtons(channelID, messageID string, text string, buttons []Button) error
}

// MessageEditorNoRetry reports a platform rate limit to the caller instead of
// sleeping through it. Background refresh loops use it to schedule their own
// retry; synchronous writers keep the default retrying behaviour.
type MessageEditorNoRetry interface {
	EditNotificationNoRetry(channelID, messageID string, text string) error
}

type MessageEditorWithButtonsNoRetry interface {
	EditNotificationWithButtonsNoRetry(channelID, messageID string, text string, buttons []Button) error
}

type ThreadStarter interface {
	StartThread(channelID, messageID, name string) (string, error)
}

type MessageReplier interface {
	ReplyNotification(channelID, replyToMessageID string, text string) (string, error)
}

type Channel interface {
	Name() string
	Start(ctx context.Context, handler func(IncomingMessage)) error
	Stop() error
	Notify(channelID string, text string) error
}
