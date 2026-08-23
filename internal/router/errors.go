package router

import (
	"errors"
	"fmt"
)

// errPassthroughCanceled reports that executePassthrough dropped the message
// because its task context was canceled between slot acquisition and
// execution (shutdown race). It is not a routing failure: passthrough
// swallows it so shutdown stays quiet, while passthroughQueued uses it to
// distinguish a real dispatch from a canceled drop instead of treating the
// nil return as success.
var errPassthroughCanceled = errors.New("router: passthrough canceled before dispatch")

type replyError struct {
	message string
	cause   error
}

func (e *replyError) Error() string {
	if e.cause == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.cause)
}

func (e *replyError) Unwrap() error {
	return e.cause
}

func safeReplyError(message string, cause error) error {
	return &replyError{message: message, cause: cause}
}
