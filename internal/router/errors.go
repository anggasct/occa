package router

import "fmt"

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
