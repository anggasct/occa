package discord

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/anggasct/occa/internal/channel"
)

func TestEditErrorClassifiesUnknownMessage(t *testing.T) {
	rest := func(code int) error {
		return fmt.Errorf("discord: edit notification with buttons: %w", &discordgo.RESTError{Response: &http.Response{StatusCode: code}})
	}
	cases := []struct {
		name        string
		err         error
		notFound    bool
		rateLimited bool
	}{
		{name: "404 unknown message", err: rest(http.StatusNotFound), notFound: true},
		{name: "429", err: rest(http.StatusTooManyRequests), rateLimited: true},
		{name: "500", err: rest(http.StatusInternalServerError)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := discordEditError(tc.err)
			if errors.Is(got, channel.ErrMessageNotFound) != tc.notFound {
				t.Fatalf("ErrMessageNotFound = %v, want %v (%v)", !tc.notFound, tc.notFound, got)
			}
			if errors.Is(got, channel.ErrRateLimited) != tc.rateLimited {
				t.Fatalf("ErrRateLimited = %v, want %v (%v)", !tc.rateLimited, tc.rateLimited, got)
			}
			var restErr *discordgo.RESTError
			if tc.notFound && errors.Is(got, channel.ErrMessageNotFound) && errors.As(got, &restErr) {
				t.Fatal("not-found wrapping must not leak the platform error type")
			}
		})
	}
}
