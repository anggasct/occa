package webhook

import "testing"

func TestFormatWebhookMessage(t *testing.T) {
	const separator = webhookMessageSeparator

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: separator},
		{name: "body", in: "status", want: "status\n" + separator},
		{name: "trailing whitespace", in: "status \n\t", want: "status\n" + separator},
		{name: "already separated", in: "status\n" + separator + "\n", want: "status\n" + separator},
		{name: "duplicate separators", in: "status\n" + separator + "\n" + separator, want: "status\n" + separator},
		{name: "separator only", in: separator, want: separator},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatWebhookMessage(tt.in); got != tt.want {
				t.Fatalf("formatWebhookMessage(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if got := formatWebhookMessage(formatWebhookMessage(tt.in)); got != tt.want {
				t.Fatalf("formatWebhookMessage is not idempotent: got %q, want %q", got, tt.want)
			}
		})
	}
}
