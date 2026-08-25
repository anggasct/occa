package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func computeTestSignature(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyGitHubSignature(t *testing.T) {
	secret := "my-secret-key"
	body := []byte(`{"action":"submitted","review":{"state":"approved"}}`)
	validSig := computeTestSignature(body, secret)

	tests := []struct {
		name   string
		body   []byte
		header string
		secret string
		want   bool
	}{
		{
			name:   "valid signature",
			body:   body,
			header: validSig,
			secret: secret,
			want:   true,
		},
		{
			name:   "valid signature empty body",
			body:   []byte{},
			header: computeTestSignature([]byte{}, secret),
			secret: secret,
			want:   true,
		},
		{
			name:   "wrong secret",
			body:   body,
			header: validSig,
			secret: "wrong-secret",
			want:   false,
		},
		{
			name:   "tampered body",
			body:   []byte(`{"action":"submitted","review":{"state":"changes_requested"}}`),
			header: validSig,
			secret: secret,
			want:   false,
		},
		{
			name:   "empty header",
			body:   body,
			header: "",
			secret: secret,
			want:   false,
		},
		{
			name:   "empty secret",
			body:   body,
			header: validSig,
			secret: "",
			want:   false,
		},
		{
			name:   "missing sha256 prefix",
			body:   body,
			header: hex.EncodeToString(sha256.New().Sum(nil)),
			secret: secret,
			want:   false,
		},
		{
			name:   "sha1 prefix rejected",
			body:   body,
			header: "sha1=47eef3f9704f2c07c5fed441603d472cb05b741d",
			secret: secret,
			want:   false,
		},
		{
			name:   "truncated hex",
			body:   body,
			header: validSig[:len(validSig)-2],
			secret: secret,
			want:   false,
		},
		{
			name:   "extended hex",
			body:   body,
			header: validSig + "00",
			secret: secret,
			want:   false,
		},
		{
			name:   "invalid hex characters",
			body:   body,
			header: "sha256=zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
			secret: secret,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VerifyGitHubSignature(tt.body, tt.header, tt.secret)
			if got != tt.want {
				t.Fatalf("VerifyGitHubSignature() = %v, want %v", got, tt.want)
			}
		})
	}
}
