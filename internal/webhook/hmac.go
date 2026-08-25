package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func VerifyGitHubSignature(rawBody []byte, header, secret string) bool {
	if secret == "" || header == "" {
		return false
	}
	if !strings.HasPrefix(header, "sha256=") {
		return false
	}
	sigHex := strings.TrimPrefix(header, "sha256=")
	if len(sigHex) != 64 {
		return false
	}
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil || len(sigBytes) != 32 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(rawBody)
	expectedMAC := mac.Sum(nil)
	return hmac.Equal(sigBytes, expectedMAC)
}
