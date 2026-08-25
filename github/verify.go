package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/aidenappl/monitor-core/env"
)

// SignatureHeader is the header GitHub signs each delivery with.
const SignatureHeader = "X-Hub-Signature-256"

var (
	// ErrNoWebhookSecret means the integration is unconfigured. Callers must
	// REJECT the delivery, never accept it: an unconfigured secret makes the
	// endpoint an unauthenticated write into the timeline.
	ErrNoWebhookSecret = errors.New("github webhook secret is not configured")
	// ErrBadSignature covers a missing, malformed, or non-matching signature.
	// Deliberately one error for all three — distinguishing them tells an
	// attacker which part of their forgery to fix.
	ErrBadSignature = errors.New("invalid webhook signature")
)

// WebhookEnabled reports whether deliveries can be verified.
func WebhookEnabled() bool { return env.GitHubWebhookSecret != "" }

// VerifySignature checks a delivery's HMAC-SHA256 signature against the shared
// secret.
//
// This is the ONLY authentication on the webhook route — it is mounted outside
// the /v1 subrouter, so QueryAuthMiddleware never runs on it. GitHub cannot send
// an API key, so possession of the secret is the proof. That places the entire
// trust boundary here.
//
// The comparison is constant-time via hmac.Equal. A byte-wise early return would
// leak, through response timing, how much of a forged signature was correct,
// which is enough to recover a valid one byte by byte.
func VerifySignature(payload []byte, header string) error {
	if !WebhookEnabled() {
		return ErrNoWebhookSecret
	}

	// GitHub sends "sha256=<hex>". Anything else — including the older sha1
	// header's format — is rejected rather than coerced.
	const prefix = "sha256="
	if !strings.HasPrefix(header, prefix) {
		return ErrBadSignature
	}
	got, err := hex.DecodeString(strings.TrimPrefix(header, prefix))
	if err != nil {
		return ErrBadSignature
	}

	mac := hmac.New(sha256.New, []byte(env.GitHubWebhookSecret))
	mac.Write(payload)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return ErrBadSignature
	}
	return nil
}
