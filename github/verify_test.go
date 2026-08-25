package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/aidenappl/monitor-core/env"
)

func withWebhookSecret(t *testing.T, secret string) {
	t.Helper()
	previous := env.GitHubWebhookSecret
	env.GitHubWebhookSecret = secret
	t.Cleanup(func() { env.GitHubWebhookSecret = previous })
}

func sign(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifySignatureAcceptsGenuineDelivery(t *testing.T) {
	const secret = "s3cr3t"
	withWebhookSecret(t, secret)

	payload := []byte(`{"action":"closed","pull_request":{"number":42}}`)
	if err := VerifySignature(payload, sign(secret, payload)); err != nil {
		t.Fatalf("genuine delivery rejected: %v", err)
	}
}

// TestVerifySignatureRejectsForgeries is the security boundary for the whole
// webhook: it is mounted outside /v1, so this signature check is the ONLY thing
// standing between a stranger who knows the URL and a write to the timeline.
func TestVerifySignatureRejectsForgeries(t *testing.T) {
	const secret = "s3cr3t"
	withWebhookSecret(t, secret)

	payload := []byte(`{"action":"closed"}`)
	valid := sign(secret, payload)

	tests := []struct {
		name   string
		body   []byte
		header string
	}{
		{name: "no signature", body: payload, header: ""},
		{name: "wrong secret", body: payload, header: sign("not-the-secret", payload)},
		{name: "tampered body", body: []byte(`{"action":"merged"}`), header: valid},
		{name: "truncated signature", body: payload, header: valid[:len(valid)-2]},
		{name: "missing algorithm prefix", body: payload, header: valid[len("sha256="):]},
		{name: "sha1 prefix", body: payload, header: "sha1=" + valid[len("sha256="):]},
		{name: "not hex", body: payload, header: "sha256=zzzz"},
		{name: "empty hex", body: payload, header: "sha256="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := VerifySignature(tt.body, tt.header)
			if err == nil {
				t.Fatal("forged delivery was accepted")
			}
			if !errors.Is(err, ErrBadSignature) {
				t.Errorf("error = %v, want ErrBadSignature", err)
			}
		})
	}
}

// TestVerifySignatureFailsClosedWhenUnconfigured pins the most important
// property: with no secret set the endpoint must REJECT, not wave deliveries
// through. Failing open here would leave an unauthenticated write endpoint
// exposed on any install that had not configured GitHub.
func TestVerifySignatureFailsClosedWhenUnconfigured(t *testing.T) {
	withWebhookSecret(t, "")

	payload := []byte(`{"action":"closed"}`)
	err := VerifySignature(payload, sign("anything", payload))
	if err == nil {
		t.Fatal("delivery accepted with no webhook secret configured")
	}
	if !errors.Is(err, ErrNoWebhookSecret) {
		t.Errorf("error = %v, want ErrNoWebhookSecret", err)
	}
	if WebhookEnabled() {
		t.Error("WebhookEnabled() is true with no secret set")
	}
}

func TestWebhookEnabled(t *testing.T) {
	withWebhookSecret(t, "s3cr3t")
	if !WebhookEnabled() {
		t.Error("WebhookEnabled() = false with a secret set")
	}
}
