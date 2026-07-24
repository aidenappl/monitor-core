package sso

import (
	"context"
	"encoding/json"
	"fmt"
)

// NormalizedIdentity is the provider-agnostic result of a successful SSO login.
// Subject is the IdP's stable, unique user id (the `sub` claim for OIDC) — it,
// paired with Provider, is the ONLY correct identity key. Email is a hint used
// only for the strictly-gated verified-both-sides link (see resolve.go); it is
// never the identity.
type NormalizedIdentity struct {
	Provider      string          `json:"provider"`
	Subject       string          `json:"subject"`
	Email         string          `json:"email"`
	EmailVerified bool            `json:"email_verified"`
	Name          *string         `json:"name,omitempty"`
	Picture       *string         `json:"picture,omitempty"`
	RawClaims     json.RawMessage `json:"raw_claims,omitempty"`
}

// TokenSet is the raw IdP token material for a login. Monitor mints its own
// session and does not depend on these; they are cached (encrypted) only so the
// periodic checkpoint can introspect the upstream grant for revocation.
type TokenSet struct {
	AccessToken  string
	RefreshToken string
}

// Adapter turns a provider's OAuth2/OIDC dance into a NormalizedIdentity. One
// implementation exists per sso_providers.kind; dispatch is by NewAdapter.
type Adapter interface {
	// AuthCodeURL builds the provider authorize URL for a login attempt. state
	// is the anti-CSRF/replay token; nonce binds the id_token to this request
	// (OIDC); verifier is the PKCE code verifier whose S256 challenge is sent.
	AuthCodeURL(state, nonce, verifier string) (string, error)

	// Exchange completes the callback: swaps code for tokens (sending the PKCE
	// verifier), verifies the id_token where applicable INCLUDING the nonce, and
	// returns the normalized identity plus the raw tokens to cache.
	Exchange(ctx context.Context, code, verifier, nonce string) (*NormalizedIdentity, *TokenSet, error)
}

// NewAdapter selects the adapter for a provider by its kind.
func NewAdapter(p *Provider) (Adapter, error) {
	switch p.Kind {
	case "oidc":
		return newOIDCAdapter(p)
	case "oauth2":
		return newOAuth2Adapter(p)
	default:
		return nil, fmt.Errorf("sso: unknown provider kind %q", p.Kind)
	}
}
