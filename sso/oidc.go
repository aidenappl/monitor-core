package sso

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// oidcAdapter implements Adapter for kind="oidc" providers using OIDC discovery
// + JWKS id_token verification. It requires IssuerURL and ClientID on the row.
type oidcAdapter struct {
	provider *Provider
	oidcProv *oidc.Provider
	verifier *oidc.IDTokenVerifier
	oauth    *oauth2.Config
}

func newOIDCAdapter(p *Provider) (Adapter, error) {
	if p.IssuerURL == nil || *p.IssuerURL == "" {
		return nil, fmt.Errorf("sso: oidc provider %q missing issuer_url", p.Slug)
	}
	if p.ClientID == nil || *p.ClientID == "" {
		return nil, fmt.Errorf("sso: oidc provider %q missing client_id", p.Slug)
	}

	ctx := context.Background()
	op, err := getOIDCProvider(ctx, *p.IssuerURL)
	if err != nil {
		return nil, err
	}

	// The verifier checks signature (via the provider's JWKS), issuer, audience
	// (== client_id) and expiry. It deliberately does NOT verify the nonce —
	// go-oidc leaves that to the caller (see Exchange). Skipping it would leave
	// the flow open to id_token replay across sessions.
	verifier := op.Verifier(&oidc.Config{ClientID: *p.ClientID})

	scopes := strings.Fields(p.Scopes)
	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "email", "profile"}
	}

	return &oidcAdapter{
		provider: p,
		oidcProv: op,
		verifier: verifier,
		oauth: &oauth2.Config{
			ClientID:     *p.ClientID,
			ClientSecret: p.ClientSecret,
			Endpoint:     op.Endpoint(),
			RedirectURL:  p.RedirectURL(),
			Scopes:       scopes,
		},
	}, nil
}

func (a *oidcAdapter) AuthCodeURL(state, nonce, verifier string) (string, error) {
	opts := []oauth2.AuthCodeOption{
		oidc.Nonce(nonce),
		oauth2.S256ChallengeOption(verifier),
	}
	return a.oauth.AuthCodeURL(state, opts...), nil
}

func (a *oidcAdapter) Exchange(ctx context.Context, code, verifier, nonce string) (*NormalizedIdentity, *TokenSet, error) {
	oauthToken, err := a.oauth.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, nil, fmt.Errorf("sso: oidc code exchange: %w", err)
	}

	rawID, ok := oauthToken.Extra("id_token").(string)
	if !ok || rawID == "" {
		return nil, nil, fmt.Errorf("sso: oidc token response missing id_token")
	}

	idToken, err := a.verifier.Verify(ctx, rawID)
	if err != nil {
		return nil, nil, fmt.Errorf("sso: oidc id_token verification failed: %w", err)
	}

	// Verify the nonce OURSELVES — go-oidc verifies sig/iss/aud/exp but never the
	// nonce. This binds the id_token to the state we generated at /login and is
	// the defense against id_token replay/injection.
	if idToken.Nonce != nonce {
		return nil, nil, fmt.Errorf("sso: oidc id_token nonce mismatch")
	}

	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		return nil, nil, fmt.Errorf("sso: oidc claim decode: %w", err)
	}

	subject := claimString(claims, a.provider.SubjectClaim, "sub")
	if subject == "" {
		subject = idToken.Subject
	}
	if subject == "" {
		return nil, nil, fmt.Errorf("sso: oidc identity missing subject")
	}

	email := claimString(claims, a.provider.EmailClaim, "email")
	// Trust the id_token's email_verified claim when present; otherwise fall back
	// to the per-provider TrustEmailVerified flag (config, not a hardcoded slug).
	emailVerified := a.provider.TrustEmailVerified ||
		claimBool(claims, a.provider.EmailVerifiedClaim, "email_verified")

	raw, _ := json.Marshal(claims)
	ni := &NormalizedIdentity{
		Provider:      a.provider.Slug,
		Subject:       subject,
		Email:         email,
		EmailVerified: emailVerified,
		Name:          claimPtr(claims, "name"),
		Picture:       claimPtr(claims, "picture"),
		RawClaims:     raw,
	}
	tokens := &TokenSet{
		AccessToken:  oauthToken.AccessToken,
		RefreshToken: oauthToken.RefreshToken,
	}
	return ni, tokens, nil
}

// claimString reads a string claim by the configured field name, falling back
// to a default field name when the configured one is empty/absent.
func claimString(claims map[string]any, field, fallback string) string {
	for _, f := range []string{field, fallback} {
		if f == "" {
			continue
		}
		if v, ok := claims[f]; ok {
			if s := fmt.Sprint(v); s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}

func claimBool(claims map[string]any, field, fallback string) bool {
	for _, f := range []string{field, fallback} {
		if f == "" {
			continue
		}
		switch v := claims[f].(type) {
		case bool:
			return v
		case string:
			return v == "true"
		}
	}
	return false
}

func claimPtr(claims map[string]any, field string) *string {
	if v, ok := claims[field]; ok {
		if s := fmt.Sprint(v); s != "" && s != "<nil>" {
			return &s
		}
	}
	return nil
}
