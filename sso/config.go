// Package sso wires Monitor onto the shared SSO implementation in
// github.com/aidenappl/go-forta/sso.
//
// ─────────────────────────────────────────────────────────────────────────────
// WHAT MOVED OUT, AND WHY THIS PACKAGE STILL EXISTS
//
// The OAuth2/OIDC protocol — discovery, PKCE, state, nonce, id_token
// verification, UserInfo, introspection and the revocation checkpoint — now lives
// in the shared module. This package was where that code was written, and it was
// lifted out because two other services had forked thinner copies of it and the
// forks had drifted into real vulnerabilities. Keeping one implementation is the
// point; keeping THIS one as the implementation is not.
//
// What remains here is everything the library deliberately refuses to know:
//
//	config.go        — maps an sso_providers ROW onto ssolib.Provider, and
//	                   resolves the client secret (Keyring ref, then env, then
//	                   AES-GCM column) — three mechanisms no library should own.
//	statestore.go    — ssolib.StateStore over Monitor's settings KV table.
//	sessionstore.go  — ssolib.SessionStore over sso_sessions, with AES-256-GCM
//	                   encryption at rest.
//	resolve.go       — ssolib.UserResolver: the link/provision decision matrix.
//	                   ⚠️ THE MOST SECURITY-SENSITIVE FILE IN THE PACKAGE.
//	checkpoint.go    — installs the library's Checkpointer into the session
//	                   middleware.
//
// Deleted: adapter.go, oidc.go, oauth2.go, introspect.go, state.go. If you are
// looking for that code, it is in the module — do not re-add a local copy.
// ─────────────────────────────────────────────────────────────────────────────
package sso

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	ssolib "github.com/aidenappl/go-forta/sso"
	keyring "github.com/aidenappl/go-keyring"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
)

// callbackPath is the single callback route every provider redirects to. The
// provider is recovered from the state record rather than from the path, so one
// exact redirect_uri is registered with every IdP.
const callbackPath = "/auth/sso/callback"

// Provider pairs the library's provider view with the stored row it came from.
//
// The row is retained because Monitor's own handlers need fields the library has
// no use for — Enabled, ButtonLabel, ID — and because an admin API returns the row
// shape, not the library's.
type Provider struct {
	*ssolib.Provider

	// Row is the sso_providers record this was built from.
	Row *structs.SSOProvider
}

// LoadProvider fetches a provider by slug, resolves its secret, and maps it onto
// the library's Provider.
func LoadProvider(engine db.Queryable, slug string) (*Provider, error) {
	row, err := query.GetProviderBySlug(engine, slug)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("sso: provider %q not found", slug)
	}

	secret, err := resolveSecret(row)
	if err != nil {
		return nil, err
	}

	return &Provider{Provider: toLibProvider(row, secret), Row: row}, nil
}

// Enabled reports whether the underlying row is enabled.
func (p *Provider) Enabled() bool { return p.Row != nil && p.Row.Enabled }

// toLibProvider maps a stored row onto the library's Provider.
//
// ⚠️ The mapping is where a nullable column becomes a plain string, so every
// dereference is guarded. A nil IssuerURL reaching the library as "" produces a
// clear Validate() error naming the field; a nil dereference here produces a
// panic in the middle of a login.
func toLibProvider(row *structs.SSOProvider, secret string) *ssolib.Provider {
	return &ssolib.Provider{
		Slug:        row.Slug,
		DisplayName: row.DisplayName,
		Kind:        ssolib.Kind(row.Kind),

		IssuerURL:     deref(row.IssuerURL),
		AuthorizeURL:  deref(row.AuthorizeURL),
		TokenURL:      deref(row.TokenURL),
		UserInfoURL:   deref(row.UserInfoURL),
		IntrospectURL: deref(row.IntrospectURL),

		ClientID:     deref(row.ClientID),
		ClientSecret: secret,

		Scopes:      row.Scopes,
		RedirectURL: RedirectURL(),

		SubjectClaim:       row.SubjectClaim,
		EmailClaim:         row.EmailClaim,
		EmailVerifiedClaim: row.EmailVerifiedClaim,
		TrustEmailVerified: row.TrustEmailVerified,

		AllowAutoLink: row.AllowAutoLink,
		AutoProvision: row.AutoProvision,

		// FetchUserInfo stays OFF for kind=oidc. Monitor reads identity from the
		// id_token, which is signed; UserInfo would be an extra round trip on every
		// login for claims it already has. The library still enforces the OIDC Core
		// §5.3.2 sub check whenever it IS enabled.
		FetchUserInfo: false,
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// RedirectURL is the exact redirect_uri Monitor's callback runs at.
//
// It MUST match byte-for-byte what is registered with each IdP — OAuth compares
// redirect_uri exactly, with no normalisation of a trailing slash, case, or a
// default port — so it is derived deterministically from env.PublicBaseURL rather
// than reconstructed from the incoming request. Building it from the request would
// let a Host header decide where a code may be sent.
func RedirectURL() string {
	return strings.TrimRight(env.PublicBaseURL, "/") + callbackPath
}

// resolveSecret returns a provider's client secret.
//
// Resolution order, and each step exists for a reason:
//
//  1. Keyring reference (client_secret_ref) — the master copy, so rotating in
//     Keyring takes effect without touching the database.
//  2. An environment variable of the SAME NAME as the ref. Keyring injects
//     secrets into the environment at boot, so a ref may already be present
//     without a live Keyring connection — this is what keeps a Keyring outage
//     from breaking logins on an already-running process.
//  3. AES-256-GCM encrypted column (client_secret_enc), for a provider
//     configured entirely through the admin UI with no Keyring entry.
//
// Returns ("", nil) when none is configured, which is valid for a public client.
func resolveSecret(p *structs.SSOProvider) (string, error) {
	if p.ClientSecretRef != nil && *p.ClientSecretRef != "" {
		ref := *p.ClientSecretRef

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if v, err := keyring.Get(ctx, ref); err == nil && v != "" {
			return v, nil
		}
		if v := os.Getenv(ref); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("sso: could not resolve client_secret_ref %q via keyring or env", ref)
	}

	if p.ClientSecretEnc != nil && *p.ClientSecretEnc != "" {
		return tools.Decrypt(*p.ClientSecretEnc)
	}
	return "", nil
}
