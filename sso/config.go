// Package sso implements Monitor's pluggable, config-driven SSO subsystem. A row
// in sso_providers (see query.SSOProvider) fully describes an identity provider;
// there is no per-provider Go code beyond two adapters:
//
//   - kind="oidc"   → generic OpenID Connect via go-oidc discovery + JWKS
//     id_token verification (oidc.go). Any compliant OIDC provider (Google,
//     Okta, Entra, Auth0, …) is a config row, not a code change.
//   - kind="oauth2" → a tolerant OAuth2 adapter (forta.go) covering non-OIDC
//     providers, including Forta's {success,data{...}} envelope. Forta is just
//     the first such row; nothing here is Forta-specific.
//
// Every adapter normalizes the login to a NormalizedIdentity keyed on
// (Provider, Subject) — never on email (see resolve.go for why).
package sso

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	keyring "github.com/aidenappl/go-keyring"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
	"github.com/coreos/go-oidc/v3/oidc"
)

// Provider is a fully-resolved, runtime view of an sso_providers row: the stored
// record plus its decrypted client secret. Build one with LoadProvider.
type Provider struct {
	*structs.SSOProvider
	ClientSecret string
}

// RedirectURL is the exact redirect_uri this provider's callback runs at. It
// MUST match byte-for-byte the value registered with the IdP (OAuth redirect_uri
// exact-match rule), so it is derived deterministically from env.PublicBaseURL.
func (p *Provider) RedirectURL() string {
	base := strings.TrimRight(env.PublicBaseURL, "/")
	return base + "/auth/sso/callback"
}

// LoadProvider fetches a provider by slug and resolves its client secret.
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
	return &Provider{SSOProvider: row, ClientSecret: secret}, nil
}

// resolveSecret returns a provider's client secret. Resolution order:
//  1. Keyring reference (client_secret_ref) — looked up live, then falling back
//     to an env var of the same name (Keyring injects secrets into the env at
//     boot, so a ref may already be present without a live Keyring connection).
//  2. AES-256-GCM encrypted DB value (client_secret_enc).
//
// Returns "" (no error) when neither is configured — valid for public clients.
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

// --- OIDC discovery cache ------------------------------------------------

var (
	discoveryMu    sync.Mutex
	discoveryCache = map[string]*oidc.Provider{}
)

// getOIDCProvider returns a cached *oidc.Provider for issuer, performing OIDC
// discovery ({issuer}/.well-known/openid-configuration) once per issuer.
func getOIDCProvider(ctx context.Context, issuer string) (*oidc.Provider, error) {
	discoveryMu.Lock()
	defer discoveryMu.Unlock()
	if p, ok := discoveryCache[issuer]; ok {
		return p, nil
	}
	p, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("sso: oidc discovery for %q: %w", issuer, err)
	}
	discoveryCache[issuer] = p
	return p, nil
}
