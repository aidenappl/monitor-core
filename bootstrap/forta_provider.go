package bootstrap

import (
	"fmt"
	"log"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
)

// fortaClientSecretRef is the Keyring secret name the seeded Forta provider row
// points at for its client secret. sso.resolveSecret looks this up (Keyring
// first, then env) at login time; the raw secret never touches this seed.
const fortaClientSecretRef = "MON_SSO_FORTA_CLIENT_SECRET"

// EnsureFortaProvider seeds the "forta" SSO provider row on startup so existing
// Forta users keep a working "Continue with Forta" button through the auth
// overhaul. It is idempotent and additive:
//
//   - If a provider with slug "forta" already exists, it is a NO-OP — an operator
//     may have edited it via the admin API and we must never clobber that.
//   - If the Forta env is not configured (MON_SSO_FORTA_* empty), seeding is
//     skipped and a warning is logged. A missing Forta config just means "no
//     Forta button"; it MUST NOT fail startup (main.go logs and continues).
//
// The seeded row uses kind="oauth2" (Forta is non-OIDC — its {success,data{…}}
// envelope is handled by sso/forta.go), trusts Forta's upstream email
// verification (trust_email_verified=1, Forta has no email_verified claim), and
// enables auto-link + auto-provision so a returning Forta user resolves cleanly.
func EnsureFortaProvider(engine db.Queryable) error {
	existing, err := query.GetProviderBySlug(engine, "forta")
	if err != nil {
		return fmt.Errorf("failed to check for existing forta provider: %w", err)
	}
	if existing != nil {
		// Already present (possibly operator-edited) — leave it untouched.
		return nil
	}

	if env.FortaClientID == "" || env.FortaAuthorizeURL == "" ||
		env.FortaTokenURL == "" || env.FortaUserInfoURL == "" {
		log.Println("bootstrap: MON_SSO_FORTA_* not configured — skipping Forta provider seed (no Forta button)")
		return nil
	}

	displayName := "Forta"
	kind := "oauth2"
	authorizeURL := env.FortaAuthorizeURL
	tokenURL := env.FortaTokenURL
	userInfoURL := env.FortaUserInfoURL
	clientID := env.FortaClientID
	secretRef := fortaClientSecretRef
	scopes := env.FortaScopes
	buttonLabel := "Continue with Forta"
	trueVal := true

	req := query.CreateProviderRequest{
		Slug:               "forta",
		DisplayName:        displayName,
		Kind:               kind,
		AuthorizeURL:       &authorizeURL,
		TokenURL:           &tokenURL,
		UserInfoURL:        &userInfoURL,
		ClientID:           &clientID,
		ClientSecretRef:    &secretRef,
		Scopes:             &scopes,
		ButtonLabel:        &buttonLabel,
		TrustEmailVerified: &trueVal, // Forta emails are verified upstream.
		AllowAutoLink:      &trueVal,
		AutoProvision:      &trueVal,
		Enabled:            &trueVal,
	}
	// introspect_url is optional (used by the SSO revocation checkpoint); only
	// set it when configured.
	if env.FortaIntrospectURL != "" {
		introspectURL := env.FortaIntrospectURL
		req.IntrospectURL = &introspectURL
	}

	if _, err := query.CreateProvider(engine, req); err != nil {
		return fmt.Errorf("failed to seed forta provider: %w", err)
	}

	log.Printf("bootstrap: seeded Forta SSO provider (client_secret_ref=%s)", secretRef)
	return nil
}
