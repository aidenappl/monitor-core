package routes

import (
	"log"
	"net/http"
	"net/url"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/aidenappl/monitor-core/tools"
)

// loginErrorPath is where a failed SSO login lands, with ?error=<code>.
const loginErrorPath = "/login"

// HandleSSOCallback completes an SSO login (GET /auth/sso/callback):
//
//  1. Surface any provider-returned error.
//  2. Require code + state; validate and SINGLE-USE-consume the state (this is
//     the CSRF/replay gate — a state is accepted at most once). The provider is
//     taken from the state record, so a single callback path serves every
//     provider (matching the appleby-cloud convention and the exact redirect_uri
//     registered with each IdP).
//  3. Exchange the code (sending the PKCE verifier); the OIDC adapter verifies
//     the id_token signature/iss/aud/exp AND the nonce.
//  4. Resolve the identity to a Monitor user (link/provision rules in
//     sso.ResolveIdentity); reject inactive accounts.
//  5. Cache the (encrypted) IdP tokens for the revocation checkpoint, then mint
//     Monitor's own session and redirect to the sanitized return_url.
//
// All failures redirect to /login?error=<code> rather than rendering an error,
// since this endpoint is reached by a top-level browser navigation.
func HandleSSOCallback(w http.ResponseWriter, r *http.Request) {
	if errCode := r.URL.Query().Get("error"); errCode != "" {
		redirectLoginError(w, r, "sso_denied")
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		redirectLoginError(w, r, "sso_missing_params")
		return
	}

	// Validate + consume state (single-use). The provider slug is carried in the
	// state record (set at /login), so the callback path itself is provider-
	// agnostic.
	stateData, err := sso.ConsumeState(db.SQL, state)
	if err != nil || stateData.Provider == "" {
		redirectLoginError(w, r, "sso_state_invalid")
		return
	}
	slug := stateData.Provider

	provider, err := sso.LoadProvider(db.SQL, slug)
	if err != nil || !provider.Enabled {
		redirectLoginError(w, r, "sso_provider_unavailable")
		return
	}

	adapter, err := sso.NewAdapter(provider)
	if err != nil {
		log.Printf("sso callback: adapter build failed for %q: %v", slug, err)
		redirectLoginError(w, r, "sso_provider_unavailable")
		return
	}

	// Exchange the code and normalize the identity. For OIDC this verifies the
	// id_token INCLUDING the nonce carried in stateData.
	identity, tokens, err := adapter.Exchange(r.Context(), code, stateData.Verifier, stateData.Nonce)
	if err != nil {
		log.Printf("sso callback: exchange failed for %q: %v", slug, err)
		redirectLoginError(w, r, "sso_exchange_failed")
		return
	}

	// Authenticated LINK flow: the state was minted by POST /auth/self/identities
	// while the user held an active session, so we attach this identity to that
	// user directly instead of resolving/provisioning a login.
	if stateData.LinkUserID != 0 {
		linkSSOIdentity(w, r, db.SQL, stateData.LinkUserID, slug, *identity, safeReturnURL(stateData.ReturnURL))
		return
	}

	user, err := sso.ResolveIdentity(db.SQL, provider, *identity)
	if err != nil {
		log.Printf("sso callback: resolve identity failed for %q: %v", slug, err)
		redirectLoginError(w, r, "sso_resolve_failed")
		return
	}
	if !user.Active {
		redirectLoginError(w, r, "sso_account_disabled")
		return
	}

	// Cache the IdP tokens (encrypted at rest) so the checkpoint can introspect
	// the upstream grant later. A failure here is non-fatal to login — worst
	// case the session simply isn't checkpointed.
	if err := cacheSSOSession(user.ID, slug, tokens); err != nil {
		log.Printf("sso callback: failed to cache sso session for user %d: %v", user.ID, err)
	}

	if err := issueSession(w, user.ID); err != nil {
		log.Printf("sso callback: failed to issue session for user %d: %v", user.ID, err)
		redirectLoginError(w, r, "sso_session_failed")
		return
	}

	http.Redirect(w, r, safeReturnURL(stateData.ReturnURL), http.StatusFound)
}

// linkSSOIdentity attaches a freshly-authenticated SSO identity to an already
// signed-in user (the authenticated-link flow). It refuses to steal an identity
// already bound to a DIFFERENT user, treats a re-link of the same user as a
// no-op success, and redirects back to the settings return_url either way. No
// new session is issued — the caller was already authenticated.
func linkSSOIdentity(w http.ResponseWriter, r *http.Request, engine db.Queryable, linkUserID int64, slug string, ni sso.NormalizedIdentity, returnURL string) {
	existing, err := query.GetIdentityByProviderSubject(engine, ni.Provider, ni.Subject)
	if err != nil {
		log.Printf("sso link: lookup identity failed for %q: %v", slug, err)
		redirectLinkResult(w, r, returnURL, "link_error", "sso_link_failed")
		return
	}
	if existing != nil {
		if existing.UserID == linkUserID {
			log.Printf("sso link: %s:%s already linked to user %d (no-op)", ni.Provider, ni.Subject, linkUserID)
			redirectLinkResult(w, r, returnURL, "linked", slug)
			return
		}
		log.Printf("sso link: refusing to link %s:%s to user %d — already owned by user %d", ni.Provider, ni.Subject, linkUserID, existing.UserID)
		redirectLinkResult(w, r, returnURL, "link_error", "sso_already_linked")
		return
	}

	var providerEmail *string
	if ni.Email != "" {
		e := ni.Email
		providerEmail = &e
	}
	if _, err := query.CreateIdentity(engine, query.CreateIdentityRequest{
		UserID:         linkUserID,
		Provider:       ni.Provider,
		ProviderUserID: ni.Subject,
		ProviderEmail:  providerEmail,
		EmailVerified:  ni.EmailVerified,
		IdentityData:   ni.RawClaims,
	}); err != nil {
		log.Printf("sso link: create identity failed for user %d: %v", linkUserID, err)
		redirectLinkResult(w, r, returnURL, "link_error", "sso_link_failed")
		return
	}

	log.Printf("sso link: linked %s:%s onto user %d", ni.Provider, ni.Subject, linkUserID)
	redirectLinkResult(w, r, returnURL, "linked", slug)
}

// redirectLinkResult sends the browser back to the settings return_url with a
// single result query param (linked=<slug> or link_error=<code>).
func redirectLinkResult(w http.ResponseWriter, r *http.Request, returnURL, key, val string) {
	u, err := url.Parse(returnURL)
	if err != nil {
		u = &url.URL{Path: "/"}
	}
	q := u.Query()
	q.Set(key, val)
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// cacheSSOSession encrypts and upserts the IdP tokens for the checkpoint.
func cacheSSOSession(userID int64, slug string, tokens *sso.TokenSet) error {
	encAccess, err := tools.Encrypt(tokens.AccessToken)
	if err != nil {
		return err
	}
	encRefresh := ""
	if tokens.RefreshToken != "" {
		encRefresh, err = tools.Encrypt(tokens.RefreshToken)
		if err != nil {
			return err
		}
	}
	return query.UpsertSSOSession(db.SQL, userID, slug, encAccess, encRefresh)
}

// redirectLoginError sends the browser to /login?error=<code>.
func redirectLoginError(w http.ResponseWriter, r *http.Request, code string) {
	u := url.URL{Path: loginErrorPath}
	q := u.Query()
	q.Set("error", code)
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}
