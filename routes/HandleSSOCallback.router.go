package routes

import (
	"net/http"
	"net/url"

	oidcsession "github.com/aidenappl/go-oidc-session"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/aidenappl/monitor-core/telemetry"
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
//  5. Open an SSO session (sso.Manager().IssueSession — the provider's tokens,
//     encrypted, behind the mon_sso_session cookie) and mint Monitor's own
//     token family, then redirect to the sanitized return_url. Every later
//     /auth/refresh re-confirms the SSO session, so the family lives exactly as
//     long as the provider session.
//
// All failures redirect to /login?error=<code> rather than rendering an error,
// since this endpoint is reached by a top-level browser navigation.
func HandleSSOCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")

	// The login must finish in the browser that started it (login CSRF): the
	// state is only accepted alongside the cookie HandleSSOLogin set.
	bound := ssoStateCookieMatches(r, state)
	clearSSOStateCookie(w)

	if errCode := r.URL.Query().Get("error"); errCode != "" {
		// The provider's "not signed in" answer to a silent (prompt=none)
		// attempt: the login page shows the form, with no error.
		if ssoInteractionRequired(errCode) {
			returnURL := "/"
			if sd, err := oidcsession.ConsumeState(r.Context(), sso.NewStateStore(db.SQL), state); err == nil {
				returnURL = safeReturnURL(sd.ReturnURL)
			}
			redirectLoginRequired(w, r, returnURL)
			return
		}
		ssoCallbackFailed(r, "", "sso_denied", nil, nil)
		redirectLoginError(w, r, "sso_denied")
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" || state == "" || !bound {
		ssoCallbackFailed(r, "", "sso_missing_params", nil, nil)
		redirectLoginError(w, r, "sso_missing_params")
		return
	}

	// Validate + consume state (single-use). The provider slug is carried in the
	// state record (set at /login), so the callback path itself is provider-
	// agnostic.
	stateData, err := oidcsession.ConsumeState(r.Context(), sso.NewStateStore(db.SQL), state)
	if err != nil || stateData.Provider == "" {
		ssoCallbackFailed(r, "", "sso_state_invalid", err, nil)
		redirectLoginError(w, r, "sso_state_invalid")
		return
	}
	slug := stateData.Provider

	provider, err := sso.LoadProvider(db.SQL, slug)
	if err != nil || !provider.Enabled() {
		ssoCallbackFailed(r, slug, "sso_provider_unavailable", err, nil)
		redirectLoginError(w, r, "sso_provider_unavailable")
		return
	}

	adapter, err := oidcsession.NewAdapter(r.Context(), provider.Provider)
	if err != nil {
		ssoCallbackBroken(r, slug, "sso_adapter_failed", err)
		redirectLoginError(w, r, "sso_provider_unavailable")
		return
	}

	// Exchange the code and normalize the identity. For OIDC this verifies the
	// id_token INCLUDING the nonce carried in stateData.
	identity, tokens, err := adapter.Exchange(r.Context(), code, stateData.Verifier, stateData.Nonce)
	if err != nil {
		ssoCallbackBroken(r, slug, "sso_exchange_failed", err)
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

	user, err := sso.ResolveIdentity(r.Context(), db.SQL, provider.Provider, *identity)
	if err != nil {
		// A refusal by policy (an unverified account, auto-provisioning off) as
		// often as a failure, so a warning; the text says which.
		ssoCallbackFailed(r, slug, "sso_resolve_failed", err, nil)
		redirectLoginError(w, r, "sso_resolve_failed")
		return
	}
	if !user.Active {
		ssoCallbackFailed(r, slug, "sso_account_disabled", nil, map[string]any{"user_id": user.ID})
		redirectLoginError(w, r, "sso_account_disabled")
		return
	}

	if _, err := sso.Manager().IssueSession(w, r, slug, user.ID, *identity, *tokens); err != nil {
		ssoCallbackBroken(r, slug, "sso_session_failed", err)
		redirectLoginError(w, r, "sso_session_failed")
		return
	}

	if err := issueSession(w, user.ID, user.Role); err != nil {
		ssoCallbackBroken(r, slug, "sso_session_failed", err)
		redirectLoginError(w, r, "sso_session_failed")
		return
	}

	middleware.SetUser(w, user.ID)
	telemetry.Info(r.Context(), "auth.login.succeeded", map[string]any{
		"user_id":  user.ID,
		"method":   "sso",
		"provider": slug,
	})
	http.Redirect(w, r, webAppURL(safeReturnURL(stateData.ReturnURL)), http.StatusFound)
}

// ssoCallbackFailed reports a callback that ended on /login?error=<reason>
// because of the caller or the IdP's answer — denied, a stale state, a disabled
// account. The response is a 302 either way, so this event, not the request's,
// is the record of the failure. Coalesced per reason: the callback is
// unauthenticated, and anything can hit it in a loop.
func ssoCallbackFailed(r *http.Request, provider, reason string, err error, extra map[string]any) {
	data := map[string]any{
		"provider":  provider,
		"reason":    reason,
		"client_ip": middleware.GetClientIPFromContext(r.Context()),
		"outcome":   "redirected to /login?error=" + reason,
	}
	for k, v := range extra {
		data[k] = v
	}
	telemetry.WarnCoalesced(r.Context(), "auth.sso.callback.failed:"+reason, "auth.sso.callback.failed", err, data)
}

// ssoCallbackBroken reports a callback that failed on THIS side or the IdP's —
// a provider that cannot be built, a code exchange the IdP refused, a session
// that could not be minted. An error, with the IdP's answer: nobody can sign in
// through this provider until it is fixed.
func ssoCallbackBroken(r *http.Request, provider, reason string, err error) {
	telemetry.ErrorCoalesced(r.Context(), "auth.sso.callback.broken:"+provider+":"+reason, "auth.sso.callback.broken", err, map[string]any{
		"provider":   provider,
		"reason":     reason,
		"dependency": "idp",
		"outcome":    "redirected to /login?error=" + reason + "; the user was not signed in",
	})
}

// linkSSOIdentity attaches a freshly-authenticated SSO identity to an already
// signed-in user (the authenticated-link flow). It refuses to steal an identity
// already bound to a DIFFERENT user, treats a re-link of the same user as a
// no-op success, and redirects back to the settings return_url either way. No
// new session is issued — the caller was already authenticated.
func linkSSOIdentity(w http.ResponseWriter, r *http.Request, engine db.Queryable, linkUserID int64, slug string, ni oidcsession.Identity, returnURL string) {
	if err := sso.LinkIdentity(engine, linkUserID, ni); err != nil {
		// sso.LinkIdentity refuses to move an identity already owned by a different
		// user. That refusal is the security property — without it, linking is a way
		// to transfer an identity between accounts — so it lives in one place rather
		// than being re-implemented per call site.
		telemetry.WarnErr(r.Context(), "auth.identity.link.failed", err, map[string]any{
			"user_id":  linkUserID,
			"provider": slug,
			"reason":   "link_refused",
			"outcome":  "redirected with link_error=sso_already_linked",
		})
		redirectLinkResult(w, r, returnURL, "link_error", "sso_already_linked")
		return
	}
	telemetry.Info(r.Context(), "auth.identity.linked", map[string]any{
		"user_id":  linkUserID,
		"provider": slug,
		"via":      "settings",
	})
	redirectLinkResult(w, r, returnURL, "linked", slug)
}

// redirectLinkResult sends the browser back to the settings return_url with a
// single result query param (linked=<slug> or link_error=<code>).
func redirectLinkResult(w http.ResponseWriter, r *http.Request, returnURL, key, val string) {
	u, err := url.Parse(webAppURL(returnURL))
	if err != nil {
		u, _ = url.Parse(webAppURL("/"))
	}
	q := u.Query()
	q.Set(key, val)
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// redirectLoginError sends the browser to the web app's /login?error=<code>.
func redirectLoginError(w http.ResponseWriter, r *http.Request, code string) {
	u, _ := url.Parse(webAppURL(loginErrorPath))
	q := u.Query()
	q.Set("error", code)
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// redirectLoginRequired sends the browser to /login?error=login_required,
// keeping where it was going.
func redirectLoginRequired(w http.ResponseWriter, r *http.Request, returnURL string) {
	u, _ := url.Parse(webAppURL(loginErrorPath))
	q := u.Query()
	q.Set("error", "login_required")
	if returnURL != "/" {
		// monitor-web's login page round-trips its destination as ?next=.
		q.Set("next", returnURL)
	}
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// ssoInteractionRequired reports the OIDC Core §3.1.2.6 errors that mean the
// provider would have to show the user something.
func ssoInteractionRequired(code string) bool {
	switch code {
	case "login_required", "interaction_required", "consent_required", "account_selection_required":
		return true
	}
	return false
}
