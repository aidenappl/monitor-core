package middleware

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/env"
)

const (
	csrfCookieName = "mon-csrf"
	csrfHeaderName = "X-CSRF-Token"
)

// csrfExemptPaths are the auth endpoints that must work before a CSRF cookie
// can exist / be echoed: the initial login, the token refresh, and the SSO
// callback (a top-level redirect from the IdP that carries no custom header).
var csrfExemptPaths = map[string]bool{
	"/auth/login":    true,
	"/auth/register": true,
	"/auth/refresh":  true,

	// GitHub webhook deliveries are server-to-server POSTs carrying no cookie,
	// no Bearer token and no X-Api-Key, so every exemption above misses them and
	// CSRF would reject the delivery outright.
	//
	// Exempting is sound rather than a hole: CSRF defends against a browser
	// auto-attaching a session cookie, and this route has no cookie auth at all.
	// Its authentication is the HMAC-SHA256 signature over the body, which a
	// cross-site form cannot produce. See github.VerifySignature.
	"/webhooks/github": true,
}

// SSO callbacks are /auth/sso/{slug}/callback — a top-level IdP redirect that
// carries no custom header. Matched by suffix since the slug varies. (They are
// GET, hence already method-exempt below; this keeps the intent explicit.)
func isSSOCallbackPath(path string) bool {
	return strings.HasPrefix(path, "/auth/sso/") && strings.HasSuffix(path, "/callback")
}

// isBackchannelLogoutPath matches /auth/sso/{slug}/backchannel-logout.
//
// ─────────────────────────────────────────────────────────────────────────────
// ⚠️ EXEMPT, AND WITHOUT THE EXEMPTION THE FEATURE IS SILENTLY DEAD.
//
// This is a server-to-server POST from the identity provider (OIDC Back-Channel
// Logout 1.0 §2.5). It carries no cookie, no Bearer token and no X-Api-Key, so
// it falls through every other exemption above and is refused 403 "missing csrf
// cookie". Forta then retries six times, marks the delivery exhausted, and the
// operator sees a receiver that looks broken — while revocation quietly stays at
// poll speed. Observed in production on 2026-08-08, from an endpoint whose unit
// test passed because it exercised the router WITHOUT this middleware.
//
// Exempting it is correct, not a concession. CSRF defends against a browser
// being tricked into spending AMBIENT AUTHORITY — cookies it already holds. This
// endpoint has none: it reads no cookie and no session, and its sole
// authentication is the signature on the logout token, verified against the
// provider's JWKS and against our client_id. A forged cross-site POST arriving
// here without a validly signed token is rejected by that verification, which is
// strictly stronger than a double-submit cookie. The CSRF check can only ever
// reject the legitimate caller.
// ─────────────────────────────────────────────────────────────────────────────
func isBackchannelLogoutPath(path string) bool {
	return strings.HasPrefix(path, "/auth/sso/") && strings.HasSuffix(path, "/backchannel-logout")
}

// CSRFMiddleware implements the double-submit-cookie pattern. A non-HttpOnly
// mon-csrf cookie (SameSite=Strict) must be echoed back in the X-CSRF-Token
// header on unsafe requests, and the two are compared in constant time. Safe
// methods, Bearer-authenticated (stateless) clients, and the auth bootstrap
// paths are exempt. Errors use codes 4030 (missing) and 4031 (mismatch).
func CSRFMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ensure a CSRF cookie exists so the SPA has a token to echo.
		if _, err := r.Cookie(csrfCookieName); err != nil {
			setCSRFCookie(w)
		}

		// Safe methods never mutate state.
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		// Bearer-token clients are stateless API callers, not cookie-bearing
		// browsers, so they are not subject to CSRF.
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			next.ServeHTTP(w, r)
			return
		}

		// X-Api-Key clients (event ingestion via go-monitor, admin API-key query
		// callers) authenticate with a custom header a cross-site form cannot set,
		// so — like Bearer — they are not subject to CSRF. This keeps the
		// X-Api-Key → POST /v1/events ingest path working under the global CSRF
		// middleware.
		if r.Header.Get("X-Api-Key") != "" {
			next.ServeHTTP(w, r)
			return
		}

		if csrfExemptPaths[r.URL.Path] || isSSOCallbackPath(r.URL.Path) || isBackchannelLogoutPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		cookie, err := r.Cookie(csrfCookieName)
		if err != nil || cookie.Value == "" {
			writeCSRFError(w, `{"success":false,"message":"missing csrf cookie","error_code":4030}`)
			return
		}
		headerToken := r.Header.Get(csrfHeaderName)
		if headerToken == "" || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(headerToken)) != 1 {
			writeCSRFError(w, `{"success":false,"message":"csrf token mismatch","error_code":4031}`)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeCSRFError(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(body))
}

func setCSRFCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    generateCSRFToken(),
		Path:     "/",
		Domain:   env.CookieDomain,
		HttpOnly: false, // JavaScript needs to read this to echo it in the header
		Secure:   !env.CookieInsecure,
		SameSite: http.SameSiteStrictMode,
	})
}

func generateCSRFToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}
