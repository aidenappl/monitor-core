package routes

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"

	oidcsession "github.com/aidenappl/go-oidc-session"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
)

// Step-up authentication (ACCOUNT-01).
//
// Adding a password to an SSO-only account creates a sign-in method that
// outlives the SSO grant: revoke the user in Forta and the password still
// works. Before this, PUT /auth/self would add one on the strength of the
// session alone, so anyone holding a hijacked or borrowed session could turn it
// into permanent access. It now requires the session's AUTHENTICATION — not its
// last refresh — to be younger than REAUTH_MAX_AGE. Accounts that already have a
// password keep the current-password check instead.
//
// The re-auth path for monitor-web is an SSO sign-in with prompt=login and
// return_url back to /settings/security. That callback stamps a fresh
// jwt.Claims.AuthTime, which refreshes carry forward unchanged.

// REAUTH_MAX_AGE is how recent an authentication must be to add a password.
const REAUTH_MAX_AGE = 10 * time.Minute

// ERROR_CODE_REAUTH_REQUIRED is the application error_code monitor-web routes
// on to start the step-up flow (403, message "reauth_required").
const ERROR_CODE_REAUTH_REQUIRED = 4011

// recentlyAuthenticated reports whether the request's session authenticated
// within REAUTH_MAX_AGE. An unknown authentication time is never recent.
func recentlyAuthenticated(r *http.Request, userID int64, now time.Time) bool {
	at, ok := middleware.SessionAuthTime(r, userID)
	if !ok || at.IsZero() {
		return false
	}
	return now.Sub(at) <= REAUTH_MAX_AGE
}

// ssoSilentCookie marks an SSO login that was started with prompt=none. Its
// value is the login's state, so it binds to exactly that login.
const ssoSilentCookie = "mon_sso_silent"

func setSSOSilentCookie(w http.ResponseWriter, state string) {
	http.SetCookie(w, &http.Cookie{
		Name:     ssoSilentCookie,
		Value:    state,
		Path:     "/auth/sso",
		MaxAge:   int(oidcsession.StateTTL.Seconds()),
		HttpOnly: true,
		Secure:   !env.CookieInsecure,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearSSOSilentCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     ssoSilentCookie,
		Value:    "",
		Path:     "/auth/sso",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   !env.CookieInsecure,
		SameSite: http.SameSiteLaxMode,
	})
}

func ssoLoginWasSilent(r *http.Request, state string) bool {
	c, err := r.Cookie(ssoSilentCookie)
	return err == nil && state != "" && subtle.ConstantTimeCompare([]byte(c.Value), []byte(state)) == 1
}

// ssoAuthTime decides when an SSO sign-in AUTHENTICATED the user.
//
//   - The IdP's `auth_time` claim wins when it carries one (Forta's id_token
//     does). It is the IdP's statement of when the user last proved who they
//     are, so a prompt=none round trip reports the original login rather than
//     now. It is clamped to now: a clock-skewed future value must not buy a
//     longer window.
//   - Without one, a silent (prompt=none) login proves nothing new: zero.
//   - Otherwise the callback time — an interactive IdP round trip in this
//     browser. That is weaker than auth_time (an IdP may skip its own prompt),
//     and is the best a provider without the claim allows.
func ssoAuthTime(identity oidcsession.Identity, silent bool, now time.Time) time.Time {
	var claims struct {
		AuthTime json.Number `json:"auth_time"`
	}
	if len(identity.RawClaims) > 0 && json.Unmarshal(identity.RawClaims, &claims) == nil && claims.AuthTime != "" {
		if f, err := claims.AuthTime.Float64(); err == nil && f > 0 {
			at := time.Unix(int64(f), 0)
			if at.After(now) {
				return now
			}
			return at
		}
	}
	if silent {
		return time.Time{}
	}
	return now
}
