package routes

import (
	"errors"
	"net/http"
	"time"

	oidcsession "github.com/aidenappl/go-oidc-session"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
)

// ─────────────────────────────────────────────────────────────────────────────
// SSO GATE ON /auth/refresh
//
// Monitor keeps its own 15-minute access JWT because zones verify it
// statelessly. What ties a Monitor session to the identity provider is this
// gate: every refresh first runs the SSO session (mon_sso_session) through
// sso.Manager().Authenticate, which refreshes the provider's tokens when they
// are due and deletes the session when the provider refuses. So:
//
//   - provider session alive  → the refresh family rotates as before; with no
//     family yet (a fresh or silent SSO sign-in), one is minted here;
//   - provider session over   → the family is revoked and the browser gets 401,
//     and monitor-web re-authenticates silently through the provider;
//   - provider unreachable    → 503, not 401: a 401 would send the browser to
//     re-authenticate against the provider that is down.
//
// There is no separate Monitor clock and no introspection checkpoint: a
// session lasts as long as the provider session, and revocation lands within
// one access-token lifetime (back-channel logout deletes the SSO session, and
// the next refresh fails).
// ─────────────────────────────────────────────────────────────────────────────

type ssoGateResult int

const (
	// ssoGateAbsent — no SSO session cookie: a password session, or an SSO
	// family from before the gate (refused below for SSO-only accounts).
	ssoGateAbsent ssoGateResult = iota
	ssoGateLive
	ssoGateEnded
	ssoGateUnavailable
)

func refreshSSOGate(w http.ResponseWriter, r *http.Request) (*oidcsession.Session, ssoGateResult) {
	c, err := r.Cookie(sso.SessionCookie)
	if err != nil || c.Value == "" {
		return nil, ssoGateAbsent
	}
	s, err := sso.Manager().Authenticate(w, r)
	switch {
	case err == nil:
		return s, ssoGateLive
	case errors.Is(err, oidcsession.ErrNoSession), errors.Is(err, oidcsession.ErrSessionEnded):
		return nil, ssoGateEnded
	default:
		// ErrProviderUnavailable, or the session store itself failing.
		if !errors.Is(err, oidcsession.ErrProviderUnavailable) {
			telemetry.WarnErr(r.Context(), "auth.sso.session.lookup.failed", err, map[string]any{
				"dependency": "mariadb",
				"outcome":    "refresh answered 503",
			})
		}
		return nil, ssoGateUnavailable
	}
}

// refuseRefreshSSOEnded ends a Monitor session whose SSO session is over.
func refuseRefreshSSOEnded(w http.ResponseWriter, r *http.Request, row *structs.RefreshToken) {
	if row != nil {
		if err := query.RevokeFamily(db.SQL, row.FamilyID); err != nil {
			telemetry.WarnErr(r.Context(), "auth.refresh.sso_revoke.failed", err, map[string]any{
				"user_id":    row.UserID,
				"dependency": "mariadb",
				"outcome":    "the SSO session is over and refresh was refused; the family itself was not revoked",
			})
		}
	}
	clearTokenCookies(w)
	middleware.ExpectedClientError(w)
	responder.Error(w, http.StatusUnauthorized, "sso session ended")
}

func refuseRefreshSSOUnavailable(w http.ResponseWriter) {
	responder.Error(w, http.StatusServiceUnavailable, "identity provider unavailable, retry shortly")
}

// issueFromSSOSession mints a fresh Monitor family for a live SSO session that
// has none yet — the first refresh after an SSO sign-in in this browser.
func issueFromSSOSession(w http.ResponseWriter, r *http.Request, s *oidcsession.Session) {
	user, err := query.GetUserByID(db.SQL, s.UserID)
	if err != nil || user == nil || !user.Active {
		refreshFailed(w, r, "user_inactive", s.UserID, err)
		responder.Error(w, http.StatusUnauthorized, "user not found or inactive")
		return
	}
	// No sign-in happened here — the family is re-minted from an SSO session
	// that already existed — so the authentication time is unknown (zero), and
	// a step-up (ACCOUNT-01) still needs a real sign-in.
	if err := issueSession(w, user.ID, user.Role, time.Time{}); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to mint tokens", err)
		return
	}
	middleware.SetUser(w, user.ID)
	responder.New(w, user, "token refreshed")
}

// requiresSSOSession reports whether a user can only be signed in through
// SSO: no password identity. A family for such a user that arrives without an
// SSO session predates the gate, and must not live on its own clock.
func requiresSSOSession(r *http.Request, userID int64) bool {
	pw, err := query.GetIdentityByUserAndProvider(db.SQL, userID, "password")
	if err != nil {
		telemetry.WarnErr(r.Context(), "auth.refresh.identity_lookup.failed", err, map[string]any{
			"user_id":    userID,
			"dependency": "mariadb",
			"outcome":    "treated as password-capable; refresh continued",
		})
		return false
	}
	return pw == nil
}
