package routes

import (
	"crypto/sha256"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/telemetry"
)

// HandleLogout clears the session cookies and revokes the CURRENT session's
// refresh family server-side. Runs behind SessionMiddleware, so the user is in
// context (POST /auth/logout).
//
// Which family is "current", in preference order:
//  1. The refresh cookie, when a client presents it (non-browser clients). The
//     stored row is authoritative.
//  2. The access token's fid claim (jwt.Claims.FamilyID). This is the browser
//     path: mon-refresh-token is path-scoped to the refresh endpoint (and
//     monitor-web's proxy rewrites that path), so a browser NEVER sends it here.
//  3. RevokeAllForUser — only when neither is available, i.e. an access token
//     minted before the fid claim existed. Logging out then ends every device,
//     the pre-fid behaviour, for at most one access lifetime.
//
// (SSO-session cleanup — dropping cached IdP tokens — is Phase 3.)
func HandleLogout(w http.ResponseWriter, r *http.Request) {
	user, ok := middleware.GetUserFromContext(r.Context())
	if !ok || user == nil {
		// SessionMiddleware should guarantee this, but never revoke blindly.
		clearTokenCookies(w)
		middleware.ExpectedClientError(w)
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	// The user asked to be logged out and is, whatever happens here: the cookies
	// are cleared regardless. A revocation that fails leaves the refresh tokens
	// valid server-side, which is worth a warning, not a failed logout.
	family, path := logoutFamily(r, user.ID)
	var rErr error
	switch path {
	case "all":
		rErr = query.RevokeAllForUser(db.SQL, user.ID)
	default:
		rErr = query.RevokeFamily(db.SQL, family)
	}
	if rErr != nil {
		logoutRevokeFailed(r, user.ID, path, rErr)
	}

	clearTokenCookies(w)
	// The success of the revocation is the scope on this one event rather than a
	// second one: a logout is a request, and the request event is the other.
	telemetry.Info(r.Context(), "auth.logout.succeeded", map[string]any{
		"user_id":      user.ID,
		"revoke_scope": path,
		"revoked":      rErr == nil,
	})
	responder.New(w, nil, "logged out")
}

// logoutFamily resolves the family to revoke and names the path that found it:
// "refresh_cookie", "access_fid", or "all" (no family — revoke everything).
//
// A presented refresh cookie that resolves to another user's row is ignored
// rather than trusted: logout must only ever end the caller's own session.
func logoutFamily(r *http.Request, userID int64) ([]byte, string) {
	if cookie, err := r.Cookie(cookieRefreshToken); err == nil && cookie.Value != "" {
		hash := sha256.Sum256([]byte(cookie.Value))
		row, lErr := query.GetRefreshTokenByHash(db.SQL, hash[:])
		if lErr != nil {
			// Not the end of the logout: it falls through to the fid claim, and
			// then to revoking everything. Reported because the presented cookie
			// was the most precise scope available and it was lost.
			telemetry.WarnErr(r.Context(), "auth.logout.family.lookup.failed", lErr, map[string]any{
				"user_id":    userID,
				"dependency": "mariadb",
				"outcome":    "fell back to the access token's family, or to revoking every session",
			})
		} else if row != nil && row.UserID == userID {
			return row.FamilyID, "refresh_cookie"
		}
	}
	if family, ok := middleware.SessionFamilyID(r, userID); ok {
		return family, "access_fid"
	}
	return nil, "all"
}

// logoutRevokeFailed reports a logout whose server-side revocation did not
// happen. The client is logged out either way, so this is a warning.
func logoutRevokeFailed(r *http.Request, userID int64, scope string, err error) {
	telemetry.WarnErr(r.Context(), "auth.logout.revoke.failed", err, map[string]any{
		"user_id":      userID,
		"revoke_scope": scope,
		"dependency":   "mariadb",
		"outcome":      "cookies cleared, but the refresh token(s) stay valid server-side until they expire",
	})
}
