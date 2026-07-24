package routes

import (
	"crypto/sha256"
	"log"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
)

// HandleLogout clears the session cookies and revokes the user's refresh
// token(s) server-side. Runs behind SessionMiddleware, so the user is in
// context (POST /auth/logout).
//
// Cookie-path note: mon-refresh-token is scoped to Path=/auth/refresh, so a
// browser does NOT send it to /auth/logout. When it is absent we revoke ALL of
// the user's refresh tokens (RevokeAllForUser) so logout is effective even
// without knowing the specific family; when a client does present it (path
// broadened, or a non-browser client), we revoke just that family.
// (SSO-session cleanup — dropping cached IdP tokens — is Phase 3.)
func HandleLogout(w http.ResponseWriter, r *http.Request) {
	user, ok := middleware.GetUserFromContext(r.Context())
	if !ok || user == nil {
		// SessionMiddleware should guarantee this, but never revoke blindly.
		clearTokenCookies(w)
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return
	}

	if cookie, err := r.Cookie(cookieRefreshToken); err == nil && cookie.Value != "" {
		hash := sha256.Sum256([]byte(cookie.Value))
		if row, lErr := query.GetRefreshTokenByHash(db.SQL, hash[:]); lErr == nil && row != nil {
			if rErr := query.RevokeFamily(db.SQL, row.FamilyID); rErr != nil {
				log.Printf("HandleLogout: failed to revoke family for user %d: %v", user.ID, rErr)
			}
		}
	} else if rErr := query.RevokeAllForUser(db.SQL, user.ID); rErr != nil {
		log.Printf("HandleLogout: failed to revoke tokens for user %d: %v", user.ID, rErr)
	}

	clearTokenCookies(w)
	responder.New(w, nil, "logged out")
}
