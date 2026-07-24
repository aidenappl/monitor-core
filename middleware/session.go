package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
)

// This file implements Monitor's OWN session auth (Phase 2), independent of
// Forta. It deliberately does NOT touch the existing query_auth.go / ingest_auth.go
// middleware — Phase 3 rewires those to consume this. Until then both coexist.

// UserContextKey is where SessionMiddleware stashes the authenticated
// *structs.User. contextKey is declared in logging.go (same package).
const UserContextKey contextKey = "mon-user"

// SSOCheckpoint is an injectable hook for periodic SSO-grant revocation checks.
// It is nil in Phase 2 (checkpoint skipped). Phase 3.4 assigns it to
// checkpointSSOGrant, which — for users whose session is backed by an SSO
// identity — introspects the IdP on a TTL and returns false when the upstream
// grant has been revoked (fail-open on network errors). When the hook returns
// false the request is rejected, killing the local session. A nil hook always
// passes. The hook itself decides whether a given user is SSO-backed; the
// middleware simply consults it for every JWT-authenticated user.
var SSOCheckpoint func(userID int64) bool

// GetUserFromContext returns the user injected by SessionMiddleware.
func GetUserFromContext(ctx context.Context) (*structs.User, bool) {
	user, ok := ctx.Value(UserContextKey).(*structs.User)
	return user, ok
}

// GetUserID extracts the authenticated user's id from context.
func GetUserID(ctx context.Context) (int64, bool) {
	user, ok := GetUserFromContext(ctx)
	if !ok || user == nil {
		return 0, false
	}
	return user.ID, true
}

// SessionMiddleware authenticates a request from either:
//  1. Authorization: Bearer <access-jwt>
//  2. the mon-access-token cookie
//
// It validates the access JWT (HS512, issuer-pinned), loads the user, requires
// user.Active, optionally runs the SSO revocation checkpoint, and stashes the
// user in context. Requests that fail any step get a 401.
func SessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token := extractBearerToken(r); token != "" {
			if user := validateSessionToken(token); user != nil {
				next.ServeHTTP(w, r.WithContext(withUser(r.Context(), user)))
				return
			}
		}

		if cookie, err := r.Cookie("mon-access-token"); err == nil && cookie.Value != "" {
			if user := validateSessionToken(cookie.Value); user != nil {
				next.ServeHTTP(w, r.WithContext(withUser(r.Context(), user)))
				return
			}
		}

		responder.Error(w, http.StatusUnauthorized, "authentication required")
	})
}

func withUser(ctx context.Context, user *structs.User) context.Context {
	return context.WithValue(ctx, UserContextKey, user)
}

// validateSessionToken resolves an access JWT to an active user, applying the
// SSO revocation checkpoint when the hook is installed.
func validateSessionToken(tokenStr string) *structs.User {
	userID, err := jwt.ValidateAccessToken(tokenStr)
	if err != nil {
		return nil
	}

	user, err := query.GetUserByID(db.SQL, userID)
	if err != nil || user == nil || !user.Active {
		return nil
	}

	if SSOCheckpoint != nil && !SSOCheckpoint(userID) {
		return nil
	}

	return user
}

// Protected wraps a single HandlerFunc with SessionMiddleware.
func Protected(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		SessionMiddleware(http.HandlerFunc(next)).ServeHTTP(w, r)
	}
}

// RejectPending blocks role=="pending" users (freshly SSO-provisioned accounts
// awaiting admin approval) from protected routes.
func RejectPending(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUserFromContext(r.Context())
		if ok && user != nil && user.Role == "pending" {
			// error_code 4004 → the web client redirects to /pending.
			responder.ErrorWithCode(w, http.StatusForbidden, "your account is pending admin approval", 4004)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireAdmin requires role=="admin". Must run after SessionMiddleware.
func RequireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUserFromContext(r.Context())
		if !ok || user == nil {
			responder.Error(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if user.Role != "admin" {
			// error_code 4003 → the web client redirects to /unauthorized.
			responder.ErrorWithCode(w, http.StatusForbidden, "admin access required", 4003)
			return
		}
		next(w, r)
	}
}

// RequireEditor requires role=="admin" or "editor". Must run after SessionMiddleware.
func RequireEditor(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := GetUserFromContext(r.Context())
		if !ok || user == nil {
			responder.Error(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if user.Role != "admin" && user.Role != "editor" {
			// error_code 4003 → the web client redirects to /unauthorized.
			responder.ErrorWithCode(w, http.StatusForbidden, "editor access required", 4003)
			return
		}
		next(w, r)
	}
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return parts[1]
}
