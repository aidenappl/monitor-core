package middleware

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
)

// This file implements Monitor's own session auth: Monitor-issued JWTs carried
// in mon-* cookies (or a Bearer access token). query_auth.go consumes the same
// validation path for dashboard/query requests; ingest_auth.go stays API-key only.

// UserContextKey is where SessionMiddleware stashes the authenticated
// *structs.User. contextKey is declared in logging.go (same package).
const UserContextKey contextKey = "mon-user"

// ActorContextKey is where the auth middleware stashes the resolved
// *structs.Actor — the principal behind the request, whether that is a session
// user, an API key, or Monitor itself.
const ActorContextKey contextKey = "mon-actor"

// EnvMasterKeyLabel is the actor label for requests authenticated by the
// env-based master key (env.IngestKey). It has no database row and therefore no
// name of its own, so audit rows attribute it to this fixed identifier.
const EnvMasterKeyLabel = "env-master-key"

// matchesEnvMasterKey reports whether a presented X-Api-Key is the env-based
// master key. Both auth middlewares route through here so the two comparisons
// cannot drift apart, and so the constant-time property holds on both: `==` on
// strings returns at the first differing byte, which leaks the shared prefix to
// anyone who can time the response and is exactly the compare
// subtle.ConstantTimeCompare exists to replace (csrf.go uses it for the same
// reason).
//
// The two emptiness guards are load-bearing, not defensive noise. With
// MONITOR_API_KEY unset, ConstantTimeCompare("", "") returns 1 — so without
// them every request arriving with no X-Api-Key header would authenticate as
// the master key.
func matchesEnvMasterKey(presented string) bool {
	if env.IngestKey == "" || presented == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(env.IngestKey)) == 1
}

// WithActor returns a context carrying the resolved actor.
func WithActor(ctx context.Context, actor *structs.Actor) context.Context {
	return context.WithValue(ctx, ActorContextKey, actor)
}

// GetActor returns the actor resolved by the auth middleware.
//
// Handlers that record who did something should use this rather than
// GetUserFromContext: an API-key caller (monitor-mcp, CI) has no user, and
// GetUserFromContext returns nothing for them.
func GetActor(ctx context.Context) (*structs.Actor, bool) {
	actor, ok := ctx.Value(ActorContextKey).(*structs.Actor)
	if !ok || actor == nil {
		return nil, false
	}
	return actor, true
}

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

// withUser stashes the authenticated user and, in the same step, the actor
// derived from them. The two are set together deliberately: a caller that has a
// user but no actor would silently produce unattributed audit rows.
func withUser(ctx context.Context, user *structs.User) context.Context {
	ctx = context.WithValue(ctx, UserContextKey, user)
	return WithActor(ctx, structs.UserActor(user))
}

// validateSessionToken resolves an access JWT to an active user, applying the
// SSO revocation checkpoint when the hook is installed.
//
// ⚠️ TWO PATHS, CHOSEN BY ROLE, because only one of them has a users table.
//
// A CONTROL PLANE owns identity: it reads the live row, requires Active, and
// runs the SSO checkpoint. Nothing about that changes.
//
// A ZONE (MON_ROLE=zone) has its own MariaDB with no users in it — bootstrap
// seeds an admin only on the control plane (main.go), by design. Its
// GetUserByID therefore returns nil for a perfectly valid session, and the zone
// 401s every request. The browser then refreshes (which succeeds, at the control
// plane), retries, 401s again, and bounces through /login in a loop — the
// symptom is a page that never loads rather than an error that says why.
//
// So a zone trusts the ACCESS TOKEN as the identity assertion. The signature is
// the check: both planes share MON_JWT_SIGNING_KEY, the parser is pinned to
// HS512 and issuer-pinned, and only the control plane can mint. The zone is not
// deciding who the user is — it is reading a statement the control plane signed.
//
// WHAT THIS COSTS, stated plainly rather than discovered later:
//   - No Active check at the zone. A disabled account keeps reading zone data
//     until its access token expires — at most 15 minutes (jwt.accessTokenExpiry).
//   - No SSO revocation checkpoint at the zone, bounded the same way.
//   - The role is a snapshot from mint time, so a demotion lags by the same 15
//     minutes at the zone and not at all at the control plane.
//
// Login, refresh and revocation all remain control-plane-only, so this opens no
// new way in; it lets an already-issued token outlive its revocation, briefly.
// Closing that gap means the control plane doing the fan-out itself, which is
// the larger design this defers to.
//
// Email and Name are deliberately left empty: the token does not carry them, and
// inventing a value would put a fabricated identity into audit rows. Nothing a
// zone serves reads them — /auth/self is a control-plane route.
func validateSessionToken(tokenStr string) *structs.User {
	userID, role, err := jwt.ValidateAccessToken(tokenStr)
	if err != nil {
		return nil
	}

	if !env.MonRole.RunsControlPlane() {
		// A token minted before Claims.Role existed carries no role. Fail CLOSED:
		// reads work, RequireEditor and RequireAdmin refuse. The next refresh
		// mints a token that has it, so a live session heals inside one access
		// lifetime without anyone being logged out.
		return &structs.User{ID: userID, Role: role, Active: true}
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
