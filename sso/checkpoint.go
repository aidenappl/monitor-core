package sso

import (
	"log"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/tools"
)

// checkpointTTL is how long a checkpoint result is trusted before the IdP grant
// is re-introspected. Short enough to catch a revocation quickly, long enough to
// keep the IdP off the per-request hot path.
const checkpointTTL = 5 * time.Minute

// checkpointSSOGrant is the middleware.SSOCheckpoint hook. For an SSO-backed
// session it periodically re-checks that the upstream IdP grant is still live:
//
//   - No sso_sessions row → the session is native (password), not SSO-backed →
//     pass (true).
//   - Checked within checkpointTTL → trust the cached result → pass.
//   - Otherwise introspect the IdP with the cached (decrypted) refresh token:
//     active=false → delete the session and FAIL (false), killing the local
//     session; a network/DB/config error → FAIL-OPEN (true) so an IdP blip does
//     not log everyone out.
func checkpointSSOGrant(userID int64) bool {
	sess, err := query.GetSSOSession(db.SQL, userID)
	if err != nil {
		log.Printf("sso: checkpoint: load session for user %d failed, failing open: %v", userID, err)
		return true
	}
	if sess == nil {
		// Not an SSO-backed session — nothing to checkpoint.
		return true
	}

	if time.Since(sess.LastCheckedAt) < checkpointTTL {
		return true
	}

	provider, err := LoadProvider(db.SQL, sess.Provider)
	if err != nil {
		log.Printf("sso: checkpoint: load provider %q failed, failing open: %v", sess.Provider, err)
		return true
	}
	if provider.IntrospectURL == nil || *provider.IntrospectURL == "" {
		// Provider cannot be introspected — nothing to check, just reset the TTL.
		_ = query.TouchSSOSession(db.SQL, userID)
		return true
	}

	// Prefer the refresh token (long-lived) for introspection; fall back to the
	// access token. Both are stored AES-256-GCM encrypted.
	encToken := sess.AccessToken
	hint := "access_token"
	if sess.RefreshToken != nil && *sess.RefreshToken != "" {
		encToken = *sess.RefreshToken
		hint = "refresh_token"
	}
	token, err := tools.Decrypt(encToken)
	if err != nil {
		log.Printf("sso: checkpoint: decrypt token for user %d failed, failing open: %v", userID, err)
		return true
	}

	resp, err := Introspect(provider, token, hint)
	if err != nil {
		log.Printf("sso: checkpoint: introspect for user %d failed, failing open: %v", userID, err)
		return true
	}
	if !resp.Active {
		log.Printf("sso: checkpoint: upstream grant revoked for user %d — killing session", userID)
		_ = query.DeleteSSOSession(db.SQL, userID)
		return false
	}

	_ = query.TouchSSOSession(db.SQL, userID)
	return true
}

// Install wires checkpointSSOGrant into the session middleware. Phase 3B calls
// this from main.go once the SSO subsystem is mounted; until then the hook stays
// nil and SessionMiddleware skips the checkpoint entirely.
func Install() {
	middleware.SSOCheckpoint = checkpointSSOGrant
}
