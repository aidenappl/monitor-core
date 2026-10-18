package routes

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
)

// issueSession starts a brand-new Monitor session for userID: it mints the
// access + refresh JWTs and writes the three session cookies (via
// setTokenCookies), then persists the SHA-256 hash of the raw refresh token as
// the head of a fresh rotation family. The raw refresh token never touches the
// database — only its hash — so a DB leak cannot resurrect a session.
//
// This is the shared entry point for every flow that begins a session from
// scratch — SSO callback (Phase 3) and native login/register (Phase 3.5). The
// rotating-refresh flow (HandleRefresh) does NOT use this; it mints successors
// inside its own rotation transaction.
//
// authTime is when the user actually authenticated (jwt.Claims.AuthTime): now
// for a password login, the IdP's auth_time for an SSO callback, and zero when
// the new family is not backed by a sign-in at all (a family re-minted from a
// live SSO session) — zero is never "recent", so it cannot pass a step-up.
func issueSession(w http.ResponseWriter, userID int64, role string, authTime time.Time) error {
	// The family is generated FIRST so the access token can carry it (fid) —
	// that is what lets logout revoke this session rather than all of them.
	family := make([]byte, 16)
	if _, err := rand.Read(family); err != nil {
		return err
	}

	rawRefresh, refreshExpiry, err := setTokenCookies(w, userID, role, hex.EncodeToString(family), authTime)
	if err != nil {
		return err
	}

	hash := sha256.Sum256([]byte(rawRefresh))

	if _, err := query.CreateRefreshToken(db.SQL, query.CreateRefreshTokenRequest{
		UserID:    userID,
		TokenHash: hash[:],
		FamilyID:  family,
		ExpiresAt: refreshExpiry,
	}); err != nil {
		return err
	}
	return nil
}
