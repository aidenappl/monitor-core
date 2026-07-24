package routes

import (
	"crypto/sha256"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
)

// HandleRefresh implements the rotating-refresh-token flow with reuse detection,
// per the OAuth 2.0 Security BCP (draft-ietf-oauth-security-topics §4.14):
//
//  1. Validate the refresh JWT (signature/exp/type/issuer).
//  2. Look up the presented token by its SHA-256 hash.
//  3. If the row is missing → reject (unknown token).
//  4. If the row is already rotated (replaced_by set) or revoked → REUSE
//     DETECTED. A refresh token is single-use; seeing a spent one means it
//     leaked (or a client raced). Revoke the ENTIRE family and reject, so a
//     stolen token can never be traded up into a live session.
//  5. If expired → reject.
//  6. Otherwise rotate: mint a new access + refresh token, insert the successor
//     in the same family, stamp the old row replaced_by, and set fresh cookies.
//
// Public + CSRF-exempt (POST /auth/refresh).
func HandleRefresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(cookieRefreshToken)
	if err != nil || cookie.Value == "" {
		responder.Error(w, http.StatusUnauthorized, "no refresh token")
		return
	}
	raw := cookie.Value

	userID, err := jwt.ValidateRefreshToken(raw)
	if err != nil {
		responder.Error(w, http.StatusUnauthorized, "invalid refresh token")
		return
	}

	hash := sha256.Sum256([]byte(raw))
	row, err := query.GetRefreshTokenByHash(db.SQL, hash[:])
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up refresh token", err)
		return
	}
	if row == nil {
		// Validly signed but never stored (or already pruned). Treat as invalid.
		responder.Error(w, http.StatusUnauthorized, "unknown refresh token")
		return
	}

	// Reuse detection: a spent (rotated) or revoked token means the family is
	// compromised — kill the whole lineage.
	if row.ReplacedBy != nil || row.RevokedAt != nil {
		if revErr := query.RevokeFamily(db.SQL, row.FamilyID); revErr != nil {
			log.Printf("HandleRefresh: failed to revoke reused family: %v", revErr)
		}
		clearTokenCookies(w)
		responder.Error(w, http.StatusUnauthorized, "refresh token reuse detected")
		return
	}

	if time.Now().After(row.ExpiresAt) {
		responder.Error(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	user, err := query.GetUserByID(db.SQL, userID)
	if err != nil || user == nil || !user.Active {
		responder.Error(w, http.StatusUnauthorized, "user not found or inactive")
		return
	}

	// Mint the successor tokens. The refresh token's hash is persisted; the raw
	// value only ever reaches the client via the cookie.
	newAccess, accessExp, err := jwt.NewAccessToken(user.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to mint access token", err)
		return
	}
	newRefresh, refreshExp, err := jwt.NewRefreshToken(user.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to mint refresh token", err)
		return
	}
	newHash := sha256.Sum256([]byte(newRefresh))

	// Rotate atomically: insert the successor + stamp the old row replaced_by.
	tx, err := db.SQL.Begin()
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to begin rotation", err)
		return
	}
	ua := r.UserAgent()
	if _, err := query.RotateRefreshToken(tx, row.ID, query.CreateRefreshTokenRequest{
		UserID:    user.ID,
		TokenHash: newHash[:],
		FamilyID:  row.FamilyID,
		ExpiresAt: refreshExp,
		UserAgent: &ua,
	}); err != nil {
		_ = tx.Rollback()
		// A concurrent rotation already spent this token — same signal as reuse.
		// Kill the family and clear the client's stale cookies.
		if errors.Is(err, query.ErrTokenAlreadyRotated) {
			if revErr := query.RevokeFamily(db.SQL, row.FamilyID); revErr != nil {
				log.Printf("HandleRefresh: failed to revoke raced family: %v", revErr)
			}
			clearTokenCookies(w)
			responder.Error(w, http.StatusUnauthorized, "refresh token reuse detected")
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to rotate refresh token", err)
		return
	}
	if err := tx.Commit(); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to commit rotation", err)
		return
	}

	writeAuthCookies(w, newAccess, newRefresh, accessExp, refreshExp)
	responder.New(w, user, "token refreshed")
}
