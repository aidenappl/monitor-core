package routes

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
)

// HandleRefresh implements the rotating-refresh-token flow with reuse detection,
// per the OAuth 2.0 Security BCP (draft-ietf-oauth-security-topics §4.14):
//
//  1. Validate the refresh JWT (signature/exp/type/issuer).
//  2. Look up the presented token by its SHA-256 hash.
//  3. If the row is missing → reject (unknown token).
//  4. Classify the row (classifyRefresh, routes/refresh_grace.go):
//     - revoked, or spent OUTSIDE the grace window → REUSE DETECTED. A spent
//     token presented long after rotation means it leaked; revoke the ENTIRE
//     family and reject, so a stolen token can never be traded up.
//     - expired → reject.
//     - spent WITHIN the grace window → a concurrent tab or a retried lost
//     response, not theft. Issue a sibling in the same family (see
//     issueGraceSibling) instead of logging the user out.
//     - otherwise rotate: mint a new access + refresh token, insert the
//     successor in the same family, stamp the old row replaced_by + used_at,
//     and set fresh cookies.
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

	now := time.Now().UTC()
	outcome := classifyRefresh(row, now)
	switch outcome {
	case refreshRevoked, refreshReuse:
		rejectRefreshReuse(w, row, outcome)
		return
	case refreshExpired:
		responder.Error(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	user, err := query.GetUserByID(db.SQL, userID)
	if err != nil || user == nil || !user.Active {
		responder.Error(w, http.StatusUnauthorized, "user not found or inactive")
		return
	}

	minted, err := mintSessionTokens(user, row.FamilyID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to mint tokens", err)
		return
	}

	if outcome == refreshGrace {
		issueGraceSibling(w, r, row, user, minted)
		return
	}

	// Rotate atomically: insert the successor + stamp the old row replaced_by
	// and used_at. The stamp is conditional on replaced_by IS NULL, so exactly
	// one of two concurrent rotations wins.
	tx, err := db.SQL.Begin()
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to begin rotation", err)
		return
	}
	ua := r.UserAgent()
	if _, err := query.RotateRefreshToken(tx, row.ID, now, query.CreateRefreshTokenRequest{
		UserID:    user.ID,
		TokenHash: minted.refreshHash[:],
		FamilyID:  row.FamilyID,
		ExpiresAt: minted.refreshExp,
		UserAgent: &ua,
	}); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, query.ErrTokenAlreadyRotated) {
			// A concurrent rotation spent this token between our read and our
			// UPDATE. The row we hold is stale, so re-read and re-classify rather
			// than guess: a sibling tab that won the race a moment ago is grace,
			// not theft.
			fresh, rerr := query.GetRefreshTokenByHash(db.SQL, hash[:])
			if rerr != nil {
				responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up refresh token", rerr)
				return
			}
			if fresh == nil {
				responder.Error(w, http.StatusUnauthorized, "unknown refresh token")
				return
			}
			switch raced := classifyRefresh(fresh, time.Now().UTC()); raced {
			case refreshGrace:
				issueGraceSibling(w, r, fresh, user, minted)
			case refreshExpired:
				responder.Error(w, http.StatusUnauthorized, "refresh token expired")
			default:
				rejectRefreshReuse(w, fresh, raced)
			}
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to rotate refresh token", err)
		return
	}
	if err := tx.Commit(); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to commit rotation", err)
		return
	}

	writeAuthCookies(w, minted.access, minted.refresh, minted.accessExp, minted.refreshExp)
	responder.New(w, user, "token refreshed")
}

// sessionTokens is a freshly minted access + refresh pair. The refresh token's
// hash is what gets persisted; the raw value only ever reaches the client via
// the cookie. The access token carries the family (fid) so logout can be scoped
// to this session — rotation and grace siblings stay in the parent's family, so
// they carry the parent's.
type sessionTokens struct {
	access      string
	accessExp   time.Time
	refresh     string
	refreshExp  time.Time
	refreshHash [sha256.Size]byte
}

func mintSessionTokens(user *structs.User, familyID []byte) (sessionTokens, error) {
	var t sessionTokens
	var err error
	if t.access, t.accessExp, err = jwt.NewAccessToken(user.ID, user.Role, hex.EncodeToString(familyID)); err != nil {
		return t, err
	}
	if t.refresh, t.refreshExp, err = jwt.NewRefreshToken(user.ID); err != nil {
		return t, err
	}
	t.refreshHash = sha256.Sum256([]byte(t.refresh))
	return t, nil
}

// issueGraceSibling answers a re-presentation of a token spent within the grace
// window. It inserts a SIBLING in the same family rather than replaying the
// previous successor — replaying would mean storing that successor in plaintext.
//
// ⚠️ It deliberately touches NO existing row: stamping replaced_by/used_at again
// would move used_at forward and slide the grace window with every retry, turning
// a 30-second allowance into an indefinite one. Revoking the family still revokes
// the sibling, because it shares family_id.
func issueGraceSibling(w http.ResponseWriter, r *http.Request, parent *structs.RefreshToken, user *structs.User, minted sessionTokens) {
	ua := r.UserAgent()
	if _, err := query.CreateRefreshToken(db.SQL, query.CreateRefreshTokenRequest{
		UserID:    user.ID,
		TokenHash: minted.refreshHash[:],
		FamilyID:  parent.FamilyID,
		ExpiresAt: minted.refreshExp,
		UserAgent: &ua,
	}); err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to issue refresh token", err)
		return
	}

	// Close the revoke-vs-sibling race. A logout or reuse detection that ran
	// RevokeFamily between our classification and the insert above revoked every
	// row it could see — but not the sibling, which did not exist yet. Re-read the
	// parent: if it is now revoked, the family is dead, so revoke again (which
	// catches the sibling) and reject rather than hand out a live session.
	current, err := query.GetRefreshTokenByHash(db.SQL, parent.TokenHash)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to look up refresh token", err)
		return
	}
	if current == nil || current.RevokedAt != nil {
		rejectRefreshReuse(w, parent, refreshRevoked)
		return
	}

	log.Printf("MON_REFRESH_GRACE: refresh token re-presented within %s of rotation; issued sibling (outcome=%s user_id=%d family_id=%x)",
		refreshGraceWindow, refreshGrace, user.ID, parent.FamilyID)

	writeAuthCookies(w, minted.access, minted.refresh, minted.accessExp, minted.refreshExp)
	responder.New(w, user, "token refreshed")
}

// rejectRefreshReuse is the reuse-detection response: revoke the whole family,
// clear the client's cookies, and reject. outcome (reuse or revoked) is logged so
// an operator can tell a replayed token from a presentation after logout.
func rejectRefreshReuse(w http.ResponseWriter, row *structs.RefreshToken, outcome refreshOutcome) {
	log.Printf("MON_REFRESH_REJECT: revoking refresh family (outcome=%s user_id=%d family_id=%x)",
		outcome, row.UserID, row.FamilyID)
	if revErr := query.RevokeFamily(db.SQL, row.FamilyID); revErr != nil {
		log.Printf("HandleRefresh: failed to revoke family (outcome=%s): %v", outcome, revErr)
	}
	clearTokenCookies(w)
	responder.Error(w, http.StatusUnauthorized, "refresh token reuse detected")
}
