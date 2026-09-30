package routes

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/jwt"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
	gojwt "github.com/golang-jwt/jwt/v5"
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
//
// TELEMETRY: a session that simply ended — no cookie, an expired token — is
// routine and reported only by the (info) request event. A token that is
// forged, unknown, reused or belongs to a disabled account is a warning
// (auth.refresh.failed), and a family that could not be revoked after reuse is
// an error: the leaked token's siblings are still live.
func HandleRefresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(cookieRefreshToken)
	if err != nil || cookie.Value == "" {
		middleware.ExpectedClientError(w)
		responder.Error(w, http.StatusUnauthorized, "no refresh token")
		return
	}
	raw := cookie.Value

	userID, err := jwt.ValidateRefreshToken(raw)
	if err != nil {
		if errors.Is(err, gojwt.ErrTokenExpired) {
			middleware.ExpectedClientError(w)
		} else {
			refreshFailed(w, r, "invalid_token", 0, err)
		}
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
		refreshFailed(w, r, "unknown_token", userID, nil)
		responder.Error(w, http.StatusUnauthorized, "unknown refresh token")
		return
	}

	now := time.Now().UTC()
	outcome := classifyRefresh(row, now)
	switch outcome {
	case refreshRevoked, refreshReuse:
		rejectRefreshReuse(w, r, row, outcome)
		return
	case refreshExpired:
		// A refresh token reaching its own expiry is a session ending normally.
		middleware.ExpectedClientError(w)
		responder.Error(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	user, err := query.GetUserByID(db.SQL, userID)
	if err != nil || user == nil || !user.Active {
		refreshFailed(w, r, "user_inactive", userID, err)
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
		// The rotation's own error is what is reported; a rollback failure after
		// it changes nothing about the outcome.
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
				middleware.ExpectedClientError(w)
				responder.Error(w, http.StatusUnauthorized, "refresh token expired")
			default:
				rejectRefreshReuse(w, r, fresh, raced)
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
		rejectRefreshReuse(w, r, parent, refreshRevoked)
		return
	}

	// Info, not a warning: this is the grace window doing its job — a concurrent
	// tab or a retried lost response, answered with a sibling instead of a
	// logout. It is worth seeing, because a rate that climbs means clients are
	// racing far more than expected.
	telemetry.Info(r.Context(), "auth.refresh.grace.sibling", map[string]any{
		"user_id":        user.ID,
		"grace_window_s": int(refreshGraceWindow / time.Second),
		"outcome":        "issued a sibling in the same family; no row was touched",
	})

	writeAuthCookies(w, minted.access, minted.refresh, minted.accessExp, minted.refreshExp)
	responder.New(w, user, "token refreshed")
}

// rejectRefreshReuse is the reuse-detection response: revoke the whole family,
// clear the client's cookies, and reject. The outcome (reuse or revoked) is the
// reported reason, so an operator can tell a replayed token from a presentation
// after logout.
func rejectRefreshReuse(w http.ResponseWriter, r *http.Request, row *structs.RefreshToken, outcome refreshOutcome) {
	refreshFailed(w, r, "reuse_detected_"+outcome.String(), row.UserID, nil)
	if revErr := query.RevokeFamily(db.SQL, row.FamilyID); revErr != nil {
		reportFamilyNotRevoked(r, row.UserID, outcome.String(), revErr)
	}
	clearTokenCookies(w)
	responder.Error(w, http.StatusUnauthorized, "refresh token reuse detected")
}

// refreshFailed reports a refused refresh that is NOT a session ending normally.
// Coalesced per client IP and reason; the request event drops to info.
func refreshFailed(w http.ResponseWriter, r *http.Request, reason string, userID int64, err error) {
	middleware.ExpectedClientError(w)
	clientIP := middleware.GetClientIPFromContext(r.Context())
	data := map[string]any{
		"reason":    reason,
		"client_ip": clientIP,
		"outcome":   "returned 401",
	}
	if userID != 0 {
		data["user_id"] = userID
	}
	telemetry.WarnCoalesced(r.Context(), "auth.refresh.failed:"+reason+":"+clientIP, "auth.refresh.failed", err, data)
}

// reportFamilyNotRevoked is the one refresh failure that is an error: reuse was
// detected, and the family it proves compromised is still valid.
func reportFamilyNotRevoked(r *http.Request, userID int64, reason string, err error) {
	telemetry.Error(r.Context(), "auth.refresh.revoke.failed", err, map[string]any{
		"user_id":    userID,
		"reason":     reason,
		"dependency": "mariadb",
		"outcome":    "the compromised refresh-token family was NOT revoked; its other tokens remain valid until they expire",
	})
}
