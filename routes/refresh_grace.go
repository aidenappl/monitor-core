package routes

import (
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

// refreshGraceWindow is how long after a refresh token is spent a SECOND
// presentation of it is still treated as the same client retrying rather than as
// a replay. Ported from forta-api (routes/refresh_rotation.router.go) — keep the
// two in step; a window that differs by service is an attacker's choice of
// service.
//
// ⚠️ WITHOUT THIS WINDOW, REUSE DETECTION LOGS PEOPLE OUT FOR USING THE PRODUCT.
// Two honest things present a spent token twice: a LOST RESPONSE (the server
// rotated, the reply never arrived, the client retries with the only token it
// has) and CONCURRENT REFRESH (two tabs notice an expired access token at the
// same instant and send the same cookie). Treating either as theft revokes the
// family. Thirty seconds covers both without mattering to a thief: a replay
// inside the window gets one access token the thief would have got anyway by
// replaying a second earlier, and outside it a replay still kills the family.
const refreshGraceWindow = 30 * time.Second

// withinRefreshGrace reports whether a token spent at usedAt is still inside the
// grace window at now.
//
// Two edge cases, both deliberate (and both Forta's):
//   - usedAt in the FUTURE is inside. App/DB clock skew can put used_at slightly
//     ahead of now; reading that as "long ago" would revoke families over a
//     millisecond of disagreement.
//   - Exactly at the boundary is INSIDE — it errs toward not revoking an
//     innocent session.
func withinRefreshGrace(usedAt, now time.Time) bool {
	return now.Sub(usedAt) <= refreshGraceWindow
}

// refreshOutcome is what HandleRefresh should do with a stored refresh token.
type refreshOutcome int

const (
	// refreshRotate: live, unspent token — rotate it.
	refreshRotate refreshOutcome = iota
	// refreshGrace: spent within refreshGraceWindow — issue a sibling in the same
	// family, touching no existing row.
	refreshGrace
	// refreshReuse: spent outside the window (or with no recorded spend time) —
	// revoke the family.
	refreshReuse
	// refreshRevoked: already revoked — no grace, revoke the family (idempotent).
	refreshRevoked
	// refreshExpired: past expires_at — reject.
	refreshExpired
)

func (o refreshOutcome) String() string {
	switch o {
	case refreshRotate:
		return "rotate"
	case refreshGrace:
		return "grace"
	case refreshReuse:
		return "reuse"
	case refreshRevoked:
		return "revoked"
	case refreshExpired:
		return "expired"
	}
	return "unknown"
}

// classifyRefresh decides what a presentation of row at now means. It is pure so
// the one decision that separates "benign retry" from "revoke this user's
// session" can be tested without a database.
//
// ORDER MATTERS (mirrors Forta's rotateRefreshHandle):
//   - Revoked first: a revoked family gets no grace, even if the token was spent
//     a second ago — revocation is the stronger fact.
//   - Expired next, BEFORE the spent check: an expired token is refused outright
//     and never earns a grace sibling. The trade-off, shared with Forta, is that a
//     spent-and-expired token is refused without revoking its family — it cannot
//     be traded up either way.
//   - Spent: grace only when used_at is recorded and recent. A spent row with
//     NULL used_at predates migration 134 and has no trustworthy spend time, so it
//     keeps the pre-134 behaviour: reuse.
func classifyRefresh(row *structs.RefreshToken, now time.Time) refreshOutcome {
	if row.RevokedAt != nil {
		return refreshRevoked
	}
	if now.After(row.ExpiresAt) {
		return refreshExpired
	}
	if row.ReplacedBy != nil {
		if row.UsedAt != nil && withinRefreshGrace(row.UsedAt.UTC(), now) {
			return refreshGrace
		}
		return refreshReuse
	}
	return refreshRotate
}
