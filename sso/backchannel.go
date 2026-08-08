package sso

import (
	"log"
	"net/http"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
)

// BackchannelLogoutHandler returns the OIDC Back-Channel Logout 1.0 receiver for
// one provider slug.
//
// ─────────────────────────────────────────────────────────────────────────────
// WHAT THIS BUYS, AND WHY THE CHECKPOINT IS STILL THE FLOOR
//
// The introspection checkpoint re-checks the upstream grant every five minutes,
// so without this a revocation keeps working for up to five minutes — and the
// moments that latency matters are exactly the moments it matters. This endpoint
// closes the window: Forta POSTs a signed logout_token the instant a grant is
// revoked, and the session ends on arrival.
//
// It does NOT replace the checkpoint. Back-channel logout is best-effort BY
// SPECIFICATION — the notification can be lost, the endpoint can be down, the
// sender's retries can be exhausted — so the poll remains the guarantee and this
// is the fast path. Do not relax CheckpointInterval because this exists.
//
// ⚠️ THE ENDPOINT IS UNAUTHENTICATED IN THE ORDINARY SENSE. No cookie, no bearer
// token; its only authentication is the signature on the logout token, verified
// against Forta's JWKS and addressed to this client_id. Every one of those checks
// lives in go-forta's handler — including the §2.4 rule that a logout token
// carrying a `nonce` MUST be refused, because such a token can be replayed into
// the id_token position and accepted as a fresh authentication. Do not
// reimplement any of it here.
// ─────────────────────────────────────────────────────────────────────────────
func BackchannelLogoutHandler(slug string) http.Handler {
	bcl := &ssolib.BackchannelLogout{
		// The SAME SessionStore the checkpoint uses. That is what makes it a
		// BackchannelLogoutTarget — the optional interface is implemented there, so
		// push and poll end sessions through one code path rather than two that can
		// disagree.
		Sessions:  NewSessionStore(db.SQL),
		Providers: loadLibProvider,
		Logf:      log.Printf,
	}
	return bcl.Handler(slug)
}
