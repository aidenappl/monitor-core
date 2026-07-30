package sso

import (
	"context"
	"log"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
)

// Install wires the shared Checkpointer into the session middleware.
//
// Called from main.go once the SSO subsystem is mounted. Until then the hook stays
// nil and SessionMiddleware skips the checkpoint entirely, which is what lets the
// service run with SSO unconfigured.
func Install() {
	checkpointer := &ssolib.Checkpointer{
		Sessions:  NewSessionStore(db.SQL),
		Providers: loadLibProvider,
		Logf:      log.Printf,
		// Interval and Grace are left at the library defaults — 5 minutes and 30
		// minutes. Overriding them here would be a policy decision made in the wrong
		// place: the reasoning for both numbers, and for why neither fail-open nor
		// fail-closed is acceptable, lives with the constants.
	}

	middleware.SSOCheckpoint = func(userID int64) bool {
		switch checkpointer.Check(context.Background(), userID) {
		case ssolib.CheckpointRevoked:
			// Definitive: the IdP said the grant is gone. Deny.
			return false

		case ssolib.CheckpointUnavailable:
			// ⚠️ NOT THE SAME THING AS REVOKED, and Monitor currently cannot express
			// the difference.
			//
			// The correct response is HTTP 503 with Retry-After, so the client waits
			// rather than discarding its credentials and stampeding the identity
			// provider that is already down. middleware.SSOCheckpoint is a bool hook,
			// so the only choices available here are allow and deny.
			//
			// DENY is chosen: this state is only reached after the 30-minute grace
			// window has already elapsed with no answer, so allowing would be the
			// unbounded fail-open the library exists to prevent. The cost is that the
			// user sees a 401 where they should see a 503.
			//
			// Widening the hook to return a status is the right fix and belongs with
			// the middleware, not here. Until then this comment is the record of what
			// is being lost.
			log.Printf("sso: checkpoint unavailable for user %d — denying (should be 503; the middleware hook cannot express it)", userID)
			return false

		default:
			return true
		}
	}
}

// loadLibProvider adapts LoadProvider to the Checkpointer's lookup signature.
//
// The provider is re-resolved on every check rather than cached, so a rotated
// client secret or a disabled provider takes effect at the next checkpoint instead
// of at the next restart.
func loadLibProvider(_ context.Context, slug string) (*ssolib.Provider, error) {
	p, err := LoadProvider(db.SQL, slug)
	if err != nil {
		return nil, err
	}
	return p.Provider, nil
}
