package sso

import (
	"context"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/telemetry"
)

// Install wires the shared Checkpointer into the session middleware.
//
// Called from main.go once the SSO subsystem is mounted. Until then the hook stays
// nil and SessionMiddleware skips the checkpoint entirely, which is what lets the
// service run with SSO unconfigured.
func Install() {
	checkpointer := newCheckpointer(NewSessionStore(db.SQL), loadLibProvider)

	// Both hooks are installed. The middleware prefers SSOCheckpointCtx, which
	// hands the Check the request's context so the introspection to the IdP
	// carries this request's X-Request-ID. SSOCheckpoint stays wired for any
	// caller that has no context to give; it decides identically.
	middleware.SSOCheckpointCtx = checkpointDecision(checkpointer)
	middleware.SSOCheckpoint = func(userID int64) bool {
		return middleware.SSOCheckpointCtx(context.Background(), userID)
	}
}

// newCheckpointer builds the library Checkpointer with monitor-core's
// correlation and logging. Split out of Install so tests can drive it against a
// fake session store and an httptest introspection endpoint.
func newCheckpointer(sessions ssolib.SessionStore, providers func(context.Context, string) (*ssolib.Provider, error)) *ssolib.Checkpointer {
	return &ssolib.Checkpointer{
		Sessions:  sessions,
		Providers: providers,
		// Correlation forwards monitor-core's own request id (set by
		// middleware.RequestIDMiddleware) as X-Request-ID on the introspection
		// call, so forta-api's log line for a checkpoint names the Monitor request
		// that caused it. go-forta drops any id that is not a UUID or 8-64 hex.
		Correlation: requestCorrelation,
		// LogfCtx replaces Logf so each checkpoint line carries the request id.
		// It takes precedence over Logf in the library, so only this one is set.
		LogfCtx: libraryLogfCtx,
		// Interval and Grace are left at the library defaults — 5 minutes and 30
		// minutes. Overriding them here would be a policy decision made in the wrong
		// place: the reasoning for both numbers, and for why neither fail-open nor
		// fail-closed is acceptable, lives with the constants.
	}
}

// requestCorrelation reads monitor-core's request id off the context. There is
// no trace id in this service, so the second value is always empty.
//
// It reads the raw context value rather than middleware.GetRequestID, which
// substitutes "unknown" for a missing id — a placeholder that must never be
// forwarded as though it identified a request.
func requestCorrelation(ctx context.Context) (string, string) {
	rid, _ := ctx.Value(middleware.RequestIDKey).(string)
	return rid, ""
}

// checkpointDecision maps the library's three-way result onto the middleware's
// bool hook.
func checkpointDecision(checkpointer *ssolib.Checkpointer) func(ctx context.Context, userID int64) bool {
	return func(ctx context.Context, userID int64) bool {
		switch checkpointer.Check(ctx, userID) {
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
			telemetry.WarnCoalesced(ctx, "sso.checkpoint.denied", "sso.checkpoint.denied", nil, map[string]any{
				"user_id":    userID,
				"reason":     "idp_unreachable_past_grace",
				"dependency": "idp",
				"outcome":    "session refused with a 401 (should be a 503; the middleware hook cannot express it)",
			})
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
