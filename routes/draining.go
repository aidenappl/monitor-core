package routes

import "sync"

// ⚠️ SSE STREAMS USED TO HOLD SHUTDOWN OPEN FOR ITS FULL GRACE PERIOD.
//
// `http.Server.Shutdown` waits for active requests to finish, and an SSE handler
// does not finish — it is a loop that ends when its client goes away. So one
// open live-tail tab held every deploy for the whole 10s shutdown timeout, then
// `http.shutdown.failed` (`shutdown_timeout`) was reported and the connections
// were cut anyway. With the 2s batcher drain and the telemetry flush after it,
// a stop could run past Docker's default 10s stop grace and be SIGKILLed —
// which loses exactly the shutdown events this telemetry exists to deliver.
//
// The fix is for streams, and ONLY streams, to end themselves when shutdown
// starts. `main` wires StartDraining to `server.RegisterOnShutdown`, which the
// server calls as Shutdown begins.
//
// ⚠️ DO NOT "SIMPLIFY" THIS INTO CANCELLING THE SERVER'S BASE CONTEXT. That
// would reach every in-flight request, cancelling an ordinary query mid-flight
// and turning a graceful drain into an abrupt one — the opposite of the point.
// Streams are the only requests that never end on their own, so they are the
// only ones that need telling.
// Guarded by a mutex and a non-blocking receive rather than a sync.Once, so a
// test can swap the channel for a fresh one without copying a lock.
var (
	drainingMu sync.Mutex
	drainingCh = make(chan struct{})
)

// StartDraining tells every open stream to close. Idempotent, and safe to call
// from the server's shutdown hook.
func StartDraining() {
	drainingMu.Lock()
	defer drainingMu.Unlock()
	select {
	case <-drainingCh: // already draining
	default:
		close(drainingCh)
	}
}

// Draining is closed once shutdown has begun. A long-lived handler selects on it
// alongside the request's own context.
func Draining() <-chan struct{} {
	drainingMu.Lock()
	defer drainingMu.Unlock()
	return drainingCh
}
