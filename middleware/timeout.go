package middleware

import (
	"context"
	"net/http"
	"time"
)

// REQUEST_TIMEOUT bounds the context of every non-streaming /v1 request.
//
// It sits under the server's WriteTimeout (30s, main.go) on purpose. Past 30s
// net/http has already given up on the response, but nothing told the handler:
// a ClickHouse query started from r.Context() kept running — and kept its
// connection from the shared pool — for as long as ClickHouse took, with nobody
// left to read the result. Cancelling at 25s ends that query server-side and
// leaves ~5s for the handler to write its error envelope before the connection's
// write deadline.
//
// It is a context deadline, NOT http.TimeoutHandler. TimeoutHandler buffers the
// whole response and hides http.Flusher, which would break both SSE streams, and
// it writes its own 503 body in place of the responder envelope.
const REQUEST_TIMEOUT = 25 * time.Second

// RequestTimeout returns middleware that derives each request's context with a
// deadline of d. router.go mounts it on the /v1 subrouter with REQUEST_TIMEOUT;
// the duration is a parameter so tests can prove the cancellation without
// waiting 25 seconds.
//
// The two SSE streams are skipped (isStreamPath): their whole point is to stay
// open, and they already end on client disconnect via r.Context().
//
// POST /v1/events (ingest) never reaches this — it is on the root router.
func RequestTimeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isStreamPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
