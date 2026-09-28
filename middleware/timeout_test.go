package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestRequestTimeoutCancelsSlowHandler: a handler that waits on its request
// context — as every ClickHouse call does, through r.Context() — is released
// with DeadlineExceeded when the timeout elapses, not when the work finishes.
// The timeout is injected at 50ms; production mounts REQUEST_TIMEOUT (25s).
func TestRequestTimeoutCancelsSlowHandler(t *testing.T) {
	const timeout = 50 * time.Millisecond

	var (
		ctxErr  error
		elapsed time.Duration
	)
	h := RequestTimeout(timeout)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		select {
		case <-r.Context().Done():
			ctxErr = r.Context().Err()
		case <-time.After(5 * time.Second):
		}
		elapsed = time.Since(start)
		w.WriteHeader(http.StatusGatewayTimeout)
	}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/data/keys", nil))

	if !errors.Is(ctxErr, context.DeadlineExceeded) {
		t.Fatalf("handler context error = %v, want context.DeadlineExceeded", ctxErr)
	}
	if elapsed < timeout || elapsed > 2*time.Second {
		t.Errorf("handler was released after %v, want ~%v", elapsed, timeout)
	}
}

// TestRequestTimeoutSetsDeadlineOnOrdinaryRoutes pins the deadline's size: a
// fast handler sees a deadline no further out than the configured timeout.
func TestRequestTimeoutSetsDeadlineOnOrdinaryRoutes(t *testing.T) {
	var (
		deadline time.Time
		ok       bool
	)
	h := RequestTimeout(REQUEST_TIMEOUT)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		deadline, ok = r.Context().Deadline()
	}))

	before := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/analytics", nil))

	if !ok {
		t.Fatal("no deadline on a /v1 request context")
	}
	if d := deadline.Sub(before); d <= 0 || d > REQUEST_TIMEOUT+time.Second {
		t.Errorf("deadline is %v out, want ~%v", d, REQUEST_TIMEOUT)
	}
}

// TestRequestTimeoutSkipsStreams: the SSE routes must keep a context that ends
// only when the client goes away.
func TestRequestTimeoutSkipsStreams(t *testing.T) {
	for _, path := range []string{"/v1/events/stream", "/v1/alerts/stream"} {
		t.Run(path, func(t *testing.T) {
			hasDeadline := true
			h := RequestTimeout(time.Millisecond)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, hasDeadline = r.Context().Deadline()
			}))
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
			if hasDeadline {
				t.Errorf("%s got a request deadline; streams must be skipped", path)
			}
		})
	}
}
