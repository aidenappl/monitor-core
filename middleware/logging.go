package middleware

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type contextKey string

const (
	RequestIDKey contextKey = "request-id"
	ClientIPKey  contextKey = "client-ip"
)

// INGEST_PATH is POST /v1/events. Its successful requests emit NO request
// event — see LoggingMiddleware.
const INGEST_PATH = "/v1/events"

// probePaths are polled every few seconds by Docker, the load balancer and fleet
// drift checks. An event per poll would bury real traffic.
var probePaths = map[string]bool{
	"/health":      true,
	"/ready":       true,
	"/healthcheck": true,
	"/version":     true,
}

// MAX_USER_AGENT bounds the user agent on a request event.
const MAX_USER_AGENT = 200

func GetClientIP(r *http.Request) string {
	if cfip := r.Header.Get("CF-Connecting-IP"); cfip != "" {
		return cfip
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx != -1 {
			if ip := strings.TrimSpace(xff[:idx]); ip != "" {
				return ip
			}
		} else if ip := strings.TrimSpace(xff); ip != "" {
			return ip
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

func GetClientIPFromContext(ctx context.Context) string {
	if ip, ok := ctx.Value(ClientIPKey).(string); ok {
		return ip
	}
	return "unknown"
}

func GetRequestID(ctx context.Context) string {
	if requestID, ok := ctx.Value(RequestIDKey).(string); ok {
		return requestID
	}
	return "unknown"
}

func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.New().String()
		clientIP := GetClientIP(r)

		ctx := context.WithValue(r.Context(), RequestIDKey, requestID)
		ctx = context.WithValue(ctx, ClientIPKey, clientIP)
		// The same id goes on every event the request produces, so the request
		// event and any standalone event from inside it are joined by one value.
		ctx = telemetry.WithRequestID(ctx, requestID)

		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// failure is why a request failed, as the responder (or a handler that writes
// its own error) reported it.
type failure struct {
	message string
	err     error
	code    int
	stack   string
	fields  map[string]any

	// reported is set when the failure already has its own error event (a
	// panic.recovered). The request event then stays a warning, so one failure
	// files one issue.
	reported bool
}

// statusResponseWriter captures what the request event needs: the status, the
// size, and — through RecordFailure — the cause, which the status alone never
// carries.
type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
	bytes      int
	failure    *failure
	userID     string
	fields     map[string]any

	// expected marks a 4xx as routine (an expired access token on its way to a
	// refresh) or as already reported by a domain event (auth.login.failed). The
	// request event is then info rather than a second warning.
	expected bool
}

func (rw *statusResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *statusResponseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += n
	return n, err
}

// Flush forwards to the embedded ResponseWriter if it supports flushing.
// Required for SSE (Server-Sent Events) streaming to work through this wrapper.
func (rw *statusResponseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack delegates to the embedded http.Hijacker (for WebSocket upgrades, etc.).
func (rw *statusResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not support Hijack")
}

// Unwrap returns the embedded ResponseWriter so http.ResponseController can reach
// through the wrapper (e.g. to set/clear write deadlines on SSE connections).
func (rw *statusResponseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// RecordFailure keeps the reason a request failed so it lands on the request's
// one event, next to the request id, route and caller it belongs to.
// responder.writeError calls it through an interface, so responder stays a leaf.
//
// A 5xx also keeps the stack as it stands here — inside the handler that
// decided to fail, which is the frame a reader needs.
func (rw *statusResponseWriter) RecordFailure(status int, message string, err error, code int, fields map[string]any) {
	f := &failure{message: message, err: err, code: code, fields: fields}
	if status >= http.StatusInternalServerError {
		f.stack = telemetry.Stack()
	}
	rw.failure = f
}

// Annotate adds fields to the request's event.
func (rw *statusResponseWriter) Annotate(fields map[string]any) {
	if rw.fields == nil {
		rw.fields = make(map[string]any, len(fields))
	}
	for k, v := range fields {
		rw.fields[k] = v
	}
}

// findStatusWriter locates this package's writer under any wrappers.
func findStatusWriter(w http.ResponseWriter) *statusResponseWriter {
	for w != nil {
		if rw, ok := w.(*statusResponseWriter); ok {
			return rw
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		w = u.Unwrap()
	}
	return nil
}

// SetUser attributes the request's event to an authenticated user. Call it only
// once authentication has SUCCEEDED: an identity read from an unverified
// credential would let a caller write any name into the record meant to catch
// them.
func SetUser(w http.ResponseWriter, userID int64) {
	if rw := findStatusWriter(w); rw != nil {
		rw.userID = strconv.FormatInt(userID, 10)
	}
}

// Annotate adds fields to the request's event — the project a read was scoped
// to, the credential kind that authorised it. Two to six identifying fields;
// never a body.
func Annotate(w http.ResponseWriter, fields map[string]any) {
	if rw := findStatusWriter(w); rw != nil {
		rw.Annotate(fields)
	}
}

// ExpectedClientError marks the 4xx this request is about to return as routine,
// or as already reported by a domain event, so its request event is info rather
// than a warning. It never lowers a 5xx.
func ExpectedClientError(w http.ResponseWriter) {
	if rw := findStatusWriter(w); rw != nil {
		rw.expected = true
	}
}

// RecordFailure is the responder's hook for handlers that write their own error
// response (http.Error, a plain-text body). Call it before writing.
func RecordFailure(w http.ResponseWriter, status int, message string, err error, fields map[string]any) {
	if rw := findStatusWriter(w); rw != nil {
		rw.RecordFailure(status, message, err, status, fields)
	}
}

// LoggingMiddleware emits ONE http.request.end event per request: 5xx is an
// error (an issue, grouped by route and cause), 4xx a warning, everything else
// info. The cause arrives through RecordFailure, so a failing handler does not
// report its own 5xx — that would file two issues for one failure.
//
// ⚠️ A SUCCESSFUL POST /v1/events EMITS NOTHING. appleby-core ships its own
// telemetry to its own ingest endpoint: a request event per ingest batch would
// be shipped in the next batch, whose request would produce another event —
// forever, one loop per flush interval. Ingest's failures are reported by the
// ingest path itself, coalesced (routes.IngestEventsHandler,
// IngestAuthMiddleware); healthy ingest shows up as a periodic summary. A 5xx
// there is still reported here, because nothing else would.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probePaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		srw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(srw, r)

		if r.Method == http.MethodPost && r.URL.Path == INGEST_PATH && srw.statusCode < http.StatusInternalServerError {
			return
		}
		emitRequest(r, srw, time.Since(start))
	})
}

func emitRequest(r *http.Request, srw *statusResponseWriter, duration time.Duration) {
	userAgent := r.UserAgent()
	if len(userAgent) > MAX_USER_AGENT {
		userAgent = userAgent[:MAX_USER_AGENT]
	}
	data := map[string]any{
		"method":         r.Method,
		"path":           routeTemplate(r),
		"request_path":   r.URL.Path,
		"status_code":    srw.statusCode,
		"duration_ms":    duration.Milliseconds(),
		"response_bytes": srw.bytes,
		"client_ip":      GetClientIPFromContext(r.Context()),
		"user_agent":     userAgent,
	}
	for k, v := range srw.fields {
		if _, core := data[k]; !core {
			data[k] = v
		}
	}

	level := "info"
	switch {
	case srw.statusCode >= http.StatusInternalServerError:
		level = "error"
		data["outcome"] = fmt.Sprintf("returned %d", srw.statusCode)
	case srw.statusCode >= http.StatusBadRequest && !srw.expected:
		level = "warn"
	}

	if f := srw.failure; f != nil {
		data["error_message"] = f.message
		if f.code != 0 {
			data["error_code"] = f.code
		}
		if f.err != nil {
			telemetry.AddError(data, f.err)
		}
		if f.stack != "" {
			data["stack_trace"] = f.stack
		}
		for k, v := range f.fields {
			if _, set := data[k]; !set {
				data[k] = v
			}
		}
		if f.reported && level == "error" {
			level = "warn"
			data["reported_as"] = "panic.recovered"
		}
	}

	ctx := r.Context()
	if srw.userID != "" {
		ctx = telemetry.WithUserID(ctx, srw.userID)
	}
	telemetry.At(ctx, level, "http.request.end", data)
}

// routeTemplate is the matched route pattern ("/v1/issues/{id}"). Monitor groups
// issues by it; the raw path would split one failing endpoint into an issue per
// id.
func routeTemplate(r *http.Request) string {
	if route := mux.CurrentRoute(r); route != nil {
		if t, err := route.GetPathTemplate(); err == nil {
			return t
		}
	}
	return r.URL.Path
}

// RecoverMiddleware turns a panicking handler into a 500 and a panic.recovered
// event instead of a dropped connection with nothing recorded. Registered inside
// LoggingMiddleware, so the request's own event still fires with the 500.
func RecoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler {
				// net/http's deliberate abort, not a bug.
				panic(rec)
			}

			telemetry.ReportPanic(r.Context(), "http "+r.Method+" "+routeTemplate(r), rec, "returned 500")
			if rw := findStatusWriter(w); rw != nil {
				rw.RecordFailure(http.StatusInternalServerError, "internal server error", fmt.Errorf("panic: %v", rec), http.StatusInternalServerError, nil)
				rw.failure.reported = true
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"success":false,"message":"internal server error","data":null,"error":"Internal Server Error","error_message":"internal server error","error_code":500}` + "\n"))
		}()
		next.ServeHTTP(w, r)
	})
}
