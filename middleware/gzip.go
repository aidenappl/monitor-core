package middleware

import (
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// GZIP_MIN_SIZE is the smallest response body worth compressing, in bytes.
//
// Below it the gzip header and trailer (~20 bytes) plus the CPU cost buy nothing:
// labels, gauges, top-N and most error envelopes are a few hundred bytes and go
// out exactly as the handler wrote them. The bodies that do cross it are the
// ones that were worth this change — events?limit=100, dashboards, issue lists.
const GZIP_MIN_SIZE = 1024

// gzipWriterPool recycles compressors. A gzip.Writer carries hundreds of KB of
// deflate state, so allocating one per /v1 response would cost more than the
// compression saves.
var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(io.Discard) },
}

// streamPaths are the two SSE routes on the /v1 subrouter. They are matched by
// PATH, not by Content-Type, because every middleware that needs to skip them
// decides before the handler has set any header at all.
//
// ⚠️ Both GzipMiddleware and RequestTimeout skip these. A compressor on an SSE
// stream holds events in its deflate window until the next flush, and a request
// deadline would sever a stream that is meant to live for hours.
func isStreamPath(path string) bool {
	return path == "/v1/events/stream" || path == "/v1/alerts/stream"
}

// GzipMiddleware compresses large JSON responses on the /v1 subrouter.
//
// ─────────────────────────────────────────────────────────────────────────────
// MOUNTED ON THE /v1 SUBROUTER ONLY, NEVER THE ROOT ROUTER.
//
// That placement is the first of the skip rules, and the one that matters most:
// /auth/* (Set-Cookie responses and the public SSO icon, which sets its own
// Content-Length on a PNG), POST /v1/events ingest, /health, /ready, /version and
// the GitHub webhook are all on the root router, so this never sees them.
// ─────────────────────────────────────────────────────────────────────────────
//
// Within /v1, a response is compressed only when ALL of these hold:
//   - the request's Accept-Encoding allows gzip, and the method is not HEAD;
//   - the path is not one of the two SSE streams (isStreamPath);
//   - it is not POST /v1/api-keys — that body carries a freshly minted secret,
//     and a compressed secret beside attacker-influenced bytes is the BREACH
//     shape. It is small and rare, so skipping it costs nothing;
//   - the handler set Content-Type application/json (the responder always does)
//     and no Content-Encoding of its own;
//   - the status can carry a body worth compressing (not 204, 304 or 206);
//   - the body reaches GZIP_MIN_SIZE.
//
// When it compresses it deletes any Content-Length the handler set (it would
// describe the uncompressed body and truncate or hang the client) and adds
// Vary: Accept-Encoding so no cache can hand the gzip variant to a client that
// did not ask for it.
//
// The wrapper implements http.Flusher and Unwrap(). Unwrap is load-bearing:
// routes/stream.go and routes/alerts.go clear the server's 30s WriteTimeout with
// http.NewResponseController(w).SetWriteDeadline and DISCARD the error, so a
// wrapper that hid the real writer would leave the deadline armed with nothing
// to say so.
func GzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !gzipRequestEligible(r) {
			next.ServeHTTP(w, r)
			return
		}

		gw := &gzipResponseWriter{ResponseWriter: w}
		next.ServeHTTP(gw, r)
		// Deliberately not deferred: if the handler panics, net/http aborts the
		// connection, and finishing a half-written response here would dress a
		// crash up as a complete 200.
		gw.finish()
	})
}

// gzipRequestEligible applies the request-side skip rules. The response-side
// ones (content type, status, size) are decided by gzipResponseWriter.
func gzipRequestEligible(r *http.Request) bool {
	if r.Method == http.MethodHead {
		return false
	}
	if isStreamPath(r.URL.Path) {
		return false
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/api-keys" {
		return false
	}
	return acceptsGzip(r.Header.Values("Accept-Encoding"))
}

// acceptsGzip reports whether an Accept-Encoding header allows gzip.
//
// gzip (or its alias x-gzip) with q > 0 accepts; an explicit q=0 refuses even
// when a wildcard would otherwise allow it; otherwise a wildcard with q > 0
// accepts. An absent header means the client asked for nothing, so nothing is
// what it gets — a bare curl sees the body exactly as before.
func acceptsGzip(values []string) bool {
	gzipQ, wildcardQ := -1.0, -1.0
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			coding, q := parseCoding(part)
			switch coding {
			case "gzip", "x-gzip":
				if q > gzipQ {
					gzipQ = q
				}
			case "*":
				if q > wildcardQ {
					wildcardQ = q
				}
			}
		}
	}
	if gzipQ >= 0 {
		return gzipQ > 0
	}
	return wildcardQ > 0
}

// parseCoding splits one Accept-Encoding element into its lowercased coding and
// its q-value. A missing q is 1; an unparsable one is 0, so a malformed header
// falls back to the uncompressed body rather than guessing.
func parseCoding(part string) (string, float64) {
	params := strings.Split(part, ";")
	coding := strings.ToLower(strings.TrimSpace(params[0]))
	q := 1.0
	for _, param := range params[1:] {
		key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
			continue
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || parsed < 0 {
			parsed = 0
		}
		q = parsed
	}
	return coding, q
}

// isJSONContentType reports whether ct is application/json, ignoring
// parameters such as charset.
func isJSONContentType(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.EqualFold(strings.TrimSpace(ct), "application/json")
}

// addVary appends token to the Vary header unless it (or "*") is already there.
// rs/cors has usually added "Vary: Origin" by now; the two lines coexist.
func addVary(h http.Header, token string) {
	for _, value := range h.Values("Vary") {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "*" || strings.EqualFold(part, token) {
				return
			}
		}
	}
	h.Add("Vary", token)
}

// errGzipFinished is returned to a write that arrives after the handler has
// returned and the response has been completed.
var errGzipFinished = errors.New("gzip: write after the response was completed")

type gzipState int

const (
	// gzipPending: a JSON response whose status is known but whose header has
	// NOT been sent, because whether it is compressed depends on a body size
	// not yet reached. Writes are buffered, up to GZIP_MIN_SIZE.
	gzipPending gzipState = iota
	// gzipIdentity: the response goes out exactly as the handler wrote it.
	gzipIdentity
	// gzipCompressing: Content-Encoding: gzip has been sent; writes go through
	// the compressor.
	gzipCompressing
	// gzipFinished: finish() has run.
	gzipFinished
)

// gzipResponseWriter defers the decision to compress until it has seen enough
// of the body to know it is worth it.
type gzipResponseWriter struct {
	http.ResponseWriter

	state       gzipState
	status      int
	wroteHeader bool
	buf         []byte
	gz          *gzip.Writer
}

// WriteHeader records the status and, for any response that cannot be
// compressed, sends it straight away. For one that might be, sending waits for
// the body, because Content-Encoding and Content-Length depend on it.
func (w *gzipResponseWriter) WriteHeader(code int) {
	// 1xx informational responses precede the real one and carry no body.
	if code >= 100 && code <= 199 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.wroteHeader || w.state == gzipFinished {
		return
	}
	w.wroteHeader = true
	w.status = code
	if !w.compressible() {
		_ = w.sendIdentity()
	}
}

// compressible applies the response-side rules to what the handler has set so
// far. It runs once, at WriteHeader, which is also when net/http would freeze
// the headers.
func (w *gzipResponseWriter) compressible() bool {
	switch w.status {
	case http.StatusNoContent, http.StatusNotModified, http.StatusPartialContent:
		return false
	}
	h := w.Header()
	if h.Get("Content-Encoding") != "" {
		return false
	}
	if !isJSONContentType(h.Get("Content-Type")) {
		return false
	}
	if cl := h.Get("Content-Length"); cl != "" {
		if n, err := strconv.Atoi(cl); err == nil && n < GZIP_MIN_SIZE {
			return false
		}
	}
	return true
}

func (w *gzipResponseWriter) Write(p []byte) (int, error) {
	if w.state == gzipFinished {
		return 0, errGzipFinished
	}
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	switch w.state {
	case gzipIdentity:
		return w.ResponseWriter.Write(p)
	case gzipCompressing:
		return w.gz.Write(p)
	}

	w.buf = append(w.buf, p...)
	if len(w.buf) < GZIP_MIN_SIZE {
		return len(p), nil
	}
	if err := w.startGzip(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// startGzip sends the header for a compressed response and pushes the
// buffered bytes through the compressor.
func (w *gzipResponseWriter) startGzip() error {
	h := w.Header()
	h.Del("Content-Length")
	h.Set("Content-Encoding", "gzip")
	addVary(h, "Accept-Encoding")
	w.ResponseWriter.WriteHeader(w.status)

	w.state = gzipCompressing
	w.gz = gzipWriterPool.Get().(*gzip.Writer)
	w.gz.Reset(w.ResponseWriter)

	buffered := w.buf
	w.buf = nil
	_, err := w.gz.Write(buffered)
	return err
}

// sendIdentity sends the header and any buffered bytes unmodified, and makes
// every later write a pass-through.
func (w *gzipResponseWriter) sendIdentity() error {
	w.state = gzipIdentity
	w.ResponseWriter.WriteHeader(w.status)
	if len(w.buf) == 0 {
		return nil
	}
	buffered := w.buf
	w.buf = nil
	_, err := w.ResponseWriter.Write(buffered)
	return err
}

// Flush implements http.Flusher.
//
// A handler that flushes a still-small JSON body wants bytes on the wire now,
// which makes it a stream rather than a document: what is buffered goes out
// uncompressed and compression is abandoned for the rest of the response. A
// response already being compressed flushes the compressor and then the
// connection.
func (w *gzipResponseWriter) Flush() {
	if w.state == gzipFinished {
		return
	}
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	switch w.state {
	case gzipPending:
		_ = w.sendIdentity()
	case gzipCompressing:
		_ = w.gz.Flush()
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// Unwrap lets http.ResponseController reach the real connection — for
// SetWriteDeadline above all (see GzipMiddleware).
func (w *gzipResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// finish completes the response once the handler has returned: a body that
// never reached GZIP_MIN_SIZE goes out as written, a compressed one gets its
// gzip trailer, and the compressor goes back to the pool.
func (w *gzipResponseWriter) finish() {
	switch w.state {
	case gzipPending:
		// Nothing written and no WriteHeader: leave it to net/http, which
		// answers 200 with an empty body exactly as it would have.
		if w.wroteHeader {
			_ = w.sendIdentity()
		}
	case gzipCompressing:
		_ = w.gz.Close()
		w.gz.Reset(io.Discard)
		gzipWriterPool.Put(w.gz)
		w.gz = nil
	}
	w.state = gzipFinished
}
