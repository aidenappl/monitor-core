package middleware

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// jsonDocument returns a JSON body of at least n bytes.
func jsonDocument(n int) []byte {
	var b bytes.Buffer
	b.WriteString(`{"success":true,"message":"request was successful","data":[`)
	for i := 0; b.Len() < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":%d,"service":"monitor-core","level":"info"}`, i)
	}
	b.WriteString("]}")
	return b.Bytes()
}

// rawClient returns a client that neither adds Accept-Encoding nor decodes the
// response, so a test sees exactly the bytes and headers on the wire — which is
// where a stale Content-Length or a missing gzip trailer actually breaks.
func rawClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// fetch performs one request against srv and returns the response with its
// body fully read.
func fetch(t *testing.T, client *http.Client, method, url, acceptEncoding string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s %s body: %v", method, url, err)
	}
	return resp, body
}

func gunzip(t *testing.T, body []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("response is not valid gzip: %v", err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("decompressing: %v (a truncated stream means the gzip trailer never went out)", err)
	}
	return out
}

func hasVaryAcceptEncoding(h http.Header) bool {
	for _, v := range h.Values("Vary") {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "Accept-Encoding") {
				return true
			}
		}
	}
	return false
}

// TestGzipCompressesLargeJSON is the positive case, over a real connection: a
// JSON body of at least GZIP_MIN_SIZE is gzipped, carries Vary, and does NOT
// carry the Content-Length the handler computed for the uncompressed bytes —
// the client reads the whole body and it decompresses to exactly what the
// handler wrote.
func TestGzipCompressesLargeJSON(t *testing.T) {
	doc := jsonDocument(8 * 1024)

	cases := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "single write, as responder does",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(doc)
			},
		},
		{
			name: "many small writes straddling the threshold",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				for i := 0; i < len(doc); i += 100 {
					_, _ = w.Write(doc[i:min(i+100, len(doc))])
				}
			},
		},
		{
			name: "stale handler Content-Length is removed",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa(len(doc)))
				_, _ = w.Write(doc)
			},
		},
		{
			name: "explicit status and an existing Vary",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Vary", "Origin")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write(doc)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(GzipMiddleware(tc.handler))
			defer srv.Close()

			resp, body := fetch(t, rawClient(t), http.MethodGet, srv.URL+"/v1/events", "gzip, deflate, br")

			if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
				t.Fatalf("Content-Encoding = %q, want gzip", got)
			}
			if !hasVaryAcceptEncoding(resp.Header) {
				t.Errorf("Vary = %q, want it to include Accept-Encoding", resp.Header.Values("Vary"))
			}
			if cl := resp.Header.Get("Content-Length"); cl == strconv.Itoa(len(doc)) {
				t.Errorf("Content-Length %s is the uncompressed length; it must be deleted when compressing", cl)
			}
			if len(body) >= len(doc) {
				t.Errorf("compressed body is %d bytes, not smaller than the %d-byte original", len(body), len(doc))
			}
			if got := gunzip(t, body); !bytes.Equal(got, doc) {
				t.Errorf("decompressed body differs from what the handler wrote (%d vs %d bytes)", len(got), len(doc))
			}
		})
	}

	t.Run("existing Vary is kept beside Accept-Encoding", func(t *testing.T) {
		srv := httptest.NewServer(GzipMiddleware(cases[3].handler))
		defer srv.Close()
		resp, _ := fetch(t, rawClient(t), http.MethodGet, srv.URL+"/v1/events", "gzip")
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("status = %d, want the handler's 500", resp.StatusCode)
		}
		joined := strings.Join(resp.Header.Values("Vary"), ",")
		if !strings.Contains(joined, "Origin") || !strings.Contains(joined, "Accept-Encoding") {
			t.Errorf("Vary = %q, want both Origin and Accept-Encoding", joined)
		}
	})
}

// TestGzipLeavesSmallJSONUntouched: under GZIP_MIN_SIZE the response goes out
// byte-for-byte as written — no Content-Encoding, no Vary, and a handler's
// Content-Length still correct.
func TestGzipLeavesSmallJSONUntouched(t *testing.T) {
	for _, size := range []int{0, 200, GZIP_MIN_SIZE - 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			doc := bytes.Repeat([]byte("a"), size)
			srv := httptest.NewServer(GzipMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa(len(doc)))
				_, _ = w.Write(doc)
			})))
			defer srv.Close()

			resp, body := fetch(t, rawClient(t), http.MethodGet, srv.URL+"/v1/labels/service/values", "gzip")

			if got := resp.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("Content-Encoding = %q on a %d-byte body; below GZIP_MIN_SIZE it must be untouched", got, size)
			}
			if hasVaryAcceptEncoding(resp.Header) {
				t.Errorf("Vary: Accept-Encoding set on an uncompressed %d-byte body", size)
			}
			if resp.Header.Get("Content-Length") != strconv.Itoa(size) {
				t.Errorf("Content-Length = %q, want %d", resp.Header.Get("Content-Length"), size)
			}
			if !bytes.Equal(body, doc) {
				t.Errorf("body changed: got %d bytes, want %d", len(body), len(doc))
			}
		})
	}

	t.Run("exactly GZIP_MIN_SIZE is compressed", func(t *testing.T) {
		doc := bytes.Repeat([]byte("a"), GZIP_MIN_SIZE)
		rr := httptest.NewRecorder()
		GzipMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(doc)
		})).ServeHTTP(rr, gzipRequest(http.MethodGet, "/v1/events", "gzip"))
		if rr.Header().Get("Content-Encoding") != "gzip" {
			t.Fatalf("a %d-byte JSON body was not compressed", GZIP_MIN_SIZE)
		}
		if got := gunzip(t, rr.Body.Bytes()); !bytes.Equal(got, doc) {
			t.Error("decompressed body differs")
		}
	})
}

// TestGzipLeavesNonJSONUntouched: only application/json is compressed. The PNG
// case is the SSO icon's exact shape — an explicit Content-Length on a body over
// the threshold — which is what a content-type-blind compressor would corrupt.
func TestGzipLeavesNonJSONUntouched(t *testing.T) {
	big := bytes.Repeat([]byte("0123456789abcdef"), 512) // 8 KB

	cases := []struct {
		name          string
		contentType   string
		contentLength bool
	}{
		{"png with Content-Length", "image/png", true},
		{"plain text", "text/plain; charset=utf-8", false},
		{"event stream", "text/event-stream", false},
		{"json-ish suffix type", "application/problem+json", false},
		{"no Content-Type (sniffed by net/http)", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(GzipMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.contentType != "" {
					w.Header().Set("Content-Type", tc.contentType)
				}
				if tc.contentLength {
					w.Header().Set("Content-Length", strconv.Itoa(len(big)))
				}
				_, _ = w.Write(big)
			})))
			defer srv.Close()

			resp, body := fetch(t, rawClient(t), http.MethodGet, srv.URL+"/v1/anything", "gzip")

			if got := resp.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("Content-Encoding = %q for %q; only application/json may be compressed", got, tc.contentType)
			}
			if hasVaryAcceptEncoding(resp.Header) {
				t.Errorf("Vary: Accept-Encoding set on an uncompressed %q body", tc.contentType)
			}
			if tc.contentLength && resp.Header.Get("Content-Length") != strconv.Itoa(len(big)) {
				t.Errorf("Content-Length = %q, want the handler's %d", resp.Header.Get("Content-Length"), len(big))
			}
			if !bytes.Equal(body, big) {
				t.Errorf("body is not byte-identical (%d vs %d bytes)", len(body), len(big))
			}
		})
	}
}

func gzipRequest(method, path, acceptEncoding string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	return req
}

// TestGzipSkipRules covers every rule that must leave a large JSON body alone,
// plus positive controls on either side of each, so a rule that over-matches
// fails here too.
func TestGzipSkipRules(t *testing.T) {
	doc := jsonDocument(4 * 1024)

	cases := []struct {
		name           string
		method         string
		path           string
		acceptEncoding string
		status         int
		contentEnc     string
		wantGzip       bool
		wantWrapped    bool
	}{
		{name: "control: GET JSON", method: http.MethodGet, path: "/v1/events", acceptEncoding: "gzip", status: 200, wantGzip: true, wantWrapped: true},
		{name: "control: GET /v1/api-keys", method: http.MethodGet, path: "/v1/api-keys", acceptEncoding: "gzip", status: 200, wantGzip: true, wantWrapped: true},
		{name: "control: POST analytics", method: http.MethodPost, path: "/v1/analytics", acceptEncoding: "gzip", status: 200, wantGzip: true, wantWrapped: true},
		{name: "no Accept-Encoding", method: http.MethodGet, path: "/v1/events", status: 200},
		{name: "gzip refused with q=0", method: http.MethodGet, path: "/v1/events", acceptEncoding: "gzip;q=0, br", status: 200},
		{name: "identity only", method: http.MethodGet, path: "/v1/events", acceptEncoding: "identity", status: 200},
		{name: "HEAD", method: http.MethodHead, path: "/v1/events", acceptEncoding: "gzip", status: 200},
		{name: "event stream", method: http.MethodGet, path: "/v1/events/stream", acceptEncoding: "gzip", status: 200},
		{name: "alert stream", method: http.MethodGet, path: "/v1/alerts/stream", acceptEncoding: "gzip", status: 200},
		{name: "POST /v1/api-keys (minted secret)", method: http.MethodPost, path: "/v1/api-keys", acceptEncoding: "gzip", status: 200},
		{name: "204", method: http.MethodDelete, path: "/v1/views/1", acceptEncoding: "gzip", status: http.StatusNoContent, wantWrapped: true},
		{name: "304", method: http.MethodGet, path: "/v1/events", acceptEncoding: "gzip", status: http.StatusNotModified, wantWrapped: true},
		{name: "handler set its own Content-Encoding", method: http.MethodGet, path: "/v1/events", acceptEncoding: "gzip", status: 200, contentEnc: "br", wantWrapped: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := false
			h := GzipMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, wrapped = w.(*gzipResponseWriter)
				w.Header().Set("Content-Type", "application/json")
				if tc.contentEnc != "" {
					w.Header().Set("Content-Encoding", tc.contentEnc)
				}
				w.WriteHeader(tc.status)
				if tc.status != http.StatusNoContent && tc.status != http.StatusNotModified {
					_, _ = w.Write(doc)
				}
			}))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, gzipRequest(tc.method, tc.path, tc.acceptEncoding))

			if rr.Code != tc.status {
				t.Errorf("status = %d, want %d", rr.Code, tc.status)
			}
			if wrapped != tc.wantWrapped {
				t.Errorf("handler saw the gzip wrapper = %v, want %v", wrapped, tc.wantWrapped)
			}
			gotGzip := rr.Header().Get("Content-Encoding") == "gzip"
			if gotGzip != tc.wantGzip {
				t.Fatalf("compressed = %v, want %v (Content-Encoding %q)", gotGzip, tc.wantGzip, rr.Header().Get("Content-Encoding"))
			}
			if !tc.wantGzip && tc.contentEnc == "" && rr.Body.Len() > 0 && !bytes.Equal(rr.Body.Bytes(), doc) {
				t.Errorf("uncompressed body was altered")
			}
		})
	}
}

func TestAcceptsGzip(t *testing.T) {
	cases := []struct {
		header []string
		want   bool
	}{
		{nil, false},
		{[]string{""}, false},
		{[]string{"gzip"}, true},
		{[]string{"GZIP"}, true},
		{[]string{"x-gzip"}, true},
		{[]string{"gzip, deflate, br"}, true},
		{[]string{"br;q=1.0, gzip;q=0.8, *;q=0.1"}, true},
		{[]string{"deflate", "gzip"}, true},
		{[]string{"identity"}, false},
		{[]string{"br"}, false},
		{[]string{"gzip;q=0"}, false},
		{[]string{"gzip; q=0.000"}, false},
		{[]string{"gzip;q=nonsense"}, false},
		{[]string{"*"}, true},
		{[]string{"*;q=0"}, false},
		{[]string{"*, gzip;q=0"}, false},
	}
	for _, tc := range cases {
		if got := acceptsGzip(tc.header); got != tc.want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}
}

// TestGzipFlushBeforeThresholdStreamsUncompressed: a handler that flushes a
// small JSON body wants it on the wire now, so the wrapper sends it as written
// and stays out of the way for the rest of the response.
func TestGzipFlushBeforeThresholdStreamsUncompressed(t *testing.T) {
	head := []byte(`{"success":true,"data":[`)
	tail := jsonDocument(4 * 1024)

	srv := httptest.NewServer(GzipMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(head)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("Flush through the wrapper: %v", err)
		}
		_, _ = w.Write(tail)
	})))
	defer srv.Close()

	resp, body := fetch(t, rawClient(t), http.MethodGet, srv.URL+"/v1/events", "gzip")
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q after an early flush; want identity", got)
	}
	if want := append(append([]byte{}, head...), tail...); !bytes.Equal(body, want) {
		t.Errorf("body differs (%d vs %d bytes)", len(body), len(want))
	}
}

// TestGzipWrapperForwardsWriteDeadline is the Unwrap contract. The handler
// clears its write deadline through http.ResponseController — exactly as
// routes/stream.go and routes/alerts.go do, error discarded — then writes after
// the server's WriteTimeout has passed. That only succeeds if the controller
// reached the real connection through BOTH wrappers (gzip, then logging).
//
// WriteTimeout is scaled from production's 30s to 200ms to keep this fast; the
// mechanism is identical.
func TestGzipWrapperForwardsWriteDeadline(t *testing.T) {
	const writeTimeout = 200 * time.Millisecond
	doc := jsonDocument(4 * 1024)

	deadlineErr := make(chan error, 1)
	srv := httptest.NewUnstartedServer(LoggingMiddleware(GzipMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deadlineErr <- http.NewResponseController(w).SetWriteDeadline(time.Time{})
		time.Sleep(3 * writeTimeout)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(doc)
	}))))
	srv.Config.WriteTimeout = writeTimeout
	srv.Start()
	defer srv.Close()

	resp, body := fetch(t, rawClient(t), http.MethodGet, srv.URL+"/v1/data/keys", "gzip")

	if err := <-deadlineErr; err != nil {
		t.Fatalf("SetWriteDeadline through the gzip wrapper: %v — without Unwrap the SSE handlers' deadline clear silently fails", err)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
	}
	if got := gunzip(t, body); !bytes.Equal(got, doc) {
		t.Errorf("body written after the original deadline did not arrive intact")
	}
}

// TestStreamPathsSurviveWriteDeadlineAndTimeout composes the /v1 stack in the
// order router.go mounts it — logging (root), then RequestTimeout, then gzip —
// around an SSE handler on each stream path, and streams for well past both the
// server's WriteTimeout and the request timeout.
//
// It asserts the three things that would each silently end a live tail: the
// gzip wrapper is not installed (it would buffer events), the request context is
// not cancelled by the timeout, and the write-deadline clear reaches the
// connection so writes after WriteTimeout still land.
//
// WriteTimeout (30s in production) and REQUEST_TIMEOUT (25s) are scaled down to
// 150ms and 100ms; the stream runs ~5x longer than either.
func TestStreamPathsSurviveWriteDeadlineAndTimeout(t *testing.T) {
	const (
		writeTimeout   = 150 * time.Millisecond
		requestTimeout = 100 * time.Millisecond
		events         = 12
		interval       = 60 * time.Millisecond
	)

	for _, path := range []string{"/v1/events/stream", "/v1/alerts/stream"} {
		t.Run(path, func(t *testing.T) {
			type observed struct {
				wrapped     bool
				deadlineErr error
				ctxErr      error
				hasDeadline bool
			}
			seen := make(chan observed, 1)

			sse := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var o observed
				_, o.wrapped = w.(*gzipResponseWriter)
				_, o.hasDeadline = r.Context().Deadline()
				defer func() { seen <- o }()

				flusher, ok := w.(http.Flusher)
				if !ok {
					http.Error(w, "streaming not supported", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				o.deadlineErr = http.NewResponseController(w).SetWriteDeadline(time.Time{})
				flusher.Flush()

				for i := 0; i < events; i++ {
					time.Sleep(interval)
					if err := r.Context().Err(); err != nil {
						o.ctxErr = err
						return
					}
					fmt.Fprintf(w, "data: {\"n\":%d}\n\n", i)
					flusher.Flush()
				}
			})

			srv := httptest.NewUnstartedServer(LoggingMiddleware(RequestTimeout(requestTimeout)(GzipMiddleware(sse))))
			srv.Config.WriteTimeout = writeTimeout
			srv.Start()
			defer srv.Close()

			req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept-Encoding", "gzip")
			resp, err := rawClient(t).Do(req)
			if err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
			defer resp.Body.Close()

			if got := resp.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("Content-Encoding = %q on an SSE stream", got)
			}

			received := 0
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				if strings.HasPrefix(scanner.Text(), "data: ") {
					received++
				}
			}
			if err := scanner.Err(); err != nil {
				t.Errorf("reading the stream: %v", err)
			}

			o := <-seen
			if o.wrapped {
				t.Error("the SSE handler received the gzip wrapper; stream paths must be skipped")
			}
			if o.hasDeadline {
				t.Error("the SSE request context carries a deadline; RequestTimeout must skip stream paths")
			}
			if o.deadlineErr != nil {
				t.Errorf("SetWriteDeadline(zero) on the stream: %v", o.deadlineErr)
			}
			if o.ctxErr != nil {
				t.Errorf("stream context ended early: %v", o.ctxErr)
			}
			if received != events {
				t.Errorf("received %d events, want %d — the stream was cut after ~%v", received, events, writeTimeout)
			}
		})
	}
}

// TestGzipWriteAfterFinishIsRefused: a goroutine that outlives its handler must
// not be able to write through the wrapper once the response is complete —
// including when the handler itself never wrote anything.
func TestGzipWriteAfterFinishIsRefused(t *testing.T) {
	for _, name := range []string{"nothing written", "compressed body"} {
		t.Run(name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			gw := &gzipResponseWriter{ResponseWriter: rr}
			if name == "compressed body" {
				gw.Header().Set("Content-Type", "application/json")
				_, _ = gw.Write(jsonDocument(2 * 1024))
			}
			gw.finish()
			before := rr.Body.Len()

			if _, err := gw.Write([]byte("late")); err != errGzipFinished {
				t.Errorf("Write after finish returned %v, want errGzipFinished", err)
			}
			gw.Flush()
			if rr.Body.Len() != before {
				t.Errorf("late write reached the connection (%d -> %d bytes)", before, rr.Body.Len())
			}
		})
	}
}
