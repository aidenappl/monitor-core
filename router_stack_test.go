package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	monitor "github.com/aidenappl/go-monitor"
	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
)

// These drive the COMBINED stack exactly as buildRouter assembles it — root:
// RequestID → Logging → Recover → MuxHeader → CSRF; /v1: QueryAuth →
// RequestTimeout → Gzip — because what they pin is how the layers compose, which
// no single middleware's tests can see:
//
//   - the http.request.end event (Logging, outermost) reads its status, size and
//     cause through the gzip wrapper (innermost);
//   - a panic inside the gzip wrapper is answered by RecoverMiddleware outside it;
//   - the /v1 deadline reaches the handler, and the event's duration spans it;
//   - both SSE streams skip the /v1 layers, flush through the rest, and outlive
//     the server's WriteTimeout.

// useRequestTimeout swaps the /v1 deadline for routers built during the test.
func useRequestTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	previous := v1RequestTimeout
	v1RequestTimeout = d
	t.Cleanup(func() { v1RequestTimeout = previous })
}

func recordEvents(t *testing.T) *monitor.Recorder {
	t.Helper()
	rec := monitor.StartRecording()
	t.Cleanup(rec.Stop)
	return rec
}

// requestEvents returns the http.request.end events recorded for requestPath.
func requestEvents(rec *monitor.Recorder, requestPath string) []monitor.Event {
	var out []monitor.Event
	for _, e := range rec.Named("http.request.end") {
		if data, ok := e.Data.(map[string]any); ok && data["request_path"] == requestPath {
			out = append(out, e)
		}
	}
	return out
}

// requestEvent returns the one http.request.end recorded for requestPath.
func requestEvent(t *testing.T, rec *monitor.Recorder, requestPath string) (monitor.Event, map[string]any) {
	t.Helper()
	found := requestEvents(rec, requestPath)
	if len(found) != 1 {
		t.Fatalf("%d http.request.end events for %s, want exactly 1", len(found), requestPath)
	}
	return found[0], found[0].Data.(map[string]any)
}

// awaitRequestEvent waits for a request event that fires after the client has
// already stopped reading — a stream's, emitted once the server notices it left.
func awaitRequestEvent(t *testing.T, rec *monitor.Recorder, requestPath string, within time.Duration) (monitor.Event, map[string]any) {
	t.Helper()
	deadline := time.Now().Add(within)
	for len(requestEvents(rec, requestPath)) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no http.request.end for %s within %v of the client leaving", requestPath, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return requestEvent(t, rec, requestPath)
}

func dataInt(t *testing.T, data map[string]any, key string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(fmt.Sprint(data[key]), 10, 64)
	if err != nil {
		t.Fatalf("%s = %v, not an integer", key, data[key])
	}
	return n
}

// rawStackClient neither asks for nor decodes gzip on its own, so a test sees
// the bytes the stack actually sent.
func rawStackClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// TestGzipResponsesRecordTheirOutcomeOnTheRequestEvent: whether gzip compresses
// a /v1 response or holds it in its sub-threshold buffer until the handler
// returns, the request event records the status that finally went out, the cause
// the responder handed the failure recorder THROUGH the gzip wrapper (Unwrap),
// and response_bytes as sent — the compressed size when compressed.
func TestGzipResponsesRecordTheirOutcomeOnTheRequestEvent(t *testing.T) {
	useMasterKey(t)
	r := buildRouter(env.RoleBoth)
	v1 := v1Subrouter(t, r)

	// Enveloped twice (message and error_message), this error body is well past
	// GZIP_MIN_SIZE, so it is compressed like a large success.
	longMessage := "clickhouse refused the read " + strings.Repeat("x", 2*middleware.GZIP_MIN_SIZE)

	tests := []struct {
		name        string
		handler     http.HandlerFunc
		wantStatus  int
		wantGzip    bool
		wantLevel   string
		wantError   string // the cause on the event; "" for none
		wantMessage string // prefix of error_message; "" for none
		wantFields  map[string]string
	}{
		{
			name:       "large success is compressed and recorded as 200",
			handler:    func(w http.ResponseWriter, _ *http.Request) { responder.New(w, largePayload()) },
			wantStatus: http.StatusOK, wantGzip: true, wantLevel: monitor.LevelInfo,
		},
		{
			name: "large 5xx is compressed and keeps its cause",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				responder.ErrorWithCause(w, http.StatusServiceUnavailable, longMessage,
					errors.New("dial tcp: connection refused"), map[string]any{"query_kind": "zz-stack-test"})
			},
			wantStatus: http.StatusServiceUnavailable, wantGzip: true, wantLevel: monitor.LevelError,
			wantError: "dial tcp: connection refused", wantMessage: "clickhouse refused the read",
			wantFields: map[string]string{"query_kind": "zz-stack-test", "outcome": "returned 503"},
		},
		{
			name: "small 5xx waits in the gzip buffer and is recorded as 500 with its cause",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch events", errors.New("mariadb: bad connection"))
			},
			wantStatus: http.StatusInternalServerError, wantGzip: false, wantLevel: monitor.LevelError,
			wantError: "mariadb: bad connection", wantMessage: "failed to fetch events",
		},
		{
			name: "small 4xx waits in the gzip buffer and is recorded as 400",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				responder.Error(w, http.StatusBadRequest, "invalid limit")
			},
			wantStatus: http.StatusBadRequest, wantGzip: false, wantLevel: monitor.LevelWarn,
			wantMessage: "invalid limit",
		},
	}

	for i, tt := range tests {
		path := fmt.Sprintf("/v1/zz-stack-test/gzip-%d", i)
		v1.HandleFunc(strings.TrimPrefix(path, "/v1"), tt.handler).Methods(http.MethodGet)

		t.Run(tt.name, func(t *testing.T) {
			rec := recordEvents(t)
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("X-Api-Key", testMasterKey)
			req.Header.Set("Accept-Encoding", "gzip")
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
			gotGzip := rr.Header().Get("Content-Encoding") == "gzip"
			if gotGzip != tt.wantGzip {
				t.Fatalf("compressed = %v, want %v", gotGzip, tt.wantGzip)
			}
			sent := rr.Body.Bytes()
			plain := sent
			if gotGzip {
				zr, err := gzip.NewReader(bytes.NewReader(sent))
				if err != nil {
					t.Fatalf("not gzip: %v", err)
				}
				if plain, err = io.ReadAll(zr); err != nil {
					t.Fatalf("decompressing: %v", err)
				}
			}
			var envelope responder.Response
			if err := json.Unmarshal(plain, &envelope); err != nil {
				t.Fatalf("body is not the responder envelope: %v", err)
			}
			if envelope.Success != (tt.wantStatus < http.StatusBadRequest) {
				t.Errorf("envelope success = %v for a %d", envelope.Success, tt.wantStatus)
			}

			e, data := requestEvent(t, rec, path)
			if e.Level != tt.wantLevel {
				t.Errorf("level = %s, want %s", e.Level, tt.wantLevel)
			}
			if got := dataInt(t, data, "status_code"); got != int64(tt.wantStatus) {
				t.Errorf("status_code = %d, want %d — the event must record the status gzip finally sent", got, tt.wantStatus)
			}
			if got := fmt.Sprint(data["path"]); got != path {
				t.Errorf("path = %q, want the route template %q", got, path)
			}
			if got := dataInt(t, data, "response_bytes"); got != int64(len(sent)) {
				t.Errorf("response_bytes = %d, want %d (the bytes sent)", got, len(sent))
			}
			if gotGzip && dataInt(t, data, "response_bytes") >= int64(len(plain)) {
				t.Errorf("response_bytes = %v for a %d-byte body compressed to %d; want the compressed size", data["response_bytes"], len(plain), len(sent))
			}

			if tt.wantError == "" {
				if _, set := data["error"]; set {
					t.Errorf("error = %v on a response with no cause", data["error"])
				}
			} else if got := fmt.Sprint(data["error"]); got != tt.wantError {
				t.Errorf("error = %q, want %q — the cause did not reach the recorder through the gzip wrapper", got, tt.wantError)
			}
			if tt.wantMessage != "" && !strings.HasPrefix(fmt.Sprint(data["error_message"]), tt.wantMessage) {
				t.Errorf("error_message = %.80q, want prefix %q", data["error_message"], tt.wantMessage)
			}
			for key, want := range tt.wantFields {
				if got := fmt.Sprint(data[key]); got != want {
					t.Errorf("%s = %q, want %q", key, got, want)
				}
			}
			if tt.wantStatus >= http.StatusInternalServerError {
				if s, _ := data["stack_trace"].(string); !strings.Contains(s, "TestGzipResponsesRecordTheirOutcomeOnTheRequestEvent") {
					t.Errorf("stack does not reach the handler that failed: %.300q", s)
				}
			}
		})
	}
}

// TestPanicInsideGzipIsAnsweredByRecover: RecoverMiddleware sits OUTSIDE the
// /v1 gzip wrapper, so a panic unwinds past gzip without its finish() running.
// Before the threshold nothing has been sent, and the client gets Recover's
// clean 500. Past it the compressed 200 is already committed, and what must hold
// is that the client can never decode the half-written body as complete — the
// gzip trailer is never written. Either way the request event records the 500
// and stays a warning beside the one panic.recovered error.
func TestPanicInsideGzipIsAnsweredByRecover(t *testing.T) {
	useMasterKey(t)
	r := buildRouter(env.RoleBoth)
	v1 := v1Subrouter(t, r)
	v1.HandleFunc("/zz-stack-test/panic-buffered", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":[`)) // under GZIP_MIN_SIZE: still in gzip's buffer
		panic("nil map")
	}).Methods(http.MethodGet)
	v1.HandleFunc("/zz-stack-test/panic-compressing", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(append([]byte(`{"success":true,"data":[`), bytes.Repeat([]byte(`{"id":1},`), middleware.GZIP_MIN_SIZE)...))
		panic("nil map")
	}).Methods(http.MethodGet)

	srv := httptest.NewUnstartedServer(r)
	// Past the threshold, Recover's WriteHeader(500) is superfluous and net/http
	// logs it; that line is expected here.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.Start()
	defer srv.Close()
	client := rawStackClient(t)

	get := func(t *testing.T, path string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Api-Key", testMasterKey)
		req.Header.Set("Accept-Encoding", "gzip")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		return resp, body
	}

	assertOneFailure := func(t *testing.T, rec *monitor.Recorder, path string) {
		t.Helper()
		if n := len(rec.Named("panic.recovered")); n != 1 {
			t.Errorf("%d panic.recovered events, want 1", n)
		}
		e, data := requestEvent(t, rec, path)
		if got := dataInt(t, data, "status_code"); got != http.StatusInternalServerError {
			t.Errorf("request event status_code = %d, want 500", got)
		}
		if e.Level != monitor.LevelWarn || data["reported_as"] != "panic.recovered" {
			t.Errorf("request event = %s reported_as=%v, want warn reported_as=panic.recovered (one failure, one issue)", e.Level, data["reported_as"])
		}
	}

	t.Run("before the threshold the client gets a clean 500", func(t *testing.T) {
		rec := recordEvents(t)
		const path = "/v1/zz-stack-test/panic-buffered"
		resp, body := get(t, path)

		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Encoding"); got != "" {
			t.Errorf("Content-Encoding = %q on Recover's 500", got)
		}
		var envelope responder.Response
		if err := json.Unmarshal(body, &envelope); err != nil || envelope.Success || envelope.ErrorCode != http.StatusInternalServerError {
			t.Errorf("body = %q, want only Recover's error envelope (the buffered partial body must be dropped)", body)
		}
		assertOneFailure(t, rec, path)
	})

	t.Run("past the threshold the body never decodes as complete", func(t *testing.T) {
		rec := recordEvents(t)
		const path = "/v1/zz-stack-test/panic-compressing"
		resp, body := get(t, path)

		if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("Content-Encoding = %q; this case needs compression to have started before the panic", got)
		}
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			_, err = io.ReadAll(zr)
		}
		if err == nil {
			t.Error("a panicked handler's partial body decoded as a complete gzip response")
		}
		assertOneFailure(t, rec, path)
	})
}

// TestV1DeadlineCancelsTheHandlerThroughTheStack: the /v1 deadline reaches a
// handler behind QueryAuth and gzip, a ClickHouse-shaped failure from it is
// recorded as timed out, and the request event — emitted OUTSIDE RequestTimeout —
// times the whole request rather than being cut short by it. The deadline is
// injected at 50ms; production mounts REQUEST_TIMEOUT (25s).
func TestV1DeadlineCancelsTheHandlerThroughTheStack(t *testing.T) {
	const timeout = 50 * time.Millisecond
	useMasterKey(t)
	useRequestTimeout(t, timeout)
	r := buildRouter(env.RoleBoth)
	v1 := v1Subrouter(t, r)

	var (
		ctxErr  error
		elapsed time.Duration
	)
	const path = "/v1/zz-stack-test/slow-query"
	v1.HandleFunc(strings.TrimPrefix(path, "/v1"), func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		select {
		case <-req.Context().Done():
			ctxErr = req.Context().Err()
		case <-time.After(5 * time.Second):
		}
		elapsed = time.Since(start)
		cause := ctxErr
		if cause == nil {
			cause = errors.New("the request deadline never fired")
		}
		// What db.observedConn hands back for a read whose context ran out.
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch events",
			&db.QueryError{Kind: "zz-stack-test", Duration: elapsed, Err: cause})
	}).Methods(http.MethodGet)

	rec := recordEvents(t)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Api-Key", testMasterKey)
	req.Header.Set("Accept-Encoding", "gzip")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if !errors.Is(ctxErr, context.DeadlineExceeded) {
		t.Fatalf("handler context error = %v, want context.DeadlineExceeded", ctxErr)
	}
	if elapsed < timeout || elapsed > 2*time.Second {
		t.Errorf("handler was released after %v, want ~%v", elapsed, timeout)
	}
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want the handler's 500", rr.Code)
	}

	e, data := requestEvent(t, rec, path)
	if e.Level != monitor.LevelError || dataInt(t, data, "status_code") != http.StatusInternalServerError {
		t.Errorf("request event = %s status=%v, want error status=500", e.Level, data["status_code"])
	}
	checks := map[string]string{"timed_out": "true", "dependency": "clickhouse", "query_kind": "zz-stack-test"}
	for key, want := range checks {
		if got := fmt.Sprint(data[key]); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if got := dataInt(t, data, "duration_ms"); got < timeout.Milliseconds() {
		t.Errorf("duration_ms = %d, want at least the %v the handler waited", got, timeout)
	}
}

// TestStreamsFlushPastWriteTimeoutThroughTheCombinedStack runs BOTH real SSE
// handlers behind the real router, with the server's WriteTimeout scaled from
// 30s to 300ms and the /v1 request deadline injected at 100ms, and publishes
// only after both have passed. The event arrives only if RequestTimeout and gzip
// skipped the stream, the deadline clear reached the connection through every
// wrapper, and Flush did too — the next keepalive is 15s away, so an unflushed
// event would sit in the server's buffer past this test's wait. When the client
// leaves, the stream's one request event records a 200 held for the whole
// connection.
func TestStreamsFlushPastWriteTimeoutThroughTheCombinedStack(t *testing.T) {
	const (
		writeTimeout   = 300 * time.Millisecond
		requestTimeout = 100 * time.Millisecond
	)
	useMasterKey(t)
	useRequestTimeout(t, requestTimeout)

	eventHub := services.NewHub(4)
	alertHub := alerts.NewAlertHub(4)
	previousEvents, previousAlerts := routes.EventHub, routes.AlertNotifHub
	routes.EventHub, routes.AlertNotifHub = eventHub, alertHub
	t.Cleanup(func() { routes.EventHub, routes.AlertNotifHub = previousEvents, previousAlerts })

	srv := httptest.NewUnstartedServer(buildRouter(env.RoleBoth))
	srv.Config.WriteTimeout = writeTimeout
	srv.Start()
	defer srv.Close()

	tests := []struct {
		path    string
		marker  string
		publish func(marker string)
	}{
		{
			path: "/v1/events/stream", marker: "event-after-write-timeout",
			publish: func(marker string) {
				eventHub.Publish(&structs.Event{Project: "default", Service: "stack-test", Name: marker, Level: "info"})
			},
		},
		{
			path: "/v1/alerts/stream", marker: "alert-after-write-timeout",
			publish: func(marker string) {
				alertHub.PublishStateChange("default", "rule-1", marker, "firing", "over threshold", 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			rec := recordEvents(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Api-Key", testMasterKey)
			req.Header.Set("Accept-Encoding", "gzip")
			tr := &http.Transport{DisableCompression: true}
			defer tr.CloseIdleConnections()
			resp, err := (&http.Client{Transport: tr}).Do(req)
			if err != nil {
				t.Fatalf("opening the stream: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, body %q", resp.StatusCode, body)
			}
			if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
				t.Errorf("Content-Type = %q", got)
			}
			if got := resp.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("Content-Encoding = %q on an SSE stream", got)
			}

			// Both handlers subscribe BEFORE the Flush that sends these headers,
			// so a stream whose response has arrived is already subscribed.
			time.Sleep(3 * writeTimeout)
			tt.publish(tt.marker)

			lines := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(resp.Body)
				for scanner.Scan() {
					if strings.HasPrefix(scanner.Text(), "data: ") {
						lines <- scanner.Text()
						return
					}
				}
				close(lines)
			}()

			var line string
			select {
			case got, ok := <-lines:
				if !ok {
					t.Fatalf("the stream ended before the event arrived — cut by WriteTimeout (%v) or the request deadline (%v)", writeTimeout, requestTimeout)
				}
				line = got
			case <-time.After(3 * time.Second):
				t.Fatal("the event did not arrive within 3s of publishing — it was not flushed through the stack")
			}
			if !strings.Contains(line, tt.marker) {
				t.Errorf("unexpected event %q", line)
			}

			// The client leaves; the handler returns and the request event fires.
			resp.Body.Close()
			e, data := awaitRequestEvent(t, rec, tt.path, 5*time.Second)
			if e.Level != monitor.LevelInfo || dataInt(t, data, "status_code") != http.StatusOK {
				t.Errorf("stream request event = %s status=%v, want info status=200", e.Level, data["status_code"])
			}
			if got := fmt.Sprint(data["path"]); got != tt.path {
				t.Errorf("path = %q, want %q", got, tt.path)
			}
			if got := dataInt(t, data, "response_bytes"); got < int64(len(line)) {
				t.Errorf("response_bytes = %d, want at least the %d-byte event line the client read", got, len(line))
			}
			if got := dataInt(t, data, "duration_ms"); got < (3 * writeTimeout).Milliseconds() {
				t.Errorf("duration_ms = %d, want the whole connection (at least %v)", got, 3*writeTimeout)
			}
		})
	}
}
