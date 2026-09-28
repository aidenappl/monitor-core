package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

// These tests pin WHERE the /v1 compression and request-deadline middleware are
// mounted, through the real router. The middleware's own rules are tested in
// middleware/gzip_test.go and middleware/timeout_test.go; what can only be seen
// here is that the root router — /auth/*, the public SSO icon, ingest — is
// never wrapped, and that the real SSE route keeps streaming past WriteTimeout
// through the whole stack.

const testMasterKey = "router-http-test-master-key"

// useMasterKey makes the env master key authenticate /v1 requests for the
// duration of a test, reading the default project.
func useMasterKey(t *testing.T) {
	t.Helper()
	key, project := env.IngestKey, env.DefaultProjectSlug
	env.IngestKey = testMasterKey
	env.DefaultProjectSlug = "default"
	t.Cleanup(func() {
		env.IngestKey = key
		env.DefaultProjectSlug = project
	})
}

// v1Subrouter finds the /v1 subrouter inside a built router, so a test can add
// a route that runs behind exactly the middleware the real /v1 routes get.
func v1Subrouter(t *testing.T, r *mux.Router) *mux.Router {
	t.Helper()
	var v1 *mux.Router
	_ = r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		if v1 != nil || len(ancestors) == 0 {
			return nil
		}
		if tpl, err := route.GetPathTemplate(); err == nil && strings.HasPrefix(tpl, "/v1/") {
			v1 = router
		}
		return nil
	})
	if v1 == nil {
		t.Fatal("no /v1 subrouter found in the built router")
	}
	return v1
}

// largePayload is comfortably over middleware.GZIP_MIN_SIZE once enveloped.
func largePayload() []map[string]string {
	rows := make([]map[string]string, 200)
	for i := range rows {
		rows[i] = map[string]string{"id": strconv.Itoa(i), "service": "monitor-core", "level": "info"}
	}
	return rows
}

func TestV1CompressionAndDeadlineAreMountedOnV1Only(t *testing.T) {
	useMasterKey(t)
	r := buildRouter(env.RoleBoth)
	v1 := v1Subrouter(t, r)

	type seen struct {
		deadline    time.Time
		hasDeadline bool
	}
	var v1Seen, rootSeen seen

	v1.HandleFunc("/zz-router-test/large", func(w http.ResponseWriter, req *http.Request) {
		v1Seen.deadline, v1Seen.hasDeadline = req.Context().Deadline()
		responder.New(w, largePayload())
	}).Methods(http.MethodGet)
	r.HandleFunc("/zz-router-test/root-large", func(w http.ResponseWriter, req *http.Request) {
		rootSeen.deadline, rootSeen.hasDeadline = req.Context().Deadline()
		responder.New(w, largePayload())
	}).Methods(http.MethodGet)

	t.Run("v1 JSON over 1 KB is gzipped with Vary and a 25s deadline", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/zz-router-test/large", nil)
		req.Header.Set("X-Api-Key", testMasterKey)
		req.Header.Set("Accept-Encoding", "gzip")
		before := time.Now()
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, body %q", rr.Code, rr.Body.String())
		}
		if got := rr.Header().Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("Content-Encoding = %q, want gzip on a large /v1 JSON response", got)
		}
		if vary := strings.Join(rr.Header().Values("Vary"), ","); !strings.Contains(vary, "Accept-Encoding") {
			t.Errorf("Vary = %q, want Accept-Encoding", vary)
		}
		zr, err := gzip.NewReader(rr.Body)
		if err != nil {
			t.Fatalf("not gzip: %v", err)
		}
		var envelope responder.Response
		if err := json.NewDecoder(zr).Decode(&envelope); err != nil || !envelope.Success {
			t.Fatalf("decompressed body is not the success envelope: %v", err)
		}

		if !v1Seen.hasDeadline {
			t.Fatal("a /v1 handler's context has no deadline; RequestTimeout is not mounted")
		}
		// The deadline is set after `before`, so it is at least REQUEST_TIMEOUT
		// out, plus however long the stack took to reach the middleware.
		if d := v1Seen.deadline.Sub(before); d < middleware.REQUEST_TIMEOUT || d > middleware.REQUEST_TIMEOUT+5*time.Second {
			t.Errorf("deadline is %v out, want ~%v", d, middleware.REQUEST_TIMEOUT)
		}
	})

	t.Run("root-router JSON is never gzipped and has no deadline", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/zz-router-test/root-large", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d", rr.Code)
		}
		if got := rr.Header().Get("Content-Encoding"); got != "" {
			t.Errorf("Content-Encoding = %q on a root-router route; gzip belongs to /v1 only", got)
		}
		if rootSeen.hasDeadline {
			t.Error("a root-router handler got the /v1 request deadline")
		}
	})

	t.Run("the SSO icon is served byte-identical", func(t *testing.T) {
		mock := useSQLMock(t)
		icon := bytes.Repeat([]byte{0x89, 'P', 'N', 'G', 0, 1, 2, 3}, 512) // 4 KB, over the threshold
		mock.ExpectQuery(regexp.QuoteMeta("SELECT icon_cache_type, icon_cache_data FROM sso_providers WHERE slug = ? AND enabled = 1")).
			WithArgs("forta").
			WillReturnRows(sqlmock.NewRows([]string{"icon_cache_type", "icon_cache_data"}).AddRow("image/png", icon))

		req := httptest.NewRequest(http.MethodGet, "/auth/sso/icon/forta", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d", rr.Code)
		}
		if got := rr.Header().Get("Content-Encoding"); got != "" {
			t.Errorf("Content-Encoding = %q on the SSO icon", got)
		}
		if got := rr.Header().Get("Content-Length"); got != strconv.Itoa(len(icon)) {
			t.Errorf("Content-Length = %q, want %d", got, len(icon))
		}
		if !bytes.Equal(rr.Body.Bytes(), icon) {
			t.Errorf("icon bytes changed (%d vs %d)", rr.Body.Len(), len(icon))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// useSQLMock swaps db.SQL for a sqlmock connection for the duration of a test.
func useSQLMock(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	original := db.SQL
	db.SQL = conn
	t.Cleanup(func() {
		db.SQL = original
		_ = conn.Close()
	})
	return mock
}

// TestEventStreamOutlivesWriteTimeoutThroughRouter runs the REAL
// StreamEventsHandler behind the REAL router and publishes an event only after
// the server's WriteTimeout has passed. The event arrives only if every wrapper
// between the handler and the connection lets http.ResponseController reach it,
// and neither the gzip wrapper nor the request deadline was put on the stream.
//
// WriteTimeout is scaled from production's 30s to 300ms.
func TestEventStreamOutlivesWriteTimeoutThroughRouter(t *testing.T) {
	const writeTimeout = 300 * time.Millisecond

	useMasterKey(t)
	hub := services.NewHub(4)
	originalHub := routes.EventHub
	routes.EventHub = hub
	t.Cleanup(func() { routes.EventHub = originalHub })

	srv := httptest.NewUnstartedServer(buildRouter(env.RoleBoth))
	srv.Config.WriteTimeout = writeTimeout
	srv.Start()
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/events/stream", nil)
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
		t.Errorf("Content-Encoding = %q on the SSE stream", got)
	}

	for hub.SubscriberCount() == 0 {
		if ctx.Err() != nil {
			t.Fatal("the stream never subscribed to the hub")
		}
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(3 * writeTimeout)
	hub.Publish(&structs.Event{Project: "default", Service: "router-test", Name: "after-write-timeout", Level: "info"})

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

	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatalf("the stream ended before the event arrived — cut at WriteTimeout (%v)", writeTimeout)
		}
		if !strings.Contains(line, "after-write-timeout") {
			t.Errorf("unexpected event %q", line)
		}
	case <-ctx.Done():
		t.Fatal("no event within 10s")
	}
}
