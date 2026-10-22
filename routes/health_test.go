package routes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
)

// resetDependencyProbe puts the memo back to its zero state so each test starts
// from "never probed".
func resetDependencyProbe() {
	dependencyProbe.mu.Lock()
	defer dependencyProbe.mu.Unlock()
	dependencyProbe.checking = false
	dependencyProbe.checkedAt = time.Time{}
	dependencyProbe.clickhouseOK = false
	dependencyProbe.clickhouseWriterOK = false
	dependencyProbe.mariadbOK = false
}

// TestCachedPingDependencies_MemoisesWithinTTL is the pool-exhaustion guard.
//
// ⚠️ /health and /ready are registered on the ROOT router, ahead of every auth
// middleware, and nothing in the stack rate-limits. An uncached probe takes a
// connection from each ClickHouse pool on every request — including the
// batcher's own writer pool, which holds only two — so a loop against a
// liveness endpoint would starve ingestion. The memo is what makes that
// impossible, so it is asserted rather than assumed.
func TestCachedPingDependencies_MemoisesWithinTTL(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	ctx := context.Background()

	cachedPingDependencies(ctx)

	dependencyProbe.mu.Lock()
	firstCheckedAt := dependencyProbe.checkedAt
	dependencyProbe.mu.Unlock()

	if firstCheckedAt.IsZero() {
		t.Fatal("first call did not record a probe time; the memo never refreshes")
	}

	for i := 0; i < 50; i++ {
		cachedPingDependencies(ctx)
	}

	dependencyProbe.mu.Lock()
	lastCheckedAt := dependencyProbe.checkedAt
	dependencyProbe.mu.Unlock()

	if !lastCheckedAt.Equal(firstCheckedAt) {
		t.Errorf("probe re-ran inside the TTL: first=%v last=%v — 50 requests cost 50 connection pairs",
			firstCheckedAt, lastCheckedAt)
	}
}

// TestCachedPingDependencies_RefreshesAfterTTL pins the other half of the
// contract: memoising must not mean answering forever from one stale probe, or
// /ready would keep reporting a store healthy long after it died.
func TestCachedPingDependencies_RefreshesAfterTTL(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	ctx := context.Background()
	cachedPingDependencies(ctx)

	// Backdate the memo past the TTL rather than sleeping for it.
	dependencyProbe.mu.Lock()
	staleAt := time.Now().Add(-2 * DEPENDENCY_PING_TTL)
	dependencyProbe.checkedAt = staleAt
	dependencyProbe.mu.Unlock()

	cachedPingDependencies(ctx)

	dependencyProbe.mu.Lock()
	refreshedAt := dependencyProbe.checkedAt
	dependencyProbe.mu.Unlock()

	if !refreshedAt.After(staleAt) {
		t.Errorf("probe did not refresh after the TTL expired: still %v", refreshedAt)
	}
}

// TestCachedPingDependencies_UnprobedReportsDown covers the boot window. With no
// datastores wired (db.Conn and db.SQL are nil in this package's tests) the
// honest answer is "down". A readiness probe that answers 200 because it has not
// looked yet is the one wrong answer it can give.
func TestCachedPingDependencies_UnprobedReportsDown(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	deps := cachedPingDependencies(context.Background())
	if deps.clickhouseOK || deps.clickhouseWriterOK || deps.mariadbOK {
		t.Errorf("nil datastores reported healthy: clickhouse=%v clickhouse_writer=%v mariadb=%v",
			deps.clickhouseOK, deps.clickhouseWriterOK, deps.mariadbOK)
	}
}

// TestCachedPingDependencies_ConcurrentCallersAreSafe runs the memo under -race.
// The single-flight latch is the reason concurrent callers do not each open a
// connection, so it has to be correct under contention, not just in sequence.
func TestCachedPingDependencies_ConcurrentCallersAreSafe(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cachedPingDependencies(ctx)
		}()
	}
	wg.Wait()

	dependencyProbe.mu.Lock()
	checking := dependencyProbe.checking
	dependencyProbe.mu.Unlock()

	if checking {
		t.Error("probe latch left set after all callers returned; a later refresh would never run")
	}
}

// TestReadyReportsDisabledAlerting pins the Phase 2 degraded state.
//
// The configuration cutover guard deliberately does NOT crash-loop: CI deploys
// this image automatically, so a fatal guard would take INGESTION down on the
// default path — trading a rare silent failure for a common loud outage in the
// one system whose job is to still be recording when everything else breaks.
//
// Degrading is only defensible if the degraded state is visible, so that
// visibility is asserted rather than assumed: /ready must fail and must name
// alerting, while /health keeps returning 200 so Docker does not restart a
// process that is running correctly.
func TestReadyReportsDisabledAlerting(t *testing.T) {
	resetDependencyProbe()
	previous := AlertingDisabledReason
	AlertingDisabledReason = "the configuration cutover has not been run"
	t.Cleanup(func() {
		AlertingDisabledReason = previous
		resetDependencyProbe()
	})

	rec := httptest.NewRecorder()
	ReadyHandler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready = %d, want 503 — a process that cannot alert must not read as ready", rec.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /ready body: %v", err)
	}
	if body["alerting_ok"] != false {
		t.Errorf("alerting_ok = %v, want false", body["alerting_ok"])
	}
	if body["alerting_disabled_reason"] == nil {
		t.Error("alerting_disabled_reason absent — the operator is told it is not ready but not why")
	}
	failing, _ := body["failing"].([]interface{})
	found := false
	for _, f := range failing {
		if f == "alerting" {
			found = true
		}
	}
	if !found {
		t.Errorf("failing = %v, want it to name \"alerting\"", failing)
	}
}

// TestHealthDegradesWithADependency is the "reports healthy while broken" guard.
//
// `status` on /health was the literal "ok" beside a `mariadb_ok` that could say
// false, so a datastore outage read as a healthy service to anything that looked
// at the top-level field — a dashboard, a human, an uptime check. It must now
// answer from the same judgement /ready uses, and name what is failing.
func TestHealthDegradesWithADependency(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	// No datastores are wired in this package's tests, so both pings report down.
	rec := httptest.NewRecorder()
	HealthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	// ⚠️ 200 EVEN WHILE DEGRADED, deliberately: the container HEALTHCHECK polls
	// this path with `curl -f`, so a non-2xx here turns a MariaDB blip into a
	// restart loop. /ready is the endpoint that gets to be 503.
	if rec.Code != http.StatusOK {
		t.Errorf("/health = %d, want 200 even when degraded — the HEALTHCHECK would restart-loop", rec.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /health body: %v", err)
	}
	if body["status"] != "degraded" {
		t.Errorf("status = %v with both stores down, want \"degraded\"", body["status"])
	}
	if body["mariadb_ok"] != false {
		t.Errorf("mariadb_ok = %v, want false", body["mariadb_ok"])
	}
	failing, _ := body["failing"].([]interface{})
	if len(failing) == 0 {
		t.Fatal("failing is absent or empty — the status says degraded without saying by what")
	}
	named := map[string]bool{}
	for _, f := range failing {
		if s, ok := f.(string); ok {
			named[s] = true
		}
	}
	if !named["mariadb"] {
		t.Errorf("failing = %v, want it to name \"mariadb\"", failing)
	}
}

// TestHealthAndReadyAgreeOnWhatIsFailing pins the two endpoints to one
// judgement. They drifted before — /ready computed its own list while /health
// hardcoded "ok" — and an operator comparing them got contradictory answers
// about the same process.
func TestHealthAndReadyAgreeOnWhatIsFailing(t *testing.T) {
	resetDependencyProbe()
	t.Cleanup(resetDependencyProbe)

	health := httptest.NewRecorder()
	HealthHandler(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	ready := httptest.NewRecorder()
	ReadyHandler(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))

	var healthBody, readyBody map[string]interface{}
	if err := json.Unmarshal(health.Body.Bytes(), &healthBody); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if err := json.Unmarshal(ready.Body.Bytes(), &readyBody); err != nil {
		t.Fatalf("decode /ready: %v", err)
	}

	hf, _ := json.Marshal(healthBody["failing"])
	rf, _ := json.Marshal(readyBody["failing"])
	if string(hf) != string(rf) {
		t.Errorf("/health failing = %s but /ready failing = %s; the two endpoints disagree", hf, rf)
	}
}

// pingConn is a ClickHouse pool that answers Ping and nothing else. The embedded
// interface is nil, so any other method panics: the probe must only ever ping.
type pingConn struct {
	driver.Conn
	err   error
	pings atomic.Int32
}

func (c *pingConn) Ping(context.Context) error {
	c.pings.Add(1)
	return c.err
}

// withStores installs the two ClickHouse pools for one test, plus a MariaDB
// that answers its ping, so the verdict is decided by ClickHouse alone. A nil
// pool argument leaves that global nil — the "no dedicated writer" and "no
// connection" states.
func withStores(t *testing.T, read, writer *pingConn) {
	t.Helper()
	mariadb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	previousConn, previousWriter, previousSQL := db.Conn, db.WriterConn, db.SQL
	db.Conn, db.WriterConn, db.SQL = nil, nil, mariadb
	if read != nil {
		db.Conn = read
	}
	if writer != nil {
		db.WriterConn = writer
	}
	t.Cleanup(func() {
		db.Conn, db.WriterConn, db.SQL = previousConn, previousWriter, previousSQL
		_ = mariadb.Close()
	})
}

// TestHealthReportsTheWriterPool pins the second ClickHouse pool on both probes.
//
// The batcher flushes through db.WriterConn, not the read pool, so "ClickHouse
// answers" no longer proves "ingest can write". A replica whose reads work but
// whose writer does not accepts events only to drop them, and both endpoints
// have to say so — /health as `degraded` plus the flag and `failing`, /ready as
// a 503 — while /health itself stays 200 so the HEALTHCHECK does not
// restart-loop.
//
// With no dedicated writer, WriteBatch uses the read pool, so the writer flag
// must mirror it — and must not cost a second ping of the same pool.
func TestHealthReportsTheWriterPool(t *testing.T) {
	down := errors.New("connection refused")

	tests := []struct {
		name   string
		role   env.Role
		read   *pingConn
		writer *pingConn

		wantClickHouseOK bool
		wantWriterOK     bool
		wantFailing      []string
		wantReadPings    int32
		wantWriterPings  int32
	}{
		{
			name:             "both pools answer",
			read:             &pingConn{},
			writer:           &pingConn{},
			wantClickHouseOK: true,
			wantWriterOK:     true,
			wantReadPings:    1,
			wantWriterPings:  1,
		},
		{
			name:             "reads answer, the writer does not",
			read:             &pingConn{},
			writer:           &pingConn{err: down},
			wantClickHouseOK: true,
			wantWriterOK:     false,
			wantFailing:      []string{"clickhouse_writer"},
			wantReadPings:    1,
			wantWriterPings:  1,
		},
		{
			name:             "the writer answers, reads do not",
			read:             &pingConn{err: down},
			writer:           &pingConn{},
			wantClickHouseOK: false,
			wantWriterOK:     true,
			wantFailing:      []string{"clickhouse"},
			wantReadPings:    1,
			wantWriterPings:  1,
		},
		{
			name:             "no dedicated writer mirrors a healthy read pool",
			read:             &pingConn{},
			wantClickHouseOK: true,
			wantWriterOK:     true,
			wantReadPings:    1,
		},
		{
			name:             "no dedicated writer mirrors a dead read pool",
			read:             &pingConn{err: down},
			wantClickHouseOK: false,
			wantWriterOK:     false,
			wantFailing:      []string{"clickhouse", "clickhouse_writer"},
			wantReadPings:    1,
		},
		{
			// An app process holds neither pool by design; neither may count.
			name:             "a control plane is judged on neither pool",
			role:             env.RoleApp,
			wantClickHouseOK: false,
			wantWriterOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetDependencyProbe()
			t.Cleanup(resetDependencyProbe)
			previousRole, previousAlerting := env.MonRole, AlertingDisabledReason
			env.MonRole, AlertingDisabledReason = tt.role, ""
			t.Cleanup(func() { env.MonRole, AlertingDisabledReason = previousRole, previousAlerting })
			withStores(t, tt.read, tt.writer)

			health := httptest.NewRecorder()
			HealthHandler(health, httptest.NewRequest(http.MethodGet, "/health", nil))
			ready := httptest.NewRecorder()
			ReadyHandler(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))

			wantHealth, wantReady, wantReadyCode := "ok", "ready", http.StatusOK
			if len(tt.wantFailing) > 0 {
				wantHealth, wantReady, wantReadyCode = "degraded", "not_ready", http.StatusServiceUnavailable
			}
			if health.Code != http.StatusOK {
				t.Errorf("/health = %d, want 200 whatever the pools say", health.Code)
			}
			if ready.Code != wantReadyCode {
				t.Errorf("/ready = %d, want %d", ready.Code, wantReadyCode)
			}

			for endpoint, want := range map[string]struct {
				rec    *httptest.ResponseRecorder
				status string
			}{
				"/health": {health, wantHealth},
				"/ready":  {ready, wantReady},
			} {
				var body map[string]interface{}
				if err := json.Unmarshal(want.rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("decode %s: %v", endpoint, err)
				}
				if body["status"] != want.status {
					t.Errorf("%s status = %v, want %q", endpoint, body["status"], want.status)
				}
				if body["clickhouse_ok"] != tt.wantClickHouseOK {
					t.Errorf("%s clickhouse_ok = %v, want %v", endpoint, body["clickhouse_ok"], tt.wantClickHouseOK)
				}
				if body["clickhouse_writer_ok"] != tt.wantWriterOK {
					t.Errorf("%s clickhouse_writer_ok = %v, want %v", endpoint, body["clickhouse_writer_ok"], tt.wantWriterOK)
				}
				if body["mariadb_ok"] != true {
					t.Errorf("%s mariadb_ok = %v, want true (sqlmock answers pings)", endpoint, body["mariadb_ok"])
				}

				got, _ := json.Marshal(body["failing"])
				wantJSON := []byte("null")
				if len(tt.wantFailing) > 0 {
					wantJSON, _ = json.Marshal(tt.wantFailing)
				}
				if string(got) != string(wantJSON) {
					t.Errorf("%s failing = %s, want %s", endpoint, got, wantJSON)
				}
			}

			// Two requests, one probe: the memo holds across both endpoints.
			if tt.read != nil {
				if got := tt.read.pings.Load(); got != tt.wantReadPings {
					t.Errorf("read pool pinged %d times, want %d", got, tt.wantReadPings)
				}
			}
			if tt.writer != nil {
				if got := tt.writer.pings.Load(); got != tt.wantWriterPings {
					t.Errorf("writer pool pinged %d times, want %d", got, tt.wantWriterPings)
				}
			}
		})
	}
}
