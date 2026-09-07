package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// resetDependencyProbe puts the memo back to its zero state so each test starts
// from "never probed".
func resetDependencyProbe() {
	dependencyProbe.mu.Lock()
	defer dependencyProbe.mu.Unlock()
	dependencyProbe.checking = false
	dependencyProbe.checkedAt = time.Time{}
	dependencyProbe.clickhouseOK = false
	dependencyProbe.mariadbOK = false
}

// TestCachedPingDependencies_MemoisesWithinTTL is the pool-exhaustion guard.
//
// ⚠️ /health and /ready are registered on the ROOT router, ahead of every auth
// middleware, and nothing in the stack rate-limits. An uncached probe takes a
// connection from the ClickHouse pool — MaxOpenConns=10, shared with the
// batcher's write path — on every request, so a loop against a liveness
// endpoint would starve ingestion. The memo is what makes that impossible, so
// it is asserted rather than assumed.
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

	clickhouseOK, mariadbOK := cachedPingDependencies(context.Background())
	if clickhouseOK || mariadbOK {
		t.Errorf("nil datastores reported healthy: clickhouse=%v mariadb=%v", clickhouseOK, mariadbOK)
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
