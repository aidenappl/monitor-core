package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/aidenappl/monitor-core/db"
)

// DEPENDENCY_PING_TIMEOUT bounds each store ping. A probe that hangs is worse
// than one that fails: an orchestrator polling /ready on an interval learns
// nothing from a stalled connection, and a ClickHouse that accepts TCP but
// never answers would otherwise hold the request open until the server's
// WriteTimeout. Two seconds is far above a healthy ping and far below the poll
// interval of anything that would consume this.
const DEPENDENCY_PING_TIMEOUT = 2 * time.Second

// ReadyHandler is the READINESS probe: 200 only when BOTH stores answer, 503
// naming the ones that did not.
//
// Split from /health on purpose. Liveness must not fail on a dependency outage
// (see HealthHandler — Docker would restart a process that is fine), but
// readiness must, so a load balancer can stop routing to a replica that would
// accept events only to shred them, and so a deploy that comes up without
// ClickHouse is visibly not-ready instead of quietly lossy.
func ReadyHandler(w http.ResponseWriter, r *http.Request) {
	clickhouseOK, mariadbOK := cachedPingDependencies(r.Context())

	var failing []string
	if !clickhouseOK {
		failing = append(failing, "clickhouse")
	}
	if !mariadbOK {
		failing = append(failing, "mariadb")
	}

	status := http.StatusOK
	body := map[string]interface{}{
		"status":        "ready",
		"clickhouse_ok": clickhouseOK,
		"mariadb_ok":    mariadbOK,
	}
	if len(failing) > 0 {
		status = http.StatusServiceUnavailable
		body["status"] = "not_ready"
		body["failing"] = failing
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// DEPENDENCY_PING_TTL is how long a probe result is reused before both stores
// are asked again.
//
// /health and /ready are registered on the root router ahead of every auth
// middleware, and there is no rate limiter anywhere in the stack. Without this
// cache each unauthenticated request would take one connection from the
// ClickHouse pool (MaxOpenConns=10 — the SAME pool the batcher's PrepareBatch
// write path draws from) and one from MariaDB, so anyone able to reach the port
// could starve ingestion by polling a liveness endpoint in a loop.
//
// Five seconds is well under the container HEALTHCHECK's 30s interval, so the
// signal an orchestrator sees is never stale in practice, while a burst of any
// size costs at most one probe per store per five seconds.
const DEPENDENCY_PING_TTL = 5 * time.Second

// dependencyProbe memoises the last ping result. checking is the single-flight
// latch: concurrent callers arriving during an in-progress probe serve the
// previous answer rather than queueing more connections behind it, which is the
// whole point — a stampede must not cost more than one probe.
var dependencyProbe struct {
	mu           sync.Mutex
	checking     bool
	checkedAt    time.Time
	clickhouseOK bool
	mariadbOK    bool
}

// cachedPingDependencies returns the memoised store state, refreshing it at most
// once per DEPENDENCY_PING_TTL. Both handlers use it; neither needs a fresher
// answer than the poll intervals that consume them.
func cachedPingDependencies(ctx context.Context) (clickhouseOK, mariadbOK bool) {
	dependencyProbe.mu.Lock()
	fresh := time.Since(dependencyProbe.checkedAt) < DEPENDENCY_PING_TTL
	if fresh || dependencyProbe.checking {
		ch, my := dependencyProbe.clickhouseOK, dependencyProbe.mariadbOK
		// Before the first probe completes there is no result to serve. Report
		// the stores as down rather than up: /ready answering 200 because it has
		// not looked yet is the one wrong answer a readiness probe can give.
		if dependencyProbe.checkedAt.IsZero() && !dependencyProbe.checking {
			ch, my = false, false
		}
		dependencyProbe.mu.Unlock()
		return ch, my
	}
	dependencyProbe.checking = true
	dependencyProbe.mu.Unlock()

	ch, my := pingDependencies(ctx)

	dependencyProbe.mu.Lock()
	dependencyProbe.clickhouseOK, dependencyProbe.mariadbOK = ch, my
	dependencyProbe.checkedAt = time.Now()
	dependencyProbe.checking = false
	dependencyProbe.mu.Unlock()
	return ch, my
}

// pingDependencies checks both stores concurrently, so a total outage costs one
// timeout rather than two stacked back to back.
func pingDependencies(ctx context.Context) (clickhouseOK, mariadbOK bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		clickhouseOK = pingClickHouse(ctx)
	}()
	mariadbOK = pingMariaDB(ctx)
	<-done
	return clickhouseOK, mariadbOK
}

// pingClickHouse reports whether the event store is reachable. A nil Conn means
// the process is serving before db.Connect ran (or after Close), which is not
// ready either.
func pingClickHouse(ctx context.Context) bool {
	if db.Conn == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, DEPENDENCY_PING_TIMEOUT)
	defer cancel()
	return db.Conn.Ping(ctx) == nil
}

// pingMariaDB reports whether the relational auth store is reachable. PingContext
// (not Ping) so the timeout above is actually honoured by the driver.
func pingMariaDB(ctx context.Context) bool {
	if db.SQL == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, DEPENDENCY_PING_TIMEOUT)
	defer cancel()
	return db.SQL.PingContext(ctx) == nil
}
