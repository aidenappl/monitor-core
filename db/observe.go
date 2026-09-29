package db

import (
	"context"
	"errors"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aidenappl/monitor-core/telemetry"
)

// SLOW_QUERY_THRESHOLD is where a ClickHouse call earns a warning. The server
// kills a query at 60s (max_execution_time); a dashboard read taking a twelfth
// of that is already a page nobody waits for.
const SLOW_QUERY_THRESHOLD = 5 * time.Second

// ClickHouse error codes worth naming on an event.
const (
	CH_CODE_TIMEOUT_EXCEEDED      = 159
	CH_CODE_MEMORY_LIMIT_EXCEEDED = 241
)

// QueryError is a ClickHouse failure carrying what it takes to diagnose it
// without reproducing it: the query's KIND (the function that ran it — never the
// SQL), how long it ran, whether it timed out, and ClickHouse's own error code.
//
// Error() is unchanged from the driver's message, so callers, clients and
// errors.Is/As behave exactly as before. Only what reaches Monitor differs.
type QueryError struct {
	Kind     string
	Duration time.Duration
	Err      error
}

func (e *QueryError) Error() string { return e.Err.Error() }
func (e *QueryError) Unwrap() error { return e.Err }

// quotedLiteral matches a single-quoted SQL literal.
var quotedLiteral = regexp.MustCompile(`'(?:[^'\\]|\\.)*'`)

// TelemetryError is the message with every quoted literal elided.
//
// clickhouse-go binds parameters CLIENT-side — the values are formatted into the
// statement text — so a server message that quotes the statement quotes its
// values: a dashboard search term, a filter over a tenant's event data. None of
// that belongs in the appleby zone.
func (e *QueryError) TelemetryError() string {
	return quotedLiteral.ReplaceAllString(e.Err.Error(), "'…'")
}

// TelemetryFields is the dependency detail added to the event that reports the
// failure — the request event, when it reaches the responder.
func (e *QueryError) TelemetryFields() map[string]any {
	fields := map[string]any{
		"dependency":  "clickhouse",
		"query_kind":  e.Kind,
		"duration_ms": e.Duration.Milliseconds(),
		"timed_out":   timedOut(e.Err),
	}
	var exception *clickhouse.Exception
	if errors.As(e.Err, &exception) {
		fields["ch_code"] = exception.Code
		fields["ch_error"] = exception.Name
	}
	return fields
}

func timedOut(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var exception *clickhouse.Exception
	return errors.As(err, &exception) && exception.Code == CH_CODE_TIMEOUT_EXCEEDED
}

// SQLError is a MariaDB failure reported outside a request. The message is kept
// for callers; what reaches Monitor has quoted values elided, because MariaDB
// quotes the offending value in the errors that matter most here — "Duplicate
// entry '…'", "Incorrect string value: '…' for column 'message'" — and on the
// issue path that value is a tenant's error message.
type SQLError struct {
	Op  string
	Err error
}

func (e *SQLError) Error() string { return e.Err.Error() }
func (e *SQLError) Unwrap() error { return e.Err }

func (e *SQLError) TelemetryError() string {
	return quotedLiteral.ReplaceAllString(e.Err.Error(), "'…'")
}

func (e *SQLError) TelemetryFields() map[string]any {
	return map[string]any{"dependency": "mariadb", "db_op": e.Op}
}

// observedConn is the ClickHouse chokepoint: every read and exec goes through
// it, so every failure carries its query kind and every slow call is reported,
// without touching the thirty call sites that issue them. Batch inserts are
// wrapped in WriteBatch instead — the batcher is what decides their outcome.
type observedConn struct {
	driver.Conn
}

func (c observedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	start := time.Now()
	rows, err := c.Conn.Query(ctx, query, args...)
	return rows, observe(ctx, callerKind(2), start, err)
}

func (c observedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	start := time.Now()
	row := c.Conn.QueryRow(ctx, query, args...)
	kind := callerKind(2)
	// The driver runs the query here and parks any error on the row; it
	// surfaces at Scan, which is where it gets its context.
	if err := observe(ctx, kind, start, row.Err()); err != nil {
		return observedRow{Row: row, err: err}
	}
	return row
}

func (c observedConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	start := time.Now()
	return observe(ctx, callerKind(2), start, c.Conn.Select(ctx, dest, query, args...))
}

func (c observedConn) Exec(ctx context.Context, query string, args ...any) error {
	start := time.Now()
	return observe(ctx, callerKind(2), start, c.Conn.Exec(ctx, query, args...))
}

// observedRow returns the query's error, with its context, from every accessor.
type observedRow struct {
	driver.Row
	err error
}

func (r observedRow) Err() error                { return r.err }
func (r observedRow) Scan(...any) error         { return r.err }
func (r observedRow) ScanStruct(dest any) error { return r.err }

// observe warns about a slow call and gives a failed one its context. It reports
// no failure itself: the caller decides the outcome (a 500, a skipped rule) and
// reports it once, at that layer.
func observe(ctx context.Context, kind string, start time.Time, err error) error {
	duration := time.Since(start)
	if duration >= SLOW_QUERY_THRESHOLD {
		telemetry.WarnCoalesced(ctx, "clickhouse.query.slow:"+kind, "clickhouse.query.slow", nil, map[string]any{
			"dependency":   "clickhouse",
			"query_kind":   kind,
			"duration_ms":  duration.Milliseconds(),
			"threshold_ms": SLOW_QUERY_THRESHOLD.Milliseconds(),
			"failed":       err != nil,
		})
	}
	if err == nil {
		return nil
	}
	return &QueryError{Kind: kind, Duration: duration, Err: err}
}

// callerKind names the function skip frames up — "services.QueryEvents",
// "alerts.queryAggForRange". A stable, bounded label for a query that carries
// none of its text.
func callerKind(skip int) string {
	pc, _, _, ok := runtime.Caller(skip + 1)
	if !ok {
		return "unknown"
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "unknown"
	}
	name := fn.Name()
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return name
}
