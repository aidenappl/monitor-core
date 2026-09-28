package services

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

// These tests assert on the SQL TEXT each builder produces, which is a
// deliberate choice and the only one that proves anything here.
//
// A test that checked the returned rows would need a real ClickHouse holding two
// projects' data, and would still only prove that the paths it exercised were
// scoped. A test that called scope.ProjectPredicate directly would prove the
// predicate is correct but not that any builder uses it. The failure this change
// exists to prevent is a builder that quietly stopped applying it — the query
// still runs, still returns rows, and the rows are someone else's. Reading the
// generated string is what makes that failure visible: delete a chokepoint call
// and the substring is gone.

// withDefaultProject pins env.DefaultProjectSlug for the duration of a test.
// env.Load() never runs under `go test`, so the var is empty unless a test sets
// it, and the empty-string transition arm keys off it.
func withDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

// recordingConn is a driver.Conn that executes nothing and remembers every
// statement it was handed. Only Query and QueryRow are meaningful; the rest of
// the interface is present because driver.Conn is wide, and panics rather than
// silently succeeding so a builder that reaches ClickHouse by some other route
// cannot slip past unnoticed.
//
// QueryEvents and QueryCompare issue their two statements concurrently, so
// record takes a lock (CI runs -race) and appends a statement and its args
// together — statements[i] and args[i] always belong to one call. The ORDER of
// the calls is not deterministic: assert on every statement, or pick one out by
// its text with find, never by position among concurrent ones. Reading the
// slices after the builder returns needs no lock; its own wait on the
// goroutines orders their writes before the read.
type recordingConn struct {
	mu         sync.Mutex
	statements []string
	args       [][]any
}

func (c *recordingConn) record(query string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, query)
	c.args = append(c.args, args)
}

// find returns the one recorded statement accepted by match, with its args.
func (c *recordingConn) find(t *testing.T, match func(statement string) bool) (string, []any) {
	t.Helper()
	found := -1
	for i, statement := range c.statements {
		if match(statement) {
			if found >= 0 {
				t.Fatalf("more than one statement matched:\n\t%s\n\t%s", c.statements[found], statement)
			}
			found = i
		}
	}
	if found < 0 {
		t.Fatalf("no statement matched among %q", c.statements)
	}
	return c.statements[found], c.args[found]
}

func (c *recordingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.record(query, args...)
	return &emptyRows{}, nil
}

func (c *recordingConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.record(query, args...)
	return &emptyRow{}
}

func (c *recordingConn) Contributors() []string { return nil }
func (c *recordingConn) ServerVersion() (*driver.ServerVersion, error) {
	return &proto.ServerHandshake{}, nil
}
func (c *recordingConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	panic("recordingConn: unexpected Select — every read must go through Query/QueryRow")
}
func (c *recordingConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("recordingConn: unexpected PrepareBatch on a read path")
}
func (c *recordingConn) Exec(ctx context.Context, query string, args ...any) error {
	panic("recordingConn: unexpected Exec on a read path")
}
func (c *recordingConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	panic("recordingConn: unexpected AsyncInsert on a read path")
}
func (c *recordingConn) Ping(context.Context) error { return nil }
func (c *recordingConn) Stats() driver.Stats        { return driver.Stats{} }
func (c *recordingConn) Close() error               { return nil }

// emptyRows is an exhausted result set — enough for every builder here, since
// none of them behave differently on zero rows.
type emptyRows struct{}

func (r *emptyRows) Next() bool                       { return false }
func (r *emptyRows) Scan(dest ...any) error           { return nil }
func (r *emptyRows) ScanStruct(dest any) error        { return nil }
func (r *emptyRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *emptyRows) Totals(dest ...any) error         { return nil }
func (r *emptyRows) Columns() []string                { return nil }
func (r *emptyRows) Close() error                     { return nil }
func (r *emptyRows) Err() error                       { return nil }

// emptyRow scans nothing and reports no error, so a count or gauge read
// completes and the builder returns normally.
type emptyRow struct{}

func (r *emptyRow) Err() error                { return nil }
func (r *emptyRow) Scan(dest ...any) error    { return nil }
func (r *emptyRow) ScanStruct(dest any) error { return nil }

// withRecordingConn swaps db.Conn for a recorder and restores it afterwards.
func withRecordingConn(t *testing.T) *recordingConn {
	t.Helper()
	recorder := &recordingConn{}
	previousConn := db.Conn
	previousDatabase := db.Database
	db.Conn = recorder
	db.Database = "monitor"
	t.Cleanup(func() {
		db.Conn = previousConn
		db.Database = previousDatabase
	})
	return recorder
}

// scopedContext is a request context as QueryAuthMiddleware would have left it.
func scopedContext(project string) context.Context {
	return scope.WithProject(context.Background(), project)
}

// TestEveryBuilderEmitsTheProjectPredicate is the leak guard for this package.
// It drives all nine read entry points with a non-default project — the case
// where the predicate must be an exact match with no empty-string arm — and
// requires every statement any of them issues to carry it.
//
// Asserting on EVERY recorded statement, not just the first, is deliberate:
// QueryEvents issues a count and a page, and QueryCompare issues two gauges.
// A predicate applied to one of a pair and not the other is a real and subtle
// bug — a page of the caller's own events under a total counted across every
// project — and checking only statements[0] would miss exactly that.
func TestEveryBuilderEmitsTheProjectPredicate(t *testing.T) {
	withDefaultProject(t, "default")

	from := time.Now().Add(-time.Hour)
	to := time.Now()

	tests := []struct {
		name           string
		run            func(ctx context.Context) error
		wantStatements int
	}{
		{"QueryEvents", func(ctx context.Context) error {
			_, err := QueryEvents(ctx, QueryParams{From: from, To: to})
			return err
		}, 2},
		{"GetLabelValues", func(ctx context.Context) error {
			_, err := GetLabelValues(ctx, "service", QueryParams{From: from, To: to})
			return err
		}, 1},
		{"GetDataKeys", func(ctx context.Context) error {
			_, err := GetDataKeys(ctx, QueryParams{From: from, To: to})
			return err
		}, 1},
		{"GetDataValues", func(ctx context.Context) error {
			_, err := GetDataValues(ctx, "latency_ms", QueryParams{From: from, To: to})
			return err
		}, 1},
		{"QueryAnalytics", func(ctx context.Context) error {
			_, err := QueryAnalytics(ctx, &structs.AnalyticsQuery{
				Aggregation: structs.AggCount, GroupBy: []string{"service"}, From: from, To: to,
			})
			return err
		}, 1},
		{"QueryTimeSeries", func(ctx context.Context) error {
			_, err := QueryTimeSeries(ctx, &structs.TimeSeriesQuery{
				Aggregation: structs.AggCount, Interval: structs.IntervalHour, From: from, To: to,
			})
			return err
		}, 1},
		{"QueryTopN", func(ctx context.Context) error {
			_, err := QueryTopN(ctx, &structs.TopNQuery{
				Aggregation: structs.AggCount, GroupBy: "service", From: from, To: to,
			})
			return err
		}, 1},
		{"QueryGauge", func(ctx context.Context) error {
			_, err := QueryGauge(ctx, &structs.GaugeQuery{
				Aggregation: structs.AggCount, From: from, To: to,
			})
			return err
		}, 1},
		// Issues no SQL of its own — it must inherit scoping through QueryGauge
		// for BOTH periods, which is why two statements are expected.
		{"QueryCompare", func(ctx context.Context) error {
			_, err := QueryCompare(ctx, &structs.CompareQuery{
				Aggregation: structs.AggCount, From: from, To: to,
			})
			return err
		}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := withRecordingConn(t)

			if err := tt.run(scopedContext("atlas")); err != nil {
				t.Fatalf("%s returned an error: %v", tt.name, err)
			}

			if len(recorder.statements) != tt.wantStatements {
				t.Fatalf("issued %d statement(s), want %d: %q", len(recorder.statements), tt.wantStatements, recorder.statements)
			}

			for i, statement := range recorder.statements {
				if !strings.Contains(statement, "project = ?") {
					t.Errorf("statement %d has NO project predicate — this read returns every project's data:\n\t%s", i, statement)
				}
				if strings.Contains(statement, "project = ''") {
					t.Errorf("statement %d admits unstamped rows for the non-default project %q; the transition arm must be confined to the default project:\n\t%s", i, "atlas", statement)
				}
				if !containsArg(recorder.args[i], "atlas") {
					t.Errorf("statement %d does not bind the project slug; args = %v", i, recorder.args[i])
				}
			}
		})
	}
}

// TestBuildersRefuseAnUnscopedContext pins the fail-closed half. A route
// registered outside QueryAuthMiddleware must produce an error, not a query:
// there is no safe default answer to "whose data is this", and returning
// everything is the worst available one.
func TestBuildersRefuseAnUnscopedContext(t *testing.T) {
	withDefaultProject(t, "default")

	from := time.Now().Add(-time.Hour)
	to := time.Now()

	tests := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{"QueryEvents", func(ctx context.Context) error {
			_, err := QueryEvents(ctx, QueryParams{})
			return err
		}},
		{"GetLabelValues", func(ctx context.Context) error {
			_, err := GetLabelValues(ctx, "service", QueryParams{})
			return err
		}},
		{"GetDataKeys", func(ctx context.Context) error {
			_, err := GetDataKeys(ctx, QueryParams{})
			return err
		}},
		{"GetDataValues", func(ctx context.Context) error {
			_, err := GetDataValues(ctx, "latency_ms", QueryParams{})
			return err
		}},
		{"QueryAnalytics", func(ctx context.Context) error {
			_, err := QueryAnalytics(ctx, &structs.AnalyticsQuery{Aggregation: structs.AggCount})
			return err
		}},
		{"QueryTimeSeries", func(ctx context.Context) error {
			_, err := QueryTimeSeries(ctx, &structs.TimeSeriesQuery{Aggregation: structs.AggCount, Interval: structs.IntervalHour})
			return err
		}},
		{"QueryTopN", func(ctx context.Context) error {
			_, err := QueryTopN(ctx, &structs.TopNQuery{Aggregation: structs.AggCount, GroupBy: "service"})
			return err
		}},
		{"QueryGauge", func(ctx context.Context) error {
			_, err := QueryGauge(ctx, &structs.GaugeQuery{Aggregation: structs.AggCount})
			return err
		}},
		{"QueryCompare", func(ctx context.Context) error {
			_, err := QueryCompare(ctx, &structs.CompareQuery{Aggregation: structs.AggCount, From: from, To: to})
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := withRecordingConn(t)

			err := tt.run(context.Background())
			if !errors.Is(err, scope.ErrNoProject) {
				t.Fatalf("err = %v, want ErrNoProject", err)
			}
			if len(recorder.statements) != 0 {
				t.Errorf("issued %d statement(s) despite having no project: %q", len(recorder.statements), recorder.statements)
			}
		})
	}
}

// TestDefaultProjectAdmitsUnstampedRows covers the transition window end to end.
// Without the empty-string arm reaching the generated SQL, the dashboard goes
// blank on the deploy that lands scoping — every row written before migration
// 006 reads back with an empty project until the manual backfill has run.
func TestDefaultProjectAdmitsUnstampedRows(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	if _, err := QueryEvents(scopedContext("default"), QueryParams{}); err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}

	for i, statement := range recorder.statements {
		if !strings.Contains(statement, "(project = ? OR project = '')") {
			t.Errorf("statement %d drops pre-006 rows for the default project:\n\t%s", i, statement)
		}
	}
}

// TestGetDataValuesBindsArgumentsInClauseOrder guards a failure that produces no
// error at all.
//
// squirrel emits arguments in CLAUSE order (columns, from, where...), not in the
// order the builder methods were called. GetDataValues binds the data key twice
// — once in the SELECT expression, once in the WHERE — and the project predicate
// now sits between them. Hand-assembling that arg slice, as this code did before
// the predicate existed, slides every binding one position along: valid SQL,
// no error, wrong rows, and the project argument landing in a JSON extract.
func TestGetDataValuesBindsArgumentsInClauseOrder(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	if _, err := GetDataValues(scopedContext("atlas"), "latency_ms", QueryParams{}); err != nil {
		t.Fatalf("GetDataValues: %v", err)
	}

	statement := recorder.statements[0]
	args := recorder.args[0]

	// The placeholders appear as: SELECT key, WHERE project, WHERE key.
	want := []any{"latency_ms", "atlas", "latency_ms"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("args = %v, want %v\n\tstatement: %s", args, want, statement)
	}
}

// containsArg reports whether want appears among a statement's bound arguments.
func containsArg(args []any, want string) bool {
	for _, arg := range args {
		if s, ok := arg.(string); ok && s == want {
			return true
		}
	}
	return false
}

// rendezvousConn is a recordingConn whose Query and QueryRow each wait until
// `want` calls have arrived. A builder that issues its reads one after another
// never gets its second call in while the first is waiting, so the first times
// out and sequential is set — which is how the concurrency tests below tell
// "side by side" from "back to back" without depending on timing when they pass.
type rendezvousConn struct {
	*recordingConn
	arrivals   sync.WaitGroup
	sequential atomic.Bool
}

func withRendezvousConn(t *testing.T, want int) *rendezvousConn {
	t.Helper()
	conn := &rendezvousConn{recordingConn: withRecordingConn(t)}
	conn.arrivals.Add(want)
	db.Conn = conn
	return conn
}

func (c *rendezvousConn) wait() {
	c.arrivals.Done()
	all := make(chan struct{})
	go func() {
		c.arrivals.Wait()
		close(all)
	}()
	select {
	case <-all:
	case <-time.After(2 * time.Second):
		c.sequential.Store(true)
	}
}

func (c *rendezvousConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.record(query, args...)
	c.wait()
	return &emptyRows{}, nil
}

func (c *rendezvousConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.record(query, args...)
	c.wait()
	return &emptyRow{}
}

// TestQueryEventsRunsCountAndPageConcurrently and its QueryCompare twin pin
// CH-3/CH-6: the two reads each call makes are independent and must overlap.
func TestQueryEventsRunsCountAndPageConcurrently(t *testing.T) {
	withDefaultProject(t, "default")
	conn := withRendezvousConn(t, 2)

	if _, err := QueryEvents(scopedContext("atlas"), QueryParams{}); err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	if conn.sequential.Load() {
		t.Error("the count and the page ran one after the other")
	}
}

func TestQueryCompareRunsBothGaugesConcurrently(t *testing.T) {
	withDefaultProject(t, "default")
	conn := withRendezvousConn(t, 2)

	from := time.Now().Add(-time.Hour)
	if _, err := QueryCompare(scopedContext("atlas"), &structs.CompareQuery{
		Aggregation: structs.AggCount, From: from, To: time.Now(),
	}); err != nil {
		t.Fatalf("QueryCompare: %v", err)
	}
	if conn.sequential.Load() {
		t.Error("the two periods ran one after the other")
	}
}

// eventsPagePrefix is the page query's projection, which the row scan in
// QueryEvents reads positionally.
const eventsPagePrefix = "SELECT timestamp, service, project, env, job_id, request_id, trace_id, user_id, name, level, data FROM monitor.events WHERE "

// TestQueryEventsPageIsTwoPhaseWithoutDataFilters pins the two-phase page SQL
// and its argument vector. The inner phase repeats the project predicate, the
// caller's filters and the time range, so the args are the outer set followed
// by the inner set — and the inner set must carry the project too, since the
// inner read decides which rows make the page.
func TestQueryEventsPageIsTwoPhaseWithoutDataFilters(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	params := QueryParams{
		Filters: []Filter{{Field: "service", Operator: OpEq, Value: "api"}},
		From:    from, To: to, Limit: 25, Offset: 50,
	}
	if _, err := QueryEvents(scopedContext("atlas"), params); err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}

	page, pageArgs := recorder.find(t, func(s string) bool { return strings.HasPrefix(s, eventsPagePrefix) })
	wantPage := eventsPagePrefix +
		"project = ? AND service = ? AND timestamp >= ? AND timestamp <= ? AND " +
		"timestamp >= (SELECT min(timestamp) FROM (SELECT timestamp FROM monitor.events WHERE project = ? AND service = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 25 OFFSET 50)) " +
		"ORDER BY timestamp DESC LIMIT 25 OFFSET 50"
	if page != wantPage {
		t.Errorf("page sql =\n\t%s\nwant\n\t%s", page, wantPage)
	}
	wantPageArgs := []any{"atlas", "api", from, to, "atlas", "api", from, to}
	if !reflect.DeepEqual(pageArgs, wantPageArgs) {
		t.Errorf("page args = %v, want %v", pageArgs, wantPageArgs)
	}

	count, countArgs := recorder.find(t, func(s string) bool { return strings.HasPrefix(s, "SELECT count()") })
	wantCount := "SELECT count() FROM monitor.events WHERE project = ? AND service = ? AND timestamp >= ? AND timestamp <= ?"
	if count != wantCount {
		t.Errorf("count sql =\n\t%s\nwant\n\t%s", count, wantCount)
	}
	if want := []any{"atlas", "api", from, to}; !reflect.DeepEqual(countArgs, want) {
		t.Errorf("count args = %v, want %v", countArgs, want)
	}
}

// TestQueryEventsPageStaysSinglePhaseWithADataFilter: with a data.* filter both
// phases would parse `data` for the same rows, so the page is one statement.
func TestQueryEventsPageStaysSinglePhaseWithADataFilter(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	params := QueryParams{Filters: []Filter{{Field: "latency_ms", Operator: OpEq, Value: "5", IsData: true}}}
	if _, err := QueryEvents(scopedContext("atlas"), params); err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}

	page, pageArgs := recorder.find(t, func(s string) bool { return strings.HasPrefix(s, eventsPagePrefix) })
	wantPage := eventsPagePrefix + "project = ? AND position(data, ?) > 0 AND " + wantGuardedString + " = ? ORDER BY timestamp DESC LIMIT 100 OFFSET 0"
	if page != wantPage {
		t.Errorf("page sql =\n\t%s\nwant\n\t%s", page, wantPage)
	}
	if want := []any{"atlas", "5", "5"}; !reflect.DeepEqual(pageArgs, want) {
		t.Errorf("page args = %v, want %v", pageArgs, want)
	}
}

// TestQueryEventsTwoPhaseScopesBothPhasesForTheDefaultProject: the transition
// arm reaches the inner read too, or pre-006 rows could never set the boundary.
func TestQueryEventsTwoPhaseScopesBothPhasesForTheDefaultProject(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	if _, err := QueryEvents(scopedContext("default"), QueryParams{}); err != nil {
		t.Fatalf("QueryEvents: %v", err)
	}
	page, pageArgs := recorder.find(t, func(s string) bool { return strings.HasPrefix(s, eventsPagePrefix) })
	if n := strings.Count(page, "(project = ? OR project = '')"); n != 2 {
		t.Errorf("page carries the default-project predicate %d time(s), want 2 (outer and inner):\n\t%s", n, page)
	}
	if want := []any{"default", "default"}; !reflect.DeepEqual(pageArgs, want) {
		t.Errorf("page args = %v, want %v", pageArgs, want)
	}
}

// TestGetLabelValuesGroupsInsteadOfDistinct pins CH-2: GROUP BY aggregates a
// LowCardinality column on its dictionary keys, DISTINCT does not.
func TestGetLabelValuesGroupsInsteadOfDistinct(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	params := QueryParams{
		Filters: []Filter{
			{Field: "service", Operator: OpEq, Value: "skipped: this is the requested column"},
			{Field: "level", Operator: OpEq, Value: "error"},
		},
		From: from,
	}
	if _, err := GetLabelValues(scopedContext("atlas"), "service", params); err != nil {
		t.Fatalf("GetLabelValues: %v", err)
	}

	want := "SELECT service FROM monitor.events WHERE project = ? AND level = ? AND timestamp >= ? GROUP BY service ORDER BY service LIMIT 1000"
	if recorder.statements[0] != want {
		t.Errorf("sql =\n\t%s\nwant\n\t%s", recorder.statements[0], want)
	}
	if wantArgs := []any{"atlas", "error", from}; !reflect.DeepEqual(recorder.args[0], wantArgs) {
		t.Errorf("args = %v, want %v", recorder.args[0], wantArgs)
	}
}
