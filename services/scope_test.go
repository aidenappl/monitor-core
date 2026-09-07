package services

import (
	"context"
	"errors"
	"reflect"
	"strings"
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
type recordingConn struct {
	statements []string
	args       [][]any
}

func (c *recordingConn) record(query string, args ...any) {
	c.statements = append(c.statements, query)
	c.args = append(c.args, args)
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
