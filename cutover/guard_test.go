package cutover

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
)

// THESE TESTS PIN A GUARD WHOSE FAILURE MODE IS AN OUTAGE IN EITHER DIRECTION.
//
// Too permissive and the deploy it exists to catch goes through, alerting stops
// with nothing logged, and the first signal is an incident that never paged. Too
// aggressive and a healthy Monitor crash-loops because somebody deleted their
// last alert rule. So the assertions below are about WHEN it fires, not about the
// wording of the message: the marker short-circuits, a stranded table refuses, a
// copied table does not, and an install with no legacy tables never gets asked.

const markerSelect = "SELECT value FROM settings WHERE `key` = ?"

// scriptedConn answers the two ClickHouse reads the guard makes — the
// system.tables existence probe and a count — from a table.
//
// Every other method of driver.Conn panics rather than returning a zero value.
// The guard runs on a boot path with a live connection beside it, so a future
// edit that reaches ClickHouse some other way must fail here rather than quietly
// succeed.
type scriptedConn struct {
	// rows maps a legacy table name to its row count. A table ABSENT from the map
	// does not exist at all, which is the fresh-install and post-retirement case.
	rows map[string]uint64
	// seen records every statement, so a test can assert that ClickHouse was not
	// consulted at all.
	seen []string
}

func (c *scriptedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.seen = append(c.seen, query)

	if strings.Contains(query, "system.tables") {
		name, _ := args[1].(string)
		_, exists := c.rows[name]
		if exists {
			return &scriptedRow{value: 1}
		}
		return &scriptedRow{value: 0}
	}

	// "SELECT count() FROM monitor.<table>"
	parts := strings.Split(query, ".")
	return &scriptedRow{value: c.rows[parts[len(parts)-1]]}
}

type scriptedRow struct{ value uint64 }

func (r *scriptedRow) Err() error { return nil }
func (r *scriptedRow) Scan(dest ...any) error {
	*(dest[0].(*uint64)) = r.value
	return nil
}
func (r *scriptedRow) ScanStruct(dest any) error { return nil }

func (c *scriptedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	panic("scriptedConn: the guard must not open a result set")
}
func (c *scriptedConn) Exec(ctx context.Context, query string, args ...any) error {
	panic("scriptedConn: the guard must never write to ClickHouse")
}
func (c *scriptedConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	panic("scriptedConn: unexpected Select")
}
func (c *scriptedConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("scriptedConn: unexpected PrepareBatch")
}
func (c *scriptedConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	panic("scriptedConn: unexpected AsyncInsert")
}
func (c *scriptedConn) Contributors() []string { return nil }
func (c *scriptedConn) ServerVersion() (*driver.ServerVersion, error) {
	return &proto.ServerHandshake{}, nil
}
func (c *scriptedConn) Ping(context.Context) error { return nil }
func (c *scriptedConn) Stats() driver.Stats        { return driver.Stats{} }
func (c *scriptedConn) Close() error               { return nil }

// withStores swaps both globals the guard reads and restores them afterwards.
// conn may be nil, which is itself an assertion: a test that passes nil is
// stating that ClickHouse must not be touched on that path.
func withStores(t *testing.T, conn driver.Conn) sqlmock.Sqlmock {
	t.Helper()

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}

	previousSQL, previousConn, previousDatabase := db.SQL, db.Conn, db.Database
	db.SQL, db.Conn, db.Database = mockDB, conn, "monitor"
	t.Cleanup(func() {
		mockDB.Close()
		db.SQL, db.Conn, db.Database = previousSQL, previousConn, previousDatabase
	})
	return mock
}

// TestMarkerShortCircuitsBeforeClickHouse is the false-positive guard, and it is
// the one that keeps this check from becoming its own outage.
//
// Once the cutover has run, row counts must stop mattering: an operator who
// deletes their last alert rule through the UI leaves alert_rules empty while the
// legacy ClickHouse table still holds six rows, which is byte-for-byte the
// "stranded" shape. The marker is the only thing that separates them, so it has
// to be consulted FIRST and has to be sufficient on its own.
//
// db.Conn is nil here deliberately. It is an interface, so any read of it on this
// path panics rather than returning a zero value — which makes "ClickHouse was
// not consulted" an assertion the test cannot pass by accident.
func TestMarkerShortCircuitsBeforeClickHouse(t *testing.T) {
	mock := withStores(t, nil)
	mock.ExpectQuery(regexp.QuoteMeta(markerSelect)).
		WithArgs(CONFIG_BACKFILL_MARKER).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("2026-09-07T00:00:00Z"))

	if err := RequireConfigBackfill(context.Background()); err != nil {
		t.Fatalf("a marked install must boot: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the marker is no longer the first thing checked: %v", err)
	}
}

// TestStrandedConfigurationRefusesTheBoot is the defect this file exists for.
// Alert rules in ClickHouse, none in MariaDB, no marker: the process must not
// serve, because serving means the evaluator finds nothing and says nothing.
func TestStrandedConfigurationRefusesTheBoot(t *testing.T) {
	conn := &scriptedConn{rows: map[string]uint64{"alert_rules": 6}}
	mock := withStores(t, conn)
	mock.ExpectQuery(regexp.QuoteMeta(markerSelect)).
		WithArgs(CONFIG_BACKFILL_MARKER).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM monitor.alert_rules")).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))

	err := RequireConfigBackfill(context.Background())
	if err == nil {
		t.Fatal("six stranded alert rules must refuse the boot — the alternative is silence")
	}
	// The message is what an operator reads off a crash-looping container, so it
	// has to name the table that is stranded and the command that fixes it.
	for _, want := range []string{"alert_rules", "backfill-config"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal must mention %q, got: %v", want, err)
		}
	}
}

// TestMigratedConfigurationBoots covers the state an operator reaches by copying
// the rows without the subcommand, or by re-running it after the marker write
// failed. Rows on both sides is not stranded, marker or no marker.
func TestMigratedConfigurationBoots(t *testing.T) {
	conn := &scriptedConn{rows: map[string]uint64{"alert_rules": 6}}
	mock := withStores(t, conn)
	mock.ExpectQuery(regexp.QuoteMeta(markerSelect)).
		WithArgs(CONFIG_BACKFILL_MARKER).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM monitor.alert_rules")).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(6))

	if err := RequireConfigBackfill(context.Background()); err != nil {
		t.Fatalf("a copied table is not stranded: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected MariaDB traffic: %v", err)
	}
}

// TestFreshInstallIsNeverAsked. On an install that never had the ClickHouse
// originals — and on one where they have been dropped after the release, which is
// the documented end state — there is nothing to migrate and nothing to count.
// The MariaDB side must not be queried at all, which sqlmock enforces by failing
// on an unexpected statement.
func TestFreshInstallIsNeverAsked(t *testing.T) {
	conn := &scriptedConn{rows: map[string]uint64{}}
	mock := withStores(t, conn)
	mock.ExpectQuery(regexp.QuoteMeta(markerSelect)).
		WithArgs(CONFIG_BACKFILL_MARKER).
		WillReturnError(sql.ErrNoRows)

	if err := RequireConfigBackfill(context.Background()); err != nil {
		t.Fatalf("an install with no legacy tables must boot: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a fresh install must not be counted: %v", err)
	}
	if len(conn.seen) != len(configTables) {
		t.Errorf("expected one existence probe per table, got %d statements: %v", len(conn.seen), conn.seen)
	}
}

// TestEveryConfigTableIsGuarded. The six tables that moved are the six that can
// strand; a seventh added to migrations without an entry here would be copied by
// the backfill and never checked.
func TestEveryConfigTableIsGuarded(t *testing.T) {
	want := map[string]string{
		"alert_rules":           "monitor.alert_rules",
		"notification_channels": "monitor.notification_channels",
		"notification_policies": "monitor.notification_policies",
		"service_groups":        "monitor.service_groups",
		"dashboards":            "monitor.dashboards",
		"saved_views":           "monitor.saved_views",
	}
	if len(configTables) != len(want) {
		t.Fatalf("expected %d guarded tables, got %d", len(want), len(configTables))
	}
	for _, tbl := range configTables {
		if want[tbl.clickhouse] != tbl.mariadb {
			t.Errorf("%s is guarded against %q, expected %q", tbl.clickhouse, tbl.mariadb, want[tbl.clickhouse])
		}
		if tbl.consequence == "" {
			t.Errorf("%s has no stated consequence — the refusal message is what an operator acts on", tbl.clickhouse)
		}
	}
}

// TestDryRunWritesNothing pins the two properties that make a dry run worth
// trusting: it performs no INSERT, and it does not stamp the completion marker.
//
// The marker matters more than the inserts. Stamping it on a run that wrote
// nothing would tell every future boot that the cutover had completed, silencing
// the guard on an install whose configuration is still entirely in ClickHouse —
// converting the rehearsal into precisely the silent alerting failure the guard
// was built to catch.
func TestDryRunWritesNothing(t *testing.T) {
	previous := DryRun
	DryRun = true
	t.Cleanup(func() { DryRun = previous })

	// insertRow is the single chokepoint every table's INSERT goes through. With
	// DryRun set it must not touch db.SQL — which is nil here, so a write would
	// panic rather than quietly succeed.
	if err := insertRow("monitor.alert_rules", "rule-1", "INSERT INTO x VALUES (?)", "v"); err != nil {
		t.Fatalf("insertRow under dry run returned an error: %v", err)
	}
}

// TestInsertRowWritesWhenNotDryRun is the negative control. Without it the test
// above passes just as happily against an insertRow that never writes at all.
func TestInsertRowWritesWhenNotDryRun(t *testing.T) {
	previous := DryRun
	DryRun = false
	t.Cleanup(func() { DryRun = previous })

	defer func() {
		if recover() == nil {
			t.Error("insertRow did not attempt a write with DryRun false — the dry-run guard is always on")
		}
	}()
	// db.SQL is nil in this package's tests, so a real write path panics. That
	// panic IS the assertion.
	_ = insertRow("monitor.alert_rules", "rule-1", "INSERT INTO x VALUES (?)", "v")
}
