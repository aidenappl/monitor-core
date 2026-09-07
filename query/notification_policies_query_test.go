package query

import (
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// THIS FILE IS THE POINT OF THE MOVE.
//
// notification_policies came out of a ClickHouse ReplacingMergeTree that could
// not express "no two policies share a position". Two policies at one position
// give alerts/router.go a non-deterministic first match, so `continue_matching`
// short-circuits — or does not — depending on which of the tied rows the scan
// happens to return first. Nothing errors and nothing logs; alerts simply go
// somewhere else, sometimes.
//
// Migration 121 adds UNIQUE (position). These tests pin the only two write paths
// that can satisfy it, because both are the kind of code that looks correct while
// being wrong: an ordinary loop assigning 1..N violates the key on almost every
// permutation, and the obvious fix — dropping the key — puts the defect back.

// TestReorderVacatesPositionsBeforeAssigning is the load-bearing one.
//
// InnoDB validates a unique index PER ROW, not per statement and not at commit —
// MariaDB has no deferrable constraints — so moving the policy at position 3 to
// position 1 collides with whatever still holds 1. The reorder therefore has to
// empty the positive range first, which the single negating UPDATE does: negation
// is injective and its image is disjoint from its domain, so it can never violate
// the key, and afterwards every target 1..N is free.
//
// The assertion is the ORDER of the statements, in one transaction. If a future
// edit drops the vacate step, or moves it after the assignments, or takes the
// whole thing out of its transaction, this fails — and each of those is a change
// that compiles, reads fine, and breaks the first time somebody drags a policy.
func TestReorderVacatesPositionsBeforeAssigning(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM monitor.notification_policies ORDER BY position ASC, id ASC FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("a").AddRow("b").AddRow("c"))
	mock.ExpectExec("^UPDATE monitor.notification_policies SET position = -position$").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec("UPDATE monitor.notification_policies SET position = \\? WHERE id = \\?").
		WithArgs(int64(1), "c").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE monitor.notification_policies SET position = \\? WHERE id = \\?").
		WithArgs(int64(2), "a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE monitor.notification_policies SET position = \\? WHERE id = \\?").
		WithArgs(int64(3), "b").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// "b" is deliberately not named: a partial list is well-defined — the named
	// ids lead, everything else keeps its relative order and follows — so the
	// expectations above also pin that rule.
	if err := ReorderNotificationPolicies(mockDB, []string{"c", "a"}); err != nil {
		t.Fatalf("ReorderNotificationPolicies: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the reorder is no longer vacate-then-assign inside one transaction: %v", err)
	}
}

// TestVacateStatementCannotCollide states the property the SQL relies on, in the
// SQL itself: the staging pass must move positions into a range no live position
// occupies. A rewrite to `position = position + 1000` would look equivalent and
// is not — it collides with any policy already at a position 1000 higher.
func TestVacateStatementCannotCollide(t *testing.T) {
	if !strings.Contains(vacatePositionsSQL, "position = -position") {
		t.Errorf("the staging pass must negate: %q", vacatePositionsSQL)
	}
	if strings.Contains(vacatePositionsSQL, "WHERE") {
		t.Error("the staging pass must cover EVERY row — a WHERE clause leaves live positive positions for phase two to collide with")
	}
	if strings.Contains(vacatePositionsSQL, ";") {
		t.Error("the staging pass must be a single statement")
	}
}

// TestReorderRollsBackOnFailure pins that a failure part-way through leaves the
// old order intact. Without the transaction this is the state the ClickHouse
// implementation could reach and could not leave: half the list renumbered, with
// duplicate positions by construction and no way back.
func TestReorderRollsBackOnFailure(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM monitor.notification_policies").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("a").AddRow("b"))
	mock.ExpectExec("SET position = -position").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("SET position = \\? WHERE id = \\?").
		WithArgs(int64(1), "b").WillReturnError(errSimulated)
	mock.ExpectRollback()

	if err := ReorderNotificationPolicies(mockDB, []string{"b", "a"}); err == nil {
		t.Fatal("expected the reorder to fail")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a failed reorder did not roll back: %v", err)
	}
}

// TestReorderRejectsBadIDs. Skipping an unknown id, or honouring a duplicate,
// would produce an order the caller did not ask for — and the response is a
// success either way, so nothing downstream could tell.
func TestReorderRejectsBadIDs(t *testing.T) {
	tests := []struct {
		name string
		ids  []string
		want string
	}{
		{name: "unknown id", ids: []string{"a", "ghost"}, want: "not found"},
		{name: "duplicate id", ids: []string{"a", "a"}, want: "more than once"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			mock.ExpectBegin()
			mock.ExpectQuery("SELECT id FROM monitor.notification_policies").
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("a").AddRow("b"))
			mock.ExpectRollback()

			err = ReorderNotificationPolicies(mockDB, tt.ids)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
			// No UPDATE was expected, so reaching one fails the call rather than
			// silently reordering on bad input.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("bad input still wrote: %v", err)
			}
		})
	}
}

// TestReorderRefusesWithoutATransaction. A *sql.Tx cannot Begin, which stands in
// for any handle that cannot give this function its own transaction. It must
// refuse rather than fall back to a sequential loop the way
// bootstrap.EnsureAdminUser does — that fallback is precisely the defect.
func TestReorderRefusesWithoutATransaction(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	tx, err := mockDB.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	if err := ReorderNotificationPolicies(tx, []string{"a"}); err == nil {
		t.Fatal("expected a refusal when the handle cannot start a transaction")
	} else if !strings.Contains(err.Error(), "transaction") {
		t.Errorf("error = %q, want it to say a transaction is required", err)
	}
}

// TestCreateAllocatesPositionUnderLock pins the OTHER half of the ordering fix.
// getNextPosition was a bare `max(position) + 1` in Go, so two concurrent creates
// read the same maximum and both wrote it. The locking read and the insert now
// share one transaction, so the second caller waits and gets the next number
// rather than a duplicate-key error.
func TestCreateAllocatesPositionUnderLock(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT position FROM monitor.notification_policies ORDER BY position DESC LIMIT 1 FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"position"}).AddRow(int64(4)))
	mock.ExpectExec("INSERT INTO monitor.notification_policies").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	created, err := CreateNotificationPolicy(mockDB, CreateNotificationPolicyRequest{Name: "Payments"})
	if err != nil {
		t.Fatalf("CreateNotificationPolicy: %v", err)
	}
	if created.Position != 5 {
		t.Errorf("Position = %d, want 5", created.Position)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("position allocation is no longer a locking read inside the insert's transaction: %v", err)
	}
}

// TestCreateDefaultsJSONFields. The JSON columns are NOT NULL with no default, so
// an omitted matchers/channel_ids has to become "{}"/"[]" here or the insert
// fails on a field the caller never sent.
func TestCreateDefaultsJSONFields(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT position FROM monitor.notification_policies").
		WillReturnRows(sqlmock.NewRows([]string{"position"}))
	mock.ExpectExec("INSERT INTO monitor.notification_policies").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	created, err := CreateNotificationPolicy(mockDB, CreateNotificationPolicyRequest{Name: "Payments"})
	if err != nil {
		t.Fatalf("CreateNotificationPolicy: %v", err)
	}
	if created.Matchers != "{}" || created.ChannelIDs != "[]" {
		t.Errorf("matchers/channel_ids = %q/%q, want {}/[]", created.Matchers, created.ChannelIDs)
	}
	// An empty table means position 1, which is also the seeder's first slot —
	// the two are kept apart by the seeder only ever running on an empty table.
	if created.Position != 1 {
		t.Errorf("Position = %d, want 1 on an empty table", created.Position)
	}
}

// TestSeedDefaultsOnlyWhenTableIsEmpty is the gate that keeps the seeder and
// cutover.BackfillConfig from both populating this table.
//
// The old gate was "no row has is_default = 1", which is a proxy for "this is a
// fresh install" and holds only while whatever filled the table happened to carry
// that flag. cutover.BackfillConfig copies the four existing ClickHouse defaults
// across with their own ids, so a backfilled install must never seed on top of
// them — and emptiness says that directly rather than by proxy.
func TestSeedDefaultsOnlyWhenTableIsEmpty(t *testing.T) {
	t.Run("empty table seeds four", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM monitor.notification_policies").
			WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(int64(0)))
		// One statement, four rows: the set is all-or-nothing, so two processes
		// booting together cannot interleave into a half-seeded list.
		mock.ExpectExec("^INSERT INTO monitor.notification_policies .* VALUES \\(.*\\),\\(.*\\),\\(.*\\),\\(.*\\)$").
			WillReturnResult(sqlmock.NewResult(0, 4))

		if err := SeedDefaultNotificationPolicies(mockDB); err != nil {
			t.Fatalf("SeedDefaultNotificationPolicies: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the seed is no longer one four-row statement: %v", err)
		}
	})

	t.Run("non-empty table seeds nothing", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// A table holding rows that are NOT defaults still blocks the seed. Under
		// the old is_default gate this case would have seeded on top of them.
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM monitor.notification_policies").
			WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(int64(3)))

		if err := SeedDefaultNotificationPolicies(mockDB); err != nil {
			t.Fatalf("SeedDefaultNotificationPolicies: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the seed wrote into a non-empty table: %v", err)
		}
	})
}

// TestPolicySelectIsOrderedByPosition. The list is not a display order — it is
// the routing order alerts/router.go walks top-down, stopping at the first match.
// Sorted any other way, alerts route to whichever policy happens to come first.
func TestPolicySelectIsOrderedByPosition(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("ORDER BY monitor.notification_policies.position ASC").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "description", "position", "matchers", "channel_ids",
			"continue_matching", "repeat_interval_seconds", "enabled", "is_default",
			"created_at", "updated_at",
		}))

	if _, err := ListNotificationPolicies(mockDB); err != nil {
		t.Fatalf("ListNotificationPolicies: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("policies are no longer read in routing order: %v", err)
	}
}

// TestPolicyUpdateNeverTouchesPosition. Position is owned by
// ReorderNotificationPolicies, which is the only path that can move a policy
// without violating the unique key. An UPDATE that set it directly would fail
// against any occupied slot — and succeed, wrongly, against a free one.
func TestPolicyUpdateNeverTouchesPosition(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	rows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{
			"id", "name", "description", "position", "matchers", "channel_ids",
			"continue_matching", "repeat_interval_seconds", "enabled", "is_default",
			"created_at", "updated_at",
		}).AddRow("p-1", "Payments", "", int64(2), "{}", "[]", false, int64(0), true, false, testTime, testTime)
	}

	mock.ExpectQuery("SELECT .* FROM monitor.notification_policies WHERE").WillReturnRows(rows())
	mock.ExpectExec("^UPDATE monitor.notification_policies SET continue_matching = \\?, enabled = \\?, name = \\? WHERE id = \\?$").
		WithArgs(false, true, "Payments EU", "p-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.notification_policies WHERE").WillReturnRows(rows())

	if _, err := UpdateNotificationPolicy(mockDB, "p-1", UpdateNotificationPolicyRequest{
		Name: "Payments EU", Enabled: true,
	}); err != nil {
		t.Fatalf("UpdateNotificationPolicy: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the update no longer leaves position alone: %v", err)
	}
}

// TestPolicyInsertColumnsMatchScanOrder guards the correspondence that is
// invisible to the compiler. Every column here is written positionally and read
// back positionally, and most are strings, so transposing two of them produces no
// error at all — just a policy whose description is its channel list.
func TestPolicyInsertColumnsMatchScanOrder(t *testing.T) {
	if len(notificationPolicyInsertColumns) != len(notificationPolicyColumns) {
		t.Fatalf("insert list has %d columns, select list has %d",
			len(notificationPolicyInsertColumns), len(notificationPolicyColumns))
	}
	qualified := regexp.MustCompile(`^monitor\.notification_policies\.`)
	for i, col := range notificationPolicyColumns {
		want := qualified.ReplaceAllString(col, "")
		if notificationPolicyInsertColumns[i] != want {
			t.Errorf("column %d: insert says %q, select says %q",
				i, notificationPolicyInsertColumns[i], want)
		}
	}
}
