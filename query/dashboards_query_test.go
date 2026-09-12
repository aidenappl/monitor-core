package query

import (
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
)

// dashboardRows mirrors the column ORDER of dashboardColumns, which is what
// scanDashboard unpacks positionally. Adding a column to one and not the other
// is a silent mis-scan — a project landing in `name` — rather than a compile
// error, so the fixture is written out in full instead of derived.
func dashboardRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "project", "name", "description", "config", "created_at", "updated_at",
	}).AddRow("d-1", "atlas", "Overview", "", "{}", now, now)
}

// TestDashboardReadsBindTheProject asserts the tenancy predicate is IN THE SQL
// with the right bound argument, rather than applied to the rows afterwards.
//
// The distinction is the whole point of scoping: a filter applied after the read
// still fetches the other tenant's row, and the next person to add a LIMIT, a
// count, or an "is there anything here" check gets the unfiltered answer.
func TestDashboardReadsBindTheProject(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		args    []driver.Value
		call    func(engine db.Queryable) error
	}{
		{
			name:    "list",
			pattern: `FROM monitor.dashboards WHERE monitor.dashboards.project = \?`,
			args:    []driver.Value{"atlas"},
			call: func(engine db.Queryable) error {
				_, err := ListDashboards(engine, "atlas")
				return err
			},
		},
		{
			// The id is not enough on its own: ids travel into monitor-web URLs
			// and shared links, so an id-addressed read has to be scoped too or
			// the boundary rests on ids staying secret.
			name:    "get by id",
			pattern: `WHERE monitor.dashboards.id = \? AND monitor.dashboards.project = \?`,
			args:    []driver.Value{"d-1", "atlas"},
			call: func(engine db.Queryable) error {
				_, err := GetDashboard(engine, "atlas", "d-1")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			mock.ExpectQuery(tt.pattern).WithArgs(tt.args...).WillReturnRows(dashboardRows())

			if err := tt.call(mockDB); err != nil {
				t.Fatalf("call: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("the project predicate is missing or bound to something else: %v", err)
			}
		})
	}
}

// TestDashboardBuildersRefuseAnEmptyProject. No expectations are registered on
// the mock, so any statement issued fails the call — which is what distinguishes
// "refused before SQL" from "returned nothing".
//
// An unscoped read must not be reachable by omitting an argument. For the INSERT
// the stake is different and worse: `project` is NOT NULL with no default
// (migration 131), so an empty one either fails as errno 1364 naming a column
// nobody expected, or — under a lax sql_mode — writes an empty string and mints
// a row that no project can ever see again.
func TestDashboardBuildersRefuseAnEmptyProject(t *testing.T) {
	tests := []struct {
		name string
		call func(engine db.Queryable) error
	}{
		{name: "list", call: func(e db.Queryable) error { _, err := ListDashboards(e, ""); return err }},
		{name: "get", call: func(e db.Queryable) error { _, err := GetDashboard(e, "", "d-1"); return err }},
		{name: "create", call: func(e db.Queryable) error {
			_, err := CreateDashboard(e, "", CreateDashboardRequest{Name: "Overview"})
			return err
		}},
		{name: "update", call: func(e db.Queryable) error {
			_, err := UpdateDashboard(e, "", "d-1", UpdateDashboardRequest{Name: "Renamed"})
			return err
		}},
		{name: "delete", call: func(e db.Queryable) error { _, err := DeleteDashboard(e, "", "d-1"); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			err = tt.call(mockDB)
			if !errors.Is(err, ErrNoDashboardProject) {
				t.Fatalf("err = %v, want ErrNoDashboardProject", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement reached the database anyway: %v", err)
			}
		})
	}
}

// TestCreateDashboardWritesTheProject pins the project as a BOUND ARGUMENT in the
// INSERT, in the position the column list puts it. Phase 0 shipped a Create that
// silently dropped a field it had been handed; a struct field that never reaches
// the statement looks identical from the outside.
func TestCreateDashboardWritesTheProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`INSERT INTO monitor.dashboards \(id,project,name,description,config,created_at,updated_at\)`).
		WithArgs(sqlmock.AnyArg(), "atlas", "Overview", "", "{}", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	d, err := CreateDashboard(mockDB, "atlas", CreateDashboardRequest{Name: "Overview", Config: "{}"})
	if err != nil {
		t.Fatalf("CreateDashboard: %v", err)
	}
	if d.Project != "atlas" {
		t.Errorf("Project = %q, want the project it was filed under", d.Project)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDashboardMutationsCarryTheProjectInTheirOwnWhere.
//
// UpdateDashboard reads before it writes, and it would be easy to call that read
// the tenancy check. It is not: the pair is not atomic, and a future caller that
// skips or reorders the read would rewrite another tenant's dashboard with
// nothing left to stop it. So the UPDATE and the DELETE are asserted to bind the
// project THEMSELVES.
func TestDashboardMutationsCarryTheProjectInTheirOwnWhere(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// The pre-read, the write, and the re-read the caller gets back.
		mock.ExpectQuery(`WHERE monitor.dashboards.id = \? AND monitor.dashboards.project = \?`).
			WithArgs("d-1", "atlas").WillReturnRows(dashboardRows())
		mock.ExpectExec(`UPDATE monitor.dashboards SET name = \? WHERE id = \? AND project = \?`).
			WithArgs("Renamed", "d-1", "atlas").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(`WHERE monitor.dashboards.id = \? AND monitor.dashboards.project = \?`).
			WithArgs("d-1", "atlas").WillReturnRows(dashboardRows())

		if _, err := UpdateDashboard(mockDB, "atlas", "d-1", UpdateDashboardRequest{Name: "Renamed"}); err != nil {
			t.Fatalf("UpdateDashboard: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the UPDATE no longer defends itself: %v", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectExec(`DELETE FROM monitor.dashboards WHERE id = \? AND project = \?`).
			WithArgs("d-1", "atlas").
			WillReturnResult(sqlmock.NewResult(0, 1))

		if _, err := DeleteDashboard(mockDB, "atlas", "d-1"); err != nil {
			t.Fatalf("DeleteDashboard: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the DELETE no longer defends itself: %v", err)
		}
	})
}
