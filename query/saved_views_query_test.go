package query

import (
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
)

// savedViewRows mirrors the column ORDER of savedViewColumns, which scanSavedView
// unpacks positionally. Written out rather than derived: a mismatch is a silent
// mis-scan, not a compile error.
func savedViewRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "project", "name", "query_params", "page", "created_at",
	}).AddRow("v-1", "atlas", "5xx only", "level=error", "events", time.Now())
}

// TestSavedViewBuildersRefuseAnEmptyProject. No expectations are registered, so
// any statement issued fails the call — that is what separates "refused before
// SQL" from "returned nothing".
//
// The empty project matters more here than the empty page one line away from it:
// a missing page means EVERY page by design, and reading the tenant the same
// forgiving way is exactly the mistake that would serve every project's saved
// queries — filters that carry other tenants' service names, paths and hosts.
func TestSavedViewBuildersRefuseAnEmptyProject(t *testing.T) {
	tests := []struct {
		name string
		call func(engine db.Queryable) error
	}{
		{name: "list", call: func(e db.Queryable) error { _, err := ListSavedViews(e, "", "events"); return err }},
		{name: "create", call: func(e db.Queryable) error {
			_, err := CreateSavedView(e, "", CreateSavedViewRequest{Name: "5xx only"})
			return err
		}},
		{name: "delete", call: func(e db.Queryable) error { _, err := DeleteSavedView(e, "", "v-1"); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			err = tt.call(mockDB)
			if !errors.Is(err, ErrNoSavedViewProject) {
				t.Fatalf("err = %v, want ErrNoSavedViewProject", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement reached the database anyway: %v", err)
			}
		})
	}
}

// TestCreateSavedViewWritesTheProject pins the project as a bound argument in the
// INSERT. `project` is NOT NULL with no default (migration 132): a value that
// never reaches the statement is errno 1364 at runtime, and the only place that
// can be caught earlier is here.
func TestCreateSavedViewWritesTheProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`INSERT INTO monitor.saved_views \(id,project,name,query_params,page,created_at\)`).
		WithArgs(sqlmock.AnyArg(), "atlas", "5xx only", "level=error", "issues", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	v, err := CreateSavedView(mockDB, "atlas", CreateSavedViewRequest{
		Name: "5xx only", QueryParams: "level=error", Page: "issues",
	})
	if err != nil {
		t.Fatalf("CreateSavedView: %v", err)
	}
	if v.Project != "atlas" {
		t.Errorf("Project = %q, want the project it was filed under", v.Project)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeleteSavedViewCarriesTheProjectInItsOwnWhere.
//
// This delete has no read in front of it to borrow a tenant from — the handler
// deletes by id alone — so without the predicate an id lifted from another
// project's list response would delete that project's view and report success.
func TestDeleteSavedViewCarriesTheProjectInItsOwnWhere(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`DELETE FROM monitor.saved_views WHERE id = \? AND project = \?`).
		WithArgs("v-1", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := DeleteSavedView(mockDB, "atlas", "v-1"); err != nil {
		t.Fatalf("DeleteSavedView: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the DELETE no longer defends itself: %v", err)
	}
}

// TestListSavedViewsScansTheProject closes the loop from column list to struct:
// the scan is positional, so a project read into the wrong field would surface as
// a view whose name is a project slug rather than as any kind of error.
func TestListSavedViewsScansTheProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`FROM monitor.saved_views WHERE monitor.saved_views.project = \?`).
		WithArgs("atlas").
		WillReturnRows(savedViewRows())

	views, err := ListSavedViews(mockDB, "atlas", "")
	if err != nil {
		t.Fatalf("ListSavedViews: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("got %d views, want 1", len(views))
	}
	if views[0].Project != "atlas" || views[0].Name != "5xx only" {
		t.Errorf("scanned %+v, want the project in Project and the name in Name", views[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
