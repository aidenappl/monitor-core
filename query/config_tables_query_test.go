package query

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The four remaining moved tables share a file the way registry_query_test.go
// covers zones and projects together: notification_channels, service_groups,
// dashboards and saved_views are near-identical CRUD, and asserting the same
// INSERT/SELECT/DELETE shape four times would be four copies of one test.
//
// What is worth pinning is the ONE way they differ from each other, which is not
// obvious from reading any of them alone: whether a JSON-shaped column is
// validated. Migrations 119-122 store the server-parsed blobs in JSON columns and
// 123-124 deliberately leave the client-owned ones as LONGTEXT, so the same
// "looks like JSON" field is refused in one table and accepted in another. That
// asymmetry is a decision, and a future change that "makes them consistent"
// would either start rejecting dashboard configs the UI has always sent, or stop
// catching an unroutable policy.

// TestJSONValidationAppliesOnlyToServerParsedColumns is that asymmetry, stated as
// a pair.
//
// The refusals: Monitor itself Unmarshals these. A malformed `config` is a
// channel that fails inside the notifier's goroutine, where the error is a log
// line; a malformed `services` is a group ResolveServiceGroups silently skips, so
// the policy keyed on it stops routing with nothing to see.
//
// The acceptances: Monitor never parses these. `config` and `query_params` are
// written by monitor-web and read back by monitor-web, and the dashboard handler
// has never required a config at all — so the empty string is a legitimate stored
// value and validating it would break the callers that send it.
func TestJSONValidationAppliesOnlyToServerParsedColumns(t *testing.T) {
	t.Run("notification channel config is refused before SQL", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// No expectations registered, so any statement issued fails the call —
		// which is what proves the guard refused it rather than the column.
		_, err = CreateNotificationChannel(mockDB, "default", CreateNotificationChannelRequest{
			Name: "ops", Type: "slack", Config: "not json",
		})
		if err == nil || !strings.Contains(err.Error(), "config must be valid JSON") {
			t.Errorf("err = %v, want a refusal naming config", err)
		}
	})

	t.Run("service group services is refused before SQL", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		_, err = CreateServiceGroup(mockDB, "default", CreateServiceGroupRequest{
			Name: "payments", Services: "atlas-api, forta-api",
		})
		if err == nil || !strings.Contains(err.Error(), "services must be valid JSON") {
			t.Errorf("err = %v, want a refusal naming services", err)
		}
	})

	t.Run("dashboard config is stored as given", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectExec("INSERT INTO monitor.dashboards").
			WithArgs(sqlmock.AnyArg(), "default", "Overview", "", "", sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		d, err := CreateDashboard(mockDB, "default", CreateDashboardRequest{Name: "Overview"})
		if err != nil {
			t.Fatalf("CreateDashboard: %v", err)
		}
		if d.Config != "" {
			t.Errorf("Config = %q, want the empty string the caller sent", d.Config)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("a dashboard with no config no longer reaches the insert: %v", err)
		}
	})

	t.Run("saved view query_params is stored as given", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectExec("INSERT INTO monitor.saved_views").
			WithArgs(sqlmock.AnyArg(), "default", "5xx only", "level=error&limit=100", "events", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		// A query string, not JSON — which is exactly why this column is not a
		// JSON one. `page` defaults to "events" when the caller omits it.
		v, err := CreateSavedView(mockDB, "default", CreateSavedViewRequest{
			Name: "5xx only", QueryParams: "level=error&limit=100",
		})
		if err != nil {
			t.Fatalf("CreateSavedView: %v", err)
		}
		if v.Page != "events" {
			t.Errorf("Page = %q, want the events default", v.Page)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectations: %v", err)
		}
	})
}

// TestListSavedViewsPageFilter. `?page=` absent means EVERY page — the contract
// the handler has always had. A predicate that leaked in unconditionally would
// bind the empty string and return nothing, which reads as "you have no saved
// views" rather than as an error.
//
// The PROJECT is the opposite kind of parameter and is asserted alongside on
// purpose: it is present in both cases, because an absent tenant is an error
// rather than a wildcard. The two live one line apart in ListSavedViews and
// confusing them is the mistake this pins.
func TestListSavedViewsPageFilter(t *testing.T) {
	t.Run("page given filters", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.saved_views.project = \\? AND monitor.saved_views.page = \\?").
			WithArgs("atlas", "issues").
			WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "query_params", "page", "created_at"}))

		if _, err := ListSavedViews(mockDB, "atlas", "issues"); err != nil {
			t.Fatalf("ListSavedViews: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the page filter is gone: %v", err)
		}
	})

	t.Run("page absent lists everything in the project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("^SELECT (?s).* FROM monitor.saved_views WHERE monitor.saved_views.project = \\? ORDER BY").
			WithArgs("atlas").
			WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "query_params", "page", "created_at"}))

		if _, err := ListSavedViews(mockDB, "atlas", ""); err != nil {
			t.Fatalf("ListSavedViews: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("an absent page acquired a page predicate: %v", err)
		}
	})
}

// TestListsReturnEmptySlicesNotNil. Every one of these handlers used to carry an
// `if list == nil { list = []T{} }` line before responding, because a nil slice
// marshals to `null` and monitor-web maps over the result. The query layer now
// guarantees a non-nil slice, so those lines are gone from the handlers — which
// only stays true while this does.
func TestListsReturnEmptySlicesNotNil(t *testing.T) {
	// Written out rather than table-driven: the four functions return four
	// different slice types, so a table would need an interface{} round trip that
	// makes "is it nil" mean something else.
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM monitor.notification_channels").
		WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "type", "config", "config_enc", "created_at"}))
	channels, err := ListNotificationChannels(mockDB, "default")
	if err != nil {
		t.Fatalf("ListNotificationChannels: %v", err)
	}
	if channels == nil {
		t.Error("ListNotificationChannels returned nil; it marshals to null and the dashboard maps over it")
	}

	mock.ExpectQuery("FROM monitor.service_groups").
		WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "description", "services", "created_at", "updated_at"}))
	groups, err := ListServiceGroups(mockDB, "default")
	if err != nil {
		t.Fatalf("ListServiceGroups: %v", err)
	}
	if groups == nil {
		t.Error("ListServiceGroups returned nil")
	}

	mock.ExpectQuery("FROM monitor.dashboards").
		WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "description", "config", "created_at", "updated_at"}))
	boards, err := ListDashboards(mockDB, "default")
	if err != nil {
		t.Fatalf("ListDashboards: %v", err)
	}
	if boards == nil {
		t.Error("ListDashboards returned nil")
	}

	mock.ExpectQuery("FROM monitor.saved_views").
		WillReturnRows(sqlmock.NewRows([]string{"id", "project", "name", "query_params", "page", "created_at"}))
	views, err := ListSavedViews(mockDB, "default", "")
	if err != nil {
		t.Fatalf("ListSavedViews: %v", err)
	}
	if views == nil {
		t.Error("ListSavedViews returned nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
