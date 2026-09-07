package query

import (
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func zoneRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "slug", "display_name", "status", "created_at", "updated_at"}).
		AddRow(int64(1), "trailblaze", "Trailblaze", "active", now, now)
}

func projectRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at"}).
		AddRow(int64(1), int64(7), "default", "Default", "active", now, now)
}

// TestCreateZoneValidatesBeforeSQL pins that a bad slug never reaches the
// database. The CHECK constraint in 116 would catch a malformed one, but not a
// RESERVED one — that rule is Go's alone, and a reserved slug that got inserted
// could not be fixed afterwards, because slugs are immutable.
//
// sqlmock has no expectations registered here, so any statement issued fails the
// call; asserting on the error TEXT is what distinguishes "refused by
// ValidateSlug" from "refused by the mock".
func TestCreateZoneValidatesBeforeSQL(t *testing.T) {
	tests := []struct {
		name     string
		slug     string
		contains string
	}{
		{"reserved", "settings", "reserved"},
		{"uppercase is refused, not folded", "Trailblaze", "lowercase"},
		{"trailing hyphen", "abc-", "lowercase"},
		{"too short", "ab", "between"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, _, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			zone, err := CreateZone(mockDB, CreateZoneRequest{Slug: tt.slug, DisplayName: "X"})
			if err == nil {
				t.Fatalf("CreateZone(%q) succeeded, want a validation error", tt.slug)
			}
			if zone != nil {
				t.Errorf("CreateZone(%q) returned a zone alongside an error", tt.slug)
			}
			if !strings.Contains(err.Error(), tt.contains) {
				t.Errorf("CreateZone(%q) = %v, want an error mentioning %q (a validation failure, not a mock failure)", tt.slug, err, tt.contains)
			}
		})
	}
}

// TestCreateProjectValidatesBeforeSQL is the same guard on the project side.
func TestCreateProjectValidatesBeforeSQL(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	if _, err := CreateProject(mockDB, CreateProjectRequest{ZoneID: 1, Slug: "dashboard", DisplayName: "X"}); err == nil {
		t.Fatal("CreateProject with a reserved slug succeeded, want an error")
	} else if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("CreateProject = %v, want a reserved-slug error", err)
	}

	// A project with no zone is not a project. Caught before the FK so the error
	// names the field rather than surfacing errno 1452.
	if _, err := CreateProject(mockDB, CreateProjectRequest{ZoneID: 0, Slug: "payments", DisplayName: "X"}); err == nil {
		t.Fatal("CreateProject with zone_id 0 succeeded, want an error")
	}
}

// TestListZonesDefaultsExcludeDeleted pins both halves of the list contract: a
// retired zone is not offered by default, and the limit is clamped to the house
// default rather than passed through.
func TestListZonesDefaultsExcludeDeleted(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM zones WHERE status = . ORDER BY slug ASC LIMIT 50").
		WithArgs("active").
		WillReturnRows(zoneRows())

	zones, err := ListZones(mockDB, ListZonesRequest{Limit: 100000})
	if err != nil {
		t.Fatalf("ListZones: %v", err)
	}
	if len(zones) != 1 {
		t.Fatalf("got %d zones, want 1", len(zones))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestListZonesIncludeDeleted drops the status filter entirely — the operator
// asked to see retired zones, so there is no WHERE clause and no bound argument.
func TestListZonesIncludeDeleted(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM zones ORDER BY slug ASC LIMIT 50").WillReturnRows(zoneRows())

	if _, err := ListZones(mockDB, ListZonesRequest{IncludeDeleted: true}); err != nil {
		t.Fatalf("ListZones: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestGetProjectBySlugScopesByZone is the guard on the failure that only appears
// once a second zone exists. A project slug is unique WITHIN a zone, so a lookup
// that dropped zone_id would still return a row today — the right one, by
// accident — and start returning another zone's project later, with nothing
// about the change to point at.
func TestGetProjectBySlugScopesByZone(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM projects WHERE .*zone_id.*").
		WithArgs("default", int64(7)).
		WillReturnRows(projectRows())

	project, err := GetProjectBySlug(mockDB, 7, "default")
	if err != nil {
		t.Fatalf("GetProjectBySlug: %v", err)
	}
	if project == nil {
		t.Fatal("GetProjectBySlug returned nil, want the seeded project")
	}
	if project.ZoneID != 7 {
		t.Errorf("project.ZoneID = %d, want 7", project.ZoneID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
