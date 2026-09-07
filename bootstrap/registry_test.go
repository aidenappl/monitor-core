package bootstrap

import (
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/env"
)

// withSeedSlugs points the bootstrap at fixed slugs for one test and restores
// whatever was there afterwards. env vars are package globals, and this file
// runs alongside anything else in the package that reads them.
func withSeedSlugs(t *testing.T, zone, project string) {
	t.Helper()
	prevZone, prevProject := env.ZoneSlug, env.DefaultProjectSlug
	env.ZoneSlug, env.DefaultProjectSlug = zone, project
	t.Cleanup(func() {
		env.ZoneSlug, env.DefaultProjectSlug = prevZone, prevProject
	})
}

// TestEnsureZoneAndProjectIsNoOpWhenSeeded is the idempotency contract. This
// runs on EVERY boot, so a version that re-inserted — or "repaired" the
// display_name an operator had edited — would undo real state on every restart.
//
// The proof is the absence of an ExpectExec: sqlmock fails any statement it was
// not told to expect, so an INSERT or UPDATE here is a test failure.
func TestEnsureZoneAndProjectIsNoOpWhenSeeded(t *testing.T) {
	withSeedSlugs(t, "trailblaze", "default")

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	now := time.Now()
	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnRows(sqlmock.NewRows([]string{"id", "slug", "display_name", "status", "created_at", "updated_at"}).
			AddRow(int64(1), "trailblaze", "Renamed By An Operator", "active", now, now))
	mock.ExpectQuery("FROM projects WHERE .*").
		WithArgs("default", int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at"}).
			AddRow(int64(1), int64(1), "default", "Default", "active", now, now))

	if err := EnsureZoneAndProject(mockDB); err != nil {
		t.Fatalf("EnsureZoneAndProject: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestEnsureZoneAndProjectRejectsBadSlugsBeforeSQL pins that a misconfigured env
// var stops the boot instead of minting a permanent row. A slug cannot be edited
// afterwards and its name is never reusable, so an INSERT of a typo is not
// recoverable — only retirable.
func TestEnsureZoneAndProjectRejectsBadSlugsBeforeSQL(t *testing.T) {
	tests := []struct {
		name     string
		zone     string
		project  string
		contains string
	}{
		{"reserved zone", "admin", "default", "MON_ZONE_SLUG"},
		{"malformed zone", "Trailblaze", "default", "MON_ZONE_SLUG"},
		{"reserved project", "trailblaze", "settings", "MON_DEFAULT_PROJECT"},
		{"empty project", "trailblaze", "", "MON_DEFAULT_PROJECT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withSeedSlugs(t, tt.zone, tt.project)

			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			err = EnsureZoneAndProject(mockDB)
			if err == nil {
				t.Fatal("EnsureZoneAndProject succeeded, want a validation error")
			}
			if !strings.Contains(err.Error(), tt.contains) {
				t.Errorf("error = %v, want it to name %s", err, tt.contains)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestDefaultDisplayName(t *testing.T) {
	tests := []struct {
		slug string
		want string
	}{
		{"trailblaze", "Trailblaze"},
		{"default", "Default"},
		{"second-zone", "Second Zone"},
		{"team-service-v2", "Team Service V2"},
	}

	for _, tt := range tests {
		if got := defaultDisplayName(tt.slug); got != tt.want {
			t.Errorf("defaultDisplayName(%q) = %q, want %q", tt.slug, got, tt.want)
		}
	}
}
