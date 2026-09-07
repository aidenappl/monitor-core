package registry

import (
	"fmt"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/structs"
)

// withZoneSlug points the cache at a fixed zone for one test. env vars are
// package globals and this file runs alongside anything else in the package that
// reads them.
func withZoneSlug(t *testing.T, slug string) {
	t.Helper()
	previous := env.ZoneSlug
	env.ZoneSlug = slug
	t.Cleanup(func() { env.ZoneSlug = previous })
}

// withEmptyCache resets the package-level snapshot around a test. The cache is a
// process-wide global by design — one registry per process — so without this a
// test would inherit whatever the previous one loaded and pass for the wrong
// reason.
func withEmptyCache(t *testing.T) {
	t.Helper()
	mu.Lock()
	previous := current
	current = snapshot{projects: map[string]structs.Project{}}
	mu.Unlock()

	t.Cleanup(func() {
		mu.Lock()
		current = previous
		mu.Unlock()
	})
}

func zoneRow(id int64, slug string) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "slug", "display_name", "status", "created_at", "updated_at"}).
		AddRow(id, slug, strings.ToUpper(slug[:1])+slug[1:], "active", now, now)
}

func projectRowsFor(zoneID int64, slugs ...string) *sqlmock.Rows {
	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at"})
	for i, slug := range slugs {
		rows.AddRow(int64(i+1), zoneID, slug, slug, "active", now, now)
	}
	return rows
}

// TestRefreshCacheLoadsActiveProjectsForTheConfiguredZone is the load contract:
// the zone comes from env.ZoneSlug, the projects come from that zone's id, and
// only the slugs that came back are selectable.
//
// The WithArgs assertions are the point rather than decoration — sqlmock is the
// only thing here that can prove the project query was scoped by zone id at all,
// and a query that dropped it would return the right rows today (one zone) and
// another zone's rows later.
func TestRefreshCacheLoadsActiveProjectsForTheConfiguredZone(t *testing.T) {
	withZoneSlug(t, "trailblaze")
	withEmptyCache(t)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnRows(zoneRow(7, "trailblaze"))
	mock.ExpectQuery("FROM projects WHERE zone_id = . AND status = .").
		WithArgs(int64(7), "active").
		WillReturnRows(projectRowsFor(7, "default", "atlas"))

	if err := refreshCacheFrom(mockDB); err != nil {
		t.Fatalf("refreshCacheFrom: %v", err)
	}

	for _, slug := range []string{"default", "atlas"} {
		if !HasActiveProject(slug) {
			t.Errorf("HasActiveProject(%q) = false, want true", slug)
		}
	}
	for _, slug := range []string{"payments", "", "Atlas"} {
		if HasActiveProject(slug) {
			t.Errorf("HasActiveProject(%q) = true, want false", slug)
		}
	}

	project, ok := LookupProject("atlas")
	if !ok {
		t.Fatal("LookupProject(atlas) returned nothing after a successful load")
	}
	if project.ZoneID != 7 {
		t.Errorf("cached project ZoneID = %d, want 7", project.ZoneID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRefreshCacheKeepsThePreviousCacheOnFailure pins the failure direction. A
// transient MariaDB outage must not make every project unselectable at once —
// that would 400 every session holding a project in its URL, turning a database
// blip into a blank dashboard for everyone.
func TestRefreshCacheKeepsThePreviousCacheOnFailure(t *testing.T) {
	withZoneSlug(t, "trailblaze")
	withEmptyCache(t)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnRows(zoneRow(7, "trailblaze"))
	mock.ExpectQuery("FROM projects WHERE zone_id = . AND status = .").
		WithArgs(int64(7), "active").
		WillReturnRows(projectRowsFor(7, "atlas"))

	if err := refreshCacheFrom(mockDB); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnError(fmt.Errorf("connection reset by peer"))

	if err := refreshCacheFrom(mockDB); err == nil {
		t.Fatal("a failing refresh reported success")
	}
	if !HasActiveProject("atlas") {
		t.Error("a failed refresh emptied the cache; it must keep serving the previous registry")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRefreshCacheRefusesAnUnknownZone covers the row disappearing underneath a
// running process. bootstrap.EnsureZoneAndProject is fail-fast and runs before
// Init, so this can only mean the zone was removed or MON_ZONE_SLUG was moved —
// and emptying the cache in response would turn one bad configuration change
// into every session losing its selected project.
func TestRefreshCacheRefusesAnUnknownZone(t *testing.T) {
	withZoneSlug(t, "trailblaze")
	withEmptyCache(t)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnRows(zoneRow(7, "trailblaze"))
	mock.ExpectQuery("FROM projects WHERE zone_id = . AND status = .").
		WithArgs(int64(7), "active").
		WillReturnRows(projectRowsFor(7, "atlas"))

	if err := refreshCacheFrom(mockDB); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	// No project query is expected on the second pass — sqlmock fails any
	// statement it was not told to expect, so a load that carried on past a
	// missing zone is a test failure rather than a silent empty cache.
	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnRows(sqlmock.NewRows([]string{"id", "slug", "display_name", "status", "created_at", "updated_at"}))

	err = refreshCacheFrom(mockDB)
	if err == nil {
		t.Fatal("refreshCacheFrom succeeded with no zone row, want an error")
	}
	if !strings.Contains(err.Error(), "trailblaze") {
		t.Errorf("error = %v, want it to name the configured zone", err)
	}
	if !HasActiveProject("atlas") {
		t.Error("a missing zone emptied the cache; the previous registry must survive")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLoadProjectsPagesPastTheHouseDefault is the guard on the silent
// truncation this package pages to avoid.
//
// query.ListProjects clamps an unset Limit to db.DEFAULT_LIMIT (50), so a naive
// single-shot load would cache 50 projects and leave everything after them
// permanently unselectable, with no error and nothing in the logs. The proof is
// that a full first page is followed by a second query at the right offset, and
// that a slug from the second page answers true.
func TestLoadProjectsPagesPastTheHouseDefault(t *testing.T) {
	withZoneSlug(t, "trailblaze")
	withEmptyCache(t)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	firstPage := make([]string, PROJECT_PAGE_SIZE)
	for i := range firstPage {
		firstPage[i] = fmt.Sprintf("project-%03d", i)
	}

	mock.ExpectQuery("FROM zones WHERE slug = .").
		WithArgs("trailblaze").
		WillReturnRows(zoneRow(7, "trailblaze"))
	mock.ExpectQuery(fmt.Sprintf("FROM projects WHERE zone_id = . AND status = . ORDER BY slug ASC LIMIT %d$", PROJECT_PAGE_SIZE)).
		WithArgs(int64(7), "active").
		WillReturnRows(projectRowsFor(7, firstPage...))
	mock.ExpectQuery(fmt.Sprintf("FROM projects WHERE zone_id = . AND status = . ORDER BY slug ASC LIMIT %d OFFSET %d", PROJECT_PAGE_SIZE, PROJECT_PAGE_SIZE)).
		WithArgs(int64(7), "active").
		WillReturnRows(projectRowsFor(7, "on-the-second-page"))

	if err := refreshCacheFrom(mockDB); err != nil {
		t.Fatalf("refreshCacheFrom: %v", err)
	}

	if !HasActiveProject("on-the-second-page") {
		t.Error("a project past the first page is not selectable — the load truncated")
	}
	if !HasActiveProject("project-000") {
		t.Error("a project from the first page went missing")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestHasActiveProjectOnAColdCache pins the fail-closed direction. Before the
// first successful load nothing is selectable, so an explicit selection is
// refused rather than trusted — the alternative would trust a client-supplied
// slug during exactly the window in which the server knows least.
func TestHasActiveProjectOnAColdCache(t *testing.T) {
	withEmptyCache(t)

	if HasActiveProject("atlas") {
		t.Error("HasActiveProject answered true on a cache that has never loaded")
	}
	if _, ok := LookupProject("atlas"); ok {
		t.Error("LookupProject answered on a cache that has never loaded")
	}
}
