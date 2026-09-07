package query

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// zoneRows mirrors the column ORDER of zoneColumns, which is what scanZone
// unpacks positionally. Written out in full rather than derived from that slice
// on purpose: a column added to one and not the other is a silent mis-scan — a
// URL landing in reachability_detail, say — not a compile error, and deriving the
// fixture would make the two wrong together and still green.
func zoneRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "slug", "display_name", "status",
		"ingest_url", "query_url",
		"reachability", "reachability_detail", "reported_zone", "last_probe_at",
		"created_at", "updated_at",
	}).
		AddRow(int64(1), "trailblaze", "Trailblaze", "active",
			"https://events.example.com", "https://zone.example.com",
			"unknown", "", "", nil, now, now)
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

// TestRetireZoneRefusesWhileProjectsAreLive pins the guard AND where it lives.
//
// The expectation below matches the whole predicate, NOT EXISTS included, so a
// future edit that "simplifies" RetireZone by counting first and updating second
// fails here. That refactor is the dangerous one: it leaves the error message
// intact while opening a window in which a project created between the two
// statements lands in a zone on its way to retired — a tenant whose events keep
// arriving while the zone that would list it is hidden from every switcher.
func TestRetireZoneRefusesWhileProjectsAreLive(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("UPDATE zones SET status = .+ WHERE id = .+ AND status = .+ AND NOT EXISTS \\(SELECT 1 FROM projects").
		WithArgs("deleted", int64(1), "active", "active").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// The count runs only to EXPLAIN a refusal that has already happened.
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(zoneRows())
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(2))

	zone, err := RetireZone(mockDB, 1)
	if err == nil {
		t.Fatal("RetireZone succeeded with live projects, want a refusal")
	}
	if !errors.Is(err, ErrZoneHasActiveProjects) {
		t.Fatalf("RetireZone = %v, want ErrZoneHasActiveProjects so the handler can answer 409", err)
	}
	if zone != nil {
		t.Error("RetireZone returned a zone alongside the refusal")
	}
	if !strings.Contains(err.Error(), "2 active") {
		t.Errorf("RetireZone = %v, want the count in the message — it is the actionable half", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRetireZoneSucceedsWithNoActiveProjects is the negative control for the test
// above. Without it, a RetireZone that refused unconditionally — or one whose
// UPDATE never matched anything — would look exactly as correct.
func TestRetireZoneSucceedsWithNoActiveProjects(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("UPDATE zones SET status").
		WithArgs("deleted", int64(1), "active", "active").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM zones WHERE id = .").WithArgs(int64(1)).WillReturnRows(zoneRows())

	zone, err := RetireZone(mockDB, 1)
	if err != nil {
		t.Fatalf("RetireZone: %v", err)
	}
	if zone == nil {
		t.Fatal("RetireZone returned no zone — the caller needs the row it just changed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRetireProjectIsAnUpdate pins that retiring a project is a status change and
// nothing else. sqlmock fails any statement it was not told to expect, so a
// DELETE issued here would fail the test rather than pass it quietly.
func TestRetireProjectIsAnUpdate(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("UPDATE projects SET status = .+ WHERE id = .+ AND status = .").
		WithArgs("deleted", int64(1), "active").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM projects WHERE id = .").WithArgs(int64(1)).WillReturnRows(projectRows())

	project, err := RetireProject(mockDB, 1)
	if err != nil {
		t.Fatalf("RetireProject: %v", err)
	}
	if project == nil {
		t.Fatal("RetireProject returned no project")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRetireProjectSeparatesMissingFromAlreadyRetired. Both produce zero affected
// rows and they need different answers: 404 sends an operator looking for a row,
// 409 tells them the state changed under them.
func TestRetireProjectSeparatesMissingFromAlreadyRetired(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		rows *sqlmock.Rows
		want error
	}{
		{
			name: "no such project",
			rows: sqlmock.NewRows([]string{"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at"}),
			want: ErrProjectNotFound,
		},
		{
			name: "already retired",
			rows: sqlmock.NewRows([]string{"id", "zone_id", "slug", "display_name", "status", "created_at", "updated_at"}).
				AddRow(int64(1), int64(7), "payments", "Payments", "deleted", now, now),
			want: ErrProjectAlreadyRetired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			mock.ExpectExec("UPDATE projects SET status").
				WithArgs("deleted", int64(1), "active").
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("FROM projects WHERE id = .").WithArgs(int64(1)).WillReturnRows(tt.rows)

			if _, err := RetireProject(mockDB, 1); !errors.Is(err, tt.want) {
				t.Fatalf("RetireProject = %v, want %v", err, tt.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// registryDeletePattern finds any SQL that could remove a row from the two
// registry tables — a squirrel DELETE builder aimed at either, or a hand-written
// DELETE FROM naming one.
var registryDeletePattern = regexp.MustCompile(`(?i)\bdelete\s+from\s+` + "`" + `?"?(zones|projects)\b|\bsq\.Delete\(\s*(zonesTable|projectsTable|"zones"|"projects")`)

// TestNoDeleteReachesTheRegistryTables walks every .go file in the repository and
// fails if any of them can delete a zone or a project.
//
// THIS IS A STRUCTURAL GUARD, not a unit test, and it is here because the failure
// it prevents is invisible at runtime. Removing a row frees its slug, and the
// UNIQUE key that makes reuse impossible is the ONLY thing standing between a
// recycled name and a month of the previous owner's surviving events (30-day TTL)
// plus their permanent daily rollup (no TTL) silently reattaching to the new
// owner. Every reference involved stays valid; nothing errors and nothing logs.
// No test of a delete path could catch that, because the damage is done by the
// NEXT tenant, weeks later.
//
// Retirement is an UPDATE — see RetireZone and RetireProject — and there is no
// legitimate reason for either verb to appear anywhere.
func TestNoDeleteReachesTheRegistryTables(t *testing.T) {
	var offenders []string

	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Hidden directories hold a full copy of this repo (.claude/worktrees) and
		// the object store (.git); scanning them would report the same line twice
		// and, worse, fail on somebody else's branch.
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") && d.Name() != "." && d.Name() != ".." {
				return filepath.SkipDir
			}
			return nil
		}
		// Test files are skipped so this one's own negative control does not count
		// as an offender.
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range registryDeletePattern.FindAllString(string(source), -1) {
			offenders = append(offenders, path+": "+match)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repository: %v", err)
	}

	if len(offenders) > 0 {
		t.Errorf("a DELETE can reach the registry tables, which frees a slug for reuse:\n\t%s",
			strings.Join(offenders, "\n\t"))
	}
}

// TestRegistryDeleteDetectorActuallyDetects is the negative control for the walk
// above. A pattern that matched nothing at all would make that test permanently,
// silently green — which is the exact failure mode a structural guard is most
// prone to.
func TestRegistryDeleteDetectorActuallyDetects(t *testing.T) {
	samples := []string{
		`q := sq.Delete(zonesTable).Where(sq.Eq{"id": id})`,
		`sq.Delete("projects")`,
		"engine.Exec(\"DELETE FROM zones WHERE id = ?\", id)",
		"engine.Exec(\"delete from projects where zone_id = ?\", id)",
	}
	for _, sample := range samples {
		if !registryDeletePattern.MatchString(sample) {
			t.Errorf("registryDeletePattern missed %q — the repository walk proves nothing", sample)
		}
	}

	// And the other direction: the guard must not flag the retirement path, or the
	// pressure to weaken it starts immediately.
	for _, sample := range []string{
		`sq.Update(zonesTable).Set("status", "deleted")`,
		`// There is deliberately no DeleteZone anywhere`,
		`sq.Delete(issuesTable)`,
	} {
		if registryDeletePattern.MatchString(sample) {
			t.Errorf("registryDeletePattern flagged %q, which is not a registry delete", sample)
		}
	}
}
