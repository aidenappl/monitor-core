package query

import (
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
)

// serviceRepoRows mirrors the column ORDER of serviceRepoColumns, which
// scanServiceRepo unpacks positionally. The project moved to the FRONT with
// migration 133 (it is the first key column), so a fixture that still led with
// `service` would scan a service name into Project and pass every assertion that
// only checks for "not empty".
func serviceRepoRows(rows ...[]driver.Value) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{
		"project", "service", "provider", "owner", "repo",
		"default_branch", "inserted_at", "updated_at",
	})
	now := time.Now()
	if len(rows) == 0 {
		return r.AddRow("atlas", "api", "github", "acme", "api-server", "main", now, now)
	}
	for _, row := range rows {
		r.AddRow(append(row, now, now)...)
	}
	return r
}

// TestServiceRepoReadsBindTheProject.
//
// The failure this prevents is not a leak of the row's contents so much as a
// MIS-ATTRIBUTION: before migration 133 the primary key was `service` alone, so
// "which repository is `api` built from" had one answer for the whole zone. Two
// tenants both running `api` got each other's repository behind "view source",
// and the link worked, which is what made it invisible.
func TestServiceRepoReadsBindTheProject(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		args    []driver.Value
		call    func(engine db.Queryable) error
	}{
		{
			name:    "get by service",
			pattern: `WHERE monitor.service_repos.service = \? AND monitor.service_repos.project = \?`,
			args:    []driver.Value{"api", "atlas"},
			call: func(e db.Queryable) error {
				_, err := GetServiceRepo(e, "atlas", "api")
				return err
			},
		},
		{
			name:    "list",
			pattern: `FROM monitor.service_repos WHERE monitor.service_repos.project = \?`,
			args:    []driver.Value{"atlas"},
			call: func(e db.Queryable) error {
				_, err := ListServiceRepos(e, "atlas")
				return err
			},
		},
		{
			// The bulk read the issue list is built on. Keying its result map by
			// service is only sound because this predicate is here: within one
			// project a service name is unique, across projects it is not.
			name:    "bulk read for a page of issues",
			pattern: `WHERE monitor.service_repos.service IN \(\?,\?\) AND monitor.service_repos.project = \?`,
			args:    []driver.Value{"api", "web", "atlas"},
			call: func(e db.Queryable) error {
				_, err := ListServiceReposForProject(e, "atlas", []string{"api", "web"})
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

			mock.ExpectQuery(tt.pattern).WithArgs(tt.args...).WillReturnRows(serviceRepoRows())

			if err := tt.call(mockDB); err != nil {
				t.Fatalf("call: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("the project predicate is missing or bound to something else: %v", err)
			}
		})
	}
}

// TestServiceRepoBuildersRefuseAnEmptyProject. No expectations registered, so any
// statement issued fails the call.
func TestServiceRepoBuildersRefuseAnEmptyProject(t *testing.T) {
	tests := []struct {
		name string
		call func(engine db.Queryable) error
	}{
		{name: "get", call: func(e db.Queryable) error { _, err := GetServiceRepo(e, "", "api"); return err }},
		{name: "list", call: func(e db.Queryable) error { _, err := ListServiceRepos(e, ""); return err }},
		{name: "bulk", call: func(e db.Queryable) error {
			_, err := ListServiceReposForProject(e, "", []string{"api"})
			return err
		}},
		{name: "upsert", call: func(e db.Queryable) error {
			_, err := UpsertServiceRepo(e, "", UpsertServiceRepoRequest{Service: "api", Owner: "acme", Repo: "api-server"})
			return err
		}},
		{name: "delete", call: func(e db.Queryable) error { _, err := DeleteServiceRepo(e, "", "api"); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			err = tt.call(mockDB)
			if !errors.Is(err, ErrNoServiceRepoProject) {
				t.Fatalf("err = %v, want ErrNoServiceRepoProject", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement reached the database anyway: %v", err)
			}
		})
	}
}

// TestUpsertServiceRepoTargetsTheCompositeKey is the one that has to be right.
//
// ON DUPLICATE KEY fires on whichever unique index the row collides with, so the
// clause's MEANING changed when migration 133 replaced the primary key `service`
// with `(project, service)`. It now says "this project already maps this
// service"; before, it said "anybody maps this service", and a second tenant
// mapping `api` silently rewrote the first tenant's row.
//
// Three things are asserted, and each has its own failure mode:
//   - the project is inserted, and FIRST, matching the key's column order;
//   - `service` is bound after it, so the pair the key is made of both reach the
//     statement;
//   - the UPDATE half sets only the non-key columns. Setting `project` there
//     would be a no-op on a collision and a tenant move if the expressions ever
//     drifted apart.
func TestUpsertServiceRepoTargetsTheCompositeKey(t *testing.T) {
	// The matcher records every statement on its way past, so the ON DUPLICATE
	// KEY half can be read rather than only pattern-matched. It still delegates
	// to the default regexp matcher, so the expectations below mean what they
	// normally mean.
	var statements []string
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(
		sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
			statements = append(statements, actualSQL)
			return sqlmock.QueryMatcherRegexp.Match(expectedSQL, actualSQL)
		}),
	))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`INSERT INTO monitor.service_repos \(project, service, provider, owner, repo, default_branch\)`).
		WithArgs("atlas", "api", "acme", "api-server", nil).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// The upsert reads the row back, scoped, so the caller is handed what the
	// database now holds rather than what it was asked to write.
	mock.ExpectQuery(`WHERE monitor.service_repos.service = \? AND monitor.service_repos.project = \?`).
		WithArgs("api", "atlas").
		WillReturnRows(serviceRepoRows())

	saved, err := UpsertServiceRepo(mockDB, "atlas", UpsertServiceRepoRequest{
		Service: "api", Owner: "acme", Repo: "api-server",
	})
	if err != nil {
		t.Fatalf("UpsertServiceRepo: %v", err)
	}
	if saved == nil || saved.Project != "atlas" {
		t.Fatalf("saved = %+v, want the mapping read back in project atlas", saved)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	// The statement itself, read rather than only pattern-matched: the ON
	// DUPLICATE KEY half must not touch either key column.
	var insert string
	for _, stmt := range statements {
		if strings.Contains(stmt, "ON DUPLICATE KEY UPDATE") {
			insert = stmt
			break
		}
	}
	if insert == "" {
		t.Fatal("the upsert issued no ON DUPLICATE KEY statement; it is no longer an upsert")
	}

	update := insert[strings.Index(insert, "ON DUPLICATE KEY UPDATE"):]
	for _, key := range []string{"project", "service"} {
		if regexp.MustCompile(`(?m)^\s*` + key + `\s*=`).MatchString(update) {
			t.Errorf("ON DUPLICATE KEY UPDATE assigns %q; it is a key column, not an updatable one:\n%s", key, update)
		}
	}
}

// TestListServicesForRepoStaysCrossProjectAndReturnsPairs.
//
// This lookup MUST NOT acquire a project predicate, and the assertion is written
// that way round on purpose: a GitHub delivery names owner/repo and nothing else,
// so scoping it would mean the webhook resolves nothing at all and every
// mapping-derived annotation silently disappears.
//
// What makes that safe is the shape of the result. Returning (project, service)
// pairs is what lets the caller re-scope; a []string of service names would be
// unusable, because `api` in one project and `api` in another are different
// services that the caller could not tell apart.
func TestListServicesForRepoStaysCrossProjectAndReturnsPairs(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`FROM monitor.service_repos WHERE monitor.service_repos.owner = \? AND monitor.service_repos.provider = \? AND monitor.service_repos.repo = \?`).
		WithArgs("acme", "github", "api-server").
		WillReturnRows(sqlmock.NewRows([]string{"project", "service"}).
			AddRow("atlas", "api").
			AddRow("johnnies", "api-gateway"))

	mapped, err := ListServicesForRepo(mockDB, "acme", "api-server")
	if err != nil {
		t.Fatalf("ListServicesForRepo: %v", err)
	}
	if len(mapped) != 2 {
		t.Fatalf("got %d mappings, want both projects' rows", len(mapped))
	}
	if mapped[0].Project != "atlas" || mapped[0].Service != "api" {
		t.Errorf("mapped[0] = %+v, want atlas/api", mapped[0])
	}
	if mapped[1].Project != "johnnies" || mapped[1].Service != "api-gateway" {
		t.Errorf("mapped[1] = %+v, want johnnies/api-gateway", mapped[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestListServiceReposForOmitsAmbiguousServices covers the deprecated unscoped
// bulk read, whose one caller (enrichIssues, routes/issues.go) has not been moved
// to the scoped variant yet.
//
// A service name mapped in two projects has no correct answer without a tenant,
// and the tempting implementation — last row into the map wins — picks one at
// random and puts a foreign owner/repo on somebody's issue. Dropping the entry
// degrades to "unmapped", which every caller already renders correctly.
func TestListServiceReposForOmitsAmbiguousServices(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`FROM monitor.service_repos WHERE monitor.service_repos.service IN \(\?,\?\)`).
		WithArgs("api", "web").
		WillReturnRows(serviceRepoRows(
			[]driver.Value{"atlas", "api", "github", "acme", "api-server", "main"},
			[]driver.Value{"johnnies", "api", "github", "other", "their-api", "main"},
			[]driver.Value{"atlas", "web", "github", "acme", "web-app", "main"},
		))

	byService, err := ListServiceReposFor(mockDB, []string{"api", "web"})
	if err != nil {
		t.Fatalf("ListServiceReposFor: %v", err)
	}
	if _, ok := byService["api"]; ok {
		t.Errorf("api resolved to %+v; a service mapped in two projects must resolve to nothing rather than to one of them", byService["api"])
	}
	if repo, ok := byService["web"]; !ok || repo.Repo != "web-app" {
		t.Errorf("web = %+v (present=%v), want the single unambiguous mapping", repo, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeleteServiceRepoCarriesTheProjectInItsOwnWhere. Without the predicate,
// unmapping `api` in one project unmaps it in every project running a service by
// that name — and those tenants see their issues lose their repository links with
// no event anywhere to explain it.
func TestDeleteServiceRepoCarriesTheProjectInItsOwnWhere(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec(`DELETE FROM monitor.service_repos WHERE project = \? AND service = \?`).
		WithArgs("atlas", "api").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := DeleteServiceRepo(mockDB, "atlas", "api"); err != nil {
		t.Fatalf("DeleteServiceRepo: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the DELETE no longer defends itself: %v", err)
	}
}
