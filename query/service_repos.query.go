package query

import (
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

const serviceReposTable = "monitor.service_repos"

var serviceRepoColumns = []string{
	"monitor.service_repos.project",
	"monitor.service_repos.service",
	"monitor.service_repos.provider",
	"monitor.service_repos.owner",
	"monitor.service_repos.repo",
	"monitor.service_repos.default_branch",
	"monitor.service_repos.inserted_at",
	"monitor.service_repos.updated_at",
}

// ErrNoServiceRepoProject is returned by every read and write here that was
// handed an empty project, for the reason ErrNoIssueProject documents.
var ErrNoServiceRepoProject = errors.New("no project supplied for a service repository read")

// scopeServiceRepos attaches the tenancy predicate to a read of
// monitor.service_repos. Table-specific rather than generic, mirroring
// scopeIssues.
//
// THE PROJECT IS HALF THE PRIMARY KEY HERE, not an extra filter (migration 133).
// `service` alone was the key, and a service name is unique only within one
// project's event stream — so an unscoped read by service name does not narrow
// to a row at all, it narrows to a SET of rows, one per tenant that happens to
// run a service by that name. Whichever the scan reached last used to win.
func scopeServiceRepos(q sq.SelectBuilder, project string) (sq.SelectBuilder, error) {
	if project == "" {
		return q, ErrNoServiceRepoProject
	}
	return q.Where(sq.Eq{"monitor.service_repos.project": project}), nil
}

type serviceRepoScanner interface {
	Scan(dest ...interface{}) error
}

func scanServiceRepo(row serviceRepoScanner) (*structs.ServiceRepo, error) {
	var s structs.ServiceRepo
	var defaultBranch sql.NullString

	if err := row.Scan(&s.Project, &s.Service, &s.Provider, &s.Owner, &s.Repo, &defaultBranch,
		&s.InsertedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	if defaultBranch.Valid {
		s.DefaultBranch = &defaultBranch.String
	}
	return &s, nil
}

// GetServiceRepo returns one project's mapping for a service, or (nil, nil) when
// that service is unmapped IN THAT PROJECT. Unmapped is a normal state, not an
// error.
//
// The project is part of the lookup, so a mapping another tenant made for the
// same service name reads as absent rather than as somebody else's repository —
// which, before 133, is what an issue's "view source" link would have pointed at.
func GetServiceRepo(engine db.Queryable, project, service string) (*structs.ServiceRepo, error) {
	q := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		Where(sq.Eq{"monitor.service_repos.service": service}).Limit(1)
	q, err := scopeServiceRepos(q, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	repo, err := scanServiceRepo(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan service repo: %w", err)
	}
	return repo, nil
}

// ListServiceRepos returns one project's mappings, ordered by service name.
func ListServiceRepos(engine db.Queryable, project string) ([]structs.ServiceRepo, error) {
	q := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		OrderBy("monitor.service_repos.service ASC")
	q, err := scopeServiceRepos(q, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	return queryServiceRepos(engine, qStr, args)
}

// ListServiceReposForProject fetches one project's mappings for a set of
// services in ONE query, keyed by service. The issues list uses this shape so
// rendering N issues does not become N round trips.
//
// Keying by service is only sound BECAUSE the read is scoped: within one project
// a service name identifies at most one row (the primary key is (project,
// service)), while across projects it does not.
func ListServiceReposForProject(engine db.Queryable, project string, services []string) (map[string]structs.ServiceRepo, error) {
	if project == "" {
		return nil, ErrNoServiceRepoProject
	}
	if len(services) == 0 {
		return map[string]structs.ServiceRepo{}, nil
	}

	q := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		Where(sq.Eq{"monitor.service_repos.service": services})
	q, err := scopeServiceRepos(q, project)
	if err != nil {
		return nil, err
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	repos, err := queryServiceRepos(engine, qStr, args)
	if err != nil {
		return nil, err
	}

	byService := map[string]structs.ServiceRepo{}
	for _, repo := range repos {
		byService[repo.Service] = repo
	}
	return byService, nil
}

// ListServiceReposFor is the UNSCOPED bulk lookup, and it is deprecated.
//
// ⚠️ IT HAS EXACTLY ONE CALLER — enrichIssues in routes/issues.go, which decorates
// a page of issues with their repositories. That caller holds the project (every
// issue on the page carries one, and the listing that produced them was scoped)
// and should call ListServiceReposForProject; this function exists only until it
// does, and must not acquire a second caller.
//
// WHAT IT DOES ABOUT AMBIGUITY, since it cannot resolve it: a service name mapped
// in more than one project is OMITTED from the result entirely. The obvious
// implementation — last row into the map wins — would attribute one tenant's
// repository to another tenant's issue, putting a foreign owner/repo into the
// issue list and a foreign URL behind "view source". Dropping the entry instead
// degrades to the unmapped state, which every caller already handles: the chip
// simply does not render. A missing link is a nuisance; a link to somebody else's
// repository is a leak, and the two failures are not worth trading.
func ListServiceReposFor(engine db.Queryable, services []string) (map[string]structs.ServiceRepo, error) {
	if len(services) == 0 {
		return map[string]structs.ServiceRepo{}, nil
	}

	qStr, args, err := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		Where(sq.Eq{"monitor.service_repos.service": services}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	repos, err := queryServiceRepos(engine, qStr, args)
	if err != nil {
		return nil, err
	}

	byService := map[string]structs.ServiceRepo{}
	ambiguous := map[string]bool{}
	for _, repo := range repos {
		if _, seen := byService[repo.Service]; seen {
			ambiguous[repo.Service] = true
			continue
		}
		byService[repo.Service] = repo
	}
	for service := range ambiguous {
		delete(byService, service)
	}
	return byService, nil
}

// ListServicesForRepo returns every service built from one repository, ACROSS
// EVERY PROJECT IN THE ZONE, as (project, service) pairs.
//
// This is the one read in this file that is deliberately not scoped, and it is
// not an oversight. Its caller is the GitHub webhook, and a delivery names
// owner/repo and nothing else — GitHub presents no tenant, so there is no project
// to scope by at the moment the lookup happens. Migration 133 leaves
// idx_service_repos_lookup (provider, owner, repo) alone for exactly this read.
//
// It returns PAIRS rather than bare service names because the caller has to
// re-scope afterwards. `api` in project A and `api` in project B are different
// services, and a []string cannot tell them apart — so a webhook for a repo
// mapped in two projects would annotate one tenant's issue with the other
// tenant's service inventory. The pair is what lets the handler use only the
// mappings belonging to the project of the issue it is about to touch.
//
// Several services routinely share a repo (auth-service-v1 and -v2), so this
// returns a set, never a single value.
func ListServicesForRepo(engine db.Queryable, owner, repo string) ([]structs.MappedService, error) {
	q := sq.Select("monitor.service_repos.project", "monitor.service_repos.service").
		From(serviceReposTable).
		Where(sq.Eq{
			"monitor.service_repos.provider": "github",
			"monitor.service_repos.owner":    owner,
			"monitor.service_repos.repo":     repo,
		}).
		OrderBy("monitor.service_repos.project ASC", "monitor.service_repos.service ASC")

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	mapped := []structs.MappedService{}
	for rows.Next() {
		var m structs.MappedService
		if err := rows.Scan(&m.Project, &m.Service); err != nil {
			return nil, fmt.Errorf("failed to scan service: %w", err)
		}
		mapped = append(mapped, m)
	}
	return mapped, rows.Err()
}

// queryServiceRepos runs a prepared service-repo SELECT and scans the rows. The
// column list and the scanner are positional, so every read sharing one function
// is what keeps them from drifting apart into a silent mis-scan.
func queryServiceRepos(engine db.Queryable, qStr string, args []interface{}) ([]structs.ServiceRepo, error) {
	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	repos := []structs.ServiceRepo{}
	for rows.Next() {
		repo, err := scanServiceRepo(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan service repo: %w", err)
		}
		repos = append(repos, *repo)
	}
	return repos, rows.Err()
}

// UpsertServiceRepoRequest maps one service to a repository. The project is a
// separate argument rather than a field: it comes from the credential, never from
// the request body.
type UpsertServiceRepoRequest struct {
	Service       string
	Owner         string
	Repo          string
	DefaultBranch *string
}

// UpsertServiceRepo creates or replaces one project's mapping for a service.
//
// ⚠️ THE ON DUPLICATE KEY TARGET CHANGED WITH THE KEY. Migration 133 replaced the
// primary key `service` with `(project, service)`, and MariaDB fires ON DUPLICATE
// KEY on ANY unique index the row collides with — so the clause below now means
// "this project already maps this service" rather than "anyone maps this
// service". That is the whole point of 133: before it, project B mapping `api`
// silently rewrote project A's row, and every issue in A then linked to B's
// repository.
//
// The other index on this table, idx_service_repos_lookup (provider, owner,
// repo), is a PLAIN KEY and not a unique one (115), so it cannot fire this clause
// — which matters, because several services legitimately share one repository and
// a unique index there would make the second mapping overwrite the first.
//
// `project` and `service` are absent from the UPDATE list deliberately: they are
// the key being matched on, and setting a key column to the value that matched it
// is either a no-op or, if the expressions ever drift, a row moving tenants.
func UpsertServiceRepo(engine db.Queryable, project string, req UpsertServiceRepoRequest) (*structs.ServiceRepo, error) {
	if project == "" {
		return nil, ErrNoServiceRepoProject
	}
	if req.Service == "" {
		return nil, fmt.Errorf("service is required")
	}
	if req.Owner == "" || req.Repo == "" {
		return nil, fmt.Errorf("owner and repo are required")
	}

	const upsert = `INSERT INTO monitor.service_repos (project, service, provider, owner, repo, default_branch)
	VALUES (?, ?, 'github', ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		owner          = VALUES(owner),
		repo           = VALUES(repo),
		default_branch = VALUES(default_branch)`

	if _, err := engine.Exec(upsert, project, req.Service, req.Owner, req.Repo, nullableString(req.DefaultBranch)); err != nil {
		return nil, fmt.Errorf("failed to upsert service repo: %w", err)
	}
	return GetServiceRepo(engine, project, req.Service)
}

// DeleteServiceRepo removes one project's mapping and reports whether it existed.
//
// The project is in the DELETE's own WHERE, not inherited from a preceding read:
// without it, unmapping `api` in one project would unmap it in every project that
// runs a service by that name, and each of those tenants would see their issues
// quietly lose their repository links with no event to explain it.
func DeleteServiceRepo(engine db.Queryable, project, service string) (bool, error) {
	if project == "" {
		return false, ErrNoServiceRepoProject
	}

	qStr, args, err := sq.Delete(serviceReposTable).
		Where(sq.Eq{"service": service, "project": project}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete service repo: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}
