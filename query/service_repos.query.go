package query

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

const serviceReposTable = "monitor.service_repos"

var serviceRepoColumns = []string{
	"monitor.service_repos.service",
	"monitor.service_repos.provider",
	"monitor.service_repos.owner",
	"monitor.service_repos.repo",
	"monitor.service_repos.default_branch",
	"monitor.service_repos.inserted_at",
	"monitor.service_repos.updated_at",
}

type serviceRepoScanner interface {
	Scan(dest ...interface{}) error
}

func scanServiceRepo(row serviceRepoScanner) (*structs.ServiceRepo, error) {
	var s structs.ServiceRepo
	var defaultBranch sql.NullString

	if err := row.Scan(&s.Service, &s.Provider, &s.Owner, &s.Repo, &defaultBranch,
		&s.InsertedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	if defaultBranch.Valid {
		s.DefaultBranch = &defaultBranch.String
	}
	return &s, nil
}

// GetServiceRepo returns the mapping for a service, or (nil, nil) when the
// service is unmapped. Unmapped is a normal state, not an error.
func GetServiceRepo(engine db.Queryable, service string) (*structs.ServiceRepo, error) {
	q := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		Where(sq.Eq{"monitor.service_repos.service": service}).Limit(1)

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

// ListServiceRepos returns every mapping, ordered by service name.
func ListServiceRepos(engine db.Queryable) ([]structs.ServiceRepo, error) {
	q := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		OrderBy("monitor.service_repos.service ASC")

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

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

// ListServiceReposFor fetches mappings for a set of services in ONE query, keyed
// by service. The issues list uses this so rendering N issues does not become N
// round trips.
func ListServiceReposFor(engine db.Queryable, services []string) (map[string]structs.ServiceRepo, error) {
	if len(services) == 0 {
		return map[string]structs.ServiceRepo{}, nil
	}

	q := sq.Select(serviceRepoColumns...).From(serviceReposTable).
		Where(sq.Eq{"monitor.service_repos.service": services})

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	byService := map[string]structs.ServiceRepo{}
	for rows.Next() {
		repo, err := scanServiceRepo(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan service repo: %w", err)
		}
		byService[repo.Service] = *repo
	}
	return byService, rows.Err()
}

// ListServicesForRepo returns every service built from one repository — the
// reverse lookup, used to answer "which services does this PR affect".
//
// Several services routinely share a repo (auth-service-v1 and -v2), so this
// returns a set, never a single value.
func ListServicesForRepo(engine db.Queryable, owner, repo string) ([]string, error) {
	q := sq.Select("monitor.service_repos.service").From(serviceReposTable).
		Where(sq.Eq{
			"monitor.service_repos.provider": "github",
			"monitor.service_repos.owner":    owner,
			"monitor.service_repos.repo":     repo,
		}).
		OrderBy("monitor.service_repos.service ASC")

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	services := []string{}
	for rows.Next() {
		var service string
		if err := rows.Scan(&service); err != nil {
			return nil, fmt.Errorf("failed to scan service: %w", err)
		}
		services = append(services, service)
	}
	return services, rows.Err()
}

// UpsertServiceRepoRequest maps one service to a repository.
type UpsertServiceRepoRequest struct {
	Service       string
	Owner         string
	Repo          string
	DefaultBranch *string
}

// UpsertServiceRepo creates or replaces a service's mapping.
func UpsertServiceRepo(engine db.Queryable, req UpsertServiceRepoRequest) (*structs.ServiceRepo, error) {
	if req.Service == "" {
		return nil, fmt.Errorf("service is required")
	}
	if req.Owner == "" || req.Repo == "" {
		return nil, fmt.Errorf("owner and repo are required")
	}

	const upsert = `INSERT INTO monitor.service_repos (service, provider, owner, repo, default_branch)
	VALUES (?, 'github', ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		owner          = VALUES(owner),
		repo           = VALUES(repo),
		default_branch = VALUES(default_branch)`

	if _, err := engine.Exec(upsert, req.Service, req.Owner, req.Repo, nullableString(req.DefaultBranch)); err != nil {
		return nil, fmt.Errorf("failed to upsert service repo: %w", err)
	}
	return GetServiceRepo(engine, req.Service)
}

// DeleteServiceRepo removes a mapping and reports whether it existed.
func DeleteServiceRepo(engine db.Queryable, service string) (bool, error) {
	qStr, args, err := sq.Delete(serviceReposTable).Where(sq.Eq{"service": service}).ToSql()
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
