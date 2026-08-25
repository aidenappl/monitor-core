package query

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// issuesTable is schema-qualified because the DSN's default database is
// monitor_auth. The MariaDB `monitor` schema is a different store from the
// ClickHouse database of the same name — this one is reached through db.SQL.
const issuesTable = "monitor.issues"

var issueColumns = []string{
	"monitor.issues.id",
	"monitor.issues.fingerprint",
	"monitor.issues.service",
	"monitor.issues.name",
	"monitor.issues.message",
	"monitor.issues.path",
	"monitor.issues.status",
	"monitor.issues.priority",
	"monitor.issues.title",
	"monitor.issues.assignee_user_id",
	"monitor.issues.occurrence_count",
	"monitor.issues.regression_count",
	"monitor.issues.first_seen",
	"monitor.issues.last_seen",
	"monitor.issues.resolved_at",
	"monitor.issues.regressed_at",
	"monitor.issues.inserted_at",
	"monitor.issues.updated_at",
}

type issueScanner interface {
	Scan(dest ...interface{}) error
}

func scanIssue(row issueScanner) (*structs.Issue, error) {
	var i structs.Issue
	var message, path, title, priority sql.NullString
	var assignee sql.NullInt64
	var firstSeen, lastSeen, resolvedAt, regressedAt sql.NullTime

	err := row.Scan(
		&i.ID, &i.Fingerprint, &i.Service, &i.Name, &message, &path,
		&i.Status, &priority, &title, &assignee,
		&i.OccurrenceCount, &i.RegressionCount,
		&firstSeen, &lastSeen, &resolvedAt, &regressedAt,
		&i.InsertedAt, &i.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if message.Valid {
		i.Message = &message.String
	}
	if path.Valid {
		i.Path = &path.String
	}
	if title.Valid {
		i.Title = &title.String
	}
	if priority.Valid {
		p := structs.IssuePriority(priority.String)
		i.Priority = &p
	}
	if assignee.Valid {
		i.AssigneeUserID = &assignee.Int64
	}
	if firstSeen.Valid {
		i.FirstSeen = &firstSeen.Time
	}
	if lastSeen.Valid {
		i.LastSeen = &lastSeen.Time
	}
	if resolvedAt.Valid {
		i.ResolvedAt = &resolvedAt.Time
	}
	if regressedAt.Valid {
		i.RegressedAt = &regressedAt.Time
	}
	return &i, nil
}

// UpsertIssueOccurrenceRequest is one error event folded into its issue.
type UpsertIssueOccurrenceRequest struct {
	ID          string // deterministic UUIDv5 from the fingerprint
	Fingerprint string
	Service     string
	Name        string
	Message     string
	Path        string
	SeenAt      time.Time
}

// upsertIssueSQL folds one occurrence into an issue in a SINGLE statement.
//
// This is the whole point of moving the issue row to MariaDB. The previous
// ClickHouse implementation read the row, incremented in Go, and wrote a new
// version back — which under concurrent workers lost increments, a defect
// documented in issues/AGENTS.md and only ever mitigated process-locally by a
// 64-way mutex shard. `occurrence_count = occurrence_count + 1` inside ON
// DUPLICATE KEY UPDATE is atomic under a row lock and needs no mutex at all.
//
// ORDER OF ASSIGNMENTS IS LOAD-BEARING. MariaDB evaluates the SET list left to
// right, and a later expression sees values already updated by earlier ones.
// Every clause that tests the PREVIOUS status must therefore come before status
// is itself reassigned, which is why `status` is last. Moving it earlier would
// silently make the regression bookkeeping test the new value against itself.
//
// The transition table (recurrence on an existing issue):
//
//	resolved     -> unresolved, stamp regressed_at, ++regression_count, clear resolved_at
//	in_progress  -> unchanged  (never clobber an agent mid-work)
//	unresolved   -> unchanged
//	ignored      -> unchanged  (mute is mute)
//
// VALUES() is MariaDB's way of referring to the row that would have been
// inserted; the MySQL 8 `AS new` row-alias syntax is not available here.
const upsertIssueSQL = `INSERT INTO monitor.issues
	(id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen)
VALUES (?, ?, ?, ?, ?, ?, 'unresolved', 1, ?, ?)
ON DUPLICATE KEY UPDATE
	occurrence_count = occurrence_count + 1,
	message          = VALUES(message),
	path             = VALUES(path),
	first_seen       = LEAST(COALESCE(first_seen, VALUES(first_seen)), VALUES(first_seen)),
	last_seen        = GREATEST(COALESCE(last_seen, VALUES(last_seen)), VALUES(last_seen)),
	regression_count = IF(status = 'resolved', regression_count + 1, regression_count),
	regressed_at     = IF(status = 'resolved', VALUES(last_seen), regressed_at),
	resolved_at      = IF(status = 'resolved', NULL, resolved_at),
	status           = IF(status = 'resolved', 'unresolved', status)`

// UpsertIssueOccurrence records one occurrence and returns the resulting issue.
//
// To tell whether THIS call caused a regression, compare the returned
// RegressedAt against req.SeenAt. Callers should append the resulting
// `regressed` timeline entry with a dedupe_key derived from that timestamp, so a
// retry or a timestamp collision collapses to one row rather than duplicating.
func UpsertIssueOccurrence(engine db.Queryable, req UpsertIssueOccurrenceRequest) (*structs.Issue, error) {
	var message, path any
	if req.Message != "" {
		message = req.Message
	}
	if req.Path != "" {
		path = req.Path
	}

	_, err := engine.Exec(upsertIssueSQL,
		req.ID, req.Fingerprint, req.Service, req.Name, message, path,
		req.SeenAt, req.SeenAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert issue occurrence: %w", err)
	}

	return GetIssue(engine, req.ID)
}

// GetIssue returns one issue by id, or (nil, nil) when it does not exist.
func GetIssue(engine db.Queryable, id string) (*structs.Issue, error) {
	q := sq.Select(issueColumns...).From(issuesTable).Where(sq.Eq{"monitor.issues.id": id}).Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	issue, err := scanIssue(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan issue: %w", err)
	}
	return issue, nil
}

// GetIssueByFingerprint returns one issue by fingerprint, or (nil, nil).
//
// A real query error is distinguished from "no such issue": treating an error as
// absence is what previously let a transient blip mint a duplicate issue.
func GetIssueByFingerprint(engine db.Queryable, fingerprint string) (*structs.Issue, error) {
	q := sq.Select(issueColumns...).From(issuesTable).
		Where(sq.Eq{"monitor.issues.fingerprint": fingerprint}).Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	issue, err := scanIssue(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan issue: %w", err)
	}
	return issue, nil
}

// IssueSort is an allow-listed sort column. Sort input reaches SQL as an
// identifier, not a bound parameter, so it is mapped through this type rather
// than interpolated — the same injection boundary the ClickHouse data.* fields
// are held to.
type IssueSort string

const (
	IssueSortLastSeen    IssueSort = "last_seen"
	IssueSortFirstSeen   IssueSort = "first_seen"
	IssueSortOccurrences IssueSort = "occurrences"
)

// IsValid reports whether the sort names an allow-listed column. Exported so
// handlers can reject a bad value with a 400 before any query is built.
func (s IssueSort) IsValid() bool {
	_, ok := s.column()
	return ok
}

func (s IssueSort) column() (string, bool) {
	switch s {
	case IssueSortLastSeen, "":
		return "monitor.issues.last_seen", true
	case IssueSortFirstSeen:
		return "monitor.issues.first_seen", true
	case IssueSortOccurrences:
		return "monitor.issues.occurrence_count", true
	}
	return "", false
}

// ListIssuesRequest filters the issue list. Pointer fields distinguish "not
// supplied" from a zero value.
type ListIssuesRequest struct {
	Status         *structs.IssueStatus
	Service        *string
	AssigneeUserID *int64
	// Unassigned filters to issues with no assignee. A separate field rather than
	// a sentinel id: "no assignee" is a different question from "assignee = N",
	// and encoding it as a magic number produces a query that silently matches
	// nothing instead of failing loudly.
	Unassigned bool
	Search     *string
	HasPR      *bool
	From       *time.Time
	To         *time.Time
	Sort       IssueSort
	Descending bool
	Limit      int
	Offset     int
}

func applyIssueFilters(q sq.SelectBuilder, req ListIssuesRequest) sq.SelectBuilder {
	if req.Status != nil {
		q = q.Where(sq.Eq{"monitor.issues.status": string(*req.Status)})
	}
	if req.Service != nil {
		q = q.Where(sq.Eq{"monitor.issues.service": *req.Service})
	}
	if req.Unassigned {
		q = q.Where(sq.Eq{"monitor.issues.assignee_user_id": nil})
	} else if req.AssigneeUserID != nil {
		q = q.Where(sq.Eq{"monitor.issues.assignee_user_id": *req.AssigneeUserID})
	}
	if req.From != nil {
		q = q.Where(sq.GtOrEq{"monitor.issues.last_seen": *req.From})
	}
	if req.To != nil {
		q = q.Where(sq.LtOrEq{"monitor.issues.last_seen": *req.To})
	}
	if req.Search != nil && strings.TrimSpace(*req.Search) != "" {
		// Bound parameters, never interpolation — the search term is attacker-
		// controlled text from a query string.
		like := "%" + strings.TrimSpace(*req.Search) + "%"
		q = q.Where(sq.Or{
			sq.Like{"monitor.issues.name": like},
			sq.Like{"monitor.issues.message": like},
			sq.Like{"monitor.issues.title": like},
			sq.Like{"monitor.issues.path": like},
		})
	}
	if req.HasPR != nil {
		exists := "EXISTS (SELECT 1 FROM monitor.issue_links l WHERE l.issue_id = monitor.issues.id AND l.kind = 'pull_request')"
		if *req.HasPR {
			q = q.Where(exists)
		} else {
			q = q.Where("NOT " + exists)
		}
	}
	return q
}

// ListIssues returns a filtered, sorted page of issues.
func ListIssues(engine db.Queryable, req ListIssuesRequest) ([]structs.Issue, error) {
	sortCol, ok := req.Sort.column()
	if !ok {
		return nil, fmt.Errorf("invalid sort column %q", req.Sort)
	}

	if req.Limit <= 0 || req.Limit > db.MAX_LIMIT {
		req.Limit = db.DEFAULT_LIMIT
	}

	direction := "ASC"
	if req.Descending {
		direction = "DESC"
	}

	q := sq.Select(issueColumns...).From(issuesTable)
	q = applyIssueFilters(q, req)
	// id is a tiebreaker so a page boundary is stable when timestamps collide.
	q = q.OrderBy(sortCol+" "+direction, "monitor.issues.id ASC").Limit(uint64(req.Limit))
	if req.Offset > 0 {
		q = q.Offset(uint64(req.Offset))
	}

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	var issues []structs.Issue
	for rows.Next() {
		issue, err := scanIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan issue: %w", err)
		}
		issues = append(issues, *issue)
	}
	return issues, rows.Err()
}

// CountIssues returns the total matching the same filters, ignoring pagination.
func CountIssues(engine db.Queryable, req ListIssuesRequest) (int, error) {
	q := sq.Select("COUNT(*)").From(issuesTable)
	q = applyIssueFilters(q, req)

	qStr, args, err := q.ToSql()
	if err != nil {
		return 0, fmt.Errorf("failed to build sql query: %w", err)
	}

	var count int
	if err := engine.QueryRow(qStr, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count issues: %w", err)
	}
	return count, nil
}

// UpdateIssueRequest carries the human/agent-settable fields. A nil field is
// left alone; the Clear* flags express "set this back to NULL", which a nil
// pointer cannot.
type UpdateIssueRequest struct {
	Status         *structs.IssueStatus
	Priority       *structs.IssuePriority
	Title          *string
	AssigneeUserID *int64

	ClearPriority bool
	ClearTitle    bool
	ClearAssignee bool
}

// IsEmpty reports whether the request would change nothing.
func (r UpdateIssueRequest) IsEmpty() bool {
	return r.Status == nil && r.Priority == nil && r.Title == nil && r.AssigneeUserID == nil &&
		!r.ClearPriority && !r.ClearTitle && !r.ClearAssignee
}

// UpdateIssue applies a partial update and returns the updated row.
//
// resolved_at is maintained here rather than by the caller so it cannot drift
// from status: moving to resolved stamps it, moving away clears it.
func UpdateIssue(engine db.Queryable, id string, req UpdateIssueRequest) (*structs.Issue, error) {
	if req.IsEmpty() {
		return GetIssue(engine, id)
	}

	u := sq.Update(issuesTable)

	if req.Status != nil {
		if !req.Status.IsValid() {
			return nil, fmt.Errorf("invalid issue status %q", *req.Status)
		}
		u = u.Set("status", string(*req.Status))
		if *req.Status == structs.IssueStatusResolved {
			u = u.Set("resolved_at", sq.Expr("CURRENT_TIMESTAMP(3)"))
		} else {
			u = u.Set("resolved_at", nil)
		}
	}

	switch {
	case req.ClearPriority:
		u = u.Set("priority", nil)
	case req.Priority != nil:
		if !req.Priority.IsValid() {
			return nil, fmt.Errorf("invalid issue priority %q", *req.Priority)
		}
		u = u.Set("priority", string(*req.Priority))
	}

	switch {
	case req.ClearTitle:
		u = u.Set("title", nil)
	case req.Title != nil:
		u = u.Set("title", *req.Title)
	}

	switch {
	case req.ClearAssignee:
		u = u.Set("assignee_user_id", nil)
	case req.AssigneeUserID != nil:
		u = u.Set("assignee_user_id", *req.AssigneeUserID)
	}

	qStr, args, err := u.Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update issue: %w", err)
	}

	return GetIssue(engine, id)
}
