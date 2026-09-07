package query

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// ErrNoIssueProject is returned by every read in this file that was handed an
// empty project. It is the MariaDB counterpart of scope.ErrNoProject and exists
// for the same reason: "I do not know whose issues these are" must be an error a
// caller has to handle, never a query that quietly returns all of them.
//
// It is declared here rather than imported from scope/ because that package owns
// the ClickHouse predicate and this one is a plain column match — sharing the
// sentinel would suggest a shared mechanism that does not exist. The rule they
// enforce is the same; the SQL is not.
var ErrNoIssueProject = errors.New("no project supplied for an issue read")

// scopeIssues attaches the tenancy predicate to a read of monitor.issues.
//
// WHY THIS EXISTS AT ALL, given the issue id is already a UUIDv5 over a
// fingerprint that contains the project. Because the id is only unguessable, not
// unreachable: a caller does not have to derive one, it can read one off the
// LIST. Before this predicate, GET /v1/issues returned every project's rows —
// and an issue row is not metadata, it carries `message`, the error text lifted
// verbatim off the event by issues.extractMessage, plus the service, the path,
// the occurrence count and the first/last-seen window. That is the substance of
// another tenant's failures, served through the endpoint the dashboard's front
// page is built on, with the raw events correctly scoped the whole time.
//
// Every id-addressed read is scoped too, not just the list. Narrowing only the
// list would leave the boundary resting on ids staying secret, and ids travel:
// into monitor-web URLs, into alert payloads, into comments and GitHub links.
// A tenancy check that a shared URL defeats is not a tenancy check.
func scopeIssues(q sq.SelectBuilder, project string) (sq.SelectBuilder, error) {
	if project == "" {
		return q, ErrNoIssueProject
	}
	return q.Where(sq.Eq{"monitor.issues.project": project}), nil
}

// issuesTable is schema-qualified because the DSN's default database is
// monitor_auth. The MariaDB `monitor` schema is a different store from the
// ClickHouse database of the same name — this one is reached through db.SQL.
const issuesTable = "monitor.issues"

var issueColumns = []string{
	"monitor.issues.id",
	"monitor.issues.fingerprint",
	"monitor.issues.project",
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
		&i.ID, &i.Fingerprint, &i.Project, &i.Service, &i.Name, &message, &path,
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
	// Project is the tenant this issue belongs to. It is not an independent
	// field: the caller must pass the same value that went into Fingerprint, or
	// the stored column will describe a different tenant from the one the row's
	// identity is derived from. issues.processError resolves it once for exactly
	// that reason.
	Project string
	Service string
	Name    string
	Message string
	Path    string
	SeenAt  time.Time
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
//
// `project` is INSERTED but never UPDATED, and the asymmetry is deliberate. An
// issue's project is not an attribute that can change — it is part of what
// identifies the row, baked into the fingerprint the primary key is derived
// from, so any row this statement collides with necessarily already holds the
// same project. Assigning it in the SET list would therefore be a guaranteed
// no-op on the only path that can reach it, while turning the one statement that
// folds tenant data into one that is also capable of REWRITING a tenant. There
// is no input that should be able to move an issue between projects; the way to
// express that in SQL is to never write the column after the insert.
const upsertIssueSQL = `INSERT INTO monitor.issues
	(id, fingerprint, project, service, name, message, path, status, occurrence_count, first_seen, last_seen)
VALUES (?, ?, ?, ?, ?, ?, ?, 'unresolved', 1, ?, ?)
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
		req.ID, req.Fingerprint, req.Project, req.Service, req.Name, message, path,
		req.SeenAt, req.SeenAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert issue occurrence: %w", err)
	}

	return GetIssue(engine, req.Project, req.ID)
}

// GetIssue returns one issue by id WITHIN a project, or (nil, nil) when the
// project does not own an issue with that id.
//
// The project is a positional argument rather than a field on a request struct
// so that adding it broke every existing call site at compile time. That was the
// point: the handlers each had to be revisited and given a tenant, which is not
// something a reviewer would have noticed being skipped.
//
// A foreign issue is reported as ABSENT, not as forbidden. (nil, nil) is what
// the handlers turn into a 404, so an id belonging to another project is
// indistinguishable from an id that never existed — no probing an id's existence
// by the shape of the refusal.
func GetIssue(engine db.Queryable, project, id string) (*structs.Issue, error) {
	q := sq.Select(issueColumns...).From(issuesTable).Where(sq.Eq{"monitor.issues.id": id}).Limit(1)
	q, err := scopeIssues(q, project)
	if err != nil {
		return nil, err
	}

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
//
// This one takes no project, and that is not an oversight. The project is the
// FIRST component hashed into a fingerprint (issues.generateFingerprint), so a
// fingerprint is already a per-project value — a caller holding one for project A
// cannot use it to reach project B's row, because no such row shares it. Unlike
// an id, a fingerprint is also not something the API hands out on a list.
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
	// Project is the tenant whose issues are being listed. It is MANDATORY and
	// server-derived, and it is not a pointer for exactly that reason: every
	// other field here is optional and says so by being one, while an absent
	// project is not "no filter", it is a bug. ListIssues and CountIssues both
	// refuse a zero value rather than widening to the whole zone.
	//
	// It is also not settable from the query string. issueQueryParams parses the
	// caller's filters; the handler stamps this from the credential afterwards,
	// so a `?project=` parameter is ignored rather than honoured.
	Project string

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
	q, err := scopeIssues(q, req.Project)
	if err != nil {
		return nil, err
	}
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
//
// Scoped through the same helper as ListIssues rather than trusting the pairing:
// a count is a disclosure on its own. "How many unresolved issues does the
// service named X have" is answerable from a total alone, with no row returned.
func CountIssues(engine db.Queryable, req ListIssuesRequest) (int, error) {
	q := sq.Select("COUNT(*)").From(issuesTable)
	q, err := scopeIssues(q, req.Project)
	if err != nil {
		return 0, err
	}
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

// UpdateIssue applies a partial update WITHIN a project and returns the updated
// row, or (nil, nil) when the project does not own an issue with that id.
//
// resolved_at is maintained here rather than by the caller so it cannot drift
// from status: moving to resolved stamps it, moving away clears it.
//
// The project is carried into the UPDATE's own WHERE, not merely checked by the
// handler beforehand. The handler's check is what produces the 404 and is not
// going anywhere, but a read-then-write pair enforces tenancy in the gap between
// two statements, and the next caller of this function will not necessarily
// write the read half. Here the mutation itself cannot cross a tenant: resolving,
// re-titling or re-assigning another project's issue matches no row.
func UpdateIssue(engine db.Queryable, project, id string, req UpdateIssueRequest) (*structs.Issue, error) {
	if project == "" {
		return nil, ErrNoIssueProject
	}
	if req.IsEmpty() {
		return GetIssue(engine, project, id)
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

	// Unqualified column names here, unlike every read above: an UPDATE names a
	// single table and issues no join, so there is nothing for `id` or `project`
	// to be ambiguous against.
	qStr, args, err := u.Where(sq.Eq{"id": id, "project": project}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to update issue: %w", err)
	}

	return GetIssue(engine, project, id)
}
