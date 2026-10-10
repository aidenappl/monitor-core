package query

import (
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// MAX_BULK_ISSUES caps one bulk status change.
//
// The cap bounds the write, not the selection. Issues already at the target
// status are never targets, so a filter matching more than this is drained by
// repeating the same call: each pass moves the next batch out of the match set,
// and BulkUpdateIssueStatusResult.Remaining says when to stop. An explicit id
// list has no such property, so it is refused above the cap instead.
//
// 1000 is what one /v1 request can finish well inside its 25s bound — the UPDATE
// is a single statement, but each changed issue gets its own timeline row.
const MAX_BULK_ISSUES = 1000

// ErrBulkTooManyIDs is returned when an explicit id list exceeds MAX_BULK_ISSUES.
var ErrBulkTooManyIDs = fmt.Errorf("at most %d ids may be updated in one request", MAX_BULK_ISSUES)

// ErrBulkNoTarget is returned when a bulk request names neither ids nor a filter,
// or names both. "Neither" must not widen to every issue in the project, and
// "both" has no single obvious meaning.
var ErrBulkNoTarget = errors.New("exactly one of ids or filter is required")

// BulkUpdateIssueStatusRequest moves many issues to one status.
//
// Exactly one of IDs or Filter is set. Filter reuses the list endpoint's filter
// surface so "resolve everything matching what I'm looking at" selects the same
// rows the list showed; its Sort, Limit and Offset are ignored.
type BulkUpdateIssueStatusRequest struct {
	// Project is server-derived and mandatory, exactly as on ListIssuesRequest.
	Project string
	Status  structs.IssueStatus
	IDs     []string
	Filter  *ListIssuesRequest
	DryRun  bool
}

// BulkIssueChange is one issue a bulk write moved, with the status it left — the
// handler needs the old value to write an accurate timeline entry.
type BulkIssueChange struct {
	ID   string              `json:"id"`
	From structs.IssueStatus `json:"from"`
}

// BulkUpdateIssueStatusResult reports a bulk write.
//
// Matched counts every issue the request selected that was not already at the
// target status, before the cap. Updated is how many this call changed (zero on
// a dry run). Remaining is Matched - Updated: for a filter, the number a repeat
// call would still move.
type BulkUpdateIssueStatusResult struct {
	Matched   int               `json:"matched"`
	Updated   int               `json:"updated"`
	Remaining int               `json:"remaining"`
	Changes   []BulkIssueChange `json:"-"`
}

// bulkTargetWhere builds the selection shared by the count and the locking read:
// scoped to the project, narrowed to the ids or filter, and excluding issues that
// are already at the target status so a no-op is neither written nor counted.
func bulkTargetWhere(q sq.SelectBuilder, req BulkUpdateIssueStatusRequest) (sq.SelectBuilder, error) {
	q, err := scopeIssues(q, req.Project)
	if err != nil {
		return q, err
	}
	if req.IDs != nil {
		q = q.Where(sq.Eq{"monitor.issues.id": req.IDs})
	} else {
		q = applyIssueFilters(q, *req.Filter)
	}
	return q.Where(sq.NotEq{"monitor.issues.status": string(req.Status)}), nil
}

// BulkUpdateIssueStatus moves the selected issues to req.Status in one
// transaction and returns what changed.
//
// The selection is read FOR UPDATE inside the same transaction as the UPDATE, so
// the returned "from" statuses are exactly the ones overwritten — an issue that
// regressed between a separate read and the write would otherwise be reported
// with a stale prior status on its timeline. The UPDATE carries the project in
// its own WHERE, as UpdateIssue does, so the write cannot cross a tenant even if
// the read half were ever dropped.
//
// resolved_at follows status by the same rule as UpdateIssue.
func BulkUpdateIssueStatus(engine db.Queryable, req BulkUpdateIssueStatusRequest) (*BulkUpdateIssueStatusResult, error) {
	if req.Project == "" {
		return nil, ErrNoIssueProject
	}
	if !req.Status.IsValid() {
		return nil, fmt.Errorf("invalid issue status %q", req.Status)
	}
	if (req.IDs == nil) == (req.Filter == nil) {
		return nil, ErrBulkNoTarget
	}
	if req.IDs != nil {
		if len(req.IDs) == 0 {
			return nil, ErrBulkNoTarget
		}
		if len(req.IDs) > MAX_BULK_ISSUES {
			return nil, ErrBulkTooManyIDs
		}
	}

	countQ, err := bulkTargetWhere(sq.Select("COUNT(*)").From(issuesTable), req)
	if err != nil {
		return nil, err
	}
	countSQL, countArgs, err := countQ.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	result := &BulkUpdateIssueStatusResult{}
	if req.DryRun {
		if err := engine.QueryRow(countSQL, countArgs...).Scan(&result.Matched); err != nil {
			return nil, fmt.Errorf("failed to count bulk targets: %w", err)
		}
		result.Remaining = result.Matched
		return result, nil
	}

	beginner, ok := engine.(txBeginner)
	if !ok {
		// Already inside a caller's transaction; that transaction is the isolation.
		return bulkUpdateIssueStatus(engine, req, countSQL, countArgs)
	}
	tx, err := beginner.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin bulk issue update: %w", err)
	}
	result, err = bulkUpdateIssueStatus(tx, req, countSQL, countArgs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit bulk issue update: %w", err)
	}
	return result, nil
}

func bulkUpdateIssueStatus(engine db.Queryable, req BulkUpdateIssueStatusRequest, countSQL string, countArgs []interface{}) (*BulkUpdateIssueStatusResult, error) {
	result := &BulkUpdateIssueStatusResult{}
	if err := engine.QueryRow(countSQL, countArgs...).Scan(&result.Matched); err != nil {
		return nil, fmt.Errorf("failed to count bulk targets: %w", err)
	}

	// Oldest-seen last: when a filter overflows the cap, the issues still firing
	// are the ones moved first.
	selQ, err := bulkTargetWhere(sq.Select("monitor.issues.id", "monitor.issues.status").From(issuesTable), req)
	if err != nil {
		return nil, err
	}
	selSQL, selArgs, err := selQ.
		OrderBy("monitor.issues.last_seen DESC", "monitor.issues.id ASC").
		Limit(MAX_BULK_ISSUES).
		Suffix("FOR UPDATE").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(selSQL, selArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to select bulk targets: %w", err)
	}
	ids := make([]string, 0, MAX_BULK_ISSUES)
	for rows.Next() {
		var c BulkIssueChange
		var from string
		if err := rows.Scan(&c.ID, &from); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan bulk target: %w", err)
		}
		c.From = structs.IssueStatus(from)
		result.Changes = append(result.Changes, c)
		ids = append(ids, c.ID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to read bulk targets: %w", err)
	}
	rows.Close()

	if len(ids) == 0 {
		return result, nil
	}

	u := sq.Update(issuesTable).Set("status", string(req.Status))
	if req.Status == structs.IssueStatusResolved {
		u = u.Set("resolved_at", sq.Expr("CURRENT_TIMESTAMP(3)"))
	} else {
		u = u.Set("resolved_at", nil)
	}
	updSQL, updArgs, err := u.Where(sq.Eq{"id": ids, "project": req.Project}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}
	if _, err := engine.Exec(updSQL, updArgs...); err != nil {
		return nil, fmt.Errorf("failed to bulk update issues: %w", err)
	}

	result.Updated = len(ids)
	result.Remaining = result.Matched - result.Updated
	if result.Remaining < 0 {
		result.Remaining = 0
	}
	return result, nil
}
