package query

import (
	"database/sql"
	"encoding/json"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

const timelineTable = "monitor.issue_timeline"

var timelineColumns = []string{
	"monitor.issue_timeline.id",
	"monitor.issue_timeline.issue_id",
	"monitor.issue_timeline.type",
	"monitor.issue_timeline.actor_kind",
	"monitor.issue_timeline.actor_user_id",
	"monitor.issue_timeline.actor_api_key_id",
	"monitor.issue_timeline.actor_label",
	"monitor.issue_timeline.body",
	"monitor.issue_timeline.metadata",
	"monitor.issue_timeline.dedupe_key",
	"monitor.issue_timeline.created_at",
	"monitor.issue_timeline.edited_at",
	"monitor.issue_timeline.deleted_at",
}

type timelineScanner interface {
	Scan(dest ...interface{}) error
}

func scanTimelineEntry(row timelineScanner) (*structs.IssueTimelineEntry, error) {
	var e structs.IssueTimelineEntry
	var actorUserID sql.NullInt64
	var actorAPIKeyID, body, dedupeKey sql.NullString
	var metadata []byte
	var editedAt, deletedAt sql.NullTime

	err := row.Scan(
		&e.ID, &e.IssueID, &e.Type, &e.ActorKind,
		&actorUserID, &actorAPIKeyID, &e.ActorLabel,
		&body, &metadata, &dedupeKey,
		&e.CreatedAt, &editedAt, &deletedAt,
	)
	if err != nil {
		return nil, err
	}

	if actorUserID.Valid {
		e.ActorUserID = &actorUserID.Int64
	}
	if actorAPIKeyID.Valid {
		e.ActorAPIKeyID = &actorAPIKeyID.String
	}
	if body.Valid {
		e.Body = &body.String
	}
	if len(metadata) > 0 {
		e.Metadata = json.RawMessage(metadata)
	}
	if dedupeKey.Valid {
		e.DedupeKey = &dedupeKey.String
	}
	if editedAt.Valid {
		e.EditedAt = &editedAt.Time
	}
	if deletedAt.Valid {
		e.DeletedAt = &deletedAt.Time
	}
	return &e, nil
}

// AppendTimelineEntryRequest is one append-only fact about an issue.
//
// Actor is required. An entry with no actor is an audit row that cannot say who
// acted, which is the failure mode the whole Actor type exists to prevent.
type AppendTimelineEntryRequest struct {
	IssueID   string
	Type      structs.TimelineEntryType
	Actor     *structs.Actor
	Body      *string
	Metadata  any
	DedupeKey *string
}

// AppendTimelineEntry inserts one timeline entry.
//
// When DedupeKey is set the insert is idempotent per (issue_id, dedupe_key): a
// repeat with the same body is a no-op, and a repeat with a changed body updates
// in place and stamps edited_at. This is Renovate's ensureComment semantics, and
// it is what lets an agent retry a task, or revisit an issue across sessions,
// without leaving five copies of the same note.
//
// Rows with no dedupe_key store NULL, and MariaDB treats each NULL as distinct
// under the unique key, so ordinary comments are never collapsed together.
func AppendTimelineEntry(engine db.Queryable, req AppendTimelineEntryRequest) (*structs.IssueTimelineEntry, error) {
	if req.Actor == nil || !req.Actor.Kind.IsValid() {
		return nil, fmt.Errorf("timeline entry requires a valid actor")
	}
	if !req.Type.IsValid() {
		return nil, fmt.Errorf("invalid timeline entry type %q", req.Type)
	}
	if req.Actor.Label == "" {
		return nil, fmt.Errorf("timeline entry requires a non-empty actor label")
	}

	var metadata any
	if req.Metadata != nil {
		encoded, err := json.Marshal(req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to encode timeline metadata: %w", err)
		}
		metadata = string(encoded)
	}

	var userID, apiKeyID any
	if req.Actor.UserID != nil {
		userID = *req.Actor.UserID
	}
	if req.Actor.APIKeyID != nil {
		apiKeyID = *req.Actor.APIKeyID
	}
	var body, dedupe any
	if req.Body != nil {
		body = *req.Body
	}
	if req.DedupeKey != nil && *req.DedupeKey != "" {
		dedupe = *req.DedupeKey
	}

	// edited_at is only stamped when the body actually changes, so a no-op retry
	// leaves the row — and any "edited" affordance in the UI — untouched.
	const insertSQL = `INSERT INTO monitor.issue_timeline
		(issue_id, type, actor_kind, actor_user_id, actor_api_key_id, actor_label, body, metadata, dedupe_key)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		edited_at = IF(NOT (body <=> VALUES(body)), CURRENT_TIMESTAMP(3), edited_at),
		body      = VALUES(body),
		metadata  = VALUES(metadata),
		deleted_at = NULL`

	if _, err := engine.Exec(insertSQL,
		req.IssueID, string(req.Type), string(req.Actor.Kind),
		userID, apiKeyID, req.Actor.Label,
		body, metadata, dedupe,
	); err != nil {
		return nil, fmt.Errorf("failed to append timeline entry: %w", err)
	}

	if dedupe != nil {
		return GetTimelineEntryByDedupeKey(engine, req.IssueID, *req.DedupeKey)
	}
	return getLastTimelineEntry(engine, req.IssueID)
}

// GetTimelineEntryByDedupeKey returns the entry an agent's dedupe key resolves
// to, or (nil, nil).
func GetTimelineEntryByDedupeKey(engine db.Queryable, issueID, dedupeKey string) (*structs.IssueTimelineEntry, error) {
	q := sq.Select(timelineColumns...).From(timelineTable).
		Where(sq.Eq{"monitor.issue_timeline.issue_id": issueID}).
		Where(sq.Eq{"monitor.issue_timeline.dedupe_key": dedupeKey}).
		Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	entry, err := scanTimelineEntry(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan timeline entry: %w", err)
	}
	return entry, nil
}

func getLastTimelineEntry(engine db.Queryable, issueID string) (*structs.IssueTimelineEntry, error) {
	q := sq.Select(timelineColumns...).From(timelineTable).
		Where(sq.Eq{"monitor.issue_timeline.issue_id": issueID}).
		OrderBy("monitor.issue_timeline.id DESC").Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	entry, err := scanTimelineEntry(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan timeline entry: %w", err)
	}
	return entry, nil
}

// ListTimelineRequest pages one issue's timeline.
type ListTimelineRequest struct {
	IssueID string
	// Type optionally narrows to one entry kind, e.g. just the status
	// transitions when computing time-in-state.
	Type *structs.TimelineEntryType
	// IncludeDeleted returns soft-deleted comments too. Off by default.
	IncludeDeleted bool
	Limit          int
	Offset         int
}

func applyTimelineFilters(q sq.SelectBuilder, req ListTimelineRequest) sq.SelectBuilder {
	q = q.Where(sq.Eq{"monitor.issue_timeline.issue_id": req.IssueID})
	if req.Type != nil {
		q = q.Where(sq.Eq{"monitor.issue_timeline.type": string(*req.Type)})
	}
	if !req.IncludeDeleted {
		q = q.Where(sq.Eq{"monitor.issue_timeline.deleted_at": nil})
	}
	return q
}

// ListTimeline returns one issue's timeline oldest-first, which is the order the
// detail view renders. This is the single indexed read the polymorphic table
// exists to enable — (issue_id, created_at).
func ListTimeline(engine db.Queryable, req ListTimelineRequest) ([]structs.IssueTimelineEntry, error) {
	if req.Limit <= 0 || req.Limit > db.MAX_LIMIT {
		req.Limit = db.DEFAULT_LIMIT
	}

	q := sq.Select(timelineColumns...).From(timelineTable)
	q = applyTimelineFilters(q, req)
	q = q.OrderBy("monitor.issue_timeline.created_at ASC", "monitor.issue_timeline.id ASC").
		Limit(uint64(req.Limit))
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

	var entries []structs.IssueTimelineEntry
	for rows.Next() {
		entry, err := scanTimelineEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan timeline entry: %w", err)
		}
		entries = append(entries, *entry)
	}
	return entries, rows.Err()
}

// CountTimeline returns the total matching the same filters.
func CountTimeline(engine db.Queryable, req ListTimelineRequest) (int, error) {
	q := sq.Select("COUNT(*)").From(timelineTable)
	q = applyTimelineFilters(q, req)

	qStr, args, err := q.ToSql()
	if err != nil {
		return 0, fmt.Errorf("failed to build sql query: %w", err)
	}

	var count int
	if err := engine.QueryRow(qStr, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count timeline entries: %w", err)
	}
	return count, nil
}

// EditComment replaces a comment's body. Only comments are editable — a status
// transition or a PR merge is a historical fact, not a draft.
func EditComment(engine db.Queryable, issueID string, entryID int64, body string) (*structs.IssueTimelineEntry, error) {
	u := sq.Update(timelineTable).
		Set("body", body).
		Set("edited_at", sq.Expr("CURRENT_TIMESTAMP(3)")).
		Where(sq.Eq{
			"id":         entryID,
			"issue_id":   issueID,
			"type":       string(structs.TimelineComment),
			"deleted_at": nil,
		})

	qStr, args, err := u.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to edit comment: %w", err)
	}
	if affected, err := res.RowsAffected(); err == nil && affected == 0 {
		return nil, nil
	}

	return getTimelineEntryByID(engine, issueID, entryID)
}

// SoftDeleteComment marks a comment deleted without removing the row, so the
// timeline keeps its shape and the audit trail stays intact.
func SoftDeleteComment(engine db.Queryable, issueID string, entryID int64) (bool, error) {
	u := sq.Update(timelineTable).
		Set("deleted_at", sq.Expr("CURRENT_TIMESTAMP(3)")).
		Where(sq.Eq{
			"id":         entryID,
			"issue_id":   issueID,
			"type":       string(structs.TimelineComment),
			"deleted_at": nil,
		})

	qStr, args, err := u.ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete comment: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}

func getTimelineEntryByID(engine db.Queryable, issueID string, entryID int64) (*structs.IssueTimelineEntry, error) {
	q := sq.Select(timelineColumns...).From(timelineTable).
		Where(sq.Eq{"monitor.issue_timeline.id": entryID}).
		Where(sq.Eq{"monitor.issue_timeline.issue_id": issueID}).
		Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	entry, err := scanTimelineEntry(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan timeline entry: %w", err)
	}
	return entry, nil
}
