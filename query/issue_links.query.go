package query

import (
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

const issueLinksTable = "monitor.issue_links"

var issueLinkColumns = []string{
	"monitor.issue_links.id",
	"monitor.issue_links.issue_id",
	"monitor.issue_links.provider",
	"monitor.issue_links.kind",
	"monitor.issue_links.url",
	"monitor.issue_links.owner",
	"monitor.issue_links.repo",
	"monitor.issue_links.number",
	"monitor.issue_links.title",
	"monitor.issue_links.state",
	"monitor.issue_links.merged",
	"monitor.issue_links.author",
	"monitor.issue_links.state_synced_at",
	"monitor.issue_links.linked_by_label",
	"monitor.issue_links.inserted_at",
	"monitor.issue_links.updated_at",
}

type issueLinkScanner interface {
	Scan(dest ...interface{}) error
}

func scanIssueLink(row issueLinkScanner) (*structs.IssueLink, error) {
	var l structs.IssueLink
	var owner, repo, title, state, author sql.NullString
	var number sql.NullInt64
	var merged sql.NullBool
	var syncedAt sql.NullTime

	err := row.Scan(
		&l.ID, &l.IssueID, &l.Provider, &l.Kind, &l.URL,
		&owner, &repo, &number, &title, &state, &merged, &author,
		&syncedAt, &l.LinkedByLabel, &l.InsertedAt, &l.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if owner.Valid {
		l.Owner = &owner.String
	}
	if repo.Valid {
		l.Repo = &repo.String
	}
	if number.Valid {
		n := int(number.Int64)
		l.Number = &n
	}
	if title.Valid {
		l.Title = &title.String
	}
	if state.Valid {
		l.State = &state.String
	}
	if merged.Valid {
		l.Merged = &merged.Bool
	}
	if author.Valid {
		l.Author = &author.String
	}
	if syncedAt.Valid {
		l.StateSyncedAt = &syncedAt.Time
	}
	return &l, nil
}

// CreateIssueLinkRequest records an external reference against an issue.
//
// The GitHub-derived fields are optional on purpose: a link is stored even when
// the GitHub call fails, degrading the chip to a bare URL rather than failing
// the write. GitHub being unreachable must never block issue triage.
type CreateIssueLinkRequest struct {
	IssueID       string
	Kind          structs.IssueLinkKind
	URL           string
	Owner         *string
	Repo          *string
	Number        *int
	Title         *string
	State         *string
	Merged        *bool
	Author        *string
	LinkedByLabel string
}

// CreateIssueLink inserts a link, or refreshes it if the same URL is already
// linked to the issue. Re-linking is how a stale chip gets re-synced.
func CreateIssueLink(engine db.Queryable, req CreateIssueLinkRequest) (*structs.IssueLink, error) {
	if !req.Kind.IsValid() {
		return nil, fmt.Errorf("invalid issue link kind %q", req.Kind)
	}
	if req.URL == "" {
		return nil, fmt.Errorf("issue link requires a url")
	}
	if req.LinkedByLabel == "" {
		return nil, fmt.Errorf("issue link requires a linked_by label")
	}

	// state_synced_at is only stamped when GitHub actually answered, so a NULL
	// distinguishes "never reached GitHub" from "GitHub said the PR is open".
	var syncedAt any
	if req.State != nil || req.Merged != nil || req.Title != nil {
		syncedAt = time.Now().UTC()
	}

	const insertSQL = `INSERT INTO monitor.issue_links
		(issue_id, provider, kind, url, owner, repo, number, title, state, merged, author, state_synced_at, linked_by_label)
	VALUES (?, 'github', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		kind            = VALUES(kind),
		owner           = VALUES(owner),
		repo            = VALUES(repo),
		number          = VALUES(number),
		title           = COALESCE(VALUES(title), title),
		state           = COALESCE(VALUES(state), state),
		merged          = COALESCE(VALUES(merged), merged),
		author          = COALESCE(VALUES(author), author),
		state_synced_at = COALESCE(VALUES(state_synced_at), state_synced_at)`

	if _, err := engine.Exec(insertSQL,
		req.IssueID, string(req.Kind), req.URL,
		nullableString(req.Owner), nullableString(req.Repo), nullableInt(req.Number),
		nullableString(req.Title), nullableString(req.State), nullableBool(req.Merged),
		nullableString(req.Author), syncedAt, req.LinkedByLabel,
	); err != nil {
		return nil, fmt.Errorf("failed to create issue link: %w", err)
	}

	return GetIssueLinkByURL(engine, req.IssueID, req.URL)
}

// GetIssueLinkByURL returns one link, or (nil, nil).
func GetIssueLinkByURL(engine db.Queryable, issueID, url string) (*structs.IssueLink, error) {
	q := sq.Select(issueLinkColumns...).From(issueLinksTable).
		Where(sq.Eq{"monitor.issue_links.issue_id": issueID}).
		Where(sq.Eq{"monitor.issue_links.url": url}).
		Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	link, err := scanIssueLink(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan issue link: %w", err)
	}
	return link, nil
}

// ListIssueLinks returns every link on one issue.
func ListIssueLinks(engine db.Queryable, issueID string) ([]structs.IssueLink, error) {
	links, err := listIssueLinksWhere(engine, sq.Eq{"monitor.issue_links.issue_id": issueID})
	if err != nil {
		return nil, err
	}
	return links[issueID], nil
}

// ListIssueLinksForIssues fetches links for a set of issues in ONE query, keyed
// by issue id. The list view uses this so rendering N issues does not become N
// round trips.
func ListIssueLinksForIssues(engine db.Queryable, issueIDs []string) (map[string][]structs.IssueLink, error) {
	if len(issueIDs) == 0 {
		return map[string][]structs.IssueLink{}, nil
	}
	return listIssueLinksWhere(engine, sq.Eq{"monitor.issue_links.issue_id": issueIDs})
}

func listIssueLinksWhere(engine db.Queryable, where sq.Sqlizer) (map[string][]structs.IssueLink, error) {
	q := sq.Select(issueLinkColumns...).From(issueLinksTable).
		Where(where).
		OrderBy("monitor.issue_links.inserted_at ASC", "monitor.issue_links.id ASC")

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	byIssue := map[string][]structs.IssueLink{}
	for rows.Next() {
		link, err := scanIssueLink(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan issue link: %w", err)
		}
		byIssue[link.IssueID] = append(byIssue[link.IssueID], *link)
	}
	return byIssue, rows.Err()
}

// UpdateIssueLinkStateRequest refreshes the cached GitHub view of a link. Used
// by the webhook receiver, which is told the new state rather than polling for it.
type UpdateIssueLinkStateRequest struct {
	Owner  string
	Repo   string
	Number int
	State  *string
	Merged *bool
	Title  *string
	Author *string
}

// UpdateIssueLinkState updates every link pointing at one GitHub PR, across all
// issues that reference it, and reports how many rows changed. A PR referenced
// by three issues updates all three from one webhook delivery.
func UpdateIssueLinkState(engine db.Queryable, req UpdateIssueLinkStateRequest) (int64, error) {
	u := sq.Update(issueLinksTable).
		Set("state_synced_at", sq.Expr("CURRENT_TIMESTAMP(3)")).
		Where(sq.Eq{
			"provider": "github",
			"owner":    req.Owner,
			"repo":     req.Repo,
			"number":   req.Number,
		})

	if req.State != nil {
		u = u.Set("state", *req.State)
	}
	if req.Merged != nil {
		u = u.Set("merged", *req.Merged)
	}
	if req.Title != nil {
		u = u.Set("title", *req.Title)
	}
	if req.Author != nil {
		u = u.Set("author", *req.Author)
	}

	qStr, args, err := u.ToSql()
	if err != nil {
		return 0, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to update issue link state: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return affected, nil
}

// ListIssueIDsForPR returns the issues referencing one GitHub PR, so a webhook
// delivery knows which timelines to append to.
func ListIssueIDsForPR(engine db.Queryable, owner, repo string, number int) ([]string, error) {
	q := sq.Select("DISTINCT monitor.issue_links.issue_id").From(issueLinksTable).
		Where(sq.Eq{
			"monitor.issue_links.provider": "github",
			"monitor.issue_links.owner":    owner,
			"monitor.issue_links.repo":     repo,
			"monitor.issue_links.number":   number,
		})

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan issue id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// DeleteIssueLink removes one link and reports whether it existed.
func DeleteIssueLink(engine db.Queryable, issueID string, linkID int64) (bool, error) {
	qStr, args, err := sq.Delete(issueLinksTable).
		Where(sq.Eq{"id": linkID, "issue_id": issueID}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete issue link: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}

func nullableString(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableInt(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableBool(v *bool) any {
	if v == nil {
		return nil
	}
	return *v
}
