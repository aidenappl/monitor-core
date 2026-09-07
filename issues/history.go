package issues

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// KNOWN GAP — the occurrence rollup has no project COLUMN, though it is no
// longer capable of mixing two projects under one key. Recorded 2026-09-06,
// revised when issue fingerprints became project-scoped.
//
// WHAT CHANGED. issues.generateFingerprint now takes the project as its first
// component, and an issue id is a UUIDv5 over that fingerprint, so an issue id
// is per-project by construction. monitor.issue_occurrences_daily is keyed on
// (issue_id, day), which means one rollup key can no longer accumulate two
// tenants' occurrences the way it could when a fingerprint was just
// service + name + path. That was the leak-shaped half of this gap, and it
// closed without this table being touched at all.
//
// WHAT REMAINS, AND WHY IT IS NOT A LEAK. The table still carries no project
// column — 005 keys it on (issue_id, day) and the materialized view that fills
// it selects no tenant — so nothing HERE can filter by project without going
// through monitor.issues first. That is the right shape anyway: every caller
// below already holds an issue it read from that table, and the issue read path
// is where the tenancy boundary belongs. A predicate added here would narrow a
// histogram under an issue the caller can already read in full, hiding nothing
// and making the sparkline disagree with the issue's own occurrence_count.
//
// THAT ARGUMENT NOW HOLDS, which it did not when it was first written. It rests
// entirely on "the caller already holds an issue it is allowed to read", and
// until query.scopeIssues landed the issue read path was NOT scoped — so an
// unscoped list handed out any tenant's issue id and these two functions turned
// it into that tenant's per-day event counts. The reasoning was sound and its
// premise was false, which is the more dangerous of the two ways to be wrong.
// If anyone ever widens the issue read path again, these reads widen with it.
//
// So what is left is a schema nicety rather than a boundary: re-keying on
// (project, issue_id, day) would let the rollup be aggregated per project
// without a join. It stays undone because it needs a table rebuild plus a
// DROP/CREATE of the materialized view, which the boot runner cannot perform —
// every file it holds must be idempotent DDL, replayed on every restart.
//
// ONE CONSEQUENCE to know about while reading these. Issues predating
// db/migrations/118_issues_project.sql keep ids derived without a project, so
// their rollup rows are frozen: no new occurrence will ever land on them, and
// this table has NO TTL, unlike the 30-day monitor.events. They stay readable
// for as long as their issue exists and become unreachable orphans the moment it
// is deleted. The sweep is migrations/manual/delete_orphaned_issue_rollups.sql,
// run by hand, and it keys on absence from monitor.issues so it cannot touch the
// history of an issue somebody can still open.

// OccurrenceDay is one day's worth of an issue's activity. Aliased to the
// canonical shape in structs, which Issue.History embeds.
type OccurrenceDay = structs.OccurrenceDay

// GetOccurrenceHistory returns per-day counts for an issue between from and to
// (inclusive), oldest first.
//
// This reads the no-TTL rollup, not monitor.events, which is the point: raw
// events expire after 30 days but these rows do not, so an issue that first fired
// six months ago can still show when it fired and how often. Sparse days are
// omitted rather than zero-filled — the caller decides how to render gaps.
//
// The -Merge combinators are required: AggregatingMergeTree stores partial
// aggregation states, and reading the columns directly would return opaque
// binary rather than numbers.
func GetOccurrenceHistory(ctx context.Context, issueID string, from, to time.Time) ([]OccurrenceDay, error) {
	if issueID == "" {
		return nil, fmt.Errorf("issue id is required")
	}

	const q = `SELECT
			day,
			countMerge(occurrences) AS occurrences,
			minMerge(first_seen) AS first_seen,
			maxMerge(last_seen) AS last_seen
		FROM %s.issue_occurrences_daily
		WHERE issue_id = ? AND day >= ? AND day <= ?
		GROUP BY day
		ORDER BY day ASC`

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(q, db.Database), issueID, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query occurrence history: %w", err)
	}
	defer rows.Close()

	history := []OccurrenceDay{}
	for rows.Next() {
		var d OccurrenceDay
		if err := rows.Scan(&d.Day, &d.Occurrences, &d.FirstSeen, &d.LastSeen); err != nil {
			return nil, fmt.Errorf("failed to scan occurrence day: %w", err)
		}
		history = append(history, d)
	}
	return history, rows.Err()
}

// GetOccurrenceHistoryBulk returns per-day counts for MANY issues in ONE query.
//
// The list view renders an activity strip per row, which is the difference
// between "5 occurrences" and knowing whether those five were this morning or
// spread over a month. Fetching that per row would be a query per issue; this is
// a single grouped read over the rollup, so a hundred-row page costs one round
// trip rather than a hundred.
//
// Issues with no recorded days are absent from the map rather than present with
// an empty slice — the caller renders "no breakdown" differently from "quiet".
func GetOccurrenceHistoryBulk(ctx context.Context, issueIDs []string, from, to time.Time) (map[string][]OccurrenceDay, error) {
	if len(issueIDs) == 0 {
		return map[string][]OccurrenceDay{}, nil
	}

	const q = `SELECT
			issue_id,
			day,
			countMerge(occurrences) AS occurrences
		FROM %s.issue_occurrences_daily
		WHERE issue_id IN (?) AND day >= ? AND day <= ?
		GROUP BY issue_id, day
		ORDER BY issue_id, day ASC`

	rows, err := db.Conn.Query(ctx, fmt.Sprintf(q, db.Database), issueIDs, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query bulk occurrence history: %w", err)
	}
	defer rows.Close()

	byIssue := map[string][]OccurrenceDay{}
	for rows.Next() {
		var issueID string
		var d OccurrenceDay
		if err := rows.Scan(&issueID, &d.Day, &d.Occurrences); err != nil {
			return nil, fmt.Errorf("failed to scan occurrence day: %w", err)
		}
		byIssue[issueID] = append(byIssue[issueID], d)
	}
	return byIssue, rows.Err()
}
