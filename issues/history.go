package issues

import (
	"context"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

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
