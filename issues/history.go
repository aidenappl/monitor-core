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
