package issues

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aidenappl/monitor-core/db"
)

// BackfillFromClickHouse copies the legacy ClickHouse issues table into MariaDB.
//
// This is the one-time cutover for issue ownership. It is safe to re-run: rows
// are matched on the primary key, which is the deterministic UUIDv5 derived from
// the fingerprint, so a row backfilled twice updates itself rather than
// duplicating. That determinism is also why no id remapping is needed — the two
// stores agree on identity by construction.
//
// Conflict rule: the greater occurrence_count wins, and the seen-window is
// widened rather than replaced. A backfill running alongside live ingestion can
// therefore only ever be additive; it cannot walk a live counter backwards.
// Triage state already set in MariaDB (status, assignee, priority, title) is left
// untouched — ClickHouse has no opinion on those beyond status, and a human's
// triage must not be reverted by a data migration.
//
// The ClickHouse table is read, never written or dropped. Retire it manually once
// a release has passed.
func BackfillFromClickHouse(ctx context.Context) (int, error) {
	rows, err := db.Conn.Query(ctx, fmt.Sprintf(
		"SELECT id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at FROM %s.issues FINAL",
		db.Database,
	))
	if err != nil {
		return 0, fmt.Errorf("failed to read legacy issues: %w", err)
	}
	defer rows.Close()

	// Status is carried across as-is: 'unresolved', 'resolved' and 'ignored' are
	// all still valid in the new four-value enum, so no mapping is required.
	// 'in_progress' simply never appears in legacy data.
	const upsert = `INSERT INTO monitor.issues
		(id, fingerprint, service, name, message, path, status, occurrence_count, first_seen, last_seen, resolved_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		occurrence_count = GREATEST(occurrence_count, VALUES(occurrence_count)),
		first_seen       = LEAST(COALESCE(first_seen, VALUES(first_seen)), COALESCE(VALUES(first_seen), first_seen)),
		last_seen        = GREATEST(COALESCE(last_seen, VALUES(last_seen)), COALESCE(VALUES(last_seen), last_seen)),
		message          = COALESCE(message, VALUES(message)),
		path             = COALESCE(path, VALUES(path))`

	copied := 0
	for rows.Next() {
		var (
			id, fingerprint, service, name, message, path, status string
			occurrenceCount                                       uint64
			firstSeen, lastSeen                                   time.Time
			resolvedAt                                            *time.Time
		)
		if err := rows.Scan(&id, &fingerprint, &service, &name, &message, &path,
			&status, &occurrenceCount, &firstSeen, &lastSeen, &resolvedAt); err != nil {
			return copied, fmt.Errorf("failed to scan legacy issue: %w", err)
		}

		var messageArg, pathArg any
		if message != "" {
			messageArg = message
		}
		if path != "" {
			pathArg = path
		}

		if _, err := db.SQL.Exec(upsert,
			id, fingerprint, service, name, messageArg, pathArg, status,
			occurrenceCount,
			firstSeen.UTC().Truncate(time.Millisecond),
			lastSeen.UTC().Truncate(time.Millisecond),
			resolvedAt,
		); err != nil {
			return copied, fmt.Errorf("failed to backfill issue %s: %w", id, err)
		}
		copied++
	}
	if err := rows.Err(); err != nil {
		return copied, fmt.Errorf("failed while reading legacy issues: %w", err)
	}

	log.Printf("issues: backfilled %d issue(s) from ClickHouse into MariaDB", copied)
	return copied, nil
}
