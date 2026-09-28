package query

import (
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/structs"
)

// timelineRow mirrors the column ORDER of timelineColumns, which
// scanTimelineEntry unpacks positionally.
func timelineRow(id int64, issueID string, dedupeKey any) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "issue_id", "type", "actor_kind", "actor_user_id", "actor_api_key_id",
		"actor_label", "body", "metadata", "dedupe_key", "created_at", "edited_at", "deleted_at",
	}).AddRow(
		id, issueID, "comment", "system", nil, nil,
		"monitor", "note", nil, dedupeKey, time.Now(), nil, nil,
	)
}

// TestAppendTimelineEntryReadsBackItsOwnRow pins which row AppendTimelineEntry
// returns.
//
// The plain path used to re-read "the issue's newest entry" (ORDER BY id DESC
// LIMIT 1), so two comments posted to one issue at once could each be answered
// with the other's row. It must now read back the id the INSERT itself produced.
// sqlmock rejects any statement it was not told to expect, so a regression to
// the newest-entry read fails here rather than only under concurrency.
//
// The dedupe path is pinned unchanged: an upsert that updated or left an
// existing row has no meaningful insert id, so it resolves by its key.
func TestAppendTimelineEntryReadsBackItsOwnRow(t *testing.T) {
	const issueID = "iss-1"
	insertPattern := `INSERT INTO monitor.issue_timeline`

	strPtr := func(s string) *string { return &s }

	tests := []struct {
		name      string
		dedupeKey *string
		expect    func(mock sqlmock.Sqlmock)
		wantID    int64
		wantErr   string
	}{
		{
			name: "no dedupe key reads back by insert id",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(insertPattern).
					WithArgs(issueID, "comment", "system", nil, nil, "monitor", "note", nil, nil).
					WillReturnResult(sqlmock.NewResult(42, 1))
				mock.ExpectQuery(`WHERE monitor.issue_timeline.id = \? AND monitor.issue_timeline.issue_id = \? LIMIT 1`).
					WithArgs(int64(42), issueID).
					WillReturnRows(timelineRow(42, issueID, nil))
			},
			wantID: 42,
		},
		{
			// A no-op retry: ON DUPLICATE KEY UPDATE changed nothing, so MariaDB
			// reports no insert id at all. The row is still found by its key.
			name:      "dedupe key resolves by key, not insert id",
			dedupeKey: strPtr("agent:triage"),
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(insertPattern).
					WithArgs(issueID, "comment", "system", nil, nil, "monitor", "note", nil, "agent:triage").
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(`WHERE monitor.issue_timeline.issue_id = \? AND monitor.issue_timeline.dedupe_key = \? LIMIT 1`).
					WithArgs(issueID, "agent:triage").
					WillReturnRows(timelineRow(7, issueID, "agent:triage"))
			},
			wantID: 7,
		},
		{
			// Without an id there is no row to name, and guessing one is the
			// bug being fixed — so the append reports failure and reads nothing.
			name: "unreadable insert id fails without a guess",
			expect: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(insertPattern).
					WillReturnResult(sqlmock.NewErrorResult(errors.New("driver has no insert id")))
			},
			wantErr: "failed to read timeline entry id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			tt.expect(mock)

			entry, err := AppendTimelineEntry(mockDB, AppendTimelineEntryRequest{
				IssueID:   issueID,
				Type:      structs.TimelineComment,
				Actor:     &structs.Actor{Kind: structs.ActorKindSystem, Label: "monitor"},
				Body:      strPtr("note"),
				DedupeKey: tt.dedupeKey,
			})

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Fatalf("AppendTimelineEntry: %v", err)
				}
				if entry == nil || entry.ID != tt.wantID {
					t.Fatalf("entry = %+v, want id %d", entry, tt.wantID)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}
