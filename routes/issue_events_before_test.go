package routes

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/scope"
)

func TestParseIssueEventsBefore(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Time
		wantErr bool
	}{
		{"", time.Time{}, false},
		{"2026-10-16T12:00:00Z", time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC), false},
		{"2026-10-16T12:00:00.123Z", time.Date(2026, 10, 16, 12, 0, 0, 123000000, time.UTC), false},
		{"2026-10-16T05:00:00-07:00", time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC), false},
		{"2026-10-16", time.Time{}, true},
		{"1760616000", time.Time{}, true},
		{"yesterday", time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := parseIssueEventsBefore(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseIssueEventsBefore(%q) err = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if !got.Equal(tt.want) {
				t.Errorf("parseIssueEventsBefore(%q) = %s, want %s", tt.raw, got, tt.want)
			}
		})
	}
}

// TestIssueEventsByIDQueryAppliesBeforeToBothPhases: the inner phase must take
// the cursor to choose the right rows, and the outer one too, or the min()
// boundary would let every newer occurrence back onto the page.
func TestIssueEventsByIDQueryAppliesBeforeToBothPhases(t *testing.T) {
	withDefaultProject(t, "default")
	withDatabaseName(t, "monitor")

	before := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	query, args, err := issueEventsByIDQuery(scope.WithProject(context.Background(), "atlas"), "issue-1", 50, before)
	if err != nil {
		t.Fatalf("issueEventsByIDQuery: %v", err)
	}

	want := "SELECT timestamp, service, project, env, job_id, request_id, trace_id, user_id, name, level, data FROM monitor.events " +
		"WHERE project = ? AND issue_id = ? AND timestamp < ? AND timestamp >= (SELECT min(timestamp) FROM (" +
		"SELECT timestamp FROM monitor.events WHERE project = ? AND issue_id = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT ?" +
		")) ORDER BY timestamp DESC LIMIT ?"
	if query != want {
		t.Errorf("sql =\n\t%s\nwant\n\t%s", query, want)
	}
	wantArgs := []interface{}{"atlas", "issue-1", before, "atlas", "issue-1", before, 50, 50}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %v, want %v", args, wantArgs)
	}
}
