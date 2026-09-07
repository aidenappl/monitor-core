package routes

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/scope"
)

// The issue-event reads are the ones singled out as most likely to leak, and the
// reason is structural rather than incidental: they are hand-written SQL strings
// that touch neither the column allowlists in structs/columns.go nor the
// squirrel builders in services/. Nothing about them looks like a query builder,
// so nothing about them invites the question "is this scoped?" — which is
// exactly why they get their own test rather than being covered by inspection.
//
// The scan below is additionally the one read in the codebase that already had a
// documented cross-tenant bug (returning other tenants' failures as an issue's
// occurrences, fixed by recomputing fingerprints). That it needed fixing once is
// the argument for pinning it now.

// withDefaultProject pins env.DefaultProjectSlug for the duration of a test.
func withDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

func withDatabaseName(t *testing.T, name string) {
	t.Helper()
	previous := db.Database
	db.Database = name
	t.Cleanup(func() { db.Database = previous })
}

// TestSelectIssueEventsScopesEveryQuery drives selectIssueEvents with the exact
// WHERE clauses HandleGetIssueEvents and queryEventsByIssueID pass it — the
// indexed fast path and both arms of the legacy pre-004 scan.
func TestSelectIssueEventsScopesEveryQuery(t *testing.T) {
	withDefaultProject(t, "default")
	withDatabaseName(t, "monitor")

	tests := []struct {
		name  string
		where string
		args  []interface{}
	}{
		{
			"indexed fast path",
			"issue_id = ? ORDER BY timestamp DESC LIMIT ?",
			[]interface{}{"issue-1", 50},
		},
		{
			"legacy scan with a path",
			"issue_id = '' AND service = ? AND name = ? AND level IN ('error', 'fatal') AND (JSONExtractString(data, 'path') = ? OR JSONExtractString(data, 'uri') = ?) ORDER BY timestamp DESC LIMIT ?",
			[]interface{}{"monitor-core", "request.failed", "/v1/events", "/v1/events", 1000},
		},
		{
			"legacy scan without a path",
			"issue_id = '' AND service = ? AND name = ? AND level IN ('error', 'fatal') ORDER BY timestamp DESC LIMIT ?",
			[]interface{}{"monitor-core", "request.failed", 1000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := selectIssueEvents(scope.WithProject(context.Background(), "atlas"), tt.where, tt.args...)
			if err != nil {
				t.Fatalf("selectIssueEvents: %v", err)
			}

			if !strings.Contains(query, "WHERE project = ? AND ") {
				t.Errorf("query is not project-scoped — it returns every project's events:\n\t%s", query)
			}
			if strings.Contains(query, "project = ''") {
				t.Errorf("query admits unstamped rows for a non-default project:\n\t%s", query)
			}

			// The project argument must come FIRST, because its placeholder is
			// the first `?` in the statement. Appending it instead would bind
			// every value one position out — valid SQL, no error, wrong rows.
			if len(args) != len(tt.args)+1 {
				t.Fatalf("args = %v, want the project prepended to %v", args, tt.args)
			}
			if args[0] != "atlas" {
				t.Errorf("args[0] = %v, want the project slug bound to the leading placeholder", args[0])
			}
			for i, want := range tt.args {
				if args[i+1] != want {
					t.Errorf("args[%d] = %v, want %v — the caller's bindings shifted", i+1, args[i+1], want)
				}
			}
		})
	}
}

// TestSelectIssueEventsAdmitsUnstampedRowsForDefault covers the transition
// window. The issue detail view is the page most likely to be looking at events
// older than the deploy, so losing pre-006 rows here is the most visible form of
// the blank-dashboard failure.
func TestSelectIssueEventsAdmitsUnstampedRowsForDefault(t *testing.T) {
	withDefaultProject(t, "default")
	withDatabaseName(t, "monitor")

	query, _, err := selectIssueEvents(scope.WithProject(context.Background(), "default"), "issue_id = ?", "issue-1")
	if err != nil {
		t.Fatalf("selectIssueEvents: %v", err)
	}
	if !strings.Contains(query, "WHERE (project = ? OR project = '') AND ") {
		t.Errorf("default project loses its pre-006 history:\n\t%s", query)
	}
}

// TestSelectIssueEventsRefusesAnUnscopedContext pins the fail-closed half.
// HandleGetIssueEvents surfaces this as a 500 rather than serving a page of
// every project's errors.
func TestSelectIssueEventsRefusesAnUnscopedContext(t *testing.T) {
	withDefaultProject(t, "default")
	withDatabaseName(t, "monitor")

	query, args, err := selectIssueEvents(context.Background(), "issue_id = ?", "issue-1")
	if !errors.Is(err, scope.ErrNoProject) {
		t.Fatalf("err = %v, want ErrNoProject", err)
	}
	if query != "" || args != nil {
		t.Errorf("returned a usable query (%q, %v) alongside the error", query, args)
	}
}

// TestSelectIssueEventsSelectsTheScannedColumns pins the projection against the
// scan in HandleGetIssueEvents. The two are coupled positionally — Scan reads
// them in order — so a column added here without a matching Scan destination is
// a runtime error on a page nothing else covers.
func TestSelectIssueEventsSelectsTheScannedColumns(t *testing.T) {
	withDefaultProject(t, "default")
	withDatabaseName(t, "monitor")

	query, _, err := selectIssueEvents(scope.WithProject(context.Background(), "atlas"), "issue_id = ?", "issue-1")
	if err != nil {
		t.Fatalf("selectIssueEvents: %v", err)
	}

	want := "SELECT timestamp, service, project, env, job_id, request_id, trace_id, user_id, name, level, data FROM monitor.events"
	if !strings.HasPrefix(query, want) {
		t.Errorf("projection changed; the row scans in issues.go read these columns positionally:\n\tgot:  %s\n\twant: %s...", query, want)
	}
}
