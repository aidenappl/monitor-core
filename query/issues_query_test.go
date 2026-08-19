package query

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

// TestUpsertIssueSQLIsSingleStatement is the guard on the fix for the
// occurrence_count drift bug. Correctness rests entirely on the fold being ONE
// statement — the moment it becomes read-then-write, concurrent workers lose
// increments again, which is exactly what the ClickHouse implementation did.
func TestUpsertIssueSQLIsSingleStatement(t *testing.T) {
	if strings.Contains(upsertIssueSQL, ";") {
		t.Error("upsert must be a single statement — no semicolons")
	}
	if n := strings.Count(strings.ToUpper(upsertIssueSQL), "INSERT"); n != 1 {
		t.Errorf("expected exactly 1 INSERT, found %d", n)
	}
	if !strings.Contains(strings.ToUpper(upsertIssueSQL), "ON DUPLICATE KEY UPDATE") {
		t.Error("upsert must use ON DUPLICATE KEY UPDATE")
	}
	if !strings.Contains(upsertIssueSQL, "occurrence_count = occurrence_count + 1") {
		t.Error("the counter must be incremented in SQL, not read into Go and written back")
	}
	if strings.Contains(strings.ToUpper(upsertIssueSQL), "SELECT") {
		t.Error("upsert must not read before writing")
	}
}

// TestUpsertIssueSQLAssignmentOrder pins the load-bearing ordering. MariaDB
// evaluates the SET list left to right, so every clause that tests the PREVIOUS
// status must appear before status is reassigned. If `status = IF(...)` moved
// earlier, the regression bookkeeping would test the new value against itself
// and silently stop counting regressions.
func TestUpsertIssueSQLAssignmentOrder(t *testing.T) {
	update := upsertIssueSQL[strings.Index(upsertIssueSQL, "ON DUPLICATE KEY UPDATE"):]

	statusAssign := regexp.MustCompile(`(?m)^\s*status\s*=`).FindStringIndex(update)
	if statusAssign == nil {
		t.Fatal("no status assignment found in the ON DUPLICATE KEY UPDATE clause")
	}

	dependents := []string{"regression_count", "regressed_at", "resolved_at"}
	for _, col := range dependents {
		t.Run(col, func(t *testing.T) {
			assign := regexp.MustCompile(`(?m)^\s*` + col + `\s*=`).FindStringIndex(update)
			if assign == nil {
				t.Fatalf("no %s assignment found", col)
			}
			if assign[0] > statusAssign[0] {
				t.Errorf("%s is assigned after status; it would test the already-updated value", col)
			}
		})
	}
}

// TestUpsertIssueSQLTransitionsOnlyFromResolved pins the transition table: the
// automated path must only ever move an issue OUT of resolved. Anything that
// clobbered in_progress would undo an agent's triage on every recurrence.
func TestUpsertIssueSQLTransitionsOnlyFromResolved(t *testing.T) {
	conditionals := regexp.MustCompile(`IF\(status\s*=\s*'([a-z_]+)'`).FindAllStringSubmatch(upsertIssueSQL, -1)
	if len(conditionals) == 0 {
		t.Fatal("expected the status-conditional expressions to be present")
	}
	for _, match := range conditionals {
		if match[1] != string(structs.IssueStatusResolved) {
			t.Errorf("found a transition conditioned on %q; only 'resolved' may transition automatically", match[1])
		}
	}
}

func TestIssueSortColumn(t *testing.T) {
	tests := []struct {
		name   string
		sort   IssueSort
		want   string
		wantOK bool
	}{
		{name: "default is last_seen", sort: "", want: "monitor.issues.last_seen", wantOK: true},
		{name: "last_seen", sort: IssueSortLastSeen, want: "monitor.issues.last_seen", wantOK: true},
		{name: "first_seen", sort: IssueSortFirstSeen, want: "monitor.issues.first_seen", wantOK: true},
		{name: "occurrences", sort: IssueSortOccurrences, want: "monitor.issues.occurrence_count", wantOK: true},
		{name: "unknown is rejected", sort: IssueSort("created_at"), wantOK: false},
		{name: "injection attempt is rejected", sort: IssueSort("last_seen; DROP TABLE monitor.issues--"), wantOK: false},
		{name: "injection via union is rejected", sort: IssueSort("1 UNION SELECT secret_value FROM secrets"), wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.sort.column()
			if ok != tt.wantOK {
				t.Fatalf("column() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("column() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestListIssuesRejectsUnknownSort asserts the allow-list is actually consulted
// on the query path, not merely defined. Sort reaches SQL as an identifier
// rather than a bound parameter, so this is the injection boundary.
func TestListIssuesRejectsUnknownSort(t *testing.T) {
	_, err := ListIssues(nil, ListIssuesRequest{Sort: IssueSort("last_seen; DROP TABLE monitor.issues--")})
	if err == nil {
		t.Fatal("expected an error for an unknown sort column")
	}
	if !strings.Contains(err.Error(), "invalid sort column") {
		t.Errorf("error = %q, want it to mention an invalid sort column", err)
	}
}

func TestUpdateIssueRequestIsEmpty(t *testing.T) {
	status := structs.IssueStatusResolved
	title := "t"

	tests := []struct {
		name string
		req  UpdateIssueRequest
		want bool
	}{
		{name: "nothing set", req: UpdateIssueRequest{}, want: true},
		{name: "status set", req: UpdateIssueRequest{Status: &status}, want: false},
		{name: "title set", req: UpdateIssueRequest{Title: &title}, want: false},
		{name: "clear flag alone counts as a change", req: UpdateIssueRequest{ClearAssignee: true}, want: false},
		{name: "clear title alone counts as a change", req: UpdateIssueRequest{ClearTitle: true}, want: false},
		{name: "clear priority alone counts as a change", req: UpdateIssueRequest{ClearPriority: true}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.req.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIssueStatusIsValid(t *testing.T) {
	tests := []struct {
		name   string
		status structs.IssueStatus
		want   bool
	}{
		{name: "unresolved", status: structs.IssueStatusUnresolved, want: true},
		{name: "in_progress", status: structs.IssueStatusInProgress, want: true},
		{name: "resolved", status: structs.IssueStatusResolved, want: true},
		{name: "ignored", status: structs.IssueStatusIgnored, want: true},
		{name: "backlog was deliberately dropped", status: structs.IssueStatus("backlog"), want: false},
		{name: "empty", status: structs.IssueStatus(""), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsValid(); got != tt.want {
				t.Errorf("IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestListIssuesRequestZeroValueIsUsable guards that a bare request sorts by the
// default column rather than erroring — the list endpoint relies on it.
func TestListIssuesRequestZeroValueIsUsable(t *testing.T) {
	req := ListIssuesRequest{}
	if _, ok := req.Sort.column(); !ok {
		t.Error("the zero-value sort must resolve to the default column")
	}
}

func TestUpsertIssueOccurrenceRequestCarriesTimestamps(t *testing.T) {
	// first_seen and last_seen are both bound from SeenAt on insert; the update
	// path then folds them with LEAST/GREATEST. This asserts the SQL binds the
	// timestamp twice, which is what makes an out-of-order event widen the
	// window rather than corrupt it.
	if n := strings.Count(upsertIssueSQL, "?"); n != 8 {
		t.Errorf("expected 8 bound parameters (6 identity + 2 timestamps), found %d", n)
	}
	if !strings.Contains(upsertIssueSQL, "LEAST(COALESCE(first_seen") {
		t.Error("first_seen must fold with LEAST so a late-arriving older event widens the window")
	}
	if !strings.Contains(upsertIssueSQL, "GREATEST(COALESCE(last_seen") {
		t.Error("last_seen must fold with GREATEST so an out-of-order event never moves it backwards")
	}
}
