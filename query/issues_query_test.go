package query

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/db"
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
	if n := strings.Count(upsertIssueSQL, "?"); n != 9 {
		t.Errorf("expected 9 bound parameters (7 identity + 2 timestamps), found %d", n)
	}
	if !strings.Contains(upsertIssueSQL, "LEAST(COALESCE(first_seen") {
		t.Error("first_seen must fold with LEAST so a late-arriving older event widens the window")
	}
	if !strings.Contains(upsertIssueSQL, "GREATEST(COALESCE(last_seen") {
		t.Error("last_seen must fold with GREATEST so an out-of-order event never moves it backwards")
	}
}

// TestUpsertIssueSQLWritesProjectOnceAndNeverUpdatesIt pins the asymmetry in the
// statement: project is part of the INSERT and deliberately absent from the
// ON DUPLICATE KEY UPDATE list.
//
// An issue's project is not an attribute that can change — it is a component of
// the fingerprint the primary key is derived from, so any row this statement
// collides with already holds the same value. Adding project to the SET list
// would be a permanent no-op on the only path that reaches it, in exchange for
// making the occurrence fold capable of REWRITING which tenant an issue belongs
// to. There is no input that should be able to do that.
func TestUpsertIssueSQLWritesProjectOnceAndNeverUpdatesIt(t *testing.T) {
	insert, update, found := strings.Cut(upsertIssueSQL, "ON DUPLICATE KEY UPDATE")
	if !found {
		t.Fatal("upsert must use ON DUPLICATE KEY UPDATE")
	}

	if !strings.Contains(insert, "project") {
		t.Error("project must be written on insert; the column is NOT NULL and is what uq_issues_project_fingerprint is built on")
	}
	if regexp.MustCompile(`(?m)^\s*project\s*=`).MatchString(update) {
		t.Error("project is assigned in the update clause; the occurrence fold must never move an issue between tenants")
	}
}

// issueRows mirrors the column ORDER of issueColumns, which scanIssue unpacks
// positionally. Written out in full for the same reason apiKeyRows is: a column
// added to one and not the other is a silent mis-scan, not a compile error.
func issueRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "fingerprint", "project", "service", "name", "message", "path",
		"status", "priority", "title", "assignee_user_id",
		"occurrence_count", "regression_count",
		"first_seen", "last_seen", "resolved_at", "regressed_at",
		"inserted_at", "updated_at",
	}).AddRow(
		"11111111-1111-5111-8111-111111111111", "fp-1", "atlas", "atlas-api", "http.request.failed",
		"connection refused", "/v1/hotels", "unresolved", nil, nil, nil,
		int64(4), int64(0), now, now, nil, nil, now, now,
	)
}

// The issue tests below exist because monitor.issues is the one store carrying
// event data that scope.ProjectPredicate never touches — it is MariaDB, reached
// through db.SQL, so none of the ClickHouse chokepoints apply to it and the AST
// audit in scope/chokepoint_test.go does not look at it either.
//
// And an issue row is not metadata. `message` is the error text lifted verbatim
// off the event by issues.extractMessage, alongside the service, the path, the
// occurrence count and the seen-window. Unscoped, GET /v1/issues answered "what
// is failing, where, how often" for every tenant at once — while the raw events
// behind those same failures were correctly scoped the whole time.

// TestListIssuesScopesToProject pins the predicate on the list. This is the
// endpoint the dashboard's front page is built on and the one that hands out the
// ids every other issue endpoint is addressed by, so it is where an unscoped read
// does the most damage.
func TestListIssuesScopesToProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("monitor.issues.project = ?").
		WithArgs("atlas").
		WillReturnRows(issueRows())

	list, err := ListIssues(mockDB, ListIssuesRequest{Project: "atlas"})
	if err != nil {
		t.Fatalf("ListIssues: %v", err)
	}
	if len(list) != 1 || list[0].Project != "atlas" {
		t.Fatalf("got %d issues, want 1 in project atlas", len(list))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestIssueReadsRefuseWithoutAProject is the fail-closed half. An empty project
// must be an error, never a query that omits the predicate — the whole point of
// routing these through scopeIssues rather than an `if project != ""` at each
// call site.
//
// No expectations are registered on the mock, so ANY statement issued fails the
// call. That is what separates "refused by the guard" from "returned nothing".
func TestIssueReadsRefuseWithoutAProject(t *testing.T) {
	reads := map[string]func(db.Queryable) error{
		"ListIssues": func(e db.Queryable) error {
			_, err := ListIssues(e, ListIssuesRequest{})
			return err
		},
		"CountIssues": func(e db.Queryable) error {
			_, err := CountIssues(e, ListIssuesRequest{})
			return err
		},
		"GetIssue": func(e db.Queryable) error {
			_, err := GetIssue(e, "", "11111111-1111-5111-8111-111111111111")
			return err
		},
		"UpdateIssue": func(e db.Queryable) error {
			s := structs.IssueStatusResolved
			_, err := UpdateIssue(e, "", "11111111-1111-5111-8111-111111111111", UpdateIssueRequest{Status: &s})
			return err
		},
	}

	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			if err := read(mockDB); !errors.Is(err, ErrNoIssueProject) {
				t.Errorf("error = %v, want ErrNoIssueProject", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement was issued with no project: %v", err)
			}
		})
	}
}

// TestGetIssueScopesToProject checks the id-addressed read, which is the one it
// would be easiest to argue does not need scoping: the id is a UUIDv5 over a
// fingerprint that already contains the project, so a foreign id cannot be
// DERIVED. It can, however, be read off a list, a monitor-web URL, an alert
// payload or a comment — and an unguessable id is not an access control.
func TestGetIssueScopesToProject(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	const id = "11111111-1111-5111-8111-111111111111"
	mock.ExpectQuery("monitor.issues.project = ?").
		WithArgs(id, "atlas").
		WillReturnRows(issueRows())

	issue, err := GetIssue(mockDB, "atlas", id)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if issue == nil {
		t.Fatal("GetIssue returned no issue")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateIssueScopesTheWriteItself pins the project into the UPDATE's own
// WHERE, not just into the handler's preceding read.
//
// The handler check is what produces the 404 and is staying, but tenancy
// enforced across two statements is enforced in the gap between them, and the
// next caller of UpdateIssue will not necessarily write the read half. Resolving
// or re-assigning another project's issue has to match no row.
func TestUpdateIssueScopesTheWriteItself(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	const id = "11111111-1111-5111-8111-111111111111"
	status := structs.IssueStatusResolved

	// sq.Eq sorts its map keys, so the trailing bindings are (id, project).
	mock.ExpectExec("UPDATE monitor.issues SET .* WHERE id = \\? AND project = \\?").
		WithArgs("resolved", id, "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("monitor.issues.project = ?").
		WithArgs(id, "atlas").
		WillReturnRows(issueRows())

	if _, err := UpdateIssue(mockDB, "atlas", id, UpdateIssueRequest{Status: &status}); err != nil {
		t.Fatalf("UpdateIssue: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
