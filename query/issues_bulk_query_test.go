package query

import (
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aidenappl/monitor-core/structs"
)

func TestBulkUpdateIssueStatusValidation(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	tooMany := make([]string, MAX_BULK_ISSUES+1)
	service := "svc"

	tests := []struct {
		name string
		req  BulkUpdateIssueStatusRequest
		want error
	}{
		{"no project", BulkUpdateIssueStatusRequest{Status: structs.IssueStatusResolved, IDs: []string{"a"}}, ErrNoIssueProject},
		{"neither ids nor filter", BulkUpdateIssueStatusRequest{Project: "p", Status: structs.IssueStatusResolved}, ErrBulkNoTarget},
		{"both ids and filter", BulkUpdateIssueStatusRequest{Project: "p", Status: structs.IssueStatusResolved, IDs: []string{"a"}, Filter: &ListIssuesRequest{Service: &service}}, ErrBulkNoTarget},
		{"empty ids", BulkUpdateIssueStatusRequest{Project: "p", Status: structs.IssueStatusResolved, IDs: []string{}}, ErrBulkNoTarget},
		{"too many ids", BulkUpdateIssueStatusRequest{Project: "p", Status: structs.IssueStatusResolved, IDs: tooMany}, ErrBulkTooManyIDs},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BulkUpdateIssueStatus(mockDB, tt.req); !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
		})
	}

	t.Run("invalid status", func(t *testing.T) {
		req := BulkUpdateIssueStatusRequest{Project: "p", Status: "done", IDs: []string{"a"}}
		if _, err := BulkUpdateIssueStatus(mockDB, req); err == nil {
			t.Error("expected an invalid status to be refused")
		}
	})
}

// The write must be scoped to the project and skip issues already at the target
// status, inside one transaction whose locking read supplies the "from" values.
func TestBulkUpdateIssueStatusByFilter(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	service := "scraper-service"
	req := BulkUpdateIssueStatusRequest{
		Project: "atlas",
		Status:  structs.IssueStatusResolved,
		Filter:  &ListIssuesRequest{Service: &service},
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor.issues WHERE monitor.issues.project = \? AND monitor.issues.service = \? AND monitor.issues.status <> \?`).
		WithArgs("atlas", service, "resolved").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery(`SELECT monitor.issues.id, monitor.issues.status FROM monitor.issues WHERE .* ORDER BY monitor.issues.last_seen DESC, monitor.issues.id ASC LIMIT 1000 FOR UPDATE`).
		WithArgs("atlas", service, "resolved").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).
			AddRow("id-1", "unresolved").
			AddRow("id-2", "in_progress"))
	mock.ExpectExec(`UPDATE monitor.issues SET status = \?, resolved_at = CURRENT_TIMESTAMP\(3\) WHERE id IN \(\?,\?\) AND project = \?`).
		WithArgs("resolved", "id-1", "id-2", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	res, err := BulkUpdateIssueStatus(mockDB, req)
	if err != nil {
		t.Fatalf("BulkUpdateIssueStatus: %v", err)
	}
	if res.Matched != 2 || res.Updated != 2 || res.Remaining != 0 {
		t.Errorf("result = %+v, want matched 2, updated 2, remaining 0", res)
	}
	if len(res.Changes) != 2 || res.Changes[1].From != structs.IssueStatusInProgress {
		t.Errorf("changes = %+v, want the prior statuses carried through", res.Changes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestBulkUpdateIssueStatusDryRunWritesNothing(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor.issues`).
		WithArgs("atlas", "a", "b", "unresolved").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	res, err := BulkUpdateIssueStatus(mockDB, BulkUpdateIssueStatusRequest{
		Project: "atlas", Status: structs.IssueStatusUnresolved, IDs: []string{"a", "b"}, DryRun: true,
	})
	if err != nil {
		t.Fatalf("BulkUpdateIssueStatus: %v", err)
	}
	if res.Matched != 1 || res.Updated != 0 || res.Remaining != 1 {
		t.Errorf("result = %+v, want matched 1, updated 0, remaining 1", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a dry run must issue only the count: %v", err)
	}
}

// Leaving resolved must clear resolved_at, the same rule UpdateIssue follows.
func TestBulkUpdateIssueStatusReopenClearsResolvedAt(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT COUNT`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow("a", "resolved"))
	mock.ExpectExec(`UPDATE monitor.issues SET status = \?, resolved_at = \? WHERE`).
		WithArgs("unresolved", nil, "a", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if _, err := BulkUpdateIssueStatus(mockDB, BulkUpdateIssueStatusRequest{
		Project: "atlas", Status: structs.IssueStatusUnresolved, IDs: []string{"a"},
	}); err != nil {
		t.Fatalf("BulkUpdateIssueStatus: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A priority + assignee change targets rows where EITHER differs (null-safe),
// reads the prior values for the timeline, and leaves status and resolved_at
// untouched when no status was asked for.
func TestBulkUpdateIssuesPriorityAndAssignee(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	high := structs.IssuePriorityHigh
	user := int64(7)
	req := BulkUpdateIssueStatusRequest{
		Project:     "atlas",
		SetPriority: true, Priority: &high,
		SetAssignee: true, AssigneeUserID: &user,
		IDs: []string{"a", "b"},
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor.issues WHERE monitor.issues.project = \? AND monitor.issues.id IN \(\?,\?\) AND \(NOT \(monitor.issues.priority <=> \?\) OR NOT \(monitor.issues.assignee_user_id <=> \?\)\)`).
		WithArgs("atlas", "a", "b", "high", int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery(`SELECT monitor.issues.id, monitor.issues.status, monitor.issues.priority, monitor.issues.assignee_user_id FROM monitor.issues .* FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "priority", "assignee_user_id"}).
			AddRow("a", "unresolved", nil, nil).
			AddRow("b", "in_progress", "low", int64(3)))
	mock.ExpectExec(`UPDATE monitor.issues SET priority = \?, assignee_user_id = \? WHERE id IN \(\?,\?\) AND project = \?`).
		WithArgs("high", int64(7), "a", "b", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	res, err := BulkUpdateIssueStatus(mockDB, req)
	if err != nil {
		t.Fatalf("BulkUpdateIssueStatus: %v", err)
	}
	if res.Updated != 2 {
		t.Errorf("updated = %d, want 2", res.Updated)
	}
	if res.Changes[0].FromPriority != nil || res.Changes[1].FromPriority == nil || *res.Changes[1].FromPriority != structs.IssuePriorityLow {
		t.Errorf("prior priorities not carried: %+v", res.Changes)
	}
	if res.Changes[1].FromAssignee == nil || *res.Changes[1].FromAssignee != 3 {
		t.Errorf("prior assignee not carried: %+v", res.Changes[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Clearing the priority sends NULL and skips rows already NULL (<=> NULL).
func TestBulkUpdateIssuesClearPriority(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor.issues .* AND \(NOT \(monitor.issues.priority <=> \?\)\)`).
		WithArgs("atlas", "a", nil).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "priority", "assignee_user_id"}).AddRow("a", "unresolved", "high", nil))
	mock.ExpectExec(`UPDATE monitor.issues SET priority = \? WHERE`).
		WithArgs(nil, "a", "atlas").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if _, err := BulkUpdateIssueStatus(mockDB, BulkUpdateIssueStatusRequest{
		Project: "atlas", SetPriority: true, IDs: []string{"a"},
	}); err != nil {
		t.Fatalf("BulkUpdateIssueStatus: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestBulkUpdateIssuesRefusesNothingToSet(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	_, err = BulkUpdateIssueStatus(mockDB, BulkUpdateIssueStatusRequest{Project: "atlas", IDs: []string{"a"}})
	if !errors.Is(err, ErrBulkNothingToSet) {
		t.Errorf("err = %v, want ErrBulkNothingToSet", err)
	}
	bad := structs.IssuePriority("urgent")
	if _, err := BulkUpdateIssueStatus(mockDB, BulkUpdateIssueStatusRequest{Project: "atlas", SetPriority: true, Priority: &bad, IDs: []string{"a"}}); err == nil {
		t.Error("expected an invalid priority to be refused")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a refused request must not reach the database: %v", err)
	}
}
