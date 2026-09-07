package db

import (
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

// The name reaches SQL as text, never as a bound parameter, so the accept cases
// pin what production actually runs on and the reject cases pin the shapes that
// would otherwise end a statement early or start a second one.
func TestValidateDatabaseName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{name: "the deployed default", input: "monitor", valid: true},
		{name: "underscores and digits", input: "monitor_staging2", valid: true},
		{name: "minimum length", input: "abc", valid: true},

		{name: "empty", input: "", valid: false},
		{name: "too short", input: "ab", valid: false},
		{name: "leading digit", input: "2monitor", valid: false},
		{name: "uppercase", input: "Monitor", valid: false},
		{name: "hyphen", input: "monitor-staging", valid: false},
		{name: "qualified", input: "monitor.events", valid: false},
		{name: "statement terminator", input: "monitor; DROP TABLE events", valid: false},
		{name: "backtick quoted", input: "`monitor`", valid: false},
		{name: "whitespace", input: "monitor ", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabaseName(tt.input)
			if tt.valid && err != nil {
				t.Errorf("ValidateDatabaseName(%q) = %v, want nil", tt.input, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("ValidateDatabaseName(%q) = nil, want an error", tt.input)
			}
		})
	}
}

// eventInsertColumns and eventInsertRow are two hand-maintained lists that have
// to agree position for position, and nothing in the type system makes them.
// Every column but timestamp carries a string, so transposing two of them
// inserts cleanly: no type error, no driver error, no failed batch — just rows
// with service names stored in env, or project names stored in service, that
// every later query and dashboard reads as genuine. That silence is the whole
// reason this test exists.
//
// It works by giving each field a sentinel equal to its own column name, so a
// value sitting in the wrong slot names the slot it should have been in. A
// column added to one list and not the other fails on length before that.
func TestEventInsertColumnsMatchRowOrder(t *testing.T) {
	timestamp := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

	event := &structs.Event{
		Timestamp: timestamp,
		Service:   "service",
		Project:   "project",
		Env:       "env",
		JobID:     "job_id",
		RequestID: "request_id",
		TraceID:   "trace_id",
		UserID:    "user_id",
		Name:      "name",
		Level:     "level",
		Data:      map[string]interface{}{"key": "value"},
		IssueID:   "issue_id",
	}

	// The two columns whose value is not a plain sentinel string. Everything
	// else is expected to equal the name of its own column.
	special := map[string]interface{}{
		"timestamp": timestamp,
		"data":      event.DataJSON(),
	}

	row := eventInsertRow(event)
	if len(row) != len(eventInsertColumns) {
		t.Fatalf("eventInsertRow returned %d values for %d columns %v: a column was added to one list and not the other",
			len(row), len(eventInsertColumns), eventInsertColumns)
	}

	seen := make(map[string]bool, len(eventInsertColumns))
	for i, column := range eventInsertColumns {
		if seen[column] {
			t.Errorf("column %q is listed twice, at position %d", column, i)
		}
		seen[column] = true

		want, ok := special[column]
		if !ok {
			want = column
		}
		if row[i] != want {
			t.Errorf("position %d is column %q but carries %#v, want %#v", i, column, row[i], want)
		}
	}
}

// The tenancy column has to be written, not merely declared. An events row whose
// project is empty is indistinguishable from a row written before the column
// existed, which is precisely the case the read path treats as the default
// project — so a dropped project on the write path does not fail loudly, it
// quietly files a tenant's traffic under someone else's.
func TestEventInsertWritesProject(t *testing.T) {
	row := eventInsertRow(&structs.Event{Project: "trailblaze"})

	index := -1
	for i, column := range eventInsertColumns {
		if column == "project" {
			index = i
			break
		}
	}
	if index == -1 {
		t.Fatal("eventInsertColumns does not include project: events are being written with no tenant")
	}
	if row[index] != "trailblaze" {
		t.Errorf("project column carries %#v, want %#v", row[index], "trailblaze")
	}
}
