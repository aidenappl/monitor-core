package query

import (
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// Shared fixtures for the configuration-table tests in this package. Declared
// once here rather than per file because they are package-level in Go and
// duplicating them is a compile error rather than a style question.
var (
	errSimulated = errors.New("simulated failure")
	testTime     = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
)

// alertRuleRows mirrors the column ORDER of alertRuleColumns, which is what
// scanAlertRule unpacks positionally. Written out in full rather than derived,
// for the reason apiKeyRows gives: a transposition here is a silent mis-scan, not
// a compile error, and almost every column is a string.
func alertRuleRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "name", "description", "type", "priority", "query_filters",
		"metric", "field", "condition", "threshold",
		"evaluation_interval_seconds", "for_seconds", "cooldown_seconds",
		"notification_channel_ids", "enabled", "created_at", "updated_at",
	}).AddRow("r-1", "5xx spike", "", "threshold", "P1", "[]",
		"count", "", "gt", 10.0,
		int64(60), int64(0), int64(300),
		"[]", true, testTime, testTime)
}

// TestAlertRuleSQLQuotesReservedWords is the guard on a trap that only fires at
// runtime, on the write path, in production.
//
// `condition` is a MariaDB reserved word — it is the DECLARE … CONDITION of
// stored programs. MariaDB's documented exception is that a word following a
// period in a QUALIFIED name is always read as an identifier, so
// `monitor.alert_rules.condition` in a SELECT list is legal bare. An UNQUALIFIED
// one is not: a bare `condition` in an INSERT column list or an UPDATE SET clause
// is a syntax error. Nothing catches that at compile time, `go vet` has no
// opinion, and the reads all keep working — so the first symptom is a 500 the
// first time somebody creates or edits an alert rule.
func TestAlertRuleSQLQuotesReservedWords(t *testing.T) {
	found := false
	for _, col := range alertRuleInsertColumns {
		if col == "condition" {
			t.Fatal("the INSERT column list carries a bare `condition`; MariaDB reads it as a reserved word and the statement is a syntax error")
		}
		if col == "`condition`" {
			found = true
		}
	}
	if !found {
		t.Error("the INSERT column list no longer names `condition` at all")
	}

	// And the other unqualified use: the SET clause of the partial update.
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	condition := "lt"
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())
	mock.ExpectExec("^UPDATE monitor.alert_rules SET `condition` = \\? WHERE id = \\?$").
		WithArgs("lt", "r-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())

	if _, err := UpdateAlertRule(mockDB, "r-1", UpdateAlertRuleRequest{Condition: &condition}); err != nil {
		t.Fatalf("UpdateAlertRule: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the UPDATE no longer backticks the reserved word: %v", err)
	}
}

// TestUpdateAlertRuleTouchesOnlyTheNamedColumns is the regression guard on the
// bug the pointer request type was introduced to fix.
//
// The ClickHouse implementation could not do a partial write: it read the whole
// row, merged in Go, and INSERTed a complete new version — so an omitted
// `enabled` was written back as `false` unless the merge remembered to preserve
// it. This asserts the statement itself, anchored at both ends, so an UPDATE that
// quietly gained an `enabled = ?` clause fails here rather than in production by
// silently disabling a rule somebody edited the threshold of.
func TestUpdateAlertRuleTouchesOnlyTheNamedColumns(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	threshold := 25.0
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())
	mock.ExpectExec("^UPDATE monitor.alert_rules SET threshold = \\? WHERE id = \\?$").
		WithArgs(25.0, "r-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())

	if _, err := UpdateAlertRule(mockDB, "r-1", UpdateAlertRuleRequest{Threshold: &threshold}); err != nil {
		t.Fatalf("UpdateAlertRule: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the partial update writes columns the caller did not name: %v", err)
	}
}

// TestUpdateAlertRuleLeavesUpdatedAtToTheColumn. updated_at carries ON UPDATE
// CURRENT_TIMESTAMP(3), so setting it in Go would be both redundant and less
// truthful — MariaDB stamps it only when a value actually changed.
func TestUpdateAlertRuleLeavesUpdatedAtToTheColumn(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	name := "5xx spike (EU)"
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())
	mock.ExpectExec("^UPDATE monitor.alert_rules SET name = \\? WHERE id = \\?$").
		WithArgs(name, "r-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())

	if _, err := UpdateAlertRule(mockDB, "r-1", UpdateAlertRuleRequest{Name: &name}); err != nil {
		t.Fatalf("UpdateAlertRule: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the update writes updated_at itself: %v", err)
	}
}

// TestCreateAlertRuleRejectsBadInputBeforeSQL. No expectations are registered, so
// any statement issued fails the call — which is what distinguishes "refused by
// the guard" from "refused by the ENUM". The distinction is the whole reason the
// Go-side maps survived the move: this produces a 400 naming the field, while the
// column produces errno 1265 ("Data truncated for column 'condition'") and a 500.
func TestCreateAlertRuleRejectsBadInputBeforeSQL(t *testing.T) {
	base := CreateAlertRuleRequest{Name: "spike", Type: "threshold", Condition: "gt"}

	tests := []struct {
		name    string
		mutate  func(*CreateAlertRuleRequest)
		wantMsg string
	}{
		{"missing name", func(r *CreateAlertRuleRequest) { r.Name = "" }, "name is required"},
		{"missing type", func(r *CreateAlertRuleRequest) { r.Type = "" }, "type is required"},
		{"unknown type", func(r *CreateAlertRuleRequest) { r.Type = "thresh0ld" }, "invalid type"},
		{"missing condition", func(r *CreateAlertRuleRequest) { r.Condition = "" }, "condition is required"},
		{"unknown condition", func(r *CreateAlertRuleRequest) { r.Condition = "gtt" }, "invalid condition"},
		{"unknown metric", func(r *CreateAlertRuleRequest) { r.Metric = "median" }, "invalid metric"},
		{"malformed query_filters", func(r *CreateAlertRuleRequest) { r.QueryFilters = "not json" }, "query_filters must be valid JSON"},
		{"malformed channel ids", func(r *CreateAlertRuleRequest) { r.NotificationChannelIDs = "{" }, "notification_channel_ids must be valid JSON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB, _, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			req := base
			tt.mutate(&req)

			_, err = CreateAlertRule(mockDB, req)
			if err == nil {
				t.Fatal("expected the create to be refused")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantMsg)
			}
		})
	}
}

// TestCreateAlertRuleAppliesDefaults pins the defaulting carried over verbatim
// from the ClickHouse implementation, including the asymmetry that an
// unrecognised PRIORITY is silently coerced while an unrecognised type or
// condition is an error. Priority only selects a routing policy; type and
// condition decide whether the rule can be evaluated at all.
func TestCreateAlertRuleAppliesDefaults(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("INSERT INTO monitor.alert_rules").WillReturnResult(sqlmock.NewResult(0, 1))

	rule, err := CreateAlertRule(mockDB, CreateAlertRuleRequest{
		Name: "spike", Type: "threshold", Condition: "gt", Priority: "P9",
	})
	if err != nil {
		t.Fatalf("CreateAlertRule: %v", err)
	}

	if rule.Priority != ALERT_PRIORITY_MEDIUM {
		t.Errorf("Priority = %q, want it coerced to %q", rule.Priority, ALERT_PRIORITY_MEDIUM)
	}
	if rule.Metric != "count" {
		t.Errorf("Metric = %q, want count", rule.Metric)
	}
	if rule.EvaluationIntervalSecs != 60 || rule.CooldownSeconds != 300 {
		t.Errorf("interval/cooldown = %d/%d, want 60/300", rule.EvaluationIntervalSecs, rule.CooldownSeconds)
	}
	// The JSON columns are NOT NULL with no default, so an omitted filter set has
	// to become "[]" here or the insert fails on a field the caller never sent.
	if rule.QueryFilters != "[]" || rule.NotificationChannelIDs != "[]" {
		t.Errorf("query_filters/channel_ids = %q/%q, want []/[]", rule.QueryFilters, rule.NotificationChannelIDs)
	}
	if rule.ID == "" {
		t.Error("no id was minted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestListEnabledAlertRulesFiltersOnEnabled. This is what the evaluator calls
// every 15 seconds; without the predicate every disabled rule starts firing again,
// and the only symptom is notifications for alerts somebody deliberately turned
// off.
func TestListEnabledAlertRulesFiltersOnEnabled(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("WHERE monitor.alert_rules.enabled = \\?").
		WithArgs(true).WillReturnRows(alertRuleRows())

	rules, err := ListEnabledAlertRules(mockDB)
	if err != nil {
		t.Fatalf("ListEnabledAlertRules: %v", err)
	}
	if len(rules) != 1 || rules[0].ID != "r-1" || !rules[0].Enabled {
		t.Fatalf("got %+v, want the one enabled rule", rules)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the evaluator's read is no longer filtered to enabled rules: %v", err)
	}
}

// TestAlertRuleReadsCarryNoFINAL. `FINAL` was on every ClickHouse read of this
// table because a ReplacingMergeTree can hold several versions of one row until a
// background merge collapses them — without it a disabled rule could still read
// back as enabled. There is one row per rule now, and a stray FINAL against
// MariaDB is a syntax error rather than a slow query, so this catches a
// copy-paste from the old implementation at the cheapest possible moment.
//
// A capturing matcher is used rather than a regexp expectation because the
// assertion is about what the statement does NOT contain, and a regexp
// expectation can only say what it does.
func TestAlertRuleReadsCarryNoFINAL(t *testing.T) {
	var captured []string
	matcher := sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
		captured = append(captured, actualSQL)
		return nil
	})

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("").WillReturnRows(alertRuleRows())
	if _, err := ListAlertRules(mockDB); err != nil {
		t.Fatalf("ListAlertRules: %v", err)
	}

	mock.ExpectQuery("").WillReturnRows(alertRuleRows())
	if _, err := ListEnabledAlertRules(mockDB); err != nil {
		t.Fatalf("ListEnabledAlertRules: %v", err)
	}

	if len(captured) != 2 {
		t.Fatalf("captured %d statements, want 2", len(captured))
	}
	for _, q := range captured {
		if strings.Contains(strings.ToUpper(q), "FINAL") {
			t.Errorf("a MariaDB read carries FINAL: %s", q)
		}
	}
}
