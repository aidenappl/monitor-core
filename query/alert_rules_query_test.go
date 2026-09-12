package query

import (
	"database/sql"
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
		"id", "project", "name", "description", "type", "priority", "query_filters",
		"metric", "field", "condition", "threshold",
		"evaluation_interval_seconds", "for_seconds", "cooldown_seconds",
		"notification_channel_ids", "enabled", "created_at", "updated_at",
	}).AddRow("r-1", "atlas", "5xx spike", "", "threshold", "P1", "[]",
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
	mock.ExpectExec("^UPDATE monitor.alert_rules SET `condition` = \\? WHERE id = \\? AND project = \\?$").
		WithArgs("lt", "r-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())

	if _, err := UpdateAlertRule(mockDB, "atlas", "r-1", UpdateAlertRuleRequest{Condition: &condition}); err != nil {
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
	mock.ExpectExec("^UPDATE monitor.alert_rules SET threshold = \\? WHERE id = \\? AND project = \\?$").
		WithArgs(25.0, "r-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())

	if _, err := UpdateAlertRule(mockDB, "atlas", "r-1", UpdateAlertRuleRequest{Threshold: &threshold}); err != nil {
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
	mock.ExpectExec("^UPDATE monitor.alert_rules SET name = \\? WHERE id = \\? AND project = \\?$").
		WithArgs(name, "r-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT .* FROM monitor.alert_rules WHERE").WillReturnRows(alertRuleRows())

	if _, err := UpdateAlertRule(mockDB, "atlas", "r-1", UpdateAlertRuleRequest{Name: &name}); err != nil {
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

			_, err = CreateAlertRule(mockDB, "atlas", req)
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

	rule, err := CreateAlertRule(mockDB, "atlas", CreateAlertRuleRequest{
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
	if _, err := ListAlertRules(mockDB, "atlas"); err != nil {
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

// TestAlertRuleReadsAreProjectScoped covers the three reads a caller can reach
// and the one it deliberately cannot.
//
// Before migration 127 this table had no tenancy column at all, so GET
// /v1/alert-rules returned every project's rules — names, thresholds, filters and
// channel ids — and GET /v1/alert-rules/{id} resolved any id in the zone. The
// second matters more than the first: a rule id read off somebody else's list
// could then be handed to POST /v1/alert-rules/{id}/test, whose response is an
// aggregate over that rule's events.
func TestAlertRuleReadsAreProjectScoped(t *testing.T) {
	t.Run("list binds the project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.alert_rules.project = \\?").
			WithArgs("atlas").WillReturnRows(alertRuleRows())

		rules, err := ListAlertRules(mockDB, "atlas")
		if err != nil {
			t.Fatalf("ListAlertRules: %v", err)
		}
		if len(rules) != 1 || rules[0].Project != "atlas" {
			t.Fatalf("got %+v, want the project scanned back off the row", rules)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the list shows every tenant's rules: %v", err)
		}
	})

	t.Run("get binds the project alongside the id", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("WHERE monitor.alert_rules.id = \\? AND monitor.alert_rules.project = \\?").
			WithArgs("r-1", "atlas").WillReturnRows(alertRuleRows())

		if _, err := GetAlertRule(mockDB, "atlas", "r-1"); err != nil {
			t.Fatalf("GetAlertRule: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("an id from another project still resolves: %v", err)
		}
	})

	// The evaluator's read is the ONE that must stay unscoped, and the pair of
	// assertions exists so a change that "makes them consistent" breaks exactly
	// one of them. Scoping this would stop evaluating every tenant outside
	// whatever project the caller named — alerting that reports healthy and fires
	// nothing, with no error anywhere. The tenancy is applied one layer up, in
	// Evaluator.evaluateAll, which stamps each rule's own project before
	// evaluating it.
	t.Run("the evaluator read spans every project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		mock.ExpectQuery("^SELECT (?s).* FROM monitor.alert_rules WHERE monitor.alert_rules.enabled = \\? ORDER BY").
			WithArgs(true).WillReturnRows(alertRuleRows())

		if _, err := ListEnabledAlertRules(mockDB); err != nil {
			t.Fatalf("ListEnabledAlertRules: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the evaluator's read acquired a project predicate — every other tenant's rules stop firing: %v", err)
		}
	})
}

// TestAlertRuleMutationsCarryTheProjectThemselves.
//
// The predicate is in the UPDATE's and the DELETE's OWN WHERE, not inherited
// from the read that precedes them. Read-then-write is not atomic, and a future
// caller that skips or reorders the lookup would otherwise be able to retarget
// another tenant's rule at its own channels — a redirect of somebody else's
// alerts — or delete it outright, silencing their alerting with a 401 at nothing
// and no explanation anywhere in the product.
func TestAlertRuleMutationsCarryTheProjectThemselves(t *testing.T) {
	t.Run("delete scopes without reading first", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// No SELECT is expected: a read before the delete would fail this.
		mock.ExpectExec("^DELETE FROM monitor.alert_rules WHERE id = \\? AND project = \\?$").
			WithArgs("r-1", "atlas").WillReturnResult(sqlmock.NewResult(0, 1))

		deleted, err := DeleteAlertRule(mockDB, "atlas", "r-1")
		if err != nil {
			t.Fatalf("DeleteAlertRule: %v", err)
		}
		if !deleted {
			t.Error("DeleteAlertRule reported no row deleted")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the delete is not project-scoped: %v", err)
		}
	})

	t.Run("insert binds the project", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()

		// The column is NOT NULL with no default (migration 127), so a create
		// that dropped the field would fail as errno 1364 in production and
		// nowhere else. Asserted as a bound ARGUMENT rather than assumed from
		// the returned struct.
		mock.ExpectExec("INSERT INTO monitor.alert_rules").
			WithArgs(sqlmock.AnyArg(), "atlas", "spike", "", "threshold", "P2", "[]", "count",
				"", "gt", 0.0, uint32(60), uint32(0), uint32(300), "[]", false,
				sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		rule, err := CreateAlertRule(mockDB, "atlas", CreateAlertRuleRequest{
			Name: "spike", Type: "threshold", Condition: "gt",
		})
		if err != nil {
			t.Fatalf("CreateAlertRule: %v", err)
		}
		if rule.Project != "atlas" {
			t.Errorf("Project = %q, want atlas on the returned rule", rule.Project)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("the insert does not carry the project: %v", err)
		}
	})
}

// TestAlertingBuildersRefuseAnEmptyProject.
//
// An empty project must be an ERROR, never a query that quietly returns or
// touches every tenant's rows. This is the MariaDB counterpart of
// scope.ErrNoProject, and the table covers every exported entry point of all four
// alerting tables in one place — the failure it prevents is identical in each and
// a per-file copy would drift.
//
// No expectations are registered on the mock, so any statement issued fails the
// call: that is what distinguishes "refused by the guard" from "refused by SQL".
func TestAlertingBuildersRefuseAnEmptyProject(t *testing.T) {
	calls := []struct {
		name string
		call func(mockDB *sql.DB) error
	}{
		{"ListAlertRules", func(db *sql.DB) error { _, err := ListAlertRules(db, ""); return err }},
		{"GetAlertRule", func(db *sql.DB) error { _, err := GetAlertRule(db, "", "r-1"); return err }},
		{"CreateAlertRule", func(db *sql.DB) error {
			_, err := CreateAlertRule(db, "", CreateAlertRuleRequest{Name: "n", Type: "threshold", Condition: "gt"})
			return err
		}},
		{"UpdateAlertRule", func(db *sql.DB) error {
			name := "n"
			_, err := UpdateAlertRule(db, "", "r-1", UpdateAlertRuleRequest{Name: &name})
			return err
		}},
		{"DeleteAlertRule", func(db *sql.DB) error { _, err := DeleteAlertRule(db, "", "r-1"); return err }},

		{"ListNotificationChannels", func(db *sql.DB) error { _, err := ListNotificationChannels(db, ""); return err }},
		{"GetNotificationChannel", func(db *sql.DB) error { _, err := GetNotificationChannel(db, "", "c-1"); return err }},
		{"CreateNotificationChannel", func(db *sql.DB) error {
			_, err := CreateNotificationChannel(db, "", CreateNotificationChannelRequest{Name: "n", Type: "slack"})
			return err
		}},
		{"DeleteNotificationChannel", func(db *sql.DB) error { _, err := DeleteNotificationChannel(db, "", "c-1"); return err }},

		{"ListServiceGroups", func(db *sql.DB) error { _, err := ListServiceGroups(db, ""); return err }},
		{"GetServiceGroup", func(db *sql.DB) error { _, err := GetServiceGroup(db, "", "g-1"); return err }},
		{"CreateServiceGroup", func(db *sql.DB) error {
			_, err := CreateServiceGroup(db, "", CreateServiceGroupRequest{Name: "n"})
			return err
		}},
		{"UpdateServiceGroup", func(db *sql.DB) error {
			_, err := UpdateServiceGroup(db, "", "g-1", UpdateServiceGroupRequest{Name: "n"})
			return err
		}},
		{"DeleteServiceGroup", func(db *sql.DB) error { _, err := DeleteServiceGroup(db, "", "g-1"); return err }},

		{"ListNotificationPolicies", func(db *sql.DB) error { _, err := ListNotificationPolicies(db, ""); return err }},
		{"GetNotificationPolicy", func(db *sql.DB) error { _, err := GetNotificationPolicy(db, "", "p-1"); return err }},
		{"CreateNotificationPolicy", func(db *sql.DB) error {
			_, err := CreateNotificationPolicy(db, "", CreateNotificationPolicyRequest{Name: "n"})
			return err
		}},
		{"UpdateNotificationPolicy", func(db *sql.DB) error {
			_, err := UpdateNotificationPolicy(db, "", "p-1", UpdateNotificationPolicyRequest{Name: "n"})
			return err
		}},
		{"DeleteNotificationPolicy", func(db *sql.DB) error { return DeleteNotificationPolicy(db, "", "p-1") }},
		{"ReorderNotificationPolicies", func(db *sql.DB) error {
			return ReorderNotificationPolicies(db, "", []string{"p-1"})
		}},
		{"SeedDefaultNotificationPolicies", func(db *sql.DB) error { return SeedDefaultNotificationPolicies(db, "") }},
	}

	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			err = c.call(mockDB)
			if err == nil {
				t.Fatal("an empty project was accepted — this call reads or writes across every tenant")
			}
			if !errors.Is(err, ErrNoAlertingProject) {
				t.Errorf("err = %v, want ErrNoAlertingProject so a caller can tell a wiring fault from bad input", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("a statement reached the database before the guard: %v", err)
			}
		})
	}
}
