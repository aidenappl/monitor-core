package alerts

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

func TestCheckCondition(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		condition string
		threshold float64
		want      bool
	}{
		{"gt true", 10, "gt", 5, true},
		{"gt false equal", 5, "gt", 5, false},
		{"gt false less", 3, "gt", 5, false},
		{"lt true", 3, "lt", 5, true},
		{"lt false equal", 5, "lt", 5, false},
		{"gte true equal", 5, "gte", 5, true},
		{"gte true greater", 6, "gte", 5, true},
		{"gte false", 4, "gte", 5, false},
		{"lte true equal", 5, "lte", 5, true},
		{"lte true less", 4, "lte", 5, true},
		{"lte false", 6, "lte", 5, false},
		{"eq true", 5, "eq", 5, true},
		{"eq false", 5.1, "eq", 5, false},
		{"unknown condition", 100, "between", 5, false},
		{"empty condition", 100, "", 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckCondition(tt.value, tt.condition, tt.threshold); got != tt.want {
				t.Errorf("CheckCondition(%v, %q, %v) = %v, want %v", tt.value, tt.condition, tt.threshold, got, tt.want)
			}
		})
	}
}

// ratePercent mirrors the rate_change computation in evaluateRuleState. Keeping it
// as a pure helper lets us unit-test the math (including the div-by-zero guard)
// without a database.
func ratePercent(cur, prev float64) float64 {
	if prev == 0 {
		if cur > 0 {
			return 100
		}
		return 0
	}
	return (cur - prev) / prev * 100
}

func TestRatePercent(t *testing.T) {
	tests := []struct {
		name string
		cur  float64
		prev float64
		want float64
	}{
		{"doubling", 20, 10, 100},
		{"halving", 5, 10, -50},
		{"no change", 10, 10, 0},
		{"prev zero cur positive", 5, 0, 100},
		{"prev zero cur zero", 0, 0, 0},
		{"increase 10pct", 11, 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ratePercent(tt.cur, tt.prev); got != tt.want {
				t.Errorf("ratePercent(%v, %v) = %v, want %v", tt.cur, tt.prev, got, tt.want)
			}
		})
	}
}

// absenceFiring mirrors the absence firing rule (fires when count is zero).
func absenceFiring(count float64) bool { return count == 0 }

func TestAbsenceFiring(t *testing.T) {
	if !absenceFiring(0) {
		t.Errorf("absenceFiring(0) = false, want true")
	}
	if absenceFiring(1) {
		t.Errorf("absenceFiring(1) = true, want false")
	}
}

func TestNumericFieldExpr(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		wantErr bool
	}{
		{"valid data field", "data.latency_ms", false},
		{"empty field", "", true},
		{"non-data field", "service", true},
		{"injection attempt", "data.x') = 1 OR ('1'='1", true},
		{"injection quote", "data.x'", true},
		{"nested dotted key", "data.http.status", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := numericFieldExpr(tt.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("numericFieldExpr(%q) err = %v, wantErr %v", tt.field, err, tt.wantErr)
			}
		})
	}
}

func TestBuildFilterCondition(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		operator string
		wantErr  bool
	}{
		{"valid column", "service", "eq", false},
		{"valid data field", "data.code", "eq", false},
		{"unknown column", "not_a_column", "eq", true},
		{"injection in data key", "data.x') = 1--", "eq", true},
		{"injection with quote", "data.a'; DROP TABLE events;--", "eq", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := buildFilterCondition(structs.QueryFilter{Field: tt.field, Operator: tt.operator, Value: "x"})
			if (err != nil) != tt.wantErr {
				t.Errorf("buildFilterCondition(field=%q) err = %v, wantErr %v", tt.field, err, tt.wantErr)
			}
		})
	}
}

// withDefaultProject pins env.DefaultProjectSlug for the duration of a test.
// env.Load() never runs under `go test`, so the var is empty unless a test sets
// it, and scope.ProjectPredicate's empty-string transition arm keys off it.
func withDefaultProject(t *testing.T, slug string) {
	t.Helper()
	previous := env.DefaultProjectSlug
	env.DefaultProjectSlug = slug
	t.Cleanup(func() { env.DefaultProjectSlug = previous })
}

// TestAggQueryScopesWhenContextCarriesAProject covers the HTTP half of alert
// evaluation: POST /v1/alert-rules/{id}/test reaches queryAggForRange through
// EvaluateRuleNow with the request's own context, which QueryAuthMiddleware has
// already stamped with a project.
//
// Unscoped, that endpoint was an oracle rather than a gap. A rule carries a
// caller-chosen aggregation, field and filter set, so an admin key bound to one
// project could POST a rule and read back a count or a max over EVERY project's
// events — one number per request, no row ever crossing the boundary to notice.
func TestAggQueryScopesWhenContextCarriesAProject(t *testing.T) {
	withDefaultProject(t, "default")

	ctx := scope.WithProject(context.Background(), "atlas")
	sql, args, err := buildAggQuery(ctx, &structs.AlertRule{}, "toFloat64(count())", time.Unix(0, 0), time.Unix(60, 0))
	if err != nil {
		t.Fatalf("buildAggQuery: %v", err)
	}

	if !strings.Contains(sql, "project = ?") {
		t.Errorf("statement carries no project predicate — the test endpoint reads every tenant:\n\t%s", sql)
	}
	// A non-default project matches EXACTLY. Picking up the empty-string arm here
	// would hand it every pre-006 row, which belongs to the default project.
	if strings.Contains(sql, "project = ''") {
		t.Errorf("non-default project matched the empty stamp:\n\t%s", sql)
	}
	// Order is the whole reason the predicate is appended before the filter loop:
	// these are positional placeholders, so from, to, project is the only vector
	// that lines up with the text.
	want := []interface{}{time.Unix(0, 0), time.Unix(60, 0), "atlas"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("args = %v, want %v", args, want)
	}
}

// TestAggQueryStaysZoneWideForTheTimer pins the OTHER half, which is a decision
// rather than an oversight: Evaluator.Run has a background context, no
// credential and no project, and a rule's threshold is deliberately a count of
// the whole zone. If this ever starts failing because the predicate went
// unconditional, alert_rules needs a project column in the same change — see the
// KNOWN GAP header.
func TestAggQueryStaysZoneWideForTheTimer(t *testing.T) {
	withDefaultProject(t, "default")

	sql, args, err := buildAggQuery(context.Background(), &structs.AlertRule{}, "toFloat64(count())", time.Unix(0, 0), time.Unix(60, 0))
	if err != nil {
		t.Fatalf("buildAggQuery: %v", err)
	}

	if strings.Contains(sql, "project") {
		t.Errorf("timer path acquired a project predicate with no project to scope to:\n\t%s", sql)
	}
	if len(args) != 2 {
		t.Errorf("args = %v, want just the time range", args)
	}
}

// TestAggQueryBindsFiltersAfterTheProject is the argument-ordering guard. The
// predicate and the rule's filters both bind positionally into one statement, so
// a future edit that appends the project after the filter loop produces valid
// SQL with every binding slid one place along — no error, wrong rows, and a
// project slug fed into a filter comparison.
func TestAggQueryBindsFiltersAfterTheProject(t *testing.T) {
	withDefaultProject(t, "default")

	rule := &structs.AlertRule{QueryFilters: `[{"field":"service","operator":"eq","value":"atlas-api"}]`}
	ctx := scope.WithProject(context.Background(), "atlas")

	sql, args, err := buildAggQuery(ctx, rule, "toFloat64(count())", time.Unix(0, 0), time.Unix(60, 0))
	if err != nil {
		t.Fatalf("buildAggQuery: %v", err)
	}

	projectAt := strings.Index(sql, "project = ?")
	serviceAt := strings.Index(sql, "service")
	if projectAt < 0 || serviceAt < 0 {
		t.Fatalf("expected both a project predicate and the rule's filter:\n\t%s", sql)
	}
	if projectAt > serviceAt {
		t.Errorf("project predicate appears after the rule's filter; the bound args no longer line up:\n\t%s", sql)
	}

	want := []interface{}{time.Unix(0, 0), time.Unix(60, 0), "atlas", "atlas-api"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("args = %v, want %v", args, want)
	}
}

// TestAggQueryMatchesUnstampedRowsOnlyForTheDefaultProject checks that alert
// evaluation reads the transition window the same way every other read does.
// Pre-006 rows carry an empty project and are visible to the default project
// alone; a second implementation of that rule here would drift from
// scope.ProjectPredicate the day the arm is removed.
func TestAggQueryMatchesUnstampedRowsOnlyForTheDefaultProject(t *testing.T) {
	withDefaultProject(t, "default")

	ctx := scope.WithProject(context.Background(), "default")
	sql, _, err := buildAggQuery(ctx, &structs.AlertRule{}, "toFloat64(count())", time.Unix(0, 0), time.Unix(60, 0))
	if err != nil {
		t.Fatalf("buildAggQuery: %v", err)
	}

	if !strings.Contains(sql, "(project = ? OR project = '')") {
		t.Errorf("default project did not pick up the empty-string transition arm:\n\t%s", sql)
	}
}
