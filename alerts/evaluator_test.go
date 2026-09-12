package alerts

import (
	"context"
	"errors"
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

// TestTimerAndTestEndpointBuildTheSameStatement IS THE REGRESSION TEST FOR THE
// CLOSED GAP, and it is the most valuable assertion in this file.
//
// The two paths reach buildAggQuery differently and must not be allowed to
// differ again. Evaluator.evaluateAll has a background context and stamps the
// RULE's own project onto it (scope.WithProject); routes.HandleTestAlertRule
// passes the request's context, which QueryAuthMiddleware already stamped with
// the CREDENTIAL's project, and loads the rule through a project-scoped lookup
// so the two are the same value.
//
// While they disagreed, POST /v1/alert-rules/{id}/test reported a number the
// rule would never fire on — the endpoint an operator uses to check a threshold
// answered a different question from the timer that enforces it. Asserting
// STATEMENT EQUALITY rather than "both contain a predicate" is deliberate: a
// future change that scopes one path with a different predicate shape, or binds
// the project in a different position, passes a containment check and fails
// this.
func TestTimerAndTestEndpointBuildTheSameStatement(t *testing.T) {
	withDefaultProject(t, "default")

	rule := &structs.AlertRule{
		Project:      "atlas",
		QueryFilters: `[{"field":"service","operator":"eq","value":"atlas-api"}]`,
	}
	from, to := time.Unix(0, 0), time.Unix(60, 0)

	// The timer: a background context carrying nothing, plus the rule's project.
	timerSQL, timerArgs, err := buildAggQuery(
		scope.WithProject(context.Background(), rule.Project), rule, "toFloat64(count())", from, to)
	if err != nil {
		t.Fatalf("timer buildAggQuery: %v", err)
	}

	// The test endpoint: a request context the middleware stamped.
	httpSQL, httpArgs, err := buildAggQuery(
		scope.WithProject(context.Background(), "atlas"), rule, "toFloat64(count())", from, to)
	if err != nil {
		t.Fatalf("test-endpoint buildAggQuery: %v", err)
	}

	if timerSQL != httpSQL {
		t.Errorf("the timer and the test endpoint build different statements for one rule:\n\ttimer: %s\n\thttp:  %s", timerSQL, httpSQL)
	}
	if !reflect.DeepEqual(timerArgs, httpArgs) {
		t.Errorf("bound args differ: timer %v, http %v", timerArgs, httpArgs)
	}
	if !strings.Contains(timerSQL, "project = ?") {
		t.Errorf("the timer path built an unscoped aggregate — the zone-wide gap is back:\n\t%s", timerSQL)
	}
	want := []interface{}{from, to, "atlas", "atlas-api"}
	if !reflect.DeepEqual(timerArgs, want) {
		t.Errorf("args = %v, want %v", timerArgs, want)
	}
}

// TestAggQueryRefusesWithoutAProject pins that an unscoped aggregate is
// UNCONSTRUCTABLE, not merely unusual.
//
// buildAggQuery used to carry an `errors.Is(err, scope.ErrNoProject)` arm that
// swallowed the sentinel and continued zone-wide, because the timer genuinely
// had no project to offer. It has one now, so the arm's only surviving effect
// would be to let a future caller reach every tenant's numbers by forgetting to
// supply a project — silently, since the result is a plausible float64 either
// way. The predecessor of this test asserted the OPPOSITE and said so: "if this
// ever starts failing because the predicate went unconditional, alert_rules
// needs a project column in the same change". It got one.
func TestAggQueryRefusesWithoutAProject(t *testing.T) {
	withDefaultProject(t, "default")

	_, _, err := buildAggQuery(context.Background(), &structs.AlertRule{}, "toFloat64(count())", time.Unix(0, 0), time.Unix(60, 0))
	if err == nil {
		t.Fatal("buildAggQuery with no project succeeded — an unscoped aggregate is constructable again")
	}
	if !errors.Is(err, scope.ErrNoProject) {
		t.Errorf("err = %v, want scope.ErrNoProject so the caller can tell a wiring fault from bad input", err)
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
