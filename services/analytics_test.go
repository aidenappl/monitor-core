package services

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aidenappl/monitor-core/structs"
)

// TestAnalyticsDataExpressionsAreGuarded pins every data.* extraction site in
// analytics.go to the guarded forms, and pins the numeric aggregations to the
// NULL-preserving one: a row without the key must stay out of avg/min/quantile
// and sum rather than arriving as 0.
func TestAnalyticsDataExpressionsAreGuarded(t *testing.T) {
	if got, err := buildFieldExpr("data.latency_ms"); err != nil || got != wantGuardedString {
		t.Errorf("buildFieldExpr = %q, %v; want %s", got, err, wantGuardedString)
	}
	if got, err := buildNumericFieldExpr("data.latency_ms"); err != nil || got != wantGuardedNumber {
		t.Errorf("buildNumericFieldExpr = %q, %v; want %s", got, err, wantGuardedNumber)
	}
	if exprs, _, err := buildGroupByExprs([]string{"data.latency_ms", "service"}); err != nil ||
		!reflect.DeepEqual(exprs, []string{wantGuardedString + " AS group_0", "service AS group_1"}) {
		t.Errorf("buildGroupByExprs = %q, %v", exprs, err)
	}

	aggregations := map[structs.AggregationType]string{
		structs.AggSum:         "toFloat64(sum(" + wantGuardedNumber + "))",
		structs.AggAvg:         "toFloat64(avg(" + wantGuardedNumber + "))",
		structs.AggMin:         "toFloat64(min(" + wantGuardedNumber + "))",
		structs.AggMax:         "toFloat64(max(" + wantGuardedNumber + "))",
		structs.AggP99:         "toFloat64(quantile(0.99)(" + wantGuardedNumber + "))",
		structs.AggCountUnique: "toFloat64(uniq(" + wantGuardedString + "))",
	}
	for agg, want := range aggregations {
		if got, err := buildAggregationExpr(agg, "data.latency_ms"); err != nil || got != want {
			t.Errorf("buildAggregationExpr(%s) = %q, %v; want %s", agg, got, err, want)
		}
	}
}

// TestBuildSingleFilterSQL pins analytics' filter conditions: guarded extracts,
// the value prefilter only where it is exact (and FIRST, so its placeholder
// precedes the comparison's), and column filters untouched.
func TestBuildSingleFilterSQL(t *testing.T) {
	tests := []struct {
		name     string
		filter   structs.QueryFilter
		wantSQL  string
		wantArgs []any
	}{
		{"data eq", structs.QueryFilter{Field: "data.latency_ms", Operator: "eq", Value: "api"},
			"position(data, ?) > 0 AND " + wantGuardedString + " = ?", []any{"api", "api"}},
		{"data contains", structs.QueryFilter{Field: "data.latency_ms", Operator: "contains", Value: "time"},
			"position(data, ?) > 0 AND " + wantGuardedString + " LIKE ?", []any{"time", "%time%"}},
		{"data contains wildcard", structs.QueryFilter{Field: "data.latency_ms", Operator: "contains", Value: "a_b"},
			wantGuardedString + " LIKE ?", []any{"%a_b%"}},
		{"data eq escaped", structs.QueryFilter{Field: "data.latency_ms", Operator: "eq", Value: "a&b"},
			wantGuardedString + " = ?", []any{"a&b"}},
		{"data eq number", structs.QueryFilter{Field: "data.latency_ms", Operator: "eq", Value: float64(5)},
			wantGuardedString + " = ?", []any{float64(5)}},
		{"data neq", structs.QueryFilter{Field: "data.latency_ms", Operator: "neq", Value: "api"},
			wantGuardedString + " != ?", []any{"api"}},
		{"data lt", structs.QueryFilter{Field: "data.latency_ms", Operator: "lt", Value: float64(100)},
			wantGuardedNumber + " < ?", []any{float64(100)}},
		{"data lte", structs.QueryFilter{Field: "data.latency_ms", Operator: "lte", Value: float64(100)},
			wantGuardedNumber + " <= ?", []any{float64(100)}},
		{"data in", structs.QueryFilter{Field: "data.latency_ms", Operator: "in", Value: []any{"a", "b"}},
			wantGuardedString + " IN (?, ?)", []any{"a", "b"}},
		{"column eq", structs.QueryFilter{Field: "service", Operator: "eq", Value: "api"},
			"service = ?", []any{"api"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := buildSingleFilter(tt.filter)
			if err != nil {
				t.Fatalf("buildSingleFilter: %v", err)
			}
			if sql != tt.wantSQL {
				t.Errorf("sql =\n\t%s\nwant\n\t%s", sql, tt.wantSQL)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}

	if _, _, err := buildSingleFilter(structs.QueryFilter{Field: "data.x')--", Operator: "eq", Value: "v"}); err == nil {
		t.Error("an invalid data key was accepted")
	}
	if _, _, err := buildSingleFilter(structs.QueryFilter{Field: "data.latency_ms", Operator: "bogus", Value: "v"}); err == nil {
		t.Error("an unsupported operator was accepted")
	}
}

// TestQueryTopNGroupsOnTheGuardedExtract covers the one extraction site that is
// built inline in its query function rather than through a helper.
func TestQueryTopNGroupsOnTheGuardedExtract(t *testing.T) {
	withDefaultProject(t, "default")
	recorder := withRecordingConn(t)

	if _, err := QueryTopN(scopedContext("atlas"), &structs.TopNQuery{
		Aggregation: structs.AggCount, GroupBy: "data.latency_ms", From: time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatalf("QueryTopN: %v", err)
	}
	want := "SELECT " + wantGuardedString + " AS key, toFloat64(count()) AS value FROM monitor.events WHERE "
	if !strings.HasPrefix(recorder.statements[0], want) {
		t.Errorf("sql =\n\t%s\nwant prefix\n\t%s", recorder.statements[0], want)
	}
}
