package alerts

import (
	"testing"

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
