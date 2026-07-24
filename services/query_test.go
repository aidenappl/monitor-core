package services

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
)

func TestApplyDataFilterRejectsInjection(t *testing.T) {
	builder := sq.Select("count()").From("monitor.events").PlaceholderFormat(sq.Question)

	tests := []struct {
		name    string
		field   string
		wantErr bool
	}{
		{"valid key", "latency_ms", false},
		{"valid underscore key", "status_code", false},
		{"classic injection", "x')=1--", true},
		{"quote break-out", "x') OR ('1'='1", true},
		{"drop table", "a'; DROP TABLE events;--", true},
		{"leading digit", "1field", true},
		{"empty", "", true},
		{"dot not allowed", "http.status", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := applyDataFilter(builder, Filter{Field: tt.field, Operator: OpEq, Value: "v", IsData: true})
			if (err != nil) != tt.wantErr {
				t.Errorf("applyDataFilter(field=%q) err = %v, wantErr %v", tt.field, err, tt.wantErr)
			}
		})
	}
}

func TestApplyColumnFilterRejectsUnknownColumn(t *testing.T) {
	builder := sq.Select("count()").From("monitor.events").PlaceholderFormat(sq.Question)

	if _, err := applyColumnFilter(builder, Filter{Field: "service", Operator: OpEq, Value: "api"}); err != nil {
		t.Errorf("applyColumnFilter with valid column returned error: %v", err)
	}

	if _, err := applyColumnFilter(builder, Filter{Field: "not_a_column", Operator: OpEq, Value: "x"}); err == nil {
		t.Errorf("applyColumnFilter with unknown column should error, got nil")
	}
}
