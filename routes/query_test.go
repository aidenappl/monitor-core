package routes

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

func TestIsFilterValidationErrorCoversValueErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"wrong value type", fmt.Errorf("%w: operator gt needs a number, got slow", structs.ErrInvalidFilterValue), true},
		{"wrapped twice", fmt.Errorf("query: %w", fmt.Errorf("%w: x", structs.ErrInvalidFilterValue)), true},
		{"invalid field", errors.New("invalid data field name: x')--"), true},
		{"clickhouse failure", errors.New("query failed: code: 241"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFilterValidationError(tt.err); got != tt.want {
				t.Fatalf("isFilterValidationError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
