package routes

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/services"
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

func TestParseQueryParamsOrder(t *testing.T) {
	tests := []struct {
		query   string
		want    string
		wantErr bool
	}{
		{"", services.OrderDesc, false},
		{"order=desc", services.OrderDesc, false},
		{"order=asc", services.OrderAsc, false},
		{"order=sideways", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/events?"+tt.query, nil)
			params, err := parseQueryParams(r)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseQueryParams(%q) err = %v, wantErr %v", tt.query, err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if params.Order != tt.want {
				t.Errorf("Order = %q, want %q", params.Order, tt.want)
			}
			// order is a reserved parameter, never a filter on a column called "order".
			if len(params.Filters) != 0 {
				t.Errorf("Filters = %+v, want none", params.Filters)
			}
		})
	}
}
