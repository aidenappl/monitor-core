package structs

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestDataOperand(t *testing.T) {
	tests := []struct {
		name        string
		op          string
		value       any
		wantArg     any
		wantNumeric bool
		wantRaw     bool
		wantErr     bool
	}{
		{"eq text", "eq", "api", "api", false, false, false},
		{"empty op is eq", "", "api", "api", false, false, false},
		{"eq float", "eq", float64(500), float64(500), true, false, false},
		{"eq int", "eq", 500, float64(500), true, false, false},
		{"eq json.Number", "eq", json.Number("502"), float64(502), true, false, false},
		{"eq numeric string stays text", "eq", "500", "500", false, false, false},
		{"neq bool is raw", "neq", true, "true", false, true, false},
		{"gt number", "gt", float64(1.5), float64(1.5), true, false, false},
		{"gte numeric string", "gte", " 400 ", float64(400), true, false, false},
		{"lt non-number", "lt", "slow", nil, false, false, true},
		{"lte bool", "lte", true, nil, false, false, true},
		{"contains number renders as JSON text", "contains", float64(500), "500", false, false, false},
		{"contains fraction", "contains", float64(1.25), "1.25", false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arg, numeric, raw, err := DataOperand(tt.op, tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidFilterValue) {
				t.Fatalf("err = %v, want it to wrap ErrInvalidFilterValue", err)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(arg, tt.wantArg) || numeric != tt.wantNumeric || raw != tt.wantRaw {
				t.Fatalf("DataOperand = (%#v, %v, %v), want (%#v, %v, %v)", arg, numeric, raw, tt.wantArg, tt.wantNumeric, tt.wantRaw)
			}
		})
	}
}

func TestDataInOperands(t *testing.T) {
	tests := []struct {
		name        string
		values      []any
		wantArgs    []any
		wantNumeric bool
	}{
		{"all numbers", []any{float64(500), 502}, []any{float64(500), float64(502)}, true},
		{"all text", []any{"a", "b"}, []any{"a", "b"}, false},
		{"mixed is text", []any{"a", float64(5)}, []any{"a", "5"}, false},
		{"numeric strings stay text", []any{"500"}, []any{"500"}, false},
		{"empty", []any{}, []any{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, numeric := DataInOperands(tt.values)
			if !reflect.DeepEqual(args, tt.wantArgs) || numeric != tt.wantNumeric {
				t.Fatalf("DataInOperands = (%#v, %v), want (%#v, %v)", args, numeric, tt.wantArgs, tt.wantNumeric)
			}
		})
	}
}

func TestInValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  []any
	}{
		{"array", []any{"a", float64(5)}, []any{"a", float64(5)}},
		{"string slice", []string{"a", "b"}, []any{"a", "b"}},
		{"comma string trimmed", " a , b ,, c ", []any{"a", "b", "c"}},
		{"single value", "a", []any{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InValues("name", tt.value)
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("InValues = (%#v, %v), want %#v", got, err, tt.want)
			}
		})
	}

	for _, bad := range []any{nil, float64(5), true, "", " , ", []any{}, []string{}, map[string]any{}} {
		if _, err := InValues("data.code", bad); !errors.Is(err, ErrInvalidFilterValue) {
			t.Errorf("InValues(%#v) err = %v, want ErrInvalidFilterValue", bad, err)
		} else if !strings.Contains(err.Error(), "data.code") {
			t.Errorf("InValues(%#v) error %q does not name the field", bad, err)
		}
	}
}
