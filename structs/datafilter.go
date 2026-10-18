package structs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrInvalidFilterValue marks a filter whose value cannot be compared the way
// its operator needs (a non-number for lt/gt/lte/gte). It is the caller's
// mistake, so routes answer 400 for it, never 500.
var ErrInvalidFilterValue = errors.New("invalid filter value")

// DataOperand decides how a data.* filter compares — as a number or as text —
// and returns the argument converted to match. Every data filter site
// (services analytics, services event search, alerts) calls it, so a filter
// means the same thing to all three.
//
// The comparison type follows the VALUE, because the extract must match the
// stored JSON type: JSONExtractString returns ” for a JSON number, so
// `data.status_code eq "500"` could never match a numeric status code, and a
// numeric argument against the String extract is ClickHouse error 386 ("no
// supertype for String, UInt16"), which is what the MCP's typed filters hit.
//
//   - lt/gt/lte/gte always compare numerically; a numeric string ("400") is
//     parsed, anything else is an error rather than an empty result.
//   - eq/neq ("" is eq) compare numerically for a number (float64, ints,
//     json.Number) and as text for everything else; a bool is compared as its
//     JSON text ("true"/"false") against the raw extract.
//   - contains/startswith/endswith always compare as text.
//
// numeric reports which extract to use; raw reports a bool, which compares
// against JSONExtractRaw.
func DataOperand(op string, value any) (arg any, numeric bool, raw bool, err error) {
	switch op {
	case "lt", "gt", "lte", "gte":
		f, ok := numberOf(value)
		if !ok {
			return nil, false, false, fmt.Errorf("%w: operator %s needs a number, got %v", ErrInvalidFilterValue, op, value)
		}
		return f, true, false, nil
	case "eq", "", "neq":
		if f, ok := strictNumber(value); ok {
			return f, true, false, nil
		}
		if b, ok := value.(bool); ok {
			return strconv.FormatBool(b), false, true, nil
		}
		return textOf(value), false, false, nil
	default:
		return textOf(value), false, false, nil
	}
}

// InValues normalises the value of an `in` filter on field into a list, shared
// by every filter site (services event search, services analytics, alerts) so
// `in` means the same thing to all three. An array is taken as is; []string
// covers callers building filters in Go (query-string parsing). A string is
// split on commas, each part trimmed and empties dropped — monitor-web's filter
// bar, and anyone typing into a query tool, sends `a,b` rather than ["a","b"],
// and a plain "x" is the one-element list. Any other scalar (the MCP and
// hand-written requests send `"value": 5`) is a one-element list too. nil, a
// map, and a list that ends up empty (`IN ()` is a ClickHouse syntax error) are
// ErrInvalidFilterValue naming the field, which every route answers with a 400.
func InValues(field string, value any) ([]any, error) {
	switch v := value.(type) {
	case []any:
		if len(v) > 0 {
			return v, nil
		}
	case []string:
		if len(v) > 0 {
			out := make([]any, len(v))
			for i, s := range v {
				out[i] = s
			}
			return out, nil
		}
	case string:
		var out []any
		for _, part := range strings.Split(v, ",") {
			if p := strings.TrimSpace(part); p != "" {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	case nil, map[string]any:
	default:
		return []any{v}, nil
	}
	return nil, fmt.Errorf("%w: filter on %s with operator in needs a non-empty list of values (an array, a comma-separated string, or a single value)", ErrInvalidFilterValue, field)
}

// DataInOperands is DataOperand for the `in` operator: numeric when every
// value is a number, text otherwise (numbers rendered as their JSON text).
func DataInOperands(values []any) (args []any, numeric bool) {
	args = make([]any, len(values))
	numeric = len(values) > 0
	for i, v := range values {
		f, ok := strictNumber(v)
		if !ok {
			numeric = false
			break
		}
		args[i] = f
	}
	if numeric {
		return args, true
	}
	for i, v := range values {
		args[i] = textOf(v)
	}
	return args, false
}

// strictNumber accepts a value that IS a number, not a string holding one.
func strictNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

// numberOf is strictNumber plus numeric strings.
func numberOf(v any) (float64, bool) {
	if f, ok := strictNumber(v); ok {
		return f, true
	}
	if s, ok := v.(string); ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		return f, err == nil
	}
	return 0, false
}

// textOf renders a value the way it appears as JSON text: 500, not 500.000000.
func textOf(v any) string {
	if f, ok := strictNumber(v); ok {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
