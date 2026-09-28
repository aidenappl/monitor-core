package services

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

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
		// Dots are now accepted. This case previously asserted the opposite,
		// because services/ and alerts/ carried divergent copies of the regex and
		// only the alerts copy allowed dots — so a nested key like "user.id"
		// worked in an alert rule and failed in a query. Both now share
		// structs.SafeIdentifierRegex, and the permissive side won.
		{"nested data key allowed", "http.status", false},
		// Loosening to allow dots must not loosen anything else: the characters
		// that would actually break out of the string literal stay rejected.
		{"backslash", `a\b`, true},
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

// The guard SQL below is pinned as literal text on purpose. alerts/evaluator.go
// carries its own copy of both expressions and alerts/evaluator_test.go pins the
// same two strings, so a change to either copy fails a test instead of letting
// the timer and the query API read a data key two different ways.
const (
	wantGuardedString = `if(position(data, '"latency_ms"') > 0, JSONExtractString(data, 'latency_ms'), '')`
	wantGuardedNumber = `toFloat64OrNull(if(position(data, '"latency_ms"') > 0, JSONExtractRaw(data, 'latency_ms'), ''))`
)

// TestDataExtractionGuardKeepsMissingKeysNull pins the shape that keeps the
// guard free of semantic change. The numeric form must guard the INNER extract
// and hand toFloat64OrNull the empty string, which is NULL: a row without the
// key then fails every lt/lte/gt/gte comparison and stays out of
// avg/min/quantile/count(field), exactly as it did unguarded. A 0 default in
// the outer position would make that row match `< 100` and pull averages down.
func TestDataExtractionGuardKeepsMissingKeysNull(t *testing.T) {
	if got := dataStringExpr("latency_ms"); got != wantGuardedString {
		t.Errorf("dataStringExpr = %s\n\twant %s", got, wantGuardedString)
	}
	got := dataNumberExpr("latency_ms")
	if got != wantGuardedNumber {
		t.Errorf("dataNumberExpr = %s\n\twant %s", got, wantGuardedNumber)
	}
	if !strings.HasPrefix(got, "toFloat64OrNull(if(") || !strings.HasSuffix(got, ", ''))") {
		t.Errorf("the numeric guard must sit INSIDE toFloat64OrNull with an empty-string default so a missing key stays NULL:\n\t%s", got)
	}
	if strings.Contains(got, ", 0)") {
		t.Errorf("the numeric guard defaults to 0 — rows without the key would satisfy lt/lte:\n\t%s", got)
	}
}

// TestApplyDataFilterSQL pins what each data operator emits: the guarded
// extract everywhere, and the value prefilter only where it is exact.
func TestApplyDataFilterSQL(t *testing.T) {
	const prefix = "SELECT count() FROM monitor.events WHERE "

	tests := []struct {
		name     string
		op       Operator
		value    any
		wantSQL  string
		wantArgs []any
	}{
		{"eq gets the prefilter", OpEq, "api", "position(data, ?) > 0 AND " + wantGuardedString + " = ?", []any{"api", "api"}},
		{"empty operator is eq", "", "api", "position(data, ?) > 0 AND " + wantGuardedString + " = ?", []any{"api", "api"}},
		{"eq keeps it for an underscore", OpEq, "a_b", "position(data, ?) > 0 AND " + wantGuardedString + " = ?", []any{"a_b", "a_b"}},
		{"eq keeps it for non-ASCII UTF-8", OpEq, "café", "position(data, ?) > 0 AND " + wantGuardedString + " = ?", []any{"café", "café"}},
		{"contains", OpContains, "time", "position(data, ?) > 0 AND " + wantGuardedString + " LIKE ?", []any{"time", "%time%"}},
		{"startswith", OpStartsWith, "GET", "position(data, ?) > 0 AND " + wantGuardedString + " LIKE ?", []any{"GET", "GET%"}},
		{"endswith", OpEndsWith, ".go", "position(data, ?) > 0 AND " + wantGuardedString + " LIKE ?", []any{".go", "%.go"}},
		{"neq never prefilters", OpNeq, "api", wantGuardedString + " != ?", []any{"api"}},
		{"lt is numeric, no prefilter", OpLt, "100", wantGuardedNumber + " < ?", []any{"100"}},
		{"lte is numeric, no prefilter", OpLte, "100", wantGuardedNumber + " <= ?", []any{"100"}},
		{"gt is numeric, no prefilter", OpGt, "100", wantGuardedNumber + " > ?", []any{"100"}},
		{"gte is numeric, no prefilter", OpGte, "100", wantGuardedNumber + " >= ?", []any{"100"}},
		{"contains skips an underscore wildcard", OpContains, "a_b", wantGuardedString + " LIKE ?", []any{"%a_b%"}},
		{"contains skips a percent wildcard", OpContains, "50%", wantGuardedString + " LIKE ?", []any{"%50%%"}},
		{"eq skips an escaped quote", OpEq, `say "hi"`, wantGuardedString + " = ?", []any{`say "hi"`}},
		{"eq skips the empty string", OpEq, "", wantGuardedString + " = ?", []any{""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := sq.Select("count()").From("monitor.events").PlaceholderFormat(sq.Question)
			builder, err := applyDataFilter(builder, Filter{Field: "latency_ms", Operator: tt.op, Value: tt.value, IsData: true})
			if err != nil {
				t.Fatalf("applyDataFilter: %v", err)
			}
			sql, args, err := builder.ToSql()
			if err != nil {
				t.Fatalf("ToSql: %v", err)
			}
			if want := prefix + tt.wantSQL; sql != want {
				t.Errorf("sql =\n\t%s\nwant\n\t%s", sql, want)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}
}

// TestValuePrefilterSkipsWildcardsAndEscapedValues pins when the substring
// prefilter is exact. It is only a valid precondition when json.Marshal wrote
// the value byte-for-byte AND (for LIKE) the pattern's literal text must appear;
// any other value would let the prefilter reject a row the comparison accepts.
func TestValuePrefilterSkipsWildcardsAndEscapedValues(t *testing.T) {
	tests := []struct {
		name  string
		op    Operator
		value any
		want  bool
	}{
		{"plain eq", OpEq, "api", true},
		{"plain contains", OpContains, "timeout", true},
		{"plain startswith", OpStartsWith, "/v1/", true},
		{"plain endswith", OpEndsWith, ".go", true},
		{"eq allows LIKE wildcards", OpEq, "50%_off", true},
		{"contains percent", OpContains, "50%", false},
		{"startswith underscore", OpStartsWith, "user_", false},
		{"endswith backslash", OpEndsWith, `C:\tmp`, false},
		{"eq backslash", OpEq, `a\b`, false},
		{"eq double quote", OpEq, `"q"`, false},
		{"eq less-than", OpEq, "<b>", false},
		{"eq ampersand", OpEq, "a&b", false},
		{"eq newline", OpEq, "line\nbreak", false},
		{"eq tab", OpEq, "a\tb", false},
		{"eq line separator", OpEq, "a\u2028b", false},
		{"eq paragraph separator", OpEq, "a\u2029b", false},
		{"eq invalid UTF-8", OpEq, "\xff", false},
		{"eq empty", OpEq, "", false},
		{"eq non-string", OpEq, 200, false},
		{"neq", OpNeq, "api", false},
		{"lt", OpLt, "100", false},
		{"in", OpIn, "api", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			needle, ok := valuePrefilter(tt.op, tt.value)
			if ok != tt.want {
				t.Fatalf("valuePrefilter(%s, %#v) ok = %v, want %v", tt.op, tt.value, ok, tt.want)
			}
			if ok && needle != tt.value {
				t.Errorf("needle = %q, want the value itself %q", needle, tt.value)
			}
		})
	}
}

// TestRunConcurrentlyRunsInParallelAndReturnsTheFirstError proves the two
// functions overlap (each waits for the other to start, which would deadlock a
// sequential runner — hence the timeout), that the first failure is the one
// returned, and that it cancels the context its sibling was handed.
func TestRunConcurrentlyRunsInParallelAndReturnsTheFirstError(t *testing.T) {
	boom := errors.New("boom")
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})
	siblingCancelled := make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- runConcurrently(context.Background(),
			func(ctx context.Context) error {
				close(aStarted)
				<-bStarted
				return boom
			},
			func(ctx context.Context) error {
				close(bStarted)
				<-aStarted
				<-ctx.Done()
				close(siblingCancelled)
				return ctx.Err()
			},
		)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Errorf("err = %v, want the first failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runConcurrently did not return — the functions are not running in parallel, or a failure does not cancel the sibling")
	}
	select {
	case <-siblingCancelled:
	default:
		t.Error("the sibling's context was not cancelled by the first failure")
	}

	if err := runConcurrently(context.Background(),
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
	); err != nil {
		t.Errorf("all succeeded, err = %v", err)
	}
}
