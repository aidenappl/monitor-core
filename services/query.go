package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/scope"
	"github.com/aidenappl/monitor-core/structs"
)

type Operator string

const (
	OpEq         Operator = "eq"
	OpNeq        Operator = "neq"
	OpLt         Operator = "lt"
	OpGt         Operator = "gt"
	OpLte        Operator = "lte"
	OpGte        Operator = "gte"
	OpContains   Operator = "contains"
	OpStartsWith Operator = "startswith"
	OpEndsWith   Operator = "endswith"
	OpIn         Operator = "in"
)

type Filter struct {
	Field    string
	Operator Operator
	Value    interface{}
	IsData   bool // true if this is a data.X filter
}

type QueryParams struct {
	Filters []Filter
	From    time.Time
	To      time.Time
	Limit   int
	Offset  int
}

type QueryResult struct {
	Events []*structs.Event `json:"events"`
	Total  int              `json:"total"`
}

type LabelValuesResult struct {
	Values []string `json:"values"`
}

type DataKeysResult struct {
	Keys []string `json:"keys"`
}

func eventsTable() string {
	return fmt.Sprintf("%s.events", db.Database)
}

// selectEvents is the ONLY constructor in this package for a query against
// monitor.events, and it is the single place the tenancy predicate is applied.
//
// The predicate is attached at construction rather than added by each caller
// because the failure mode of the alternative is invisible. A builder that
// forgets a .Where(project) still compiles, still runs, still returns rows, and
// the rows it returns are another project's. There is nothing to notice unless
// you already know to look. Attached here, "unscoped" is not a mistake that can
// be made — it is a query that cannot be built, because there is no other way to
// name the table.
//
// The error is propagated rather than swallowed for the same reason: a context
// with no project means the route was registered outside QueryAuthMiddleware,
// and the only safe response to "I do not know whose data this is" is to refuse
// to answer. See scope.ProjectPredicate, which also documents the empty-string
// transition arm that keeps pre-migration rows visible to the default project.
func selectEvents(ctx context.Context, columns ...string) (sq.SelectBuilder, error) {
	predicate, args, err := scope.ProjectPredicate(ctx)
	if err != nil {
		return sq.SelectBuilder{}, err
	}

	return sq.Select(columns...).
		From(eventsTable()).
		Where(predicate, args...).
		PlaceholderFormat(sq.Question), nil
}

func applyFilters(builder sq.SelectBuilder, params QueryParams) (sq.SelectBuilder, error) {
	for _, f := range params.Filters {
		var err error
		if f.IsData {
			builder, err = applyDataFilter(builder, f)
		} else {
			builder, err = applyColumnFilter(builder, f)
		}
		if err != nil {
			return builder, err
		}
	}

	if !params.From.IsZero() {
		builder = builder.Where(sq.GtOrEq{"timestamp": params.From})
	}
	if !params.To.IsZero() {
		builder = builder.Where(sq.LtOrEq{"timestamp": params.To})
	}

	return builder, nil
}

func applyColumnFilter(builder sq.SelectBuilder, f Filter) (sq.SelectBuilder, error) {
	if !structs.QueryableColumns[f.Field] {
		return builder, fmt.Errorf("invalid filter column: %s", f.Field)
	}

	switch f.Operator {
	case OpEq, "":
		builder = builder.Where(sq.Eq{f.Field: f.Value})
	case OpNeq:
		builder = builder.Where(sq.NotEq{f.Field: f.Value})
	case OpLt:
		builder = builder.Where(sq.Lt{f.Field: f.Value})
	case OpGt:
		builder = builder.Where(sq.Gt{f.Field: f.Value})
	case OpLte:
		builder = builder.Where(sq.LtOrEq{f.Field: f.Value})
	case OpGte:
		builder = builder.Where(sq.GtOrEq{f.Field: f.Value})
	case OpContains:
		builder = builder.Where(sq.Like{f.Field: fmt.Sprintf("%%%v%%", f.Value)})
	case OpStartsWith:
		builder = builder.Where(sq.Like{f.Field: fmt.Sprintf("%v%%", f.Value)})
	case OpEndsWith:
		builder = builder.Where(sq.Like{f.Field: fmt.Sprintf("%%%v", f.Value)})
	case OpIn:
		if values, ok := f.Value.([]string); ok {
			builder = builder.Where(sq.Eq{f.Field: values})
		}
	}

	return builder, nil
}

func applyDataFilter(builder sq.SelectBuilder, f Filter) (sq.SelectBuilder, error) {
	// f.Field is the JSON key (the "data." prefix is stripped during parsing) and
	// is interpolated directly into the SQL text as a string literal, so it MUST be
	// validated against structs.SafeIdentifierRegex to prevent SQL injection. That
	// regex is shared with analytics and alerts precisely so this guard cannot
	// drift from theirs. Reject invalid keys rather than silently dropping the
	// filter.
	if !structs.SafeIdentifierRegex.MatchString(f.Field) {
		return builder, fmt.Errorf("invalid data field name: %s", f.Field)
	}

	extractStr := dataStringExpr(f.Field)
	extractNum := dataNumberExpr(f.Field)

	// A cheap substring test ahead of the JSON parse. It is ANDed on, never
	// substituted, so it can only remove rows the comparison would reject anyway
	// — see valuePrefilter for when that holds and when it is skipped.
	if needle, ok := valuePrefilter(f.Operator, f.Value); ok {
		builder = builder.Where("position(data, ?) > 0", needle)
	}

	switch f.Operator {
	case OpEq, "":
		builder = builder.Where(fmt.Sprintf("%s = ?", extractStr), f.Value)
	case OpNeq:
		builder = builder.Where(fmt.Sprintf("%s != ?", extractStr), f.Value)
	case OpLt:
		builder = builder.Where(fmt.Sprintf("%s < ?", extractNum), f.Value)
	case OpGt:
		builder = builder.Where(fmt.Sprintf("%s > ?", extractNum), f.Value)
	case OpLte:
		builder = builder.Where(fmt.Sprintf("%s <= ?", extractNum), f.Value)
	case OpGte:
		builder = builder.Where(fmt.Sprintf("%s >= ?", extractNum), f.Value)
	case OpContains:
		builder = builder.Where(fmt.Sprintf("%s LIKE ?", extractStr), fmt.Sprintf("%%%v%%", f.Value))
	case OpStartsWith:
		builder = builder.Where(fmt.Sprintf("%s LIKE ?", extractStr), fmt.Sprintf("%v%%", f.Value))
	case OpEndsWith:
		builder = builder.Where(fmt.Sprintf("%s LIKE ?", extractStr), fmt.Sprintf("%%%v", f.Value))
	}

	return builder, nil
}

// dataStringExpr and dataNumberExpr are the JSON extractions for a data key,
// behind a substring guard that lets ClickHouse skip the JSON parse on rows
// whose raw text cannot contain the key. The key MUST already have passed
// structs.SafeIdentifierRegex: it is interpolated as a string literal.
//
// The guard changes no result. Every row's data is written by json.Marshal
// (structs.Event.DataJSON), which emits a key made of SafeIdentifierRegex
// characters verbatim, so a row whose text lacks `"key"` cannot have that key,
// and the unguarded extract would have returned the empty string for it too. A
// match in the wrong place (the same text as a value, or a nested key) just
// falls through to the real extract.
//
// The numeric form guards the INNER extract only. toFloat64OrNull of the empty
// string is NULL, exactly what a missing key produced before, so such a row
// still fails every lt/lte/gt/gte comparison and stays out of
// avg/min/quantile/count(field). Wrapping the whole cast with a 0 default
// instead would turn "missing" into zero — matching `< 100` and dragging every
// average toward it. alerts/evaluator.go carries the same two expressions; the
// tests on both sides pin the text.
func dataStringExpr(key string) string {
	return fmt.Sprintf("if(position(data, '\"%s\"') > 0, JSONExtractString(data, '%s'), '')", key, key)
}

func dataNumberExpr(key string) string {
	return fmt.Sprintf("toFloat64OrNull(if(position(data, '\"%s\"') > 0, JSONExtractRaw(data, '%s'), ''))", key, key)
}

// valuePrefilter returns the text a string data filter's value must appear as,
// verbatim, somewhere in the raw data column for the comparison to be true, and
// whether that text is safe to require. Callers AND `position(data, needle) > 0`
// in front of the JSON comparison.
//
// It holds only when json.Marshal writes the value byte-for-byte, so any value
// holding a character it escapes is skipped (see jsonVerbatim). For the LIKE
// operators the value must also carry no `%`, `_` or `\`: those are wildcards or
// the escape character, so the literal text need not appear in a matching row.
// An empty or non-string value is skipped too — equality with the empty string
// matches rows WITHOUT the key, and a non-string is compared by ClickHouse's own
// conversion rules, which a substring test cannot mirror. Only
// eq/contains/startswith/endswith qualify; neq and the numeric comparisons match
// rows where the value is absent.
func valuePrefilter(op Operator, value interface{}) (string, bool) {
	s, ok := value.(string)
	if !ok || s == "" {
		return "", false
	}

	switch op {
	case OpEq, "":
	case OpContains, OpStartsWith, OpEndsWith:
		if strings.ContainsAny(s, `%_\`) {
			return "", false
		}
	default:
		return "", false
	}

	if !jsonVerbatim(s) {
		return "", false
	}
	return s, true
}

// jsonVerbatim reports whether encoding/json writes s unchanged inside a JSON
// string: valid UTF-8 with no control character, quote, backslash, HTML-escaped
// `<` `>` `&`, or U+2028/U+2029.
func jsonVerbatim(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		switch {
		case r < 0x20, r == '"', r == '\\', r == '<', r == '>', r == '&', r == '\u2028', r == '\u2029':
			return false
		}
	}
	return true
}

// eventPageColumns is the page query's projection, in the order QueryEvents
// scans it.
var eventPageColumns = []string{"timestamp", "service", "project", "env", "job_id", "request_id", "trace_id", "user_id", "name", "level", "data"}

// hasDataFilter reports whether any filter reads the data column.
func hasDataFilter(filters []Filter) bool {
	for _, f := range filters {
		if f.IsData {
			return true
		}
	}
	return false
}

// buildEventPageQuery builds QueryEvents' page statement.
//
// Without a data.* filter it is two-phase: the page's rows are found from the
// narrow timestamp column first, and the wide read (data above all) is bounded
// to `timestamp >= <oldest timestamp on the page>`, which the sorting key turns
// into a granule range. The single-phase form decompresses `data` for whole
// blocks in every part to return one page. The rows are the same; ties at the
// boundary timestamp break as arbitrarily as they always did. An offset past the
// end gives min() over nothing, 1970-01-01, so the outer read scans and returns
// nothing — correct, just not faster.
//
// With a data.* filter both phases would have to parse `data` for the same
// rows, so the page stays a single statement.
//
// Both phases start at selectEvents, so each carries its own project predicate:
// the inner read decides which rows are on the page, and an unscoped one would
// let another project's timestamps set the boundary.
func buildEventPageQuery(ctx context.Context, params QueryParams) (string, []interface{}, error) {
	page, err := selectEvents(ctx, eventPageColumns...)
	if err != nil {
		return "", nil, err
	}
	page, err = applyFilters(page, params)
	if err != nil {
		return "", nil, err
	}

	if !hasDataFilter(params.Filters) {
		boundary, err := selectEvents(ctx, "timestamp")
		if err != nil {
			return "", nil, err
		}
		boundary, err = applyFilters(boundary, params)
		if err != nil {
			return "", nil, err
		}
		boundary = boundary.
			OrderBy("timestamp DESC").
			Limit(uint64(params.Limit)).
			Offset(uint64(params.Offset))
		page = page.Where(sq.Expr("timestamp >= (SELECT min(timestamp) FROM (?))", boundary))
	}

	page = page.
		OrderBy("timestamp DESC").
		Limit(uint64(params.Limit)).
		Offset(uint64(params.Offset))

	querySQL, queryArgs, err := page.ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("failed to build query: %w", err)
	}
	return querySQL, queryArgs, nil
}

// runConcurrently runs every fn at once and returns the first error. The first
// failure cancels the context the others were given, so a doomed request does
// not wait out its sibling's scan. (golang.org/x/sync/errgroup is not a
// dependency; this is the part of it needed here.)
func runConcurrently(ctx context.Context, fns ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for _, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func QueryEvents(ctx context.Context, params QueryParams) (*QueryResult, error) {
	if params.Limit <= 0 {
		params.Limit = 100
	}
	if params.Limit > 1000 {
		params.Limit = 1000
	}

	// Count query
	countBuilder, err := selectEvents(ctx, "count()")
	if err != nil {
		return nil, err
	}
	countBuilder, err = applyFilters(countBuilder, params)
	if err != nil {
		return nil, err
	}

	countSQL, countArgs, err := countBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build count query: %w", err)
	}

	// Data query
	querySQL, queryArgs, err := buildEventPageQuery(ctx, params)
	if err != nil {
		return nil, err
	}

	// The two are independent reads, so they run side by side rather than
	// back to back. Both statements are fully built above, so a validation
	// error never leaves a query in flight.
	var total uint64
	var events []*structs.Event
	err = runConcurrently(ctx,
		func(ctx context.Context) error {
			if err := db.Conn.QueryRow(ctx, countSQL, countArgs...).Scan(&total); err != nil {
				return fmt.Errorf("count query failed: %w", err)
			}
			return nil
		},
		func(ctx context.Context) error {
			rows, err := db.Conn.Query(ctx, querySQL, queryArgs...)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var e structs.Event
				var dataStr string
				if err := rows.Scan(&e.Timestamp, &e.Service, &e.Project, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.UserID, &e.Name, &e.Level, &dataStr); err != nil {
					return fmt.Errorf("scan failed: %w", err)
				}
				if dataStr != "" && dataStr != "{}" {
					// A stored row whose data is not a JSON object is returned without it.
					_ = json.Unmarshal([]byte(dataStr), &e.Data)
				}
				events = append(events, &e)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if events == nil {
		events = []*structs.Event{}
	}

	return &QueryResult{
		Events: events,
		Total:  int(total),
	}, nil
}

func GetLabelValues(ctx context.Context, label string, params QueryParams) (*LabelValuesResult, error) {
	column, ok := structs.LabelColumns[label]
	if !ok {
		return nil, fmt.Errorf("invalid label: %s", label)
	}

	// GROUP BY rather than DISTINCT: the same rows, but every label column except
	// user_id is LowCardinality and only GROUP BY aggregates on the dictionary
	// keys — DISTINCT materialises the strings, about 3x the CPU on a full scan.
	builder, err := selectEvents(ctx, column)
	if err != nil {
		return nil, err
	}
	builder = builder.GroupBy(column).OrderBy(column).Limit(1000)

	// Apply filters except the one we're getting values for.
	//
	// This skips only the CALLER's filter on the requested column. The tenancy
	// predicate is not a filter — it was attached by selectEvents above and is
	// untouched by this loop — so asking for the values of `project` still
	// returns the caller's own project rather than every project's.
	for _, f := range params.Filters {
		if !f.IsData && f.Field == column {
			continue
		}
		var err error
		if f.IsData {
			builder, err = applyDataFilter(builder, f)
		} else {
			builder, err = applyColumnFilter(builder, f)
		}
		if err != nil {
			return nil, err
		}
	}

	if !params.From.IsZero() {
		builder = builder.Where(sq.GtOrEq{"timestamp": params.From})
	}
	if !params.To.IsZero() {
		builder = builder.Where(sq.LtOrEq{"timestamp": params.To})
	}

	querySQL, queryArgs, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := db.Conn.Query(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		if v != "" {
			values = append(values, v)
		}
	}

	if values == nil {
		values = []string{}
	}

	return &LabelValuesResult{Values: values}, nil
}

func GetDataKeys(ctx context.Context, params QueryParams) (*DataKeysResult, error) {
	builder, err := selectEvents(ctx, "DISTINCT arrayJoin(JSONExtractKeys(data)) AS key")
	if err != nil {
		return nil, err
	}
	builder = builder.OrderBy("key").Limit(1000)
	builder, err = applyFilters(builder, params)
	if err != nil {
		return nil, err
	}

	querySQL, queryArgs, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := db.Conn.Query(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		keys = append(keys, k)
	}

	if keys == nil {
		keys = []string{}
	}

	return &DataKeysResult{Keys: keys}, nil
}

func GetDataValues(ctx context.Context, key string, params QueryParams) (*LabelValuesResult, error) {
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	// The key is bound through squirrel — Column for the SELECT expression, an
	// argument on the WHERE — rather than by hand-prepending both to the arg
	// slice as this did before. That prepend was correct only while the tenancy
	// predicate did not exist: squirrel emits args in CLAUSE order (columns,
	// from, where, ...), not in call order, so the project argument attached by
	// selectEvents now lands between the SELECT's placeholder and this WHERE's.
	// A hand-built slice would have silently mismatched every placeholder from
	// the second one on — bound values sliding one position along, which is not
	// an error, just wrong rows.
	builder, err := selectEvents(ctx)
	if err != nil {
		return nil, err
	}
	// Deliberately bare and BOUND: key is not SafeIdentifierRegex-validated, so it MUST stay a ? parameter — never dataStringExpr(key), which would interpolate it (injection).
	builder = builder.
		Column("DISTINCT JSONExtractString(data, ?) AS value", key).
		Where("JSONExtractString(data, ?) != ''", key).
		OrderBy("value").
		Limit(1000)
	builder, err = applyFilters(builder, params)
	if err != nil {
		return nil, err
	}

	querySQL, queryArgs, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := db.Conn.Query(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		values = append(values, v)
	}

	if values == nil {
		values = []string{}
	}

	return &LabelValuesResult{Values: values}, nil
}
