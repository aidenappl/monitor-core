package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

	extractStr := fmt.Sprintf("JSONExtractString(data, '%s')", f.Field)
	extractNum := fmt.Sprintf("toFloat64OrNull(JSONExtractRaw(data, '%s'))", f.Field)

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

	var total uint64
	if err := db.Conn.QueryRow(ctx, countSQL, countArgs...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count query failed: %w", err)
	}

	// Data query
	queryBuilder, err := selectEvents(ctx, "timestamp", "service", "project", "env", "job_id", "request_id", "trace_id", "user_id", "name", "level", "data")
	if err != nil {
		return nil, err
	}
	queryBuilder = queryBuilder.
		OrderBy("timestamp DESC").
		Limit(uint64(params.Limit)).
		Offset(uint64(params.Offset))
	queryBuilder, err = applyFilters(queryBuilder, params)
	if err != nil {
		return nil, err
	}

	querySQL, queryArgs, err := queryBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := db.Conn.Query(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var events []*structs.Event
	for rows.Next() {
		var e structs.Event
		var dataStr string
		if err := rows.Scan(&e.Timestamp, &e.Service, &e.Project, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.UserID, &e.Name, &e.Level, &dataStr); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		if dataStr != "" && dataStr != "{}" {
			json.Unmarshal([]byte(dataStr), &e.Data)
		}
		events = append(events, &e)
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

	builder, err := selectEvents(ctx, fmt.Sprintf("DISTINCT %s", column))
	if err != nil {
		return nil, err
	}
	builder = builder.OrderBy(column).Limit(1000)

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
