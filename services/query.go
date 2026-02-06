package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
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

var validColumns = map[string]bool{
	"service":    true,
	"env":        true,
	"job_id":     true,
	"request_id": true,
	"trace_id":   true,
	"user_id":    true,
	"name":       true,
	"level":      true,
}

func applyFilters(builder sq.SelectBuilder, params QueryParams) sq.SelectBuilder {
	for _, f := range params.Filters {
		if f.IsData {
			builder = applyDataFilter(builder, f)
		} else {
			builder = applyColumnFilter(builder, f)
		}
	}

	if !params.From.IsZero() {
		builder = builder.Where(sq.GtOrEq{"timestamp": params.From})
	}
	if !params.To.IsZero() {
		builder = builder.Where(sq.LtOrEq{"timestamp": params.To})
	}

	return builder
}

func applyColumnFilter(builder sq.SelectBuilder, f Filter) sq.SelectBuilder {
	if !validColumns[f.Field] {
		return builder
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

	return builder
}

func applyDataFilter(builder sq.SelectBuilder, f Filter) sq.SelectBuilder {
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

	return builder
}

func QueryEvents(ctx context.Context, params QueryParams) (*QueryResult, error) {
	if params.Limit <= 0 {
		params.Limit = 100
	}
	if params.Limit > 1000 {
		params.Limit = 1000
	}

	// Count query
	countBuilder := sq.Select("count()").
		From(eventsTable()).
		PlaceholderFormat(sq.Question)
	countBuilder = applyFilters(countBuilder, params)

	countSQL, countArgs, err := countBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build count query: %w", err)
	}

	var total uint64
	if err := db.Conn.QueryRow(ctx, countSQL, countArgs...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count query failed: %w", err)
	}

	// Data query
	queryBuilder := sq.Select("timestamp", "service", "env", "job_id", "request_id", "trace_id", "user_id", "name", "level", "data").
		From(eventsTable()).
		OrderBy("timestamp DESC").
		Limit(uint64(params.Limit)).
		Offset(uint64(params.Offset)).
		PlaceholderFormat(sq.Question)
	queryBuilder = applyFilters(queryBuilder, params)

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
		if err := rows.Scan(&e.Timestamp, &e.Service, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.UserID, &e.Name, &e.Level, &dataStr); err != nil {
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

var validLabels = map[string]string{
	"service": "service",
	"env":     "env",
	"user_id": "user_id",
	"name":    "name",
	"level":   "level",
}

func GetLabelValues(ctx context.Context, label string, params QueryParams) (*LabelValuesResult, error) {
	column, ok := validLabels[label]
	if !ok {
		return nil, fmt.Errorf("invalid label: %s", label)
	}

	builder := sq.Select(fmt.Sprintf("DISTINCT %s", column)).
		From(eventsTable()).
		OrderBy(column).
		Limit(1000).
		PlaceholderFormat(sq.Question)

	// Apply filters except the one we're getting values for
	for _, f := range params.Filters {
		if !f.IsData && f.Field == column {
			continue
		}
		if f.IsData {
			builder = applyDataFilter(builder, f)
		} else {
			builder = applyColumnFilter(builder, f)
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
	builder := sq.Select("DISTINCT arrayJoin(JSONExtractKeys(data)) AS key").
		From(eventsTable()).
		OrderBy("key").
		Limit(1000).
		PlaceholderFormat(sq.Question)
	builder = applyFilters(builder, params)

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

	builder := sq.Select("DISTINCT JSONExtractString(data, ?) AS value").
		From(eventsTable()).
		Where("JSONExtractString(data, ?) != ''").
		OrderBy("value").
		Limit(1000).
		PlaceholderFormat(sq.Question)
	builder = applyFilters(builder, params)

	querySQL, queryArgs, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	// Prepend the key arguments for JSONExtractString (SELECT and WHERE)
	queryArgs = append([]interface{}{key, key}, queryArgs...)

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
