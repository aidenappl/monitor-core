package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

type QueryParams struct {
	Service     string
	Env         string
	JobID       string
	RequestID   string
	TraceID     string
	Name        string
	Level       string
	From        time.Time
	To          time.Time
	DataFilters map[string]string
	Limit       int
	Offset      int
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

func QueryEvents(ctx context.Context, params QueryParams) (*QueryResult, error) {
	if params.Limit <= 0 {
		params.Limit = 100
	}
	if params.Limit > 1000 {
		params.Limit = 1000
	}

	// Build WHERE clauses
	var conditions []string
	var args []interface{}
	argIndex := 0

	addCondition := func(column, value string) {
		if value != "" {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", column, argIndex))
			args = append(args, value)
			argIndex++
		}
	}

	addCondition("service", params.Service)
	addCondition("env", params.Env)
	addCondition("job_id", params.JobID)
	addCondition("request_id", params.RequestID)
	addCondition("trace_id", params.TraceID)
	addCondition("name", params.Name)
	addCondition("level", params.Level)

	if !params.From.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, params.From)
		argIndex++
	}

	if !params.To.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, params.To)
		argIndex++
	}

	// Data filters (JSON field queries)
	for key, value := range params.DataFilters {
		conditions = append(conditions, fmt.Sprintf("JSONExtractString(data, $%d) = $%d", argIndex, argIndex+1))
		args = append(args, key, value)
		argIndex += 2
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count query
	countQuery := fmt.Sprintf("SELECT count() FROM %s.events %s", db.Database, whereClause)
	var total uint64
	if err := db.Conn.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count query failed: %w", err)
	}

	// Data query
	query := fmt.Sprintf(`
		SELECT timestamp, service, env, job_id, request_id, trace_id, name, level, data
		FROM %s.events
		%s
		ORDER BY timestamp DESC
		LIMIT %d OFFSET %d
	`, db.Database, whereClause, params.Limit, params.Offset)

	rows, err := db.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var events []*structs.Event
	for rows.Next() {
		var e structs.Event
		var dataStr string
		if err := rows.Scan(&e.Timestamp, &e.Service, &e.Env, &e.JobID, &e.RequestID, &e.TraceID, &e.Name, &e.Level, &dataStr); err != nil {
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
	"name":    "name",
	"level":   "level",
}

func GetLabelValues(ctx context.Context, label string, params QueryParams) (*LabelValuesResult, error) {
	column, ok := validLabels[label]
	if !ok {
		return nil, fmt.Errorf("invalid label: %s", label)
	}

	// Build WHERE clauses for filtering
	var conditions []string
	var args []interface{}
	argIndex := 0

	addCondition := func(col, value string) {
		if value != "" && col != column {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", col, argIndex))
			args = append(args, value)
			argIndex++
		}
	}

	addCondition("service", params.Service)
	addCondition("env", params.Env)
	addCondition("name", params.Name)
	addCondition("level", params.Level)

	if !params.From.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, params.From)
		argIndex++
	}

	if !params.To.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, params.To)
		argIndex++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM %s.events
		%s
		ORDER BY %s
		LIMIT 1000
	`, column, db.Database, whereClause, column)

	rows, err := db.Conn.Query(ctx, query, args...)
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
	var conditions []string
	var args []interface{}
	argIndex := 0

	addCondition := func(column, value string) {
		if value != "" {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", column, argIndex))
			args = append(args, value)
			argIndex++
		}
	}

	addCondition("service", params.Service)
	addCondition("env", params.Env)
	addCondition("name", params.Name)
	addCondition("level", params.Level)

	if !params.From.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, params.From)
		argIndex++
	}

	if !params.To.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, params.To)
		argIndex++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Extract distinct keys from JSON data column
	query := fmt.Sprintf(`
		SELECT DISTINCT arrayJoin(JSONExtractKeys(data)) AS key
		FROM %s.events
		%s
		ORDER BY key
		LIMIT 1000
	`, db.Database, whereClause)

	rows, err := db.Conn.Query(ctx, query, args...)
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

	var conditions []string
	var args []interface{}
	argIndex := 0

	addCondition := func(column, value string) {
		if value != "" {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", column, argIndex))
			args = append(args, value)
			argIndex++
		}
	}

	addCondition("service", params.Service)
	addCondition("env", params.Env)
	addCondition("name", params.Name)
	addCondition("level", params.Level)

	if !params.From.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, params.From)
		argIndex++
	}

	if !params.To.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, params.To)
		argIndex++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Get distinct values for a specific JSON key
	query := fmt.Sprintf(`
		SELECT DISTINCT JSONExtractString(data, $%d) AS value
		FROM %s.events
		%s
		HAVING value != ''
		ORDER BY value
		LIMIT 1000
	`, argIndex, db.Database, whereClause)

	args = append(args, key)

	rows, err := db.Conn.Query(ctx, query, args...)
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
