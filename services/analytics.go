package services

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// MaxTimeSeriesPoints is the maximum number of data points allowed in a time series
const MaxTimeSeriesPoints = 10000

// MaxQueryDuration is the maximum time range allowed for queries (90 days)
const MaxQueryDuration = 90 * 24 * time.Hour

// safeIdentifierRegex validates field names to prevent SQL injection
var safeIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validGroupByColumns are columns that can be used in GROUP BY
var validGroupByColumns = map[string]bool{
	"service":    true,
	"env":        true,
	"job_id":     true,
	"request_id": true,
	"trace_id":   true,
	"user_id":    true,
	"name":       true,
	"level":      true,
}

// buildAggregationExpr builds the SQL aggregation expression
func buildAggregationExpr(agg structs.AggregationType, field string) (string, error) {
	switch agg {
	case structs.AggCount:
		return "count()", nil
	case structs.AggCountUnique:
		if field == "" {
			return "", fmt.Errorf("field is required for count_unique aggregation")
		}
		col, err := buildFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("uniq(%s)", col), nil
	case structs.AggSum:
		if field == "" {
			return "", fmt.Errorf("field is required for sum aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("sum(%s)", col), nil
	case structs.AggAvg:
		if field == "" {
			return "", fmt.Errorf("field is required for avg aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("avg(%s)", col), nil
	case structs.AggMin:
		if field == "" {
			return "", fmt.Errorf("field is required for min aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("min(%s)", col), nil
	case structs.AggMax:
		if field == "" {
			return "", fmt.Errorf("field is required for max aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("max(%s)", col), nil
	case structs.AggP50:
		if field == "" {
			return "", fmt.Errorf("field is required for p50 aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.5)(%s)", col), nil
	case structs.AggP90:
		if field == "" {
			return "", fmt.Errorf("field is required for p90 aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.9)(%s)", col), nil
	case structs.AggP95:
		if field == "" {
			return "", fmt.Errorf("field is required for p95 aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.95)(%s)", col), nil
	case structs.AggP99:
		if field == "" {
			return "", fmt.Errorf("field is required for p99 aggregation")
		}
		col, err := buildNumericFieldExpr(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("quantile(0.99)(%s)", col), nil
	default:
		return "", fmt.Errorf("unsupported aggregation type: %s", agg)
	}
}

// buildFieldExpr builds a SQL expression for a field (column or JSON path)
func buildFieldExpr(field string) (string, error) {
	if strings.HasPrefix(field, "data.") {
		key := strings.TrimPrefix(field, "data.")
		if !safeIdentifierRegex.MatchString(key) {
			return "", fmt.Errorf("invalid data field name: %s", key)
		}
		return fmt.Sprintf("JSONExtractString(data, '%s')", key), nil
	}
	if !validGroupByColumns[field] {
		return "", fmt.Errorf("invalid field: %s", field)
	}
	return field, nil
}

// buildNumericFieldExpr builds a SQL expression for a numeric field
func buildNumericFieldExpr(field string) (string, error) {
	if strings.HasPrefix(field, "data.") {
		key := strings.TrimPrefix(field, "data.")
		if !safeIdentifierRegex.MatchString(key) {
			return "", fmt.Errorf("invalid data field name: %s", key)
		}
		return fmt.Sprintf("toFloat64OrNull(JSONExtractRaw(data, '%s'))", key), nil
	}
	return "", fmt.Errorf("numeric aggregation only supported on data.* fields")
}

// buildGroupByExprs builds GROUP BY expressions
func buildGroupByExprs(groupBy []string) ([]string, []string, error) {
	if len(groupBy) > 10 {
		return nil, nil, fmt.Errorf("too many group by fields (max 10)")
	}

	exprs := make([]string, 0, len(groupBy))
	aliases := make([]string, 0, len(groupBy))

	for i, g := range groupBy {
		alias := fmt.Sprintf("group_%d", i)
		if strings.HasPrefix(g, "data.") {
			key := strings.TrimPrefix(g, "data.")
			if !safeIdentifierRegex.MatchString(key) {
				return nil, nil, fmt.Errorf("invalid data field name: %s", key)
			}
			exprs = append(exprs, fmt.Sprintf("JSONExtractString(data, '%s') AS %s", key, alias))
		} else if validGroupByColumns[g] {
			exprs = append(exprs, fmt.Sprintf("%s AS %s", g, alias))
		} else {
			return nil, nil, fmt.Errorf("invalid group by field: %s", g)
		}
		aliases = append(aliases, alias)
	}
	return exprs, aliases, nil
}

// buildFilterClause builds WHERE clause from filters
func buildFilterClause(filters []structs.QueryFilter) (string, []interface{}, error) {
	if len(filters) == 0 {
		return "", nil, nil
	}

	var conditions []string
	var args []interface{}

	for _, f := range filters {
		cond, condArgs, err := buildSingleFilter(f)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, cond)
		args = append(args, condArgs...)
	}

	return strings.Join(conditions, " AND "), args, nil
}

// buildSingleFilter builds a single filter condition
func buildSingleFilter(f structs.QueryFilter) (string, []interface{}, error) {
	var fieldExpr string

	if strings.HasPrefix(f.Field, "data.") {
		key := strings.TrimPrefix(f.Field, "data.")
		if !safeIdentifierRegex.MatchString(key) {
			return "", nil, fmt.Errorf("invalid data field name: %s", key)
		}
		// Check if operator suggests numeric comparison
		switch f.Operator {
		case "lt", "gt", "lte", "gte":
			fieldExpr = fmt.Sprintf("toFloat64OrNull(JSONExtractRaw(data, '%s'))", key)
		default:
			fieldExpr = fmt.Sprintf("JSONExtractString(data, '%s')", key)
		}
	} else if validColumns[f.Field] {
		fieldExpr = f.Field
	} else {
		return "", nil, fmt.Errorf("invalid filter field: %s", f.Field)
	}

	switch f.Operator {
	case "eq", "":
		return fmt.Sprintf("%s = ?", fieldExpr), []interface{}{f.Value}, nil
	case "neq":
		return fmt.Sprintf("%s != ?", fieldExpr), []interface{}{f.Value}, nil
	case "lt":
		return fmt.Sprintf("%s < ?", fieldExpr), []interface{}{f.Value}, nil
	case "gt":
		return fmt.Sprintf("%s > ?", fieldExpr), []interface{}{f.Value}, nil
	case "lte":
		return fmt.Sprintf("%s <= ?", fieldExpr), []interface{}{f.Value}, nil
	case "gte":
		return fmt.Sprintf("%s >= ?", fieldExpr), []interface{}{f.Value}, nil
	case "contains":
		return fmt.Sprintf("%s LIKE ?", fieldExpr), []interface{}{fmt.Sprintf("%%%v%%", f.Value)}, nil
	case "startswith":
		return fmt.Sprintf("%s LIKE ?", fieldExpr), []interface{}{fmt.Sprintf("%v%%", f.Value)}, nil
	case "endswith":
		return fmt.Sprintf("%s LIKE ?", fieldExpr), []interface{}{fmt.Sprintf("%%%v", f.Value)}, nil
	case "in":
		if values, ok := f.Value.([]interface{}); ok {
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = "?"
			}
			return fmt.Sprintf("%s IN (%s)", fieldExpr, strings.Join(placeholders, ", ")), values, nil
		}
		if values, ok := f.Value.([]string); ok {
			placeholders := make([]string, len(values))
			args := make([]interface{}, len(values))
			for i, v := range values {
				placeholders[i] = "?"
				args[i] = v
			}
			return fmt.Sprintf("%s IN (%s)", fieldExpr, strings.Join(placeholders, ", ")), args, nil
		}
		return "", nil, fmt.Errorf("in operator requires array value")
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", f.Operator)
	}
}

// buildIntervalExpr builds the time bucket expression
func buildIntervalExpr(interval structs.IntervalType) (string, error) {
	switch interval {
	case structs.IntervalMinute:
		return "toStartOfMinute(timestamp)", nil
	case structs.IntervalHour:
		return "toStartOfHour(timestamp)", nil
	case structs.IntervalDay:
		return "toStartOfDay(timestamp)", nil
	case structs.IntervalWeek:
		return "toStartOfWeek(timestamp)", nil
	case structs.IntervalMonth:
		return "toStartOfMonth(timestamp)", nil
	default:
		return "", fmt.Errorf("unsupported interval: %s", interval)
	}
}

// QueryAnalytics executes an analytics query
func QueryAnalytics(ctx context.Context, query *structs.AnalyticsQuery) (*structs.AnalyticsResult, error) {
	// Build aggregation expression
	aggExpr, err := buildAggregationExpr(query.Aggregation, query.Field)
	if err != nil {
		return nil, err
	}

	// Build SELECT clause
	selectParts := []string{fmt.Sprintf("%s AS value", aggExpr)}

	// Build GROUP BY
	var groupByAliases []string
	if len(query.GroupBy) > 0 {
		groupByExprs, aliases, err := buildGroupByExprs(query.GroupBy)
		if err != nil {
			return nil, err
		}
		selectParts = append(selectParts, groupByExprs...)
		groupByAliases = aliases
	}

	// Build WHERE clause
	var whereParts []string
	var args []interface{}

	// Time range
	if !query.From.IsZero() {
		whereParts = append(whereParts, "timestamp >= ?")
		args = append(args, query.From)
	}
	if !query.To.IsZero() {
		whereParts = append(whereParts, "timestamp <= ?")
		args = append(args, query.To)
	}

	// Filters
	if len(query.Filters) > 0 {
		filterClause, filterArgs, err := buildFilterClause(query.Filters)
		if err != nil {
			return nil, err
		}
		if filterClause != "" {
			whereParts = append(whereParts, filterClause)
			args = append(args, filterArgs...)
		}
	}

	// Build query
	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), eventsTable())

	if len(whereParts) > 0 {
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	if len(groupByAliases) > 0 {
		sql += " GROUP BY " + strings.Join(groupByAliases, ", ")
	}

	// ORDER BY
	orderBy := "value"
	if query.OrderBy != "" {
		// Check if ordering by a group field
		for i, g := range query.GroupBy {
			if g == query.OrderBy {
				orderBy = groupByAliases[i]
				break
			}
		}
	}
	orderDir := "DESC"
	if !query.OrderDesc {
		orderDir = "ASC"
	}
	sql += fmt.Sprintf(" ORDER BY %s %s", orderBy, orderDir)

	// LIMIT
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	// Execute query
	rows, err := db.Conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var data []structs.AnalyticsRow
	for rows.Next() {
		// Build scan destinations
		var value float64
		groupValues := make([]string, len(groupByAliases))
		scanDest := make([]interface{}, 1+len(groupByAliases))
		scanDest[0] = &value
		for i := range groupByAliases {
			scanDest[i+1] = &groupValues[i]
		}

		if err := rows.Scan(scanDest...); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		row := structs.AnalyticsRow{
			Value: value,
		}

		if len(query.GroupBy) > 0 {
			row.Groups = make(map[string]string)
			for i, g := range query.GroupBy {
				row.Groups[g] = groupValues[i]
			}
		}

		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	if data == nil {
		data = []structs.AnalyticsRow{}
	}

	return &structs.AnalyticsResult{
		Data:  data,
		Total: len(data),
		Query: query,
	}, nil
}

// QueryTimeSeries executes a time series query
func QueryTimeSeries(ctx context.Context, query *structs.TimeSeriesQuery) (*structs.TimeSeriesResult, error) {
	// Validate time range to prevent excessive data points
	if !query.From.IsZero() && !query.To.IsZero() {
		duration := query.To.Sub(query.From)
		if duration > MaxQueryDuration {
			return nil, fmt.Errorf("time range too large (max %v)", MaxQueryDuration)
		}
		// Estimate number of data points
		var interval time.Duration
		switch query.Interval {
		case structs.IntervalMinute:
			interval = time.Minute
		case structs.IntervalHour:
			interval = time.Hour
		case structs.IntervalDay:
			interval = 24 * time.Hour
		case structs.IntervalWeek:
			interval = 7 * 24 * time.Hour
		case structs.IntervalMonth:
			interval = 30 * 24 * time.Hour
		default:
			interval = time.Hour
		}
		estimatedPoints := int(duration / interval)
		if estimatedPoints > MaxTimeSeriesPoints {
			return nil, fmt.Errorf("query would return too many data points (estimated %d, max %d); use a larger interval or smaller time range", estimatedPoints, MaxTimeSeriesPoints)
		}
	}

	// Build aggregation expression
	aggExpr, err := buildAggregationExpr(query.Aggregation, query.Field)
	if err != nil {
		return nil, err
	}

	// Build interval expression
	intervalExpr, err := buildIntervalExpr(query.Interval)
	if err != nil {
		return nil, err
	}

	// Build SELECT clause
	selectParts := []string{
		fmt.Sprintf("%s AS bucket", intervalExpr),
		fmt.Sprintf("%s AS value", aggExpr),
	}

	// Build GROUP BY aliases
	groupByParts := []string{"bucket"}
	var groupByAliases []string

	if len(query.GroupBy) > 0 {
		groupByExprs, aliases, err := buildGroupByExprs(query.GroupBy)
		if err != nil {
			return nil, err
		}
		selectParts = append(selectParts, groupByExprs...)
		groupByAliases = aliases
		groupByParts = append(groupByParts, aliases...)
	}

	// Build WHERE clause
	var whereParts []string
	var args []interface{}

	// Time range
	if !query.From.IsZero() {
		whereParts = append(whereParts, "timestamp >= ?")
		args = append(args, query.From)
	}
	if !query.To.IsZero() {
		whereParts = append(whereParts, "timestamp <= ?")
		args = append(args, query.To)
	}

	// Filters
	if len(query.Filters) > 0 {
		filterClause, filterArgs, err := buildFilterClause(query.Filters)
		if err != nil {
			return nil, err
		}
		if filterClause != "" {
			whereParts = append(whereParts, filterClause)
			args = append(args, filterArgs...)
		}
	}

	// Build query
	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), eventsTable())

	if len(whereParts) > 0 {
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	sql += " GROUP BY " + strings.Join(groupByParts, ", ")
	sql += " ORDER BY bucket ASC"

	// Execute query
	rows, err := db.Conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Collect data points, grouped by series key
	type seriesData struct {
		groups     map[string]string
		dataPoints []structs.DataPoint
	}
	seriesMap := make(map[string]*seriesData)
	var seriesOrder []string

	for rows.Next() {
		var bucket time.Time
		var value float64
		groupValues := make([]string, len(groupByAliases))

		scanDest := make([]interface{}, 2+len(groupByAliases))
		scanDest[0] = &bucket
		scanDest[1] = &value
		for i := range groupByAliases {
			scanDest[i+2] = &groupValues[i]
		}

		if err := rows.Scan(scanDest...); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		// Build series key
		seriesKey := ""
		var groups map[string]string
		if len(query.GroupBy) > 0 {
			groups = make(map[string]string)
			keyParts := make([]string, len(query.GroupBy))
			for i, g := range query.GroupBy {
				groups[g] = groupValues[i]
				keyParts[i] = groupValues[i]
			}
			seriesKey = strings.Join(keyParts, "|")
		}

		// Get or create series
		sd, exists := seriesMap[seriesKey]
		if !exists {
			sd = &seriesData{
				groups:     groups,
				dataPoints: []structs.DataPoint{},
			}
			seriesMap[seriesKey] = sd
			seriesOrder = append(seriesOrder, seriesKey)
		}

		sd.dataPoints = append(sd.dataPoints, structs.DataPoint{
			Timestamp: bucket,
			Value:     value,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	// Build result
	var series []structs.TimeSeries
	for _, key := range seriesOrder {
		sd := seriesMap[key]

		ts := structs.TimeSeries{
			Name:       key,
			Groups:     sd.groups,
			DataPoints: sd.dataPoints,
		}

		// Fill zeros if requested
		if query.FillZeros && !query.From.IsZero() && !query.To.IsZero() {
			ts.DataPoints = fillTimeSeriesZeros(ts.DataPoints, query.From, query.To, query.Interval)
		}

		series = append(series, ts)
	}

	// If no data and fillZeros requested, create empty series
	if len(series) == 0 && query.FillZeros && !query.From.IsZero() && !query.To.IsZero() {
		series = []structs.TimeSeries{{
			DataPoints: fillTimeSeriesZeros(nil, query.From, query.To, query.Interval),
		}}
	}

	if series == nil {
		series = []structs.TimeSeries{}
	}

	return &structs.TimeSeriesResult{
		Series: series,
		Query:  query,
	}, nil
}

// fillTimeSeriesZeros fills in missing time buckets with zero values
func fillTimeSeriesZeros(points []structs.DataPoint, from, to time.Time, interval structs.IntervalType) []structs.DataPoint {
	// Create a map of existing points
	existing := make(map[int64]float64)
	for _, p := range points {
		existing[p.Timestamp.Unix()] = p.Value
	}

	// Generate all expected buckets
	var result []structs.DataPoint
	current := truncateTime(from, interval)
	end := to

	for !current.After(end) {
		value := float64(0)
		if v, ok := existing[current.Unix()]; ok {
			value = v
		}
		result = append(result, structs.DataPoint{
			Timestamp: current,
			Value:     value,
		})
		current = advanceTime(current, interval)
	}

	return result
}

// truncateTime truncates time to the start of the interval
func truncateTime(t time.Time, interval structs.IntervalType) time.Time {
	switch interval {
	case structs.IntervalMinute:
		return t.Truncate(time.Minute)
	case structs.IntervalHour:
		return t.Truncate(time.Hour)
	case structs.IntervalDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case structs.IntervalWeek:
		// Go to start of week (Monday)
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		return time.Date(t.Year(), t.Month(), t.Day()-(weekday-1), 0, 0, 0, 0, t.Location())
	case structs.IntervalMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	default:
		return t
	}
}

// advanceTime advances time by one interval
func advanceTime(t time.Time, interval structs.IntervalType) time.Time {
	switch interval {
	case structs.IntervalMinute:
		return t.Add(time.Minute)
	case structs.IntervalHour:
		return t.Add(time.Hour)
	case structs.IntervalDay:
		return t.AddDate(0, 0, 1)
	case structs.IntervalWeek:
		return t.AddDate(0, 0, 7)
	case structs.IntervalMonth:
		return t.AddDate(0, 1, 0)
	default:
		return t.Add(time.Hour)
	}
}

// QueryTopN executes a top N query
func QueryTopN(ctx context.Context, query *structs.TopNQuery) (*structs.TopNResult, error) {
	// Build aggregation expression
	aggExpr, err := buildAggregationExpr(query.Aggregation, query.Field)
	if err != nil {
		return nil, err
	}

	// Build group by expression
	var groupExpr string
	if strings.HasPrefix(query.GroupBy, "data.") {
		key := strings.TrimPrefix(query.GroupBy, "data.")
		if !safeIdentifierRegex.MatchString(key) {
			return nil, fmt.Errorf("invalid data field name: %s", key)
		}
		groupExpr = fmt.Sprintf("JSONExtractString(data, '%s')", key)
	} else if validGroupByColumns[query.GroupBy] {
		groupExpr = query.GroupBy
	} else {
		return nil, fmt.Errorf("invalid group by field: %s", query.GroupBy)
	}

	// Build WHERE clause
	var whereParts []string
	var args []interface{}

	// Time range
	if !query.From.IsZero() {
		whereParts = append(whereParts, "timestamp >= ?")
		args = append(args, query.From)
	}
	if !query.To.IsZero() {
		whereParts = append(whereParts, "timestamp <= ?")
		args = append(args, query.To)
	}

	// Filters
	if len(query.Filters) > 0 {
		filterClause, filterArgs, err := buildFilterClause(query.Filters)
		if err != nil {
			return nil, err
		}
		if filterClause != "" {
			whereParts = append(whereParts, filterClause)
			args = append(args, filterArgs...)
		}
	}

	// Build query
	sql := fmt.Sprintf(
		"SELECT %s AS key, %s AS value FROM %s",
		groupExpr, aggExpr, eventsTable(),
	)

	if len(whereParts) > 0 {
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	sql += " GROUP BY key ORDER BY value DESC"

	// LIMIT
	limit := query.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 1000 {
		limit = 1000
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	// Execute query
	rows, err := db.Conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var data []structs.TopNRow
	for rows.Next() {
		var row structs.TopNRow
		if err := rows.Scan(&row.Key, &row.Value); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	if data == nil {
		data = []structs.TopNRow{}
	}

	return &structs.TopNResult{
		Data:  data,
		Query: query,
	}, nil
}

// QueryGauge executes a gauge query (single value)
func QueryGauge(ctx context.Context, query *structs.GaugeQuery) (*structs.GaugeResult, error) {
	// Build aggregation expression
	aggExpr, err := buildAggregationExpr(query.Aggregation, query.Field)
	if err != nil {
		return nil, err
	}

	// Build WHERE clause
	var whereParts []string
	var args []interface{}

	// Time range
	if !query.From.IsZero() {
		whereParts = append(whereParts, "timestamp >= ?")
		args = append(args, query.From)
	}
	if !query.To.IsZero() {
		whereParts = append(whereParts, "timestamp <= ?")
		args = append(args, query.To)
	}

	// Filters
	if len(query.Filters) > 0 {
		filterClause, filterArgs, err := buildFilterClause(query.Filters)
		if err != nil {
			return nil, err
		}
		if filterClause != "" {
			whereParts = append(whereParts, filterClause)
			args = append(args, filterArgs...)
		}
	}

	// Build query
	sql := fmt.Sprintf("SELECT %s AS value FROM %s", aggExpr, eventsTable())

	if len(whereParts) > 0 {
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	// Execute query
	var value float64
	if err := db.Conn.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return &structs.GaugeResult{
		Value: value,
		Query: query,
	}, nil
}

// QueryCompare executes a comparison query between two time periods
func QueryCompare(ctx context.Context, query *structs.CompareQuery) (*structs.CompareResult, error) {
	// Calculate previous period if not specified
	compareFrom := query.CompareFrom
	compareTo := query.CompareTo

	if compareFrom.IsZero() || compareTo.IsZero() {
		duration := query.To.Sub(query.From)
		compareTo = query.From
		compareFrom = compareTo.Add(-duration)
	}

	// Query current period
	currentQuery := &structs.GaugeQuery{
		Aggregation: query.Aggregation,
		Field:       query.Field,
		Filters:     query.Filters,
		From:        query.From,
		To:          query.To,
	}
	currentResult, err := QueryGauge(ctx, currentQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query current period: %w", err)
	}

	// Query previous period
	previousQuery := &structs.GaugeQuery{
		Aggregation: query.Aggregation,
		Field:       query.Field,
		Filters:     query.Filters,
		From:        compareFrom,
		To:          compareTo,
	}
	previousResult, err := QueryGauge(ctx, previousQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query previous period: %w", err)
	}

	// Calculate change
	change := currentResult.Value - previousResult.Value
	var changePercent float64
	if previousResult.Value != 0 {
		changePercent = (change / previousResult.Value) * 100
	}

	return &structs.CompareResult{
		Current:       currentResult.Value,
		Previous:      previousResult.Value,
		Change:        change,
		ChangePercent: changePercent,
		Query:         query,
	}, nil
}
