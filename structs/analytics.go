package structs

import "time"

// AggregationType defines the type of aggregation to perform
type AggregationType string

const (
	AggCount       AggregationType = "count"
	AggSum         AggregationType = "sum"
	AggAvg         AggregationType = "avg"
	AggMin         AggregationType = "min"
	AggMax         AggregationType = "max"
	AggCountUnique AggregationType = "count_unique"
	AggP50         AggregationType = "p50"
	AggP90         AggregationType = "p90"
	AggP95         AggregationType = "p95"
	AggP99         AggregationType = "p99"
)

// IntervalType defines time bucket intervals for time series
type IntervalType string

const (
	IntervalMinute IntervalType = "minute"
	IntervalHour   IntervalType = "hour"
	IntervalDay    IntervalType = "day"
	IntervalWeek   IntervalType = "week"
	IntervalMonth  IntervalType = "month"
)

// AnalyticsQuery represents a query for analytics data
type AnalyticsQuery struct {
	// Aggregation settings
	Aggregation AggregationType `json:"aggregation"`
	Field       string          `json:"field,omitempty"` // Required for sum, avg, min, max, percentiles

	// Grouping
	GroupBy []string `json:"group_by,omitempty"` // e.g., ["service", "name", "data.status"]

	// Filtering
	Filters []QueryFilter `json:"filters,omitempty"`

	// Time range
	From time.Time `json:"from"`
	To   time.Time `json:"to"`

	// Ordering
	OrderBy   string `json:"order_by,omitempty"` // "value" or a group_by field
	OrderDesc bool   `json:"order_desc,omitempty"`

	// Limits
	Limit int `json:"limit,omitempty"`
}

// TimeSeriesQuery represents a query for time series data
type TimeSeriesQuery struct {
	// Aggregation settings
	Aggregation AggregationType `json:"aggregation"`
	Field       string          `json:"field,omitempty"` // Required for sum, avg, min, max, percentiles

	// Time bucketing
	Interval IntervalType `json:"interval"` // minute, hour, day, week, month

	// Grouping (for multiple series)
	GroupBy []string `json:"group_by,omitempty"` // e.g., ["service"] to get a series per service

	// Filtering
	Filters []QueryFilter `json:"filters,omitempty"`

	// Time range
	From time.Time `json:"from"`
	To   time.Time `json:"to"`

	// Fill empty buckets with zero
	FillZeros bool `json:"fill_zeros,omitempty"`
}

// QueryFilter represents a filter condition
type QueryFilter struct {
	Field    string `json:"field"`    // Column name or "data.key" for JSON fields
	Operator string `json:"operator"` // eq, neq, lt, gt, lte, gte, contains, startswith, endswith, in
	Value    any    `json:"value"`
}

// AnalyticsResult represents the result of an analytics query
type AnalyticsResult struct {
	Data  []AnalyticsRow  `json:"data"`
	Total int             `json:"total"`
	Query *AnalyticsQuery `json:"query,omitempty"`
}

// AnalyticsRow represents a single row in analytics results
type AnalyticsRow struct {
	Value  float64           `json:"value"`
	Groups map[string]string `json:"groups,omitempty"`
}

// TimeSeriesResult represents the result of a time series query
type TimeSeriesResult struct {
	Series []TimeSeries     `json:"series"`
	Query  *TimeSeriesQuery `json:"query,omitempty"`
}

// TimeSeries represents a single time series
type TimeSeries struct {
	Name       string            `json:"name,omitempty"`
	Groups     map[string]string `json:"groups,omitempty"`
	DataPoints []DataPoint       `json:"data_points"`
}

// DataPoint represents a single point in a time series
type DataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// TopNQuery represents a query for top N values
type TopNQuery struct {
	// What to count/aggregate
	Aggregation AggregationType `json:"aggregation"`
	Field       string          `json:"field,omitempty"`

	// What to group by (the "N" dimension)
	GroupBy string `json:"group_by"` // e.g., "service", "name", "data.endpoint"

	// Filtering
	Filters []QueryFilter `json:"filters,omitempty"`

	// Time range
	From time.Time `json:"from"`
	To   time.Time `json:"to"`

	// Number of results
	Limit int `json:"limit"`
}

// TopNResult represents the result of a top N query
type TopNResult struct {
	Data  []TopNRow  `json:"data"`
	Query *TopNQuery `json:"query,omitempty"`
}

// TopNRow represents a single row in top N results
type TopNRow struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
}

// GaugeQuery represents a query for a single gauge value
type GaugeQuery struct {
	Aggregation AggregationType `json:"aggregation"`
	Field       string          `json:"field,omitempty"`
	Filters     []QueryFilter   `json:"filters,omitempty"`
	From        time.Time       `json:"from"`
	To          time.Time       `json:"to"`
}

// GaugeResult represents the result of a gauge query
type GaugeResult struct {
	Value float64     `json:"value"`
	Query *GaugeQuery `json:"query,omitempty"`
}

// CompareQuery represents a query comparing two time periods
type CompareQuery struct {
	Aggregation AggregationType `json:"aggregation"`
	Field       string          `json:"field,omitempty"`
	Filters     []QueryFilter   `json:"filters,omitempty"`

	// Current period
	From time.Time `json:"from"`
	To   time.Time `json:"to"`

	// Compare against (if not set, will auto-calculate based on period length)
	CompareFrom time.Time `json:"compare_from,omitempty"`
	CompareTo   time.Time `json:"compare_to,omitempty"`
}

// CompareResult represents the result of a comparison query
type CompareResult struct {
	Current       float64       `json:"current"`
	Previous      float64       `json:"previous"`
	Change        float64       `json:"change"`         // Absolute change
	ChangePercent float64       `json:"change_percent"` // Percentage change
	Query         *CompareQuery `json:"query,omitempty"`
}
