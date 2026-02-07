package routes

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/structs"
)

// maxRequestBodySize limits request body to 1MB
const maxRequestBodySize = 1 << 20

// validAggregations defines allowed aggregation types
var validAggregations = map[structs.AggregationType]bool{
	structs.AggCount:       true,
	structs.AggSum:         true,
	structs.AggAvg:         true,
	structs.AggMin:         true,
	structs.AggMax:         true,
	structs.AggCountUnique: true,
	structs.AggP50:         true,
	structs.AggP90:         true,
	structs.AggP95:         true,
	structs.AggP99:         true,
}

// validIntervals defines allowed interval types
var validIntervals = map[structs.IntervalType]bool{
	structs.IntervalMinute: true,
	structs.IntervalHour:   true,
	structs.IntervalDay:    true,
	structs.IntervalWeek:   true,
	structs.IntervalMonth:  true,
}

// AnalyticsHandler handles POST /v1/analytics requests
// Allows complex analytics queries with grouping and aggregation
func AnalyticsHandler(w http.ResponseWriter, r *http.Request) {
	// Limit request body size
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var query structs.AnalyticsQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		if err == io.EOF {
			responder.Error(w, http.StatusBadRequest, "request body is required")
			return
		}
		responder.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Validate aggregation
	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}

	result, err := services.QueryAnalytics(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute analytics query", err)
		return
	}

	responder.New(w, result)
}

// TimeSeriesHandler handles POST /v1/timeseries requests
// Returns time-bucketed data for charting
func TimeSeriesHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var query structs.TimeSeriesQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		if err == io.EOF {
			responder.Error(w, http.StatusBadRequest, "request body is required")
			return
		}
		responder.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Validate required fields
	if query.Interval == "" {
		responder.Error(w, http.StatusBadRequest, "interval is required")
		return
	}
	if !validIntervals[query.Interval] {
		responder.Error(w, http.StatusBadRequest, "invalid interval type")
		return
	}
	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}

	result, err := services.QueryTimeSeries(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "too many") || strings.Contains(err.Error(), "too large") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute time series query", err)
		return
	}

	responder.New(w, result)
}

// TopNHandler handles POST /v1/topn requests
// Returns top N values grouped by a field
func TopNHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var query structs.TopNQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		if err == io.EOF {
			responder.Error(w, http.StatusBadRequest, "request body is required")
			return
		}
		responder.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Validate required fields
	if query.GroupBy == "" {
		responder.Error(w, http.StatusBadRequest, "group_by is required")
		return
	}
	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}

	result, err := services.QueryTopN(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute top N query", err)
		return
	}

	responder.New(w, result)
}

// GaugeHandler handles POST /v1/gauge requests
// Returns a single aggregated value
func GaugeHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var query structs.GaugeQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		if err == io.EOF {
			responder.Error(w, http.StatusBadRequest, "request body is required")
			return
		}
		responder.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}

	result, err := services.QueryGauge(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute gauge query", err)
		return
	}

	responder.New(w, result)
}

// CompareHandler handles POST /v1/compare requests
// Compares current period with a previous period
func CompareHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var query structs.CompareQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		if err == io.EOF {
			responder.Error(w, http.StatusBadRequest, "request body is required")
			return
		}
		responder.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	// Validate time range
	if query.From.IsZero() || query.To.IsZero() {
		responder.Error(w, http.StatusBadRequest, "from and to are required")
		return
	}
	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}

	result, err := services.QueryCompare(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute compare query", err)
		return
	}

	responder.New(w, result)
}

// AnalyticsQueryHandler handles GET /v1/analytics requests
// Simple query-string based analytics for easy Grafana integration
func AnalyticsQueryHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	query := structs.AnalyticsQuery{
		Aggregation: structs.AggregationType(q.Get("aggregation")),
		Field:       q.Get("field"),
	}

	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}

	// Parse group_by (comma-separated)
	if groupBy := q.Get("group_by"); groupBy != "" {
		query.GroupBy = strings.Split(groupBy, ",")
	}

	// Parse time range
	query.From, query.To = parseTimeRange(q.Get("from"), q.Get("to"))

	// Parse ordering
	query.OrderBy = q.Get("order_by")
	query.OrderDesc = q.Get("order") == "desc"

	// Parse limit
	if limit := q.Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			query.Limit = l
		}
	}

	// Parse filters from query string
	query.Filters = parseFiltersFromQuery(q)

	result, err := services.QueryAnalytics(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "too many") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute analytics query", err)
		return
	}

	responder.New(w, result)
}

// TimeSeriesQueryHandler handles GET /v1/timeseries requests
// Simple query-string based time series for easy Grafana integration
func TimeSeriesQueryHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	query := structs.TimeSeriesQuery{
		Aggregation: structs.AggregationType(q.Get("aggregation")),
		Field:       q.Get("field"),
		Interval:    structs.IntervalType(q.Get("interval")),
		FillZeros:   q.Get("fill_zeros") == "true",
	}

	if query.Aggregation == "" {
		query.Aggregation = structs.AggCount
	} else if !validAggregations[query.Aggregation] {
		responder.Error(w, http.StatusBadRequest, "invalid aggregation type")
		return
	}
	if query.Interval == "" {
		query.Interval = structs.IntervalHour
	} else if !validIntervals[query.Interval] {
		responder.Error(w, http.StatusBadRequest, "invalid interval type")
		return
	}

	// Parse group_by (comma-separated)
	if groupBy := q.Get("group_by"); groupBy != "" {
		query.GroupBy = strings.Split(groupBy, ",")
	}

	// Parse time range
	query.From, query.To = parseTimeRange(q.Get("from"), q.Get("to"))

	// Parse filters from query string
	query.Filters = parseFiltersFromQuery(q)

	result, err := services.QueryTimeSeries(r.Context(), &query)
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "too many") || strings.Contains(err.Error(), "too large") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to execute time series query", err)
		return
	}

	responder.New(w, result)
}

// analyticsReservedParams are query params that are not filters
var analyticsReservedParams = map[string]bool{
	"from":        true,
	"to":          true,
	"limit":       true,
	"aggregation": true,
	"field":       true,
	"group_by":    true,
	"order_by":    true,
	"order":       true,
	"interval":    true,
	"fill_zeros":  true,
}

// parseTimeRange parses from/to time values
func parseTimeRange(from, to string) (time.Time, time.Time) {
	var fromTime, toTime time.Time

	if from != "" {
		if t, err := time.Parse(time.RFC3339, from); err == nil {
			fromTime = t
		} else if unix, err := strconv.ParseInt(from, 10, 64); err == nil {
			fromTime = time.Unix(unix, 0)
		}
	}

	if to != "" {
		if t, err := time.Parse(time.RFC3339, to); err == nil {
			toTime = t
		} else if unix, err := strconv.ParseInt(to, 10, 64); err == nil {
			toTime = time.Unix(unix, 0)
		}
	}

	return fromTime, toTime
}

// parseFiltersFromQuery extracts filters from query parameters
func parseFiltersFromQuery(q map[string][]string) []structs.QueryFilter {
	var filters []structs.QueryFilter

	for key, values := range q {
		if analyticsReservedParams[key] || len(values) == 0 {
			continue
		}

		field, operator := parseAnalyticsFilterKey(key)

		var value any
		if operator == "in" {
			value = strings.Split(values[0], ",")
		} else {
			value = values[0]
		}

		filters = append(filters, structs.QueryFilter{
			Field:    field,
			Operator: operator,
			Value:    value,
		})
	}

	return filters
}

// parseAnalyticsFilterKey parses "field__operator" into field and operator
func parseAnalyticsFilterKey(key string) (string, string) {
	parts := strings.Split(key, "__")
	if len(parts) == 1 {
		return parts[0], "eq"
	}

	field := strings.Join(parts[:len(parts)-1], "__")
	opStr := parts[len(parts)-1]

	validOps := map[string]bool{
		"eq": true, "neq": true, "lt": true, "gt": true,
		"lte": true, "gte": true, "contains": true,
		"startswith": true, "endswith": true, "in": true,
	}

	if validOps[opStr] {
		return field, opStr
	}

	return key, "eq"
}
