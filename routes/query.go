package routes

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/services"
	"github.com/gorilla/mux"
)

func QueryEventsHandler(w http.ResponseWriter, r *http.Request) {
	params, err := parseQueryParams(r)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := services.QueryEvents(r.Context(), params)
	if err != nil {
		if isFilterValidationError(err) {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to query events", err)
		return
	}

	nextURL, prevURL := buildPaginationURLs(r, params, result.Total)
	responder.NewWithCount(w, result.Events, result.Total, nextURL, prevURL)
}

func GetLabelValuesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	label := vars["label"]

	params, err := parseQueryParams(r)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := services.GetLabelValues(r.Context(), label, params)
	if err != nil {
		if strings.Contains(err.Error(), "invalid label") || isFilterValidationError(err) {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to get label values", err)
		return
	}

	responder.New(w, result.Values)
}

func GetDataKeysHandler(w http.ResponseWriter, r *http.Request) {
	params, err := parseQueryParams(r)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := services.GetDataKeys(r.Context(), params)
	if err != nil {
		if isFilterValidationError(err) {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to get data keys", err)
		return
	}

	responder.New(w, result.Keys)
}

func GetDataValuesHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		responder.Error(w, http.StatusBadRequest, "key parameter is required")
		return
	}

	params, err := parseQueryParams(r)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := services.GetDataValues(r.Context(), key, params)
	if err != nil {
		if isFilterValidationError(err) {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to get data values", err)
		return
	}

	responder.New(w, result.Values)
}

// isFilterValidationError reports whether err is a caller-fixable filter error
// (bad column / bad data field name) that should map to 400 rather than 500.
func isFilterValidationError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "invalid filter column") ||
		strings.Contains(msg, "invalid data field name") ||
		strings.Contains(msg, "invalid filter field")
}

// reservedParams are query params that are not filters
var reservedParams = map[string]bool{
	"from":   true,
	"to":     true,
	"limit":  true,
	"offset": true,
	"key":    true,
}

// validOperators maps suffix to operator
var validOperators = map[string]services.Operator{
	"eq":         services.OpEq,
	"neq":        services.OpNeq,
	"lt":         services.OpLt,
	"gt":         services.OpGt,
	"lte":        services.OpLte,
	"gte":        services.OpGte,
	"contains":   services.OpContains,
	"startswith": services.OpStartsWith,
	"endswith":   services.OpEndsWith,
	"in":         services.OpIn,
}

// parseFilterKey parses "field__operator" into field and operator
// Returns field, operator, isData
func parseFilterKey(key string) (string, services.Operator, bool) {
	isData := false
	if strings.HasPrefix(key, "data.") {
		isData = true
		key = strings.TrimPrefix(key, "data.")
	}

	parts := strings.Split(key, "__")
	if len(parts) == 1 {
		return parts[0], services.OpEq, isData
	}

	field := parts[0]
	opStr := parts[len(parts)-1]

	if op, ok := validOperators[opStr]; ok {
		return field, op, isData
	}

	// If not a valid operator, treat the whole thing as field name with eq
	return key, services.OpEq, isData
}

func parseQueryParams(r *http.Request) (services.QueryParams, error) {
	q := r.URL.Query()
	params := services.QueryParams{
		Filters: []services.Filter{},
	}

	// Parse time range
	if from := q.Get("from"); from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			if unix, err := strconv.ParseInt(from, 10, 64); err == nil {
				t = time.Unix(unix, 0)
			}
		}
		params.From = t
	}

	if to := q.Get("to"); to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			if unix, err := strconv.ParseInt(to, 10, 64); err == nil {
				t = time.Unix(unix, 0)
			}
		}
		params.To = t
	}

	// Parse pagination
	if limit := q.Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			params.Limit = l
		}
	}

	if offset := q.Get("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil {
			params.Offset = o
		}
	}

	// Parse filters
	for key, values := range q {
		if reservedParams[key] || len(values) == 0 {
			continue
		}

		field, op, isData := parseFilterKey(key)

		var value interface{}
		if op == services.OpIn {
			// For "in" operator, split by comma
			value = strings.Split(values[0], ",")
		} else {
			value = values[0]
		}

		params.Filters = append(params.Filters, services.Filter{
			Field:    field,
			Operator: op,
			Value:    value,
			IsData:   isData,
		})
	}

	return params, nil
}

func buildPaginationURLs(r *http.Request, params services.QueryParams, total int) (next, prev string) {
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}

	baseURL := r.URL.Path
	query := r.URL.Query()

	nextOffset := params.Offset + limit
	if nextOffset < total {
		query.Set("offset", strconv.Itoa(nextOffset))
		query.Set("limit", strconv.Itoa(limit))
		next = baseURL + "?" + query.Encode()
	}

	if params.Offset > 0 {
		prevOffset := params.Offset - limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		query.Set("offset", strconv.Itoa(prevOffset))
		query.Set("limit", strconv.Itoa(limit))
		prev = baseURL + "?" + query.Encode()
	}

	return next, prev
}
