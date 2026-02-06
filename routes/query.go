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
		responder.Error(w, http.StatusInternalServerError, "failed to query events")
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
		if strings.Contains(err.Error(), "invalid label") {
			responder.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		responder.Error(w, http.StatusInternalServerError, "failed to get label values")
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
		responder.Error(w, http.StatusInternalServerError, "failed to get data keys")
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
		responder.Error(w, http.StatusInternalServerError, "failed to get data values")
		return
	}

	responder.New(w, result.Values)
}

func parseQueryParams(r *http.Request) (services.QueryParams, error) {
	q := r.URL.Query()

	params := services.QueryParams{
		Service:     q.Get("service"),
		Env:         q.Get("env"),
		JobID:       q.Get("job_id"),
		RequestID:   q.Get("request_id"),
		TraceID:     q.Get("trace_id"),
		Name:        q.Get("name"),
		Level:       q.Get("level"),
		DataFilters: make(map[string]string),
	}

	// Parse time range
	if from := q.Get("from"); from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			// Try unix timestamp
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

	// Parse data filters (data.key=value format)
	for key, values := range q {
		if strings.HasPrefix(key, "data.") && len(values) > 0 {
			dataKey := strings.TrimPrefix(key, "data.")
			params.DataFilters[dataKey] = values[0]
		}
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

	// Calculate next offset
	nextOffset := params.Offset + limit
	if nextOffset < total {
		query.Set("offset", strconv.Itoa(nextOffset))
		query.Set("limit", strconv.Itoa(limit))
		next = baseURL + "?" + query.Encode()
	}

	// Calculate prev offset
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
