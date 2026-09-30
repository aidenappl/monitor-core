package responder

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	DefaultSuccessMessage = "request was successful"
	ContentTypeJSON       = "application/json"
)

type Response struct {
	Success    bool        `json:"success"`
	Message    string      `json:"message"`
	Pagination *Pagination `json:"pagination,omitempty"`
	Data       interface{} `json:"data"`

	// Error fields — populated only on failures, mirroring the standard error
	// envelope ({error, error_message, error_code}) that the web clients read to
	// display messages and route 403s (error_code 4003 → /unauthorized, 4004 →
	// /pending). `Message` is kept alongside for backward compatibility with the
	// dashboard's separate fetch layer. omitempty keeps success payloads clean.
	Error        string `json:"error,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	ErrorCode    int    `json:"error_code,omitempty"`
}

type Pagination struct {
	Count    int    `json:"count,omitempty"`
	Next     string `json:"next,omitempty"`
	Previous string `json:"previous,omitempty"`
}

func NewWithCount(w http.ResponseWriter, data interface{}, count int, next, previous string, message ...string) {
	response := Response{
		Success: true,
		Data:    data,
		Pagination: &Pagination{
			Count:    count,
			Next:     next,
			Previous: previous,
		},
		Message: DefaultSuccessMessage,
	}

	if len(message) > 0 {
		response.Message = message[0]
	}

	response.Message = strings.ToLower(response.Message)

	w.Header().Set("Content-Type", ContentTypeJSON)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func New(w http.ResponseWriter, data interface{}, message ...string) {
	response := Response{
		Success: true,
		Data:    data,
		Message: DefaultSuccessMessage,
	}

	if len(message) > 0 {
		response.Message = message[0]
	}

	response.Message = strings.ToLower(response.Message)

	w.Header().Set("Content-Type", ContentTypeJSON)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func Error(w http.ResponseWriter, statusCode int, message string) {
	writeError(w, statusCode, message, statusCode, nil, nil)
}

// ErrorWithCode is Error with an explicit application error_code (distinct from
// the HTTP status). Used for codes the web clients route on — e.g. 4003
// (forbidden → /unauthorized) and 4004 (pending → /pending).
func ErrorWithCode(w http.ResponseWriter, statusCode int, message string, errorCode int) {
	writeError(w, statusCode, message, errorCode, nil, nil)
}

// ErrorWithCause is Error with the underlying error, which goes to the request's
// Monitor event (never to the client). fields is optional context for that event
// — the ids and dependency detail a 5xx needs to be diagnosed without being
// reproduced (a rule id, a channel type, a query kind).
//
// The handler must NOT report the same failure itself: this is already its one
// event.
func ErrorWithCause(w http.ResponseWriter, statusCode int, message string, err error, fields ...map[string]any) {
	var merged map[string]any
	for _, f := range fields {
		if merged == nil {
			merged = make(map[string]any, len(f))
		}
		for k, v := range f {
			merged[k] = v
		}
	}
	writeError(w, statusCode, message, statusCode, err, merged)
}

// failureRecorder is implemented by the request middleware's response writer.
// Handing it the reason a request failed puts that reason on the request's
// Monitor event, where it is grouped into an issue by route and cause. It is an
// interface, not an import, so responder stays a leaf package.
type failureRecorder interface {
	RecordFailure(status int, message string, err error, code int, fields map[string]any)
}

// recordFailure finds the failureRecorder under any wrapping writers.
func recordFailure(w http.ResponseWriter, status int, message string, err error, code int, fields map[string]any) {
	for w != nil {
		if fr, ok := w.(failureRecorder); ok {
			fr.RecordFailure(status, message, err, code, fields)
			return
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return
		}
		w = u.Unwrap()
	}
}

func writeError(w http.ResponseWriter, statusCode int, message string, errorCode int, cause error, fields map[string]any) {
	msg := strings.ToLower(message)
	recordFailure(w, statusCode, msg, cause, errorCode, fields)

	response := Response{
		Success:      false,
		Message:      msg,
		Data:         nil,
		Error:        http.StatusText(statusCode),
		ErrorMessage: msg,
		ErrorCode:    errorCode,
	}

	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(statusCode)
	// The status is already sent; an encode failure here means the client went
	// away, and there is nothing left to tell it.
	_ = json.NewEncoder(w).Encode(response)
}
