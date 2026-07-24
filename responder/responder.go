package responder

import (
	"encoding/json"
	"log"
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
	writeError(w, statusCode, message, statusCode)
}

// ErrorWithCode is Error with an explicit application error_code (distinct from
// the HTTP status). Used for codes the web clients route on — e.g. 4003
// (forbidden → /unauthorized) and 4004 (pending → /pending).
func ErrorWithCode(w http.ResponseWriter, statusCode int, message string, errorCode int) {
	writeError(w, statusCode, message, errorCode)
}

func ErrorWithCause(w http.ResponseWriter, statusCode int, message string, err error) {
	log.Printf("[%d] %s: %v", statusCode, message, err)
	writeError(w, statusCode, message, statusCode)
}

func writeError(w http.ResponseWriter, statusCode int, message string, errorCode int) {
	msg := strings.ToLower(message)
	log.Printf("[%d] %s (code %d)", statusCode, msg, errorCode)

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
	json.NewEncoder(w).Encode(response)
}
