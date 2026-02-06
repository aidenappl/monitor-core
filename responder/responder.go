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
	response := Response{
		Success: false,
		Message: strings.ToLower(message),
		Data:    nil,
	}

	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}
