package structs

import (
	"encoding/json"
	"errors"
	"time"
)

// Event represents a single monitoring event
type Event struct {
	Timestamp time.Time              `json:"timestamp"`
	Service   string                 `json:"service"`
	Env       string                 `json:"env"`
	JobID     string                 `json:"job_id"`
	RequestID string                 `json:"request_id"`
	TraceID   string                 `json:"trace_id"`
	Name      string                 `json:"name"`
	Level     string                 `json:"level"`
	Data      map[string]interface{} `json:"data"`
}

// Validate checks that all required fields are present
func (e *Event) Validate() error {
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	if e.Service == "" {
		return errors.New("service is required")
	}
	if e.JobID == "" && e.RequestID == "" && e.TraceID == "" {
		return errors.New("at least one of job_id, request_id, or trace_id is required")
	}
	if e.Name == "" {
		return errors.New("name is required")
	}
	return nil
}

// DataJSON returns the data field as a JSON string
func (e *Event) DataJSON() string {
	if e.Data == nil {
		return "{}"
	}
	b, err := json.Marshal(e.Data)
	if err != nil {
		return "{}"
	}
	return string(b)
}
