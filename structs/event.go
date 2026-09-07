package structs

import (
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

// uuidRegex matches standard UUID format (with or without hyphens)
var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Event represents a single monitoring event
type Event struct {
	Timestamp time.Time              `json:"timestamp"`
	Service   string                 `json:"service"`
	Env       string                 `json:"env"`
	JobID     string                 `json:"job_id"`
	RequestID string                 `json:"request_id"`
	TraceID   string                 `json:"trace_id"`
	UserID    string                 `json:"user_id"`
	Name      string                 `json:"name"`
	Level     string                 `json:"level"`
	Data      map[string]interface{} `json:"data"`

	// IssueID is the issue this event belongs to, stamped server-side at ingest
	// for error/fatal events and empty otherwise. It is DERIVED, never trusted
	// from the client: the ingest path overwrites whatever arrives here, so a
	// caller cannot file its events under someone else's issue.
	//
	// It exists so an issue's events can be looked up by an indexed equality
	// match instead of rescanning candidates and recomputing fingerprints in Go.
	IssueID string `json:"issue_id,omitempty"`

	// Project is the tenant slug this event files under, stamped server-side at
	// ingest from the api_keys row behind the presented key (or, for the env
	// master key — which has no such row — from env.DefaultProjectSlug). Like
	// IssueID it is DERIVED, never trusted from the client: the ingest path
	// overwrites whatever arrives here, so a caller cannot file its events under
	// another project.
	//
	// That overwrite IS the tenancy boundary; there is nothing else enforcing
	// it. A project a caller may name is a project a caller may impersonate, and
	// the damage is silent rather than loud: the forged rows are valid events
	// that every per-project view — quotas, dashboards, alert rules, retention —
	// then counts as the victim's own traffic, with nothing in the row left to
	// tell them apart afterwards.
	Project string `json:"project,omitempty"`
}

// Validate checks that all required fields are present and IDs are valid UUIDs
func (e *Event) Validate() error {
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	if e.Service == "" {
		return errors.New("service is required")
	}
	if e.Name == "" {
		return errors.New("name is required")
	}
	if e.JobID != "" && !uuidRegex.MatchString(e.JobID) {
		return errors.New("job_id must be a valid UUID")
	}
	if e.RequestID != "" && !uuidRegex.MatchString(e.RequestID) {
		return errors.New("request_id must be a valid UUID")
	}
	if e.TraceID != "" && !uuidRegex.MatchString(e.TraceID) {
		return errors.New("trace_id must be a valid UUID")
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
