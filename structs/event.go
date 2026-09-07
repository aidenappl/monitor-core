package structs

import (
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

// correlationIDRegex is the accepted shape of job_id, request_id and trace_id.
//
// ⚠️ THIS IS A WIRE CONTRACT WITH THE SDKs, NOT A LOCAL PREFERENCE. It is the
// only thing standing between a client and total, silent data loss: parseEvents
// is all-or-nothing, so ONE rejected id 400s the whole batch, and go-monitor's
// shipper treats 4xx as non-retryable and drops it (shipper.go — "Client error
// — don't retry"). A service whose ids stop matching this loses 100% of its
// events with one line on stderr and nothing in Monitor.
//
// That is not hypothetical: go-monitor once changed its default job_id and
// request_id from a UUID to an 8-hex-character token, which this regex rejected.
// Any service adopting that build would have gone dark.
//
// So the accepted set is DELIBERATELY A SUPERSET of every format the SDKs have
// ever emitted, and widening it is always safe while narrowing it never is:
//   - a UUID, hyphenated (what v0.0.8 and every deployed service emits today)
//   - a UUID, unhyphenated (what this comment has always claimed to allow, and
//     did not — the old pattern required hyphens)
//   - a compact hex token of 8-64 characters (the log-friendly form go-monitor
//     wanted; 8 is accepted for compatibility, but see go-monitor's ids.go for
//     why anything under 16 is too collision-prone to be worth emitting)
//
// If you narrow this, you must ship monitor-core BEFORE any SDK that could emit
// the removed form — and there is no ordering in which the reverse is safe.
var correlationIDRegex = regexp.MustCompile(`^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{8,64})$`)

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
	if e.JobID != "" && !correlationIDRegex.MatchString(e.JobID) {
		return errors.New("job_id must be a UUID or a hex token of 8-64 characters")
	}
	if e.RequestID != "" && !correlationIDRegex.MatchString(e.RequestID) {
		return errors.New("request_id must be a UUID or a hex token of 8-64 characters")
	}
	if e.TraceID != "" && !correlationIDRegex.MatchString(e.TraceID) {
		return errors.New("trace_id must be a UUID or a hex token of 8-64 characters")
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
