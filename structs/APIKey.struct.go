package structs

import "time"

// Scope defines the permission level of an API key.
type Scope string

const (
	ScopeAdmin  Scope = "admin"  // Full read/write access to all routes
	ScopeIngest Scope = "ingest" // Write-only access to /v1/events ingest
)

// APIKey is a stored ingest/admin credential. Lives in MariaDB (api_keys).
// KeyHash is never serialized to API responses.
type APIKey struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Scope      Scope      `json:"scope"`
	KeyPrefix  string     `json:"key_prefix"`
	KeyHash    string     `json:"-"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}
