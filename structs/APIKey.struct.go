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
//
// ProjectID is the tenant binding, and it is the whole reason this row is
// interesting to ingestion: the project an event is filed under is derived from
// the key that authenticated it and overwritten server-side, exactly as
// Event.IssueID is. It is NOT NULL and foreign-keyed
// (db/migrations/117_api_keys_project.sql), so there is no such thing as a key
// that authenticates into no tenant.
//
// ProjectSlug is denormalised from projects.slug rather than looked up per use.
// That is safe in a way a cached display_name would not be: a slug is immutable
// and never reused, so a copy of one cannot go stale — the row it names is the
// row it will always name. It is carried so the ingest path can stamp events
// without a second query per event, and so the admin listing can show a
// human-readable tenant without a join at the caller.
//
// Name is unique per project, not globally, so two projects may each own a key
// called "ingest". KeyHash is the deliberate exception and stays globally
// unique — authentication resolves a raw header string to one row with no
// project in hand, so a per-project hash key would let two rows answer the same
// credential. See the header of migration 117.
type APIKey struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Scope       Scope      `json:"scope"`
	ProjectID   int64      `json:"project_id"`
	ProjectSlug string     `json:"project_slug"`
	KeyPrefix   string     `json:"key_prefix"`
	KeyHash     string     `json:"-"`
	CreatedAt   time.Time  `json:"created_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
}
