package structs

import "time"

// ZoneStatus is a zone's lifecycle axis.
//
// There is no 'deleted' verb here because there is no delete. A zone row is
// retired by moving to ZoneStatusDeleted and is then kept forever, so the
// UNIQUE key on slug keeps the name permanently spent — see
// db/migrations/116_create_registry.sql for why reuse is the failure to design
// against rather than the convenience to allow.
type ZoneStatus string

const (
	ZoneStatusActive  ZoneStatus = "active"
	ZoneStatusDeleted ZoneStatus = "deleted"
)

// IsValid reports whether the status is one of the known values.
func (s ZoneStatus) IsValid() bool {
	switch s {
	case ZoneStatusActive, ZoneStatusDeleted:
		return true
	}
	return false
}

// Zone is one whole ClickHouse instance. Lives in MariaDB (zones), in
// monitor_auth rather than the `monitor` schema — the registry sits next to
// api_keys, which is what binds an event to a project.
//
// A zone is an INSTANCE, not a database inside one: the ClickHouse database is
// always the one named by CLICKHOUSE_DATABASE, for every zone. Slug therefore
// never reaches ClickHouse SQL as an identifier, and no zone-derived string is
// ever interpolated into a query — which is what keeps the whole dimension out
// of the injection surface that structs/columns.go exists to police.
//
// Slug is immutable. DisplayName is the mutable, non-unique label, and exists
// precisely so that renaming a team never creates pressure to rename the
// identifier underneath it.
//
// ZoneStatus and ProjectStatus are deliberately separate types with currently
// identical memberships, for the reason columns.go gives about its four column
// sets: "is this zone live" and "is this project live" are two questions about
// one schema, and folding them into one type would mean that answering one of
// them differently in future silently re-answers the other.
type Zone struct {
	ID          int64      `json:"id"`
	Slug        string     `json:"slug"`
	DisplayName string     `json:"display_name"`
	Status      ZoneStatus `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}
