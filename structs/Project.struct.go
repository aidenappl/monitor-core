package structs

import "time"

// ProjectStatus is a project's lifecycle axis. Same two values as ZoneStatus and
// deliberately a separate type — see the note on Zone for why the memberships
// coinciding today is not a reason to share one.
type ProjectStatus string

const (
	ProjectStatusActive  ProjectStatus = "active"
	ProjectStatusDeleted ProjectStatus = "deleted"
)

// IsValid reports whether the status is one of the known values.
func (s ProjectStatus) IsValid() bool {
	switch s {
	case ProjectStatusActive, ProjectStatusDeleted:
		return true
	}
	return false
}

// Project is a tenant inside one zone — the dimension every event is filed
// under. Lives in MariaDB (projects).
//
// The project an event belongs to is DERIVED SERVER-SIDE from the authenticating
// api_keys row and OVERWRITTEN on ingest. It is never trusted from the client,
// for the same reason Event.IssueID is not: a caller that could name its own
// project could file its events under someone else's, and every downstream
// number — dashboards, alert thresholds, the issue rollup — would be wrong with
// nothing to distinguish it from real traffic.
//
// Slug is unique WITHIN its zone, not globally: two zones may each own a
// `payments`, and forcing one of them to disambiguate would leak the other
// zone's naming into it. Slug is immutable and its name is never reused, even
// after the project is retired to ProjectStatusDeleted — events outlive the row
// by up to 30 days and the daily rollup outlives it permanently, so a recycled
// slug would silently reattach one owner's history to another.
type Project struct {
	ID          int64         `json:"id"`
	ZoneID      int64         `json:"zone_id"`
	Slug        string        `json:"slug"`
	DisplayName string        `json:"display_name"`
	Status      ProjectStatus `json:"status"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}
