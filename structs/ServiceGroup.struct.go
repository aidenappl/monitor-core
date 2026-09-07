package structs

import "time"

// ServiceGroup is a named set of services, referenced by a notification policy's
// `matchers.service_group`. Lives in MariaDB (monitor.service_groups).
//
// It was `alerts.ServiceGroup` and moved here with the table (migration 122), for
// the import-cycle reason AlertRule.struct.go records. The json tags are
// unchanged.
//
// Services is a raw JSON array of service names rather than a []string, because
// that is the wire contract and because alerts.ResolveServiceGroups is the only
// thing that decodes it. A group whose value fails to parse is SKIPPED there
// rather than reported, so it silently matches nothing — which is why 122 stores
// it in a JSON column and refuses the write instead.
type ServiceGroup struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Services    string    `json:"services"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
