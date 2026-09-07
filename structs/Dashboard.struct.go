package structs

import "time"

// Dashboard is a saved dashboard layout. Lives in MariaDB (monitor.dashboards).
//
// It was `dashboards.Dashboard` and moved here with the table (migration 123).
// The json tags are unchanged.
//
// Config is an opaque, client-owned blob. monitor-web writes it and monitor-web
// reads it; this service never parses it, which is why 123 stores it in a
// LONGTEXT rather than the JSON column type the alert-config tables use. Do not
// start interpreting it here without first deciding who owns the format.
type Dashboard struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Config      string    `json:"config"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
