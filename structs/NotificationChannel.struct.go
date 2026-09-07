package structs

import "time"

// NotificationChannel is one destination a firing alert can be sent to. Lives in
// MariaDB (monitor.notification_channels).
//
// It was `alerts.Channel` and moved here with the table (migration 120), for the
// import-cycle reason AlertRule.struct.go records. The json tags are unchanged.
//
// Config is the per-type payload — webhook URL, Slack webhook, SMTP recipients,
// PagerDuty routing key — carried as raw JSON text because alerts/notifier.go
// Unmarshals a different shape per Type. It is stored UNENCRYPTED, which is what
// the ClickHouse table did too but is worth knowing when reading this type:
// anything secret a channel needs is in that string in plaintext, unlike
// SSOProvider.ClientSecret which is AES-256-GCM encrypted at rest. Migration 120
// records why that has not been changed yet and when it would be cheapest to.
type NotificationChannel struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Config    string    `json:"config"`
	CreatedAt time.Time `json:"created_at"`
}
