package structs

import "time"

// NotificationPolicy is one routing rule: match a firing alert on its priority,
// service, service group, env, status or rule name, and send it to a set of
// channels. Lives in MariaDB (monitor.notification_policies).
//
// It was `alerts.NotificationPolicy` and moved here with the table (migration
// 121), for the import-cycle reason AlertRule.struct.go records. The json tags
// are unchanged.
//
// POSITION IS SIGNED, AND THAT IS LOAD-BEARING. It was uint32 while the row
// lived in a ClickHouse UInt32; migration 121 puts a UNIQUE key on the column,
// and query.ReorderNotificationPolicies makes an arbitrary permutation possible
// under that key by negating every position in one statement before assigning
// the new 1..N. The negative half of the range is the staging space that makes
// the reorder atomic, so this must be a signed type and the column must have no
// CHECK forbidding it. A value read from the database is always >= 1 — the
// negatives exist only inside that transaction and no other session can see
// them.
type NotificationPolicy struct {
	ID string `json:"id"`
	// Project is the tenant this routing rule belongs to (migration 129), and of
	// the seven tables that gained a project it is the one where its absence did
	// active harm rather than merely leaking a list: `position` carried a
	// ZONE-GLOBAL unique key, so the routing table was one ordered list shared by
	// every project and creating a policy in one renumbered another's routing
	// order. The key is (project, position) now, so each project's ordering is
	// its own.
	Project     string `json:"project"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Position    int    `json:"position"`
	// Matchers and ChannelIDs are raw JSON text (an object and an array
	// respectively), carried as strings because that is the wire contract
	// monitor-web reads and writes. Both are stored in JSON columns, so a
	// malformed value is now refused at write time rather than silently making
	// the policy match nothing at route time.
	Matchers              string    `json:"matchers"`
	ChannelIDs            string    `json:"channel_ids"`
	ContinueMatching      bool      `json:"continue_matching"`
	RepeatIntervalSeconds uint32    `json:"repeat_interval_seconds"`
	Enabled               bool      `json:"enabled"`
	IsDefault             bool      `json:"is_default"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}
