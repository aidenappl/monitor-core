package alerts

// WHAT MOVED OUT OF THIS FILE.
//
// notification_policies moved from a ClickHouse ReplacingMergeTree to MariaDB in
// migration 121, and everything that used to be here — InitPolicies,
// seedDefaultPolicies, CreatePolicy, ListPolicies, GetPolicy, UpdatePolicy,
// DeletePolicy, ReorderPolicies and getNextPosition — is now in
// query/notification_policies.query.go. The type is structs.NotificationPolicy.
//
// The two defects alerts/AGENTS.md carried against this file are closed by that
// move rather than patched here:
//
//   * getNextPosition was a racy `max(position) + 1` with nothing behind it. The
//     column now has a UNIQUE key and the allocation happens under a lock inside
//     the insert's own transaction.
//   * ReorderPolicies rewrote rows one at a time with no transaction, because
//     ClickHouse has none, so a mid-loop failure left the ordering half
//     rewritten. It is now one transaction, and the unique key means a partial
//     rewrite cannot even be expressed.
//
// PolicyMatchers stays. It is not a persisted shape — it is the decoded form of
// the `matchers` JSON blob, and matchPolicy in router.go is the only thing that
// reads it — so it belongs with the routing logic rather than in structs/ beside
// the row it is stored inside.

// PolicyMatchers is the decoded `matchers` object of a notification policy. Every
// field is an AND: an empty field matches anything, a set one must match. See
// matchPolicy in router.go.
type PolicyMatchers struct {
	Priority     string   `json:"priority,omitempty"`
	Services     []string `json:"services,omitempty"`
	ServiceGroup string   `json:"service_group,omitempty"`
	Status       string   `json:"status,omitempty"`
	Env          string   `json:"env,omitempty"`
	RuleName     string   `json:"rule_name,omitempty"`
}
