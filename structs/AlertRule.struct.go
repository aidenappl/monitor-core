package structs

import "time"

// AlertRule is one alerting rule: what to aggregate, over what window, and the
// threshold that makes it fire. Lives in MariaDB (monitor.alert_rules).
//
// It was `alerts.Rule` and moved here with the table, in the same change that
// moved the row out of ClickHouse (migration 119). The move is not cosmetic —
// the alerts package now calls query/, and query/ needs this type, so leaving it
// in alerts/ would be an import cycle. Every json tag is byte-for-byte what the
// ClickHouse-backed version emitted; monitor-web reads these names.
//
// THE ENUM-SHAPED FIELDS ARE PLAIN STRINGS, NOT TYPED ENUMS, and that is a
// deliberate hold rather than an oversight. Type, Priority, Metric and Condition
// each have a closed value set, which migration 119 now enforces as a real
// MariaDB ENUM, and the house pattern would be a `type AlertRuleType string` with
// an IsValid method. Introducing them here means touching evaluator.go's type
// switch, CheckCondition's signature, router.go, notifier.go and their tests — a
// second refactor, orthogonal to moving the store, inside a change whose entire
// risk budget is "the row moved and nothing else did". The validation maps in
// query/alert_rules.query.go are the pre-flight that keeps producing a 400
// naming the field instead of errno 1265; the database is now the backstop
// underneath them. Do the typed-enum pass on its own.
type AlertRule struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	Priority    string `json:"priority"`
	// QueryFilters and NotificationChannelIDs are raw JSON text, carried as
	// strings rather than as decoded Go values because that is what the wire
	// contract has always been: monitor-web sends and receives them as strings.
	// They are stored in JSON columns, so MariaDB now rejects a malformed one —
	// the query layer checks them first so the caller gets a readable error.
	QueryFilters           string    `json:"query_filters"`
	Metric                 string    `json:"metric"`
	Field                  string    `json:"field"`
	Condition              string    `json:"condition"`
	Threshold              float64   `json:"threshold"`
	EvaluationIntervalSecs uint32    `json:"evaluation_interval_seconds"`
	ForSeconds             uint32    `json:"for_seconds"`
	CooldownSeconds        uint32    `json:"cooldown_seconds"`
	NotificationChannelIDs string    `json:"notification_channel_ids"`
	Enabled                bool      `json:"enabled"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}
