package db

import (
	"database/sql/driver"
	"time"
)

// Millis binds a time to a ClickHouse query at MILLISECOND precision.
//
// ⚠️ WHY THIS EXISTS: clickhouse-go formats every positional time.Time argument
// at SECOND precision — bindPositional calls format(tz, Seconds, v), whatever the
// column's type. `events.timestamp` is DateTime64(3, 'UTC'), so a bound such as
// `timestamp <= ?` with 12:21:48.730 compared against 12:21:48 and dropped the
// event it was built to find. A one-millisecond lookup window (an event deep
// link) matched nothing, a `before` cursor skipped the rest of its second, and
// every range edge was cut to the second.
//
// A driver.Valuer is unwrapped before formatting, so returning the time as a
// string makes it a quoted literal that ClickHouse parses into the column's own
// DateTime64(3, 'UTC'). Always UTC: the column's time zone, so the literal means
// the same instant. Wrap EVERY time bound compared against `timestamp`.
type Millis time.Time

// Value renders the time as 'YYYY-MM-DD hh:mm:ss.mmm' in UTC.
func (m Millis) Value() (driver.Value, error) {
	return time.Time(m).UTC().Format("2006-01-02 15:04:05.000"), nil
}
