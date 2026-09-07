package query

import (
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/google/uuid"
)

var validNotificationChannelTypes = map[string]bool{
	"webhook": true, "slack": true, "email": true, "pagerduty": true,
}

const notificationChannelsTable = "monitor.notification_channels"

var notificationChannelColumns = []string{
	"monitor.notification_channels.id",
	"monitor.notification_channels.name",
	"monitor.notification_channels.type",
	"monitor.notification_channels.config",
	"monitor.notification_channels.created_at",
}

type notificationChannelScanner interface {
	Scan(dest ...interface{}) error
}

func scanNotificationChannel(row notificationChannelScanner) (*structs.NotificationChannel, error) {
	var c structs.NotificationChannel
	if err := row.Scan(&c.ID, &c.Name, &c.Type, &c.Config, &c.CreatedAt); err != nil {
		return nil, err
	}
	return &c, nil
}

// CreateNotificationChannelRequest is the POST /v1/notification-channels body.
type CreateNotificationChannelRequest struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Config string `json:"config"`
}

// CreateNotificationChannel validates and inserts one channel.
//
// `config` is defaulted to an empty object rather than left as the empty string.
// The ClickHouse column accepted "" and every notifier branch then failed to
// Unmarshal it at send time, inside a goroutine whose error is only a log line —
// so the channel looked saved and silently never delivered. Migration 120's JSON
// column would reject "" outright; defaulting keeps the create working while
// making the stored value one the notifier can actually parse.
func CreateNotificationChannel(engine db.Queryable, req CreateNotificationChannelRequest) (*structs.NotificationChannel, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if !validNotificationChannelTypes[req.Type] {
		return nil, fmt.Errorf("invalid type: %s (must be webhook, slack, email, or pagerduty)", req.Type)
	}

	ch := structs.NotificationChannel{
		ID:        uuid.New().String(),
		Name:      req.Name,
		Type:      req.Type,
		Config:    req.Config,
		CreatedAt: time.Now().UTC(),
	}
	if ch.Config == "" {
		ch.Config = "{}"
	}
	if err := requireJSONText("config", ch.Config); err != nil {
		return nil, err
	}

	qStr, args, err := sq.Insert(notificationChannelsTable).
		Columns("id", "name", "type", "config", "created_at").
		Values(ch.ID, ch.Name, ch.Type, ch.Config, ch.CreatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert notification channel: %w", err)
	}
	return &ch, nil
}

// ListNotificationChannels returns every channel, newest first. Unpaginated for
// the reason ListAlertRules gives.
func ListNotificationChannels(engine db.Queryable) ([]structs.NotificationChannel, error) {
	q := sq.Select(notificationChannelColumns...).From(notificationChannelsTable).
		OrderBy("monitor.notification_channels.created_at DESC", "monitor.notification_channels.id ASC")

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	channels := []structs.NotificationChannel{}
	for rows.Next() {
		ch, err := scanNotificationChannel(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notification channel: %w", err)
		}
		channels = append(channels, *ch)
	}
	return channels, rows.Err()
}

// GetNotificationChannel returns one channel by id, or (nil, nil) when there is
// none. alerts/router.go calls this per matched channel on every routed alert.
func GetNotificationChannel(engine db.Queryable, id string) (*structs.NotificationChannel, error) {
	q := sq.Select(notificationChannelColumns...).From(notificationChannelsTable).
		Where(sq.Eq{"monitor.notification_channels.id": id}).Limit(1)

	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	ch, err := scanNotificationChannel(engine.QueryRow(qStr, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan notification channel: %w", err)
	}
	return ch, nil
}

// DeleteNotificationChannel removes one channel and reports whether it existed.
//
// NOTHING CLEANS UP THE REFERENCES, and that is unchanged from the ClickHouse
// implementation rather than something this move introduced: a deleted channel's
// id stays inside alert_rules.notification_channel_ids and
// notification_policies.channel_ids, both of which are JSON arrays of ids with
// no foreign key behind them. alerts/router.go tolerates it — a channel it
// cannot load is logged and skipped — so the failure is a silently undelivered
// notification. A real FK is not available while those remain JSON arrays; the
// fix is a join table, which is its own change.
func DeleteNotificationChannel(engine db.Queryable, id string) (bool, error) {
	qStr, args, err := sq.Delete(notificationChannelsTable).Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return false, fmt.Errorf("failed to build sql query: %w", err)
	}

	res, err := engine.Exec(qStr, args...)
	if err != nil {
		return false, fmt.Errorf("failed to delete notification channel: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, nil
	}
	return affected > 0, nil
}
