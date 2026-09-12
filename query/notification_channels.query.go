package query

import (
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/tools"
	"github.com/google/uuid"
)

var validNotificationChannelTypes = map[string]bool{
	"webhook": true, "slack": true, "email": true, "pagerduty": true,
}

const notificationChannelsTable = "monitor.notification_channels"

// notificationChannelsProjectColumn is the one string every scoped read of this
// table binds against. Named once because it appears in three builders.
const notificationChannelsProjectColumn = "monitor.notification_channels.project"

var notificationChannelColumns = []string{
	"monitor.notification_channels.id",
	"monitor.notification_channels.project",
	"monitor.notification_channels.name",
	"monitor.notification_channels.type",
	"monitor.notification_channels.config",
	"monitor.notification_channels.config_enc",
	"monitor.notification_channels.created_at",
}

type notificationChannelScanner interface {
	Scan(dest ...interface{}) error
}

// scanNotificationChannel reads one row and resolves its config.
//
// BOTH columns are read because a row can legitimately carry either. Since
// migration 126 every write goes to config_enc (AES-256-GCM, base64); `config`
// is the legacy plaintext column, kept NULLable so it can stay empty. Preferring
// the ciphertext and falling back to the plaintext means this works during the
// window where the migration has run and an older binary is still writing — the
// deploy-ordering hazard that exists whenever a zone lags the control plane.
//
// A DECRYPT FAILURE IS AN ERROR, NOT AN EMPTY CONFIG. Returning "" would produce
// a channel that looks saved, renders in the list, and silently never delivers —
// which is precisely the failure mode 120's header describes for an unparseable
// config. The likeliest cause is the wrong MON_CRYPTO_KEY, and that is worth a
// loud failure on read rather than a mystery at send time.
func scanNotificationChannel(row notificationChannelScanner) (*structs.NotificationChannel, error) {
	var c structs.NotificationChannel
	var legacy sql.NullString
	var encrypted sql.NullString

	if err := row.Scan(&c.ID, &c.Project, &c.Name, &c.Type, &legacy, &encrypted, &c.CreatedAt); err != nil {
		return nil, err
	}

	switch {
	case encrypted.Valid && encrypted.String != "":
		plain, err := tools.Decrypt(encrypted.String)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt config for notification channel %s (is MON_CRYPTO_KEY the key it was written with?): %w", c.ID, err)
		}
		c.Config = plain
	case legacy.Valid:
		c.Config = legacy.String
	}

	c.ConfigSummary = structs.SummariseChannelConfig(c.Type, c.Config)
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
//
// The project is a leading parameter rather than a request field, for the reason
// CreateAlertRule gives: which tenant owns a destination is decided by the
// credential, never by the body. A caller that could choose it could file a
// PagerDuty routing key into another tenant's channel list.
func CreateNotificationChannel(engine db.Queryable, project string, req CreateNotificationChannelRequest) (*structs.NotificationChannel, error) {
	// Checked first because the column is NOT NULL with no default (migration
	// 128): an empty project reaches MariaDB as errno 1364 naming the column,
	// where this names the request that had no tenant.
	if project == "" {
		return nil, ErrNoAlertingProject
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if !validNotificationChannelTypes[req.Type] {
		return nil, fmt.Errorf("invalid type: %s (must be webhook, slack, email, or pagerduty)", req.Type)
	}

	ch := structs.NotificationChannel{
		ID:        uuid.New().String(),
		Project:   project,
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

	// Encrypted BEFORE the insert and written to config_enc; `config` is left
	// NULL. The plaintext exists only in ch.Config, which is json:"-" and
	// therefore cannot be serialised back to the caller that just sent it.
	encrypted, err := tools.Encrypt(ch.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt notification channel config: %w", err)
	}
	ch.ConfigSummary = structs.SummariseChannelConfig(ch.Type, ch.Config)

	qStr, args, err := sq.Insert(notificationChannelsTable).
		Columns("id", "project", "name", "type", "config", "config_enc", "created_at").
		Values(ch.ID, ch.Project, ch.Name, ch.Type, nil, encrypted, ch.CreatedAt).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	if _, err := engine.Exec(qStr, args...); err != nil {
		return nil, fmt.Errorf("failed to insert notification channel: %w", err)
	}
	return &ch, nil
}

// ListNotificationChannels returns one project's channels, newest first.
// Unpaginated for the reason ListAlertRules gives.
//
// There is deliberately NO unscoped variant. Nothing in this service needs every
// tenant's destinations at once — the router loads channels one id at a time,
// within a rule's project — and an unscoped list here would put every zone
// tenant's PagerDuty and Slack destinations behind one endpoint.
func ListNotificationChannels(engine db.Queryable, project string) ([]structs.NotificationChannel, error) {
	q := sq.Select(notificationChannelColumns...).From(notificationChannelsTable).
		OrderBy("monitor.notification_channels.created_at DESC", "monitor.notification_channels.id ASC")

	q, err := scopeAlerting(q, notificationChannelsProjectColumn, project)
	if err != nil {
		return nil, err
	}

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

// GetNotificationChannel returns one channel by id within a project, or
// (nil, nil) when there is none. alerts/router.go calls this per matched channel
// on every routed alert, with the RULE's project.
//
// The project is part of the lookup, and here that is not only a read boundary:
// this row carries decrypted credentials (structs.NotificationChannel.Config),
// and it is what POST /v1/notification-channels/{id}/test sends through. An
// unscoped lookup would let one tenant fire a test notification into another
// tenant's PagerDuty service by id alone.
func GetNotificationChannel(engine db.Queryable, project, id string) (*structs.NotificationChannel, error) {
	q := sq.Select(notificationChannelColumns...).From(notificationChannelsTable).
		Where(sq.Eq{"monitor.notification_channels.id": id}).Limit(1)

	q, err := scopeAlerting(q, notificationChannelsProjectColumn, project)
	if err != nil {
		return nil, err
	}

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
// The project is in the DELETE's own WHERE, with no read in front of it — the
// reason DeleteAPIKey records: read-then-write is not atomic and a future caller
// may skip the read.
func DeleteNotificationChannel(engine db.Queryable, project, id string) (bool, error) {
	if project == "" {
		return false, ErrNoAlertingProject
	}

	qStr, args, err := sq.Delete(notificationChannelsTable).
		Where(sq.Eq{"id": id, "project": project}).ToSql()
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
