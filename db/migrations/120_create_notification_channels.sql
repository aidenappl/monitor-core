-- Where a firing alert is sent. Moves from a ClickHouse MergeTree ORDER BY (id)
-- created inside alerts.Init; 119 carries the argument for the whole move.
--
-- `config` is the channel's per-type payload — a webhook URL, a Slack webhook, an
-- SMTP recipient list, a PagerDuty routing key — and every branch of
-- alerts/notifier.go json.Unmarshals it before doing anything. A value that is
-- not valid JSON is therefore a channel that can only ever fail at send time,
-- inside a goroutine whose error becomes a log line. JSON rather than TEXT moves
-- that failure to the moment the channel is saved.
--
-- SECRETS LIVE IN THIS COLUMN AND ARE STORED IN PLAINTEXT. That is not new — it
-- is exactly what the ClickHouse table did — but it is worth writing down here
-- rather than leaving it implicit in a column called `config`: a PagerDuty
-- routing key and an SMTP password sit in this table unencrypted, while
-- sso_providers.client_secret (103) is AES-256-GCM encrypted with env.CryptoKey.
-- The two should agree. Encrypting this one means a format change plus a
-- migration of live rows, which is its own change; there are zero notification
-- channels on the live instance today, so the cheapest moment to do it is before
-- anyone creates one.
--
-- UNIQUE (name) for the same reason 119 gives: a channel is chosen by name in
-- the policy editor, and two rows sharing one makes the choice meaningless. Zero
-- rows exist today, so this key cannot abort the backfill.

CREATE TABLE IF NOT EXISTS monitor.notification_channels (
    id         CHAR(36)                                        NOT NULL PRIMARY KEY,
    name       VARCHAR(255)                                    NOT NULL,
    type       ENUM('webhook','slack','email','pagerduty')     NOT NULL,
    config     JSON                                            NOT NULL,
    created_at DATETIME(3)                                     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_notification_channels_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
