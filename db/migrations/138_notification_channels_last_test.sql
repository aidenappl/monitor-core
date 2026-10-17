-- Remember the last "Send test" result on each notification channel.
--
-- The test endpoint's answer used to live only in monitor-web's React state, so
-- a reload put every channel back to "Untested" — including one that had failed
-- a moment earlier — and the failure itself was a bare 500 with no reason
-- (NOTIFICATIONS-04). These columns hold the last outcome so the list can show
-- it after a reload:
--
--   last_test_at    when the test ran (NULL = never tested)
--   last_test_ok    whether delivery succeeded
--   last_test_error the human-readable reason, built by alerts.DeliveryReason
--                   from the failure's CLASS. It never holds the destination
--                   URL, a routing key or an address: the config is the
--                   credential, and this column is served to every reader.
--
-- All three are NULLable additions, so every existing row reads as "never
-- tested". IF NOT EXISTS keeps the file re-runnable.

ALTER TABLE monitor.notification_channels
    ADD COLUMN IF NOT EXISTS last_test_at    DATETIME(3)  NULL,
    ADD COLUMN IF NOT EXISTS last_test_ok    TINYINT(1)   NULL,
    ADD COLUMN IF NOT EXISTS last_test_error VARCHAR(500) NULL;
