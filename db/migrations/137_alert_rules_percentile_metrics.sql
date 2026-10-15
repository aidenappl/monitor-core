-- Let an alert rule aggregate by percentile: p50, p95, p99.
--
-- Latency alerts are written in percentiles ("p95 over 800ms"), and monitor-web's
-- rule form offered all three long before anything accepted them: the query layer
-- refused them with a 400 the form could not explain, and this ENUM would have
-- refused them after it. alerts/evaluator.go now builds quantile(...) for them.
--
-- Widening an ENUM by appending members is metadata-only in MariaDB and leaves
-- every existing value's stored index unchanged. Re-running it is a no-op, so
-- the file is idempotent like the rest of db/migrations.

ALTER TABLE monitor.alert_rules
    MODIFY COLUMN metric ENUM('count','sum','avg','min','max','p50','p95','p99') NOT NULL DEFAULT 'count';
