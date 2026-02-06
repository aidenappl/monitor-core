-- Create the monitor database
CREATE DATABASE IF NOT EXISTS monitor;

-- Create the events table
CREATE TABLE IF NOT EXISTS monitor.events
(
    -- Timestamp of the event
    timestamp DateTime64(3, 'UTC'),

    -- Service and environment metadata
    service LowCardinality(String),
    env LowCardinality(String),

    -- Correlation identifiers
    job_id String,
    request_id String,
    trace_id String,

    -- Event metadata
    name LowCardinality(String),
    level LowCardinality(String),

    -- Additional structured event data
    data JSON,

    -- Insertion time (for debugging/auditing)
    _inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, service, trace_id, request_id)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Secondary indexes for fast identifier lookups
ALTER TABLE monitor.events
    ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE monitor.events
    ADD INDEX IF NOT EXISTS idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE monitor.events
    ADD INDEX IF NOT EXISTS idx_job_id job_id TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE monitor.events
    ADD INDEX IF NOT EXISTS idx_name name TYPE bloom_filter(0.01) GRANULARITY 4;
