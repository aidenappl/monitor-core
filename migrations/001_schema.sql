CREATE DATABASE IF NOT EXISTS monitor;

CREATE TABLE IF NOT EXISTS monitor.events
(
    timestamp DateTime64(3, 'UTC'),
    service LowCardinality(String),
    env LowCardinality(String),
    job_id String,
    request_id String,
    trace_id String,
    name LowCardinality(String),
    level LowCardinality(String),
    data String,
    _inserted_at DateTime64(3, 'UTC') DEFAULT now64(3),

    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_job_id job_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_name name TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, service, trace_id, request_id)
TTL toDate(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
