-- Create the monitor database
CREATE DATABASE IF NOT EXISTS monitor;

-- Create the events table
CREATE TABLE IF NOT EXISTS monitor.events
(
    -- Timestamp of the event
    timestamp DateTime64(3),
    
    -- Service name that generated the event
    service LowCardinality(String),
    
    -- Job identifier (groups related requests within a service)
    job_id String,
    
    -- Request identifier (unique per incoming request)
    request_id String,
    
    -- Trace identifier (spans across services)
    trace_id String,
    
    -- Event name/type
    name LowCardinality(String),
    
    -- Additional event data as JSON string
    data String,
    
    -- Insertion time (for debugging/auditing)
    _inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service, timestamp, trace_id, request_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Create an index for faster trace lookups
ALTER TABLE monitor.events
    ADD INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1;

-- Create an index for faster request lookups
ALTER TABLE monitor.events
    ADD INDEX idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 1;

-- Create an index for faster job lookups
ALTER TABLE monitor.events
    ADD INDEX idx_job_id job_id TYPE bloom_filter(0.01) GRANULARITY 1;

-- Create an index for event name lookups
ALTER TABLE monitor.events
    ADD INDEX idx_name name TYPE bloom_filter(0.01) GRANULARITY 1;
