CREATE TABLE IF NOT EXISTS monitor.api_keys
(
    id String,
    name String,
    key_hash String,
    key_prefix String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3),
    last_used_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;
