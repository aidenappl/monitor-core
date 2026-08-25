ALTER TABLE monitor.events ADD COLUMN IF NOT EXISTS issue_id String AFTER data;
ALTER TABLE monitor.events ADD INDEX IF NOT EXISTS idx_issue_id issue_id TYPE bloom_filter(0.01) GRANULARITY 4;
