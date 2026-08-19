-- The issue row moves from ClickHouse to MariaDB and becomes the source of truth.
--
-- It previously lived in a ClickHouse ReplacingMergeTree created ad hoc inside
-- issues.Init. That engine cannot enforce uniqueness and deduplicates only when
-- a background merge happens to run, which produced two documented defects: four
-- rows for a single fingerprint in production (fixed by deriving the id from the
-- fingerprint), and occurrence_count drift under concurrent workers, which was
-- only ever mitigated process-locally by a 64-way mutex shard.
--
-- Owning the row relationally fixes the counter outright — the upsert in
-- query/issues.query.go is a single atomic INSERT ... ON DUPLICATE KEY UPDATE —
-- and lets the list view filter, sort, paginate and count in one indexed query.
-- Sorting is the deciding constraint: ordering by last_seen in ClickHouse while
-- filtering status in MariaDB is a cross-store sort that cannot paginate.
--
-- `id` is NOT auto-increment. It is the deterministic UUIDv5 minted by
-- issues.issueIDFor(fingerprint) under a fixed namespace, so concurrent creators
-- converge on one row and the ClickHouse backfill copies across with no id
-- remapping. That namespace must never change.
--
-- status is one enum, not two. Recurrence only ever transitions OUT of
-- 'resolved' (see UpsertIssueOccurrence), so an agent holding 'in_progress'
-- through a recurrence is never clobbered. 'unresolved' is the default and is
-- also the backlog — an untriaged issue and a triaged-but-not-started one are
-- operationally the same thing.
--
-- assignee_user_id gains a foreign key in 114, with ON DELETE SET NULL rather
-- than the default RESTRICT — deleting a user unassigns their issues instead of
-- being blocked by them. It is split out only so this file stays a pure CREATE.

CREATE TABLE IF NOT EXISTS monitor.issues (
    id               CHAR(36)                                                        NOT NULL PRIMARY KEY,
    fingerprint      CHAR(64)                                                        NOT NULL,
    service          VARCHAR(255)                                                    NOT NULL,
    name             VARCHAR(255)                                                    NOT NULL,
    message          TEXT                                                            NULL,
    path             VARCHAR(1000)                                                   NULL,
    status           ENUM('unresolved','in_progress','resolved','ignored')           NOT NULL DEFAULT 'unresolved',
    priority         ENUM('low','medium','high','critical')                          NULL,
    title            VARCHAR(500)                                                    NULL,
    assignee_user_id BIGINT                                                          NULL,
    occurrence_count BIGINT UNSIGNED                                                 NOT NULL DEFAULT 0,
    regression_count INT                                                             NOT NULL DEFAULT 0,
    first_seen       DATETIME(3)                                                     NULL,
    last_seen        DATETIME(3)                                                     NULL,
    resolved_at      DATETIME(3)                                                     NULL,
    regressed_at     DATETIME(3)                                                     NULL,
    inserted_at      DATETIME(3)                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at       DATETIME(3)                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_issues_fingerprint (fingerprint),
    KEY idx_issues_status_last_seen (status, last_seen),
    KEY idx_issues_service_last_seen (service, last_seen),
    KEY idx_issues_last_seen (last_seen),
    KEY idx_issues_first_seen (first_seen),
    KEY idx_issues_occurrence_count (occurrence_count),
    KEY idx_issues_assignee (assignee_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
