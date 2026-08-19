-- One append-only, polymorphic table backs the whole GitHub-style activity feed:
-- comments, status transitions, regressions, assignment, priority/title edits and
-- PR links all interleave in a single chronological stream.
--
-- This is the shape Sentry's Activity table and GitHub's Timeline Events API both
-- converged on, and it is chosen over separate comments/status_history tables for
-- one concrete reason: the detail view renders as
--   SELECT ... WHERE issue_id = ? ORDER BY created_at
-- on one composite index. Separate tables would force a UNION plus parallel
-- pagination across both for no benefit, since every fact here is append-only.
--
-- actor_label is DENORMALISED on purpose. An API key can be deleted and a user
-- deactivated; the row still has to name who acted. keyring-api demonstrated the
-- failure mode — resolved_key was NULL on 92% of access_logs rows because it had
-- not been denormalised from the start, so nulling the FK would have anonymised
-- almost the entire audit trail. actor_user_id / actor_api_key_id are the weak
-- references; actor_label is the durable one and is NOT NULL.
--
-- dedupe_key makes agent-authored notes idempotent, after Renovate's
-- ensureComment: an agent retrying a task, or revisiting an issue across
-- sessions, updates its note in place instead of leaving five copies. The unique
-- key is what enforces it. Rows without a dedupe_key hold NULL, and MySQL treats
-- each NULL as distinct, so ordinary comments are unaffected.

CREATE TABLE IF NOT EXISTS monitor.issue_timeline (
    id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    issue_id        CHAR(36)     NOT NULL,
    type            ENUM(
                        'comment',
                        'status_changed',
                        'regressed',
                        'assigned',
                        'unassigned',
                        'priority_changed',
                        'title_changed',
                        'pr_linked',
                        'pr_unlinked',
                        'pr_merged',
                        'pr_closed',
                        'pr_reopened'
                    )            NOT NULL,
    actor_kind      ENUM('user','api_key','system') NOT NULL,
    actor_user_id   BIGINT       NULL,
    actor_api_key_id VARCHAR(64) NULL,
    actor_label     VARCHAR(255) NOT NULL,
    body            TEXT         NULL,
    metadata        JSON         NULL,
    dedupe_key      VARCHAR(255) NULL,
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    edited_at       DATETIME(3)  NULL,
    deleted_at      DATETIME(3)  NULL,
    UNIQUE KEY uq_issue_timeline_dedupe (issue_id, dedupe_key),
    KEY idx_issue_timeline_issue_created (issue_id, created_at),
    KEY idx_issue_timeline_issue_type (issue_id, type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
