-- Linked GitHub PRs (and issues/commits) as queryable current state, alongside
-- the timeline entries that record the linking events.
--
-- Both exist deliberately. The timeline answers "what happened to this issue and
-- when"; this table answers "which issues have an open PR" — a list-view filter
-- that would otherwise mean scanning JSON metadata out of the timeline.
--
-- Link state is a cache of GitHub's, refreshed by the webhook receiver rather
-- than polled. state/merged/title/author are nullable because a link is stored
-- even when the GitHub call fails: an unreachable GitHub degrades the chip to a
-- bare URL, it never fails the write. state_synced_at records when the cache was
-- last known good.
--
-- GitHub is a field on the issue, not the system of record. Every observability
-- platform surveyed (Sentry, GlitchTip, Highlight.io, Bugsink) keeps issue state
-- locally and treats SCM linkage as a reference; delegating status to GitHub
-- inverts a pattern the whole ecosystem converged against.
--
-- No FK to monitor.issues: see the note in 111 about RESTRICT constraints. A link
-- is cleaned up with its issue by the query layer, not by the engine.

CREATE TABLE IF NOT EXISTS monitor.issue_links (
    id              BIGINT                                 NOT NULL AUTO_INCREMENT PRIMARY KEY,
    issue_id        CHAR(36)                               NOT NULL,
    provider        ENUM('github')                         NOT NULL DEFAULT 'github',
    kind            ENUM('pull_request','issue','commit')  NOT NULL,
    url             VARCHAR(1000)                          NOT NULL,
    owner           VARCHAR(255)                           NULL,
    repo            VARCHAR(255)                           NULL,
    number          INT                                    NULL,
    title           VARCHAR(500)                           NULL,
    state           VARCHAR(32)                            NULL,
    merged          TINYINT(1)                             NULL,
    author          VARCHAR(255)                           NULL,
    state_synced_at DATETIME(3)                            NULL,
    linked_by_label VARCHAR(255)                           NOT NULL,
    inserted_at     DATETIME(3)                            NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3)                            NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_issue_links_issue_url (issue_id, url(255)),
    KEY idx_issue_links_issue (issue_id),
    KEY idx_issue_links_lookup (provider, owner, repo, number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
