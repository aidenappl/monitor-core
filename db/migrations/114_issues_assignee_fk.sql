-- Referential integrity for issue assignment, WITHOUT making users undeletable.
--
-- 111 left assignee_user_id unconstrained on the reasoning that a foreign key
-- would block deleting an assigned user. That was half right: the trap is
-- RESTRICT, which is the DEFAULT and is what keyring-api shipped — every FK into
-- access_logs was RESTRICT, so any secret that had ever been read could never be
-- removed. ON DELETE SET NULL gets the integrity without the trap: deleting a
-- user unassigns their issues instead of blocking, and no dangling id survives.
--
-- Cross-schema FK (monitor.issues -> monitor_auth.users) is fine: both are
-- InnoDB on the same instance, and BIGINT signed matches on both sides.
--
-- The timeline is what preserves assignment history — an `assigned` entry
-- records actor_label denormalised, so who-assigned-whom outlives the user row
-- even after this nulls the pointer.

-- Dropped-then-added so the file is re-runnable. db.RunMigrations records a file
-- as applied only after a clean Exec, so a run that dies partway through is
-- retried IN FULL on the next boot — and DDL commits implicitly, meaning the
-- half that already succeeded is durable. A bare ADD CONSTRAINT would fail that
-- retry with errno 121 and wedge startup.

ALTER TABLE monitor.issues
    DROP FOREIGN KEY IF EXISTS fk_issues_assignee;

ALTER TABLE monitor.issues
    ADD CONSTRAINT fk_issues_assignee
        FOREIGN KEY (assignee_user_id) REFERENCES monitor_auth.users (id)
        ON DELETE SET NULL;
