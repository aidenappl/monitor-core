-- Adds a per-provider flag that tells Monitor to treat emails asserted by this
-- SSO provider as verified, even when the provider returns no email_verified
-- claim. Trust is explicit configuration, never a magic slug in adapter code.
-- Defaults OFF, so a newly-added provider must opt in before its emails can
-- satisfy the verified-both-sides auto-link gate.
--
-- IF NOT EXISTS is required, not decorative: db.RunMigrations records a file as
-- applied only after a clean Exec, and DDL commits implicitly, so a run that dies
-- partway is retried IN FULL on the next boot. A bare ADD COLUMN fails that retry
-- with errno 1060 and wedges startup. 108 and 109 already guard every ALTER this
-- way; this file was the sole exception.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS trust_email_verified TINYINT(1) NOT NULL DEFAULT 0 AFTER email_verified_claim;
