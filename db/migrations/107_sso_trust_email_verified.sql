-- Adds a per-provider flag that tells Monitor to treat emails asserted by this
-- SSO provider as verified, even when the provider returns no email_verified
-- claim. Trust is explicit configuration, never a magic slug in adapter code.
-- Defaults OFF, so a newly-added provider must opt in before its emails can
-- satisfy the verified-both-sides auto-link gate.
ALTER TABLE sso_providers
    ADD COLUMN trust_email_verified TINYINT(1) NOT NULL DEFAULT 0 AFTER email_verified_claim;
