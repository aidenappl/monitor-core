-- Adds a per-provider flag that tells Monitor to treat emails asserted by this
-- SSO provider as verified, even when the provider returns no email_verified
-- claim. This replaces a hardcoded slug=="forta" override in the Forta adapter:
-- trust is now explicit configuration, not a magic string. Defaults OFF, so a
-- newly-added provider must opt in before its emails can satisfy the
-- verified-both-sides auto-link gate. The Forta seed row sets this to 1 (Forta
-- emails are verified upstream).
ALTER TABLE sso_providers
    ADD COLUMN trust_email_verified TINYINT(1) NOT NULL DEFAULT 0 AFTER email_verified_claim;
