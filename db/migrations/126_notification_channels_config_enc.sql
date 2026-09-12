-- Encrypt notification-channel config at rest, and stop serving it.
--
-- 120's header named this and named its own deadline: "SECRETS LIVE IN THIS
-- COLUMN AND ARE STORED IN PLAINTEXT ... sso_providers.client_secret (103) is
-- AES-256-GCM encrypted with env.CryptoKey. The two should agree. ... there are
-- zero notification channels on the live instance today, so the cheapest moment
-- to do it is before anyone creates one."
--
-- That is still true — GET /v1/notification-channels returns [] on the live
-- install — so this migration moves ZERO rows. It is the last moment it will be
-- free.
--
-- THE DISCLOSURE THIS CLOSES IS BIGGER THAN THE AT-REST PROBLEM. `config` was
-- also being SERVED: it sits in notificationChannelColumns, was scanned into a
-- plain `json:"config"` field, and GET /v1/notification-channels lives on the
-- ordinary /v1 subrouter — so every Slack webhook, SMTP password and PagerDuty
-- routing key in a zone was readable by any authenticated session or any
-- admin-scope API key. Encrypting the column without changing the wire format
-- would have fixed the smaller half of that.
--
-- WHY A SECOND COLUMN RATHER THAN ENCRYPTING IN PLACE. `config` is JSON NOT
-- NULL; ciphertext is base64 and not JSON, so it cannot live there. config_enc
-- is TEXT, and `config` is relaxed to NULL so new rows can decline to write it.
-- Both statements are idempotent: db/sql.go records a migration only after the
-- whole file succeeds, and MariaDB commits DDL implicitly, so a file that fails
-- part-way is retried FROM THE TOP on the next boot.
--
-- `config` is deliberately KEPT rather than dropped. It costs nothing at zero
-- rows, it is the fallback the scanner reads for any row written between this
-- migration and the code that pairs with it, and dropping a column that held
-- credentials is a thing to do deliberately once, later, not as a side effect.
--
-- ⚠️ CONSEQUENCE FOR ZONES: this makes MON_CRYPTO_KEY genuinely load-bearing on
-- a data plane. It was previously required by env.RequireProductionSecrets in
-- every role while being consumed only by control-plane SSO code — a zone had to
-- carry a 32-byte AES key it never used. It now uses it. The requirement stops
-- being a wart by becoming true, rather than by being relaxed.

ALTER TABLE monitor.notification_channels
    ADD COLUMN IF NOT EXISTS config_enc TEXT NULL AFTER config;

ALTER TABLE monitor.notification_channels
    MODIFY COLUMN config JSON NULL;
