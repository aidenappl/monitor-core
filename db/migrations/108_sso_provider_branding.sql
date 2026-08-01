-- 108_sso_provider_branding.sql
--
-- Per-provider branding for the login page: which icon to show, what colour the
-- button is, and what order providers appear in.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THE ICON IS CACHED AS BYTES RATHER THAN RENDERED FROM icon_url
--
-- The obvious design is to store the administrator's URL and put it in an <img>
-- on the login page. That is wrong in three ways at once:
--
--   1. It leaks every UNAUTHENTICATED visitor's IP, User-Agent and Referer to a
--      third party — on the one page you can be certain every user loads.
--   2. It makes your login page's availability depend on someone else's uptime
--      and TLS certificate.
--   3. It lets whoever controls that URL change the image AFTER an administrator
--      reviewed it.
--
-- So the server fetches it ONCE at save time (sso.FetchIcon in go-forta/sso,
-- which is where the SSRF and SVG defences live), re-encodes it, and stores the
-- bytes here. The login page is then served from this application's own origin
-- via GET /auth/sso/icon/{slug}.
--
-- icon_url is retained only so an administrator can see what they typed and
-- re-fetch it. It is NEVER what the login page renders.
-- ─────────────────────────────────────────────────────────────────────────────

-- display_icon is a BUNDLED slug — 'google', 'github', 'microsoft', 'forta',
-- 'okta', 'gitlab', 'apple' — resolved to an asset the frontend ships. It is
-- deliberately a short identifier rather than a path: a path would be a value an
-- administrator controls that the login page turns into a URL.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS display_icon VARCHAR(64) NULL;

-- icon_url is the administrator's original third-party URL, kept for display and
-- re-fetch. ⚠️ NEVER render this. See the note above.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS icon_url TEXT NULL;

-- The cached asset. MEDIUMBLOB because the fetch caps at 256 KB and re-encodes
-- to PNG; BLOB (64 KB) would silently truncate a legitimate logo.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS icon_cache_data MEDIUMBLOB NULL;
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS icon_cache_type VARCHAR(64) NULL;
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS icon_cached_at DATETIME NULL;

-- icon_error records why the last fetch failed, so the admin UI can explain it.
--
-- ⚠️ A FAILED FETCH MUST NOT BLOCK SAVING THE PROVIDER. An administrator fixing a
-- logo URL must not be prevented from correcting an issuer URL in the same form.
-- The provider saves, this column records the reason, the icon stays null, and
-- the login page falls back to a text button.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS icon_error VARCHAR(255) NULL;

-- Button colours, stored as #rrggbb.
--
-- ⚠️ VALIDATED ^#[0-9a-fA-F]{6}$ ON WRITE **AND** ON RENDER, and passed to the
-- browser through a CSS custom property — never string-built into a stylesheet.
-- A colour is an administrator-supplied value that ends up inside CSS; validating
-- only on write means any row that predates the validation, or arrives by another
-- path, becomes a CSS injection. CHAR(7) makes an over-long value fail at the
-- column rather than reaching the page.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS button_color CHAR(7) NULL;
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS button_text_color CHAR(7) NULL;

-- sort_order controls the order buttons appear in. Ties break on slug so the
-- order is stable rather than whatever the storage engine returns.
ALTER TABLE sso_providers
    ADD COLUMN IF NOT EXISTS sort_order INT NOT NULL DEFAULT 0;
