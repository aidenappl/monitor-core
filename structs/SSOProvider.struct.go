package structs

import "time"

// SSOProvider is one configured identity provider row (sso_providers). A row is
// enough to drive a login end-to-end: `kind='oidc'` providers are resolved via
// OIDC discovery from IssuerURL; `kind='oauth2'` providers use the explicit
// Authorize/Token/UserInfo URLs. The client secret
// is never stored inline — it is referenced by a Keyring name (ClientSecretRef)
// or held AES-256-GCM encrypted (ClientSecretEnc); resolution prefers the ref.
// Lives in MariaDB (sso_providers).
type SSOProvider struct {
	ID                 int64   `json:"id"`
	Slug               string  `json:"slug"`
	DisplayName        string  `json:"display_name"`
	Kind               string  `json:"kind"` // "oidc" | "oauth2"
	IssuerURL          *string `json:"issuer_url,omitempty"`
	AuthorizeURL       *string `json:"authorize_url,omitempty"`
	TokenURL           *string `json:"token_url,omitempty"`
	UserInfoURL        *string `json:"userinfo_url,omitempty"`
	JWKSURL            *string `json:"jwks_url,omitempty"`
	IntrospectURL      *string `json:"introspect_url,omitempty"`
	ClientID           *string `json:"client_id,omitempty"`
	ClientSecretRef    *string `json:"-"`
	ClientSecretEnc    *string `json:"-"`
	Scopes             string  `json:"scopes"`
	EmailClaim         string  `json:"email_claim"`
	EmailVerifiedClaim string  `json:"email_verified_claim"`
	SubjectClaim       string  `json:"subject_claim"`
	// TrustEmailVerified treats emails asserted by this provider as verified even
	// when it returns no email_verified claim. Explicit config, not a
	// hardcoded provider name — defaults false so a new provider must opt in.
	TrustEmailVerified bool    `json:"trust_email_verified"`
	ButtonLabel        *string `json:"button_label,omitempty"`

	// ── Branding ────────────────────────────────────────────────────────────

	// DisplayIcon is a BUNDLED icon slug the frontend ships an asset for —
	// "google", "github", "microsoft", "forta", "okta", "gitlab", "apple".
	//
	// Deliberately a short identifier rather than a path: a path would be an
	// administrator-controlled value that the login page turns into a URL.
	DisplayIcon *string `json:"display_icon,omitempty"`

	// IconURL is the administrator's original third-party URL, kept so it can be
	// displayed in the admin form and re-fetched.
	//
	// ⚠️ NEVER RENDER THIS ON THE LOGIN PAGE. Hot-linking leaks every
	// unauthenticated visitor's IP, User-Agent and Referer to a third party, makes
	// the login page depend on their uptime, and lets them swap the image after an
	// administrator reviewed it. The server fetches it once at save time and
	// serves the cached bytes from its own origin.
	IconURL *string `json:"icon_url,omitempty"`

	// IconCacheType and IconCachedAt describe the cached asset. The BYTES are
	// deliberately NOT on this struct: it is returned by the admin API, and a
	// base64 blob in every provider listing is bandwidth nobody asked for. They
	// are read only by the icon-serving handler.
	IconCacheType *string    `json:"icon_cache_type,omitempty"`
	IconCachedAt  *time.Time `json:"icon_cached_at,omitempty"`

	// IconError is why the last icon fetch failed, surfaced so an administrator
	// can fix it. A failed fetch never blocks saving the provider.
	IconError *string `json:"icon_error,omitempty"`

	// ButtonColor and ButtonTextColor are #rrggbb.
	//
	// ⚠️ Validated on WRITE and again on RENDER, and passed to the browser through
	// a CSS custom property rather than string-built into a stylesheet. Validating
	// only on write means a row predating the validation becomes a CSS injection.
	ButtonColor     *string `json:"button_color,omitempty"`
	ButtonTextColor *string `json:"button_text_color,omitempty"`

	// SortOrder controls button order on the login page; ties break on slug.
	SortOrder     int       `json:"sort_order"`
	AllowAutoLink bool      `json:"allow_auto_link"`
	AutoProvision bool      `json:"auto_provision"`
	Enabled       bool      `json:"enabled"`
	InsertedAt    time.Time `json:"inserted_at"`
}

// HasSecret reports whether a client secret is configured (by ref or encrypted).
// The raw/decrypted secret is never exposed in API responses — only this flag.
func (p *SSOProvider) HasSecret() bool {
	return (p.ClientSecretRef != nil && *p.ClientSecretRef != "") ||
		(p.ClientSecretEnc != nil && *p.ClientSecretEnc != "")
}
