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
	TrustEmailVerified bool      `json:"trust_email_verified"`
	ButtonLabel        *string   `json:"button_label,omitempty"`
	AllowAutoLink      bool      `json:"allow_auto_link"`
	AutoProvision      bool      `json:"auto_provision"`
	Enabled            bool      `json:"enabled"`
	InsertedAt         time.Time `json:"inserted_at"`
}

// HasSecret reports whether a client secret is configured (by ref or encrypted).
// The raw/decrypted secret is never exposed in API responses — only this flag.
func (p *SSOProvider) HasSecret() bool {
	return (p.ClientSecretRef != nil && *p.ClientSecretRef != "") ||
		(p.ClientSecretEnc != nil && *p.ClientSecretEnc != "")
}
