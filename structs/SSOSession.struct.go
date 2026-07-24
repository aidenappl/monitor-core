package structs

import "time"

// SSOSession caches the IdP tokens for an SSO-backed login so the auth
// middleware can periodically re-check ("checkpoint") that the upstream grant
// is still live. Tokens are stored AES-256-GCM encrypted — callers MUST encrypt
// before persisting and decrypt after reading. One row per user (the PK).
// Provider names which sso_providers row minted the session so the checkpoint
// knows which IdP to introspect against. Lives in MariaDB (sso_sessions).
type SSOSession struct {
	UserID        int64     `json:"user_id"`
	Provider      string    `json:"provider"`
	AccessToken   string    `json:"-"`
	RefreshToken  *string   `json:"-"`
	LastCheckedAt time.Time `json:"last_checked_at"`
	InsertedAt    time.Time `json:"inserted_at"`
}
