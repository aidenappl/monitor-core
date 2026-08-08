package structs

import "time"

// SSOSession caches the IdP tokens for an SSO-backed login so the auth
// middleware can periodically re-check ("checkpoint") that the upstream grant
// is still live. Tokens are stored AES-256-GCM encrypted — callers MUST encrypt
// before persisting and decrypt after reading. One row per user (the PK).
// Provider names which sso_providers row minted the session so the checkpoint
// knows which IdP to introspect against. Lives in MariaDB (sso_sessions).
type SSOSession struct {
	UserID   int64  `json:"user_id"`
	Provider string `json:"provider"`

	// Subject and SID are how a back-channel logout finds this session: a logout
	// token names one or the other. Both are nullable and both nulls are NORMAL —
	// Subject is null on rows written before migration 109, and SID is null for
	// those plus any provider that issues no `sid`, which is conformant.
	//
	// ⚠️ Neither can be backfilled. `sid` exists only inside the id_token of the
	// login that created the row, and that token is long gone. A session stored
	// without one is unreachable by a session-scoped logout for its whole life.
	Subject *string `json:"-"`
	SID     *string `json:"-"`

	AccessToken   string    `json:"-"`
	RefreshToken  *string   `json:"-"`
	LastCheckedAt time.Time `json:"last_checked_at"`
	InsertedAt    time.Time `json:"inserted_at"`
}
