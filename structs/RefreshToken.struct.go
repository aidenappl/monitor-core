package structs

import "time"

// RefreshToken is one row of the rotating-refresh-token store. Only the SHA-256
// hash of the raw token is persisted (BINARY(32)); the raw value lives only in
// the client cookie. Tokens minted from the same login share a FamilyID; on
// rotation the old row's ReplacedBy is set to the new row's id. Presenting an
// already-rotated (ReplacedBy set) or revoked token is treated as reuse and
// revokes the whole family. Lives in MariaDB (refresh_tokens).
type RefreshToken struct {
	ID         int64      `json:"id"`
	UserID     int64      `json:"user_id"`
	TokenHash  []byte     `json:"-"`
	FamilyID   []byte     `json:"-"`
	ReplacedBy *int64     `json:"replaced_by,omitempty"`
	UserAgent  *string    `json:"user_agent,omitempty"`
	IP         []byte     `json:"-"`
	ExpiresAt  time.Time  `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	InsertedAt time.Time  `json:"inserted_at"`
}
