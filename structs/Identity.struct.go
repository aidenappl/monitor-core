package structs

import (
	"encoding/json"
	"time"
)

// Identity is one sign-in method belonging to a User, keyed on
// (provider, provider_user_id). provider="password" stores a bcrypt hash in
// PasswordHash; SSO providers store the raw claim envelope in IdentityData.
// Lives in MariaDB (identities).
type Identity struct {
	ID             int64           `json:"id"`
	UserID         int64           `json:"user_id"`
	Provider       string          `json:"provider"`
	ProviderUserID string          `json:"provider_user_id"`
	ProviderEmail  *string         `json:"provider_email,omitempty"`
	EmailVerified  bool            `json:"email_verified"`
	PasswordHash   []byte          `json:"-"`
	IdentityData   json.RawMessage `json:"identity_data,omitempty"`
	LastLoginAt    *time.Time      `json:"last_login_at,omitempty"`
	InsertedAt     time.Time       `json:"inserted_at"`
}
