package structs

import "time"

// User is a Monitor account. One user owns many identities (password, forta,
// google, …) — the true-linking model. Lives in MariaDB (users).
type User struct {
	ID            int64     `json:"id"`
	Email         string    `json:"email"`
	EmailVerified bool      `json:"email_verified"`
	Name          *string   `json:"name"`
	DisplayName   *string   `json:"display_name"`
	Role          string    `json:"role"`
	Active        bool      `json:"active"`
	UpdatedAt     time.Time `json:"updated_at"`
	InsertedAt    time.Time `json:"inserted_at"`
}
