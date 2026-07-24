package query

import (
	"database/sql"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

// UpsertSSOSession inserts or replaces the cached IdP session for a user. Tokens
// are stored as supplied — callers MUST AES-256-GCM encrypt them (tools.Encrypt)
// before passing in. provider names the sso_providers slug that minted the
// session so the checkpoint knows which IdP to introspect.
func UpsertSSOSession(engine db.Queryable, userID int64, provider, encAccessToken, encRefreshToken string) error {
	const stmt = `
		INSERT INTO sso_sessions (user_id, provider, access_token, refresh_token)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			provider = VALUES(provider),
			access_token = VALUES(access_token),
			refresh_token = VALUES(refresh_token),
			last_checked_at = CURRENT_TIMESTAMP
	`
	var refresh interface{}
	if encRefreshToken != "" {
		refresh = encRefreshToken
	}
	_, err := engine.Exec(stmt, userID, provider, encAccessToken, refresh)
	return err
}

// GetSSOSession returns the cached IdP session for a user, or (nil, nil) when
// the user's session is not SSO-backed. Tokens come back encrypted.
func GetSSOSession(engine db.Queryable, userID int64) (*structs.SSOSession, error) {
	const stmt = `
		SELECT user_id, provider, access_token, refresh_token, last_checked_at, inserted_at
		FROM sso_sessions
		WHERE user_id = ?
	`
	row := engine.QueryRow(stmt, userID)
	s := &structs.SSOSession{}
	if err := row.Scan(&s.UserID, &s.Provider, &s.AccessToken, &s.RefreshToken, &s.LastCheckedAt, &s.InsertedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return s, nil
}

// TouchSSOSession bumps last_checked_at to now after a successful introspection,
// resetting the checkpoint TTL.
func TouchSSOSession(engine db.Queryable, userID int64) error {
	_, err := engine.Exec("UPDATE sso_sessions SET last_checked_at = CURRENT_TIMESTAMP WHERE user_id = ?", userID)
	return err
}

// DeleteSSOSession removes the cached IdP session (on logout, or when a
// checkpoint finds the upstream grant revoked).
func DeleteSSOSession(engine db.Queryable, userID int64) error {
	_, err := engine.Exec("DELETE FROM sso_sessions WHERE user_id = ?", userID)
	return err
}
