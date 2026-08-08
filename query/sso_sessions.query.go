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
func UpsertSSOSession(engine db.Queryable, userID int64, provider, subject, sid, encAccessToken, encRefreshToken string) error {
	const stmt = `
		INSERT INTO sso_sessions (user_id, provider, subject, sid, access_token, refresh_token)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			provider = VALUES(provider),
			subject = VALUES(subject),
			sid = VALUES(sid),
			access_token = VALUES(access_token),
			refresh_token = VALUES(refresh_token),
			last_checked_at = CURRENT_TIMESTAMP
	`
	var refresh interface{}
	if encRefreshToken != "" {
		refresh = encRefreshToken
	}
	_, err := engine.Exec(stmt, userID, provider, nullIfEmpty(subject), nullIfEmpty(sid), encAccessToken, refresh)
	return err
}

// nullIfEmpty stores "" as SQL NULL.
//
// ⚠️ The distinction is load-bearing for `sid`. A row holding the empty string
// would MATCH a lookup built from a logout token that named no session, so a
// subject-wide event could end a session it never addressed. NULL never matches
// an equality test, which is the behaviour wanted.
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// GetSSOSession returns the cached IdP session for a user, or (nil, nil) when
// the user's session is not SSO-backed. Tokens come back encrypted.
func GetSSOSession(engine db.Queryable, userID int64) (*structs.SSOSession, error) {
	const stmt = `
		SELECT user_id, provider, subject, sid, access_token, refresh_token, last_checked_at, inserted_at
		FROM sso_sessions
		WHERE user_id = ?
	`
	row := engine.QueryRow(stmt, userID)
	s := &structs.SSOSession{}
	if err := row.Scan(&s.UserID, &s.Provider, &s.Subject, &s.SID, &s.AccessToken, &s.RefreshToken, &s.LastCheckedAt, &s.InsertedAt); err != nil {
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

// DeleteSSOSessionsBySID removes the session a back-channel logout token named by
// `sid`, and reports how many rows went.
//
// ⚠️ SCOPED BY PROVIDER, and the empty-sid guard below is not defensive padding.
// `sid` is only unique within an issuer, so an unscoped match would let one
// identity provider end another's sessions. And an empty sid must never reach
// the query: matching on an empty sid would sweep every row this app wrote before
// migration 109 if any had been stored as empty string rather than NULL.
func DeleteSSOSessionsBySID(engine db.Queryable, provider, sid string) (int, error) {
	if provider == "" || sid == "" {
		return 0, nil
	}
	res, err := engine.Exec(
		"DELETE FROM sso_sessions WHERE provider = ? AND sid = ?", provider, sid)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// DeleteSSOSessionsBySubject removes every session a subject holds with one
// provider — the correct scope for a subject-wide event such as an administrator
// revoking a grant.
//
// Zero rows is a NORMAL result, not a failure: the session may have expired,
// been logged out locally, or predate migration 109 (in which case `subject` is
// NULL and cannot be matched). The caller reports the count rather than treating
// it as an error, because there is nothing a retry would achieve.
func DeleteSSOSessionsBySubject(engine db.Queryable, provider, subject string) (int, error) {
	if provider == "" || subject == "" {
		return 0, nil
	}
	res, err := engine.Exec(
		"DELETE FROM sso_sessions WHERE provider = ? AND subject = ?", provider, subject)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}
