package query

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

var refreshTokenColumns = []string{
	"id", "user_id", "token_hash", "family_id", "replaced_by",
	"user_agent", "ip", "expires_at", "revoked_at", "inserted_at",
}

type refreshTokenScanner interface {
	Scan(dest ...interface{}) error
}

func scanRefreshToken(row refreshTokenScanner) (*structs.RefreshToken, error) {
	var rt structs.RefreshToken
	err := row.Scan(
		&rt.ID, &rt.UserID, &rt.TokenHash, &rt.FamilyID, &rt.ReplacedBy,
		&rt.UserAgent, &rt.IP, &rt.ExpiresAt, &rt.RevokedAt, &rt.InsertedAt,
	)
	return &rt, err
}

// CreateRefreshTokenRequest carries the pre-computed hash and family for a new
// refresh-token row. The caller mints the raw JWT, SHA-256-hashes it, and passes
// only the hash here — the raw token never touches the database.
type CreateRefreshTokenRequest struct {
	UserID    int64
	TokenHash []byte // SHA-256 of the raw refresh token (32 bytes)
	FamilyID  []byte // 16 bytes; shared across a rotation lineage
	ExpiresAt time.Time
	UserAgent *string
	IP        []byte
}

// CreateRefreshToken inserts a new refresh-token row and returns its id. Used to
// start a brand-new family at login (Phase 3) — rotation uses RotateRefreshToken.
func CreateRefreshToken(engine db.Queryable, req CreateRefreshTokenRequest) (int64, error) {
	query, args, err := sq.Insert("refresh_tokens").
		Columns("user_id", "token_hash", "family_id", "user_agent", "ip", "expires_at").
		Values(req.UserID, req.TokenHash, req.FamilyID, req.UserAgent, req.IP, req.ExpiresAt).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("build query: %w", err)
	}

	result, err := engine.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("create refresh token: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return id, nil
}

// GetRefreshTokenByHash resolves a stored token by its SHA-256 hash. Returns
// (nil, nil) when no row matches.
func GetRefreshTokenByHash(engine db.Queryable, hash []byte) (*structs.RefreshToken, error) {
	query, args, err := sq.Select(refreshTokenColumns...).
		From("refresh_tokens").
		Where(sq.Eq{"token_hash": hash}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rt, err := scanRefreshToken(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get refresh token: %w", err)
	}
	return rt, nil
}

// ErrTokenAlreadyRotated is returned by RotateRefreshToken when the old row was
// already stamped (replaced_by set) by a concurrent rotation. The caller MUST
// treat this as reuse: revoke the whole family and reject. This closes the race
// where two concurrent refreshes of the same token both read replaced_by IS NULL
// and each mint a distinct (distinct-hash) successor — the conditional UPDATE
// below lets exactly one win.
var ErrTokenAlreadyRotated = errors.New("refresh token already rotated")

// RotateRefreshToken performs the rotation half of the OAuth 2.0 Security BCP
// refresh flow: it inserts the successor token in the SAME family and stamps the
// old row's replaced_by with the new id, marking it spent. Both statements must
// run in one transaction — pass a *sql.Tx as engine.
//
// The stamp is a CONDITIONAL update (WHERE replaced_by IS NULL): if a concurrent
// rotation already claimed this token, RowsAffected is 0 and we return
// ErrTokenAlreadyRotated so the caller can revoke the family. The successor we
// just inserted is rolled back with the surrounding transaction.
func RotateRefreshToken(engine db.Queryable, oldTokenID int64, req CreateRefreshTokenRequest) (int64, error) {
	newID, err := CreateRefreshToken(engine, req)
	if err != nil {
		return 0, err
	}

	query, args, err := sq.Update("refresh_tokens").
		Set("replaced_by", newID).
		Where(sq.Eq{"id": oldTokenID, "replaced_by": nil}).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("build query: %w", err)
	}
	res, err := engine.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("mark old refresh token replaced: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	if affected == 0 {
		return 0, ErrTokenAlreadyRotated
	}
	return newID, nil
}

// RevokeToken revokes a single refresh-token row (idempotent — only unrevoked
// rows are stamped).
func RevokeToken(engine db.Queryable, id int64) error {
	query, args, err := sq.Update("refresh_tokens").
		Set("revoked_at", time.Now().UTC()).
		Where(sq.Eq{"id": id, "revoked_at": nil}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("revoke token: %w", err)
	}
	return nil
}

// RevokeFamily revokes every unrevoked token sharing familyID. This is the
// reuse-detection response: presenting an already-rotated refresh token proves
// either a leak or a race, so the entire lineage is killed.
func RevokeFamily(engine db.Queryable, familyID []byte) error {
	query, args, err := sq.Update("refresh_tokens").
		Set("revoked_at", time.Now().UTC()).
		Where(sq.Eq{"family_id": familyID, "revoked_at": nil}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("revoke family: %w", err)
	}
	return nil
}

// RevokeAllForUser revokes every unrevoked refresh token owned by userID. Used
// on logout: because the refresh cookie is scoped to Path=/auth/refresh it is
// not sent to /auth/logout, so the family cannot be identified from the request
// — killing all of the user's tokens guarantees logout is effective server-side.
func RevokeAllForUser(engine db.Queryable, userID int64) error {
	query, args, err := sq.Update("refresh_tokens").
		Set("revoked_at", time.Now().UTC()).
		Where(sq.Eq{"user_id": userID, "revoked_at": nil}).
		ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("revoke all for user: %w", err)
	}
	return nil
}
