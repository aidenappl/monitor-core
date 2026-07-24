package query

import (
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

var identityColumns = []string{
	"id", "user_id", "provider", "provider_user_id", "provider_email",
	"email_verified", "password_hash", "identity_data", "last_login_at", "inserted_at",
}

type identityScanner interface {
	Scan(dest ...interface{}) error
}

func scanIdentity(row identityScanner) (*structs.Identity, error) {
	var i structs.Identity
	err := row.Scan(
		&i.ID, &i.UserID, &i.Provider, &i.ProviderUserID, &i.ProviderEmail,
		&i.EmailVerified, &i.PasswordHash, &i.IdentityData, &i.LastLoginAt, &i.InsertedAt,
	)
	return &i, err
}

// GetIdentityByProviderSubject resolves the canonical (provider, sub) identity.
// This is the ONLY correct identity key — never resolve a login on email alone.
// Returns (nil, nil) when no identity matches.
func GetIdentityByProviderSubject(engine db.Queryable, provider, providerUserID string) (*structs.Identity, error) {
	query, args, err := sq.Select(identityColumns...).
		From("identities").
		Where(sq.Eq{"provider": provider, "provider_user_id": providerUserID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	i, err := scanIdentity(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get identity: %w", err)
	}
	return i, nil
}

// GetIdentityByUserAndProvider returns a user's identity for one provider (e.g.
// their "password" identity), or (nil, nil) when they have none for it.
func GetIdentityByUserAndProvider(engine db.Queryable, userID int64, provider string) (*structs.Identity, error) {
	query, args, err := sq.Select(identityColumns...).
		From("identities").
		Where(sq.Eq{"user_id": userID, "provider": provider}).
		Limit(1).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	i, err := scanIdentity(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get identity by user and provider: %w", err)
	}
	return i, nil
}

// UpdateIdentityPassword replaces the bcrypt hash on a password identity.
func UpdateIdentityPassword(engine db.Queryable, id int64, passwordHash []byte) error {
	_, err := engine.Exec("UPDATE identities SET password_hash = ? WHERE id = ?", passwordHash, id)
	return err
}

// ListIdentitiesByUser returns all sign-in methods linked to a user.
func ListIdentitiesByUser(engine db.Queryable, userID int64) ([]structs.Identity, error) {
	query, args, err := sq.Select(identityColumns...).
		From("identities").
		Where(sq.Eq{"user_id": userID}).
		OrderBy("id ASC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := engine.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list identities: %w", err)
	}
	defer rows.Close()

	identities := []structs.Identity{}
	for rows.Next() {
		i, err := scanIdentity(rows)
		if err != nil {
			return nil, fmt.Errorf("scan identity: %w", err)
		}
		identities = append(identities, *i)
	}
	return identities, rows.Err()
}

// CountIdentitiesByUser is used to enforce the "≥1 identity must remain" guard
// before an unlink.
func CountIdentitiesByUser(engine db.Queryable, userID int64) (int, error) {
	var count int
	err := engine.QueryRow("SELECT COUNT(*) FROM identities WHERE user_id = ?", userID).Scan(&count)
	return count, err
}

type CreateIdentityRequest struct {
	UserID         int64
	Provider       string
	ProviderUserID string
	ProviderEmail  *string
	EmailVerified  bool
	PasswordHash   []byte
	IdentityData   []byte
}

// CreateIdentity links a new sign-in method to an existing user and returns the
// hydrated row.
func CreateIdentity(engine db.Queryable, req CreateIdentityRequest) (*structs.Identity, error) {
	query, args, err := sq.Insert("identities").
		Columns("user_id", "provider", "provider_user_id", "provider_email", "email_verified", "password_hash", "identity_data").
		Values(req.UserID, req.Provider, req.ProviderUserID, req.ProviderEmail, req.EmailVerified, req.PasswordHash, req.IdentityData).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	result, err := engine.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("create identity: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("last insert id: %w", err)
	}
	return GetIdentityByID(engine, id)
}

// GetIdentityByID resolves a single identity. Returns (nil, nil) when absent.
func GetIdentityByID(engine db.Queryable, id int64) (*structs.Identity, error) {
	query, args, err := sq.Select(identityColumns...).
		From("identities").
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	i, err := scanIdentity(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get identity by id: %w", err)
	}
	return i, nil
}

// TouchIdentityLogin records a successful login against an identity.
func TouchIdentityLogin(engine db.Queryable, id int64) error {
	_, err := engine.Exec("UPDATE identities SET last_login_at = ? WHERE id = ?", time.Now().UTC(), id)
	return err
}

// DeleteIdentity unlinks a sign-in method. Callers must enforce the
// last-identity guard (CountIdentitiesByUser) before calling.
func DeleteIdentity(engine db.Queryable, id int64) error {
	query, args, err := sq.Delete("identities").Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}
	if _, err := engine.Exec(query, args...); err != nil {
		return fmt.Errorf("delete identity: %w", err)
	}
	return nil
}
