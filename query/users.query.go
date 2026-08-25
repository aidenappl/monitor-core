package query

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/structs"
)

var userColumns = []string{
	"id", "email", "email_verified", "name", "display_name",
	"role", "active", "updated_at", "inserted_at",
}

type userScanner interface {
	Scan(dest ...interface{}) error
}

func scanUser(row userScanner) (*structs.User, error) {
	var u structs.User
	err := row.Scan(
		&u.ID, &u.Email, &u.EmailVerified, &u.Name, &u.DisplayName,
		&u.Role, &u.Active, &u.UpdatedAt, &u.InsertedAt,
	)
	return &u, err
}

// GetUserByID resolves a user by primary key. Returns (nil, nil) when absent.
func GetUserByID(engine db.Queryable, id int64) (*structs.User, error) {
	query, args, err := sq.Select(userColumns...).
		From("users").
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	u, err := scanUser(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	return u, nil
}

// GetUserByEmail resolves a user by their (unique) email. Returns (nil, nil)
// when absent. Email is a hint for linking, never the identity key.
func GetUserByEmail(engine db.Queryable, email string) (*structs.User, error) {
	query, args, err := sq.Select(userColumns...).
		From("users").
		Where(sq.Eq{"email": email}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	u, err := scanUser(engine.QueryRow(query, args...))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user by email: %w", err)
	}
	return u, nil
}

func CountUsers(engine db.Queryable) (int, error) {
	var count int
	err := engine.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	return count, err
}

type CreateUserRequest struct {
	Email         string
	EmailVerified bool
	Name          *string
	DisplayName   *string
	Role          string
}

// CreateUser inserts a new account and returns the hydrated row.
func CreateUser(engine db.Queryable, req CreateUserRequest) (*structs.User, error) {
	if req.Role == "" {
		req.Role = "viewer"
	}

	query, args, err := sq.Insert("users").
		Columns("email", "email_verified", "name", "display_name", "role").
		Values(req.Email, req.EmailVerified, req.Name, req.DisplayName, req.Role).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	result, err := engine.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("last insert id: %w", err)
	}
	return GetUserByID(engine, id)
}

type UpdateUserRequest struct {
	Name          *string
	DisplayName   *string
	Role          *string
	Active        *bool
	EmailVerified *bool
}

// UpdateUser applies a partial update and returns the refreshed row.
func UpdateUser(engine db.Queryable, id int64, req UpdateUserRequest) (*structs.User, error) {
	q := sq.Update("users").Where(sq.Eq{"id": id})

	hasUpdate := false
	if req.Name != nil {
		q = q.Set("name", *req.Name)
		hasUpdate = true
	}
	if req.DisplayName != nil {
		q = q.Set("display_name", *req.DisplayName)
		hasUpdate = true
	}
	if req.Role != nil {
		q = q.Set("role", *req.Role)
		hasUpdate = true
	}
	if req.Active != nil {
		q = q.Set("active", *req.Active)
		hasUpdate = true
	}
	if req.EmailVerified != nil {
		q = q.Set("email_verified", *req.EmailVerified)
		hasUpdate = true
	}

	if !hasUpdate {
		return GetUserByID(engine, id)
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	if _, err := engine.Exec(query, args...); err != nil {
		return nil, fmt.Errorf("update user: %w", err)
	}
	return GetUserByID(engine, id)
}

// ListUsersByIDs fetches several users in ONE query, keyed by id.
//
// Used to resolve issue assignees for a page of issues: rendering 100 issues
// must not become 100 user lookups. Ids with no matching user are simply absent
// from the map — assignee_user_id has no foreign key precisely so a deleted user
// cannot block anything, so a dangling id is an expected state, not an error.
func ListUsersByIDs(engine db.Queryable, ids []int64) (map[int64]structs.User, error) {
	if len(ids) == 0 {
		return map[int64]structs.User{}, nil
	}

	qStr, args, err := sq.Select(userColumns...).From("users").
		Where(sq.Eq{"users.id": ids}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build sql query: %w", err)
	}

	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql query: %w", err)
	}
	defer rows.Close()

	byID := map[int64]structs.User{}
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		byID[user.ID] = *user
	}
	return byID, rows.Err()
}
