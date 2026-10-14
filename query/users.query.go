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

// ValidUserRoles are the roles an admin may assign. "pending" is how SSO
// auto-provisioning creates an account; setting it again revokes access.
var ValidUserRoles = map[string]bool{"admin": true, "editor": true, "viewer": true, "pending": true}

type ListUsersRequest struct {
	Role   *string
	Search *string // substring of email, name or display name
	Limit  int
	Offset int
}

func applyUserFilters(q sq.SelectBuilder, req ListUsersRequest) sq.SelectBuilder {
	if req.Role != nil {
		q = q.Where(sq.Eq{"role": *req.Role})
	}
	if req.Search != nil && *req.Search != "" {
		pattern := "%" + *req.Search + "%"
		q = q.Where(sq.Or{
			sq.Like{"email": pattern},
			sq.Like{"name": pattern},
			sq.Like{"display_name": pattern},
		})
	}
	return q
}

// ListUsers pages through accounts, pending ones first (they are the ones
// waiting on an admin), then newest.
func ListUsers(engine db.Queryable, req ListUsersRequest) ([]structs.User, int, error) {
	if req.Limit <= 0 || req.Limit > db.MAX_LIMIT {
		req.Limit = db.DEFAULT_LIMIT
	}

	countQ, countArgs, err := applyUserFilters(sq.Select("COUNT(*)").From("users"), req).ToSql()
	if err != nil {
		return nil, 0, fmt.Errorf("build count query: %w", err)
	}
	var total int
	if err := engine.QueryRow(countQ, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}

	q := applyUserFilters(sq.Select(userColumns...).From("users"), req).
		OrderBy("role = 'pending' DESC", "id DESC").
		Limit(uint64(req.Limit)).Offset(uint64(req.Offset))
	qStr, args, err := q.ToSql()
	if err != nil {
		return nil, 0, fmt.Errorf("build query: %w", err)
	}
	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	users := []structs.User{}
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, *u)
	}
	return users, total, rows.Err()
}

// CountActiveAdmins counts accounts that can still administer Monitor. The
// user admin refuses any change that would take this to zero.
func CountActiveAdmins(engine db.Queryable) (int, error) {
	var n int
	err := engine.QueryRow("SELECT COUNT(*) FROM users WHERE role = 'admin' AND active = 1").Scan(&n)
	return n, err
}

// ListProvidersByUsers returns the sign-in methods (identity providers) of
// several users in one query, keyed by user id.
func ListProvidersByUsers(engine db.Queryable, ids []int64) (map[int64][]string, error) {
	out := map[int64][]string{}
	if len(ids) == 0 {
		return out, nil
	}
	qStr, args, err := sq.Select("user_id", "provider").From("identities").
		Where(sq.Eq{"user_id": ids}).OrderBy("user_id", "provider").ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}
	rows, err := engine.Query(qStr, args...)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var provider string
		if err := rows.Scan(&id, &provider); err != nil {
			return nil, fmt.Errorf("scan provider: %w", err)
		}
		out[id] = append(out[id], provider)
	}
	return out, rows.Err()
}
