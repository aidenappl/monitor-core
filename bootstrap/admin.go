// Package bootstrap holds first-run provisioning that seeds a fresh Monitor
// database so an operator can sign in before any UI-driven account exists.
package bootstrap

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/tools"
)

// EnsureAdminUser seeds the initial admin account on a fresh install. It is a
// no-op once any user exists. When the users table is empty and both
// MON_ADMIN_EMAIL and MON_ADMIN_PASSWORD are set, it creates one active,
// email-verified admin user plus its "password" identity so the operator can log
// in immediately. Called from main.go after the MariaDB migrations run.
func EnsureAdminUser(engine db.Queryable) error {
	count, err := query.CountUsers(engine)
	if err != nil {
		return fmt.Errorf("failed to count users: %w", err)
	}
	if count > 0 {
		return nil
	}

	if env.AdminEmail == "" || env.AdminPassword == "" {
		log.Println("bootstrap: no users exist and MON_ADMIN_EMAIL/MON_ADMIN_PASSWORD not set — no admin created")
		return nil
	}

	email := strings.TrimSpace(strings.ToLower(env.AdminEmail))
	hash, err := tools.HashPassword(env.AdminPassword)
	if err != nil {
		return fmt.Errorf("failed to hash admin password: %w", err)
	}

	name := "Admin"
	userReq := query.CreateUserRequest{
		Email:         email,
		EmailVerified: true,
		Name:          &name,
		Role:          "admin",
	}

	// Create the admin user and its password identity atomically so a failed
	// identity insert can't leave an orphaned UNIQUE(email) admin row that blocks
	// re-seeding. engine is db.SQL in practice (a *sql.DB); fall back to
	// sequential inserts if it isn't a transaction-capable handle.
	beginner, ok := engine.(interface {
		Begin() (*sql.Tx, error)
	})
	if !ok {
		user, err := query.CreateUser(engine, userReq)
		if err != nil {
			return fmt.Errorf("failed to create admin user: %w", err)
		}
		if _, err := query.CreateIdentity(engine, adminIdentityReq(user.ID, email, hash)); err != nil {
			return fmt.Errorf("failed to create admin identity: %w", err)
		}
		log.Printf("bootstrap: created admin user %s (role=admin)", email)
		return nil
	}

	tx, err := beginner.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin admin bootstrap tx: %w", err)
	}
	user, err := query.CreateUser(tx, userReq)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to create admin user: %w", err)
	}
	if _, err := query.CreateIdentity(tx, adminIdentityReq(user.ID, email, hash)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to create admin identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit admin bootstrap: %w", err)
	}

	log.Printf("bootstrap: created admin user %s (role=admin)", email)
	return nil
}

func adminIdentityReq(userID int64, email string, hash []byte) query.CreateIdentityRequest {
	return query.CreateIdentityRequest{
		UserID:         userID,
		Provider:       "password",
		ProviderUserID: email,
		ProviderEmail:  &email,
		EmailVerified:  true,
		PasswordHash:   hash,
	}
}
