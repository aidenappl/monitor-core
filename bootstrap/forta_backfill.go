package bootstrap

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
)

// FortaUserSeed is one existing Forta user to pre-provision. The operator
// exports these from Forta (for the Monitor platform) and feeds them to
// BackfillFortaUsers so the user is never provisioned as "pending" on their
// first "Continue with Forta" after the auth cutover.
//
//   - Subject       — the Forta subject/user id. This is the IDENTITY KEY
//     ((forta, Subject) → the forta identity row). NEVER the email.
//   - Email         — the user's Forta email (a linking hint / display value).
//   - EmailVerified — Forta emails are verified upstream, so this is normally
//     true; it is written onto both the user (on create) and the identity.
//   - Name          — optional display name.
//   - Role          — the Monitor role to grant: admin | editor | viewer. This
//     is the operator's mapping of the user's Forta grant/role → Monitor role.
type FortaUserSeed struct {
	Subject       string  `json:"subject"`
	Email         string  `json:"email"`
	EmailVerified bool    `json:"email_verified"`
	Name          *string `json:"name"`
	Role          string  `json:"role"`
}

// BackfillResult tallies what BackfillFortaUsers did, so a re-run (idempotent)
// reports mostly Skipped.
type BackfillResult struct {
	// Created — a new users row was inserted (no account existed for the email)
	// plus its forta identity.
	Created int
	// Linked — a users row already existed for the email; a forta identity was
	// attached to it.
	Linked int
	// Skipped — a (forta, Subject) identity already existed; nothing to do.
	Skipped int
}

// validBackfillRoles is the set of Monitor roles a backfill may grant. The
// backfill exists to restore ACTIVE access with a real role, so "pending" (the
// auto-provision holding role) is intentionally excluded.
var validBackfillRoles = map[string]bool{
	"admin":  true,
	"editor": true,
	"viewer": true,
}

// txBeginner is satisfied by *sql.DB (and sqlmock's *sql.DB) but not by a plain
// mocked Queryable. It lets each user's upsert run in its own transaction while
// still being unit-testable.
type txBeginner interface {
	Begin() (*sql.Tx, error)
}

// BackfillFortaUsers pre-provisions existing Forta users as ACTIVE Monitor
// accounts with a linked "forta" identity, so the SSO callback resolves each
// returning user to their real account (never provisioning them as "pending").
//
// For every seed, within its own transaction:
//   - If a (forta, Subject) identity already exists → SKIP (idempotent re-run).
//   - Else if a users row exists for the email → attach a forta identity → LINKED.
//   - Else create an ACTIVE users row (with the given role, email_verified per
//     Forta) plus its forta identity → CREATED.
//
// It is safe to run repeatedly; a second run over the same list reports every
// user as Skipped. A per-user failure aborts that user's transaction and returns
// the counts accumulated so far alongside the error, so the operator can fix the
// offending row and re-run without double-creating anyone.
func BackfillFortaUsers(engine db.Queryable, users []FortaUserSeed) (BackfillResult, error) {
	var result BackfillResult

	beginner, canTx := engine.(txBeginner)

	for i, seed := range users {
		subject := strings.TrimSpace(seed.Subject)
		email := strings.TrimSpace(strings.ToLower(seed.Email))
		role := strings.TrimSpace(strings.ToLower(seed.Role))

		if subject == "" {
			return result, fmt.Errorf("backfill: seed %d (%s): empty subject", i, seed.Email)
		}
		if email == "" {
			return result, fmt.Errorf("backfill: seed %d (subject %s): empty email", i, subject)
		}
		if !validBackfillRoles[role] {
			return result, fmt.Errorf("backfill: seed %d (%s): invalid role %q (want admin|editor|viewer)", i, email, seed.Role)
		}

		outcome, err := backfillOne(engine, beginner, canTx, subject, email, role, seed)
		if err != nil {
			return result, fmt.Errorf("backfill: seed %d (%s): %w", i, email, err)
		}
		switch outcome {
		case outcomeCreated:
			result.Created++
		case outcomeLinked:
			result.Linked++
		case outcomeSkipped:
			result.Skipped++
		}
	}

	return result, nil
}

type backfillOutcome int

const (
	outcomeSkipped backfillOutcome = iota
	outcomeLinked
	outcomeCreated
)

// backfillOne processes a single seed inside its own transaction (when engine
// supports it; otherwise sequentially for unit tests with a mocked Queryable).
func backfillOne(engine db.Queryable, beginner txBeginner, canTx bool, subject, email, role string, seed FortaUserSeed) (backfillOutcome, error) {
	if !canTx {
		return backfillOneTx(engine, subject, email, role, seed)
	}

	tx, err := beginner.Begin()
	if err != nil {
		return outcomeSkipped, fmt.Errorf("begin tx: %w", err)
	}
	outcome, err := backfillOneTx(tx, subject, email, role, seed)
	if err != nil {
		_ = tx.Rollback()
		return outcomeSkipped, err
	}
	if err := tx.Commit(); err != nil {
		return outcomeSkipped, fmt.Errorf("commit tx: %w", err)
	}
	return outcome, nil
}

// backfillOneTx runs the upsert against the given handle (a *sql.Tx in prod).
func backfillOneTx(engine db.Queryable, subject, email, role string, seed FortaUserSeed) (backfillOutcome, error) {
	// 1. Already linked? Idempotent skip.
	existingIdentity, err := query.GetIdentityByProviderSubject(engine, "forta", subject)
	if err != nil {
		return outcomeSkipped, fmt.Errorf("lookup forta identity: %w", err)
	}
	if existingIdentity != nil {
		return outcomeSkipped, nil
	}

	providerEmail := email

	// 2. Existing account for this email → attach a forta identity (LINKED).
	existingUser, err := query.GetUserByEmail(engine, email)
	if err != nil {
		return outcomeSkipped, fmt.Errorf("lookup user by email: %w", err)
	}
	if existingUser != nil {
		if _, err := query.CreateIdentity(engine, query.CreateIdentityRequest{
			UserID:         existingUser.ID,
			Provider:       "forta",
			ProviderUserID: subject,
			ProviderEmail:  &providerEmail,
			EmailVerified:  seed.EmailVerified,
		}); err != nil {
			return outcomeSkipped, fmt.Errorf("link forta identity onto user %d: %w", existingUser.ID, err)
		}
		return outcomeLinked, nil
	}

	// 3. No account → create an ACTIVE user (real role) + its forta identity.
	user, err := query.CreateUser(engine, query.CreateUserRequest{
		Email:         email,
		EmailVerified: seed.EmailVerified,
		Name:          seed.Name,
		Role:          role,
	})
	if err != nil {
		return outcomeSkipped, fmt.Errorf("create user: %w", err)
	}
	if _, err := query.CreateIdentity(engine, query.CreateIdentityRequest{
		UserID:         user.ID,
		Provider:       "forta",
		ProviderUserID: subject,
		ProviderEmail:  &providerEmail,
		EmailVerified:  seed.EmailVerified,
	}); err != nil {
		return outcomeSkipped, fmt.Errorf("create forta identity for user %d: %w", user.ID, err)
	}
	return outcomeCreated, nil
}
