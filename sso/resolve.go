package sso

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
)

// Resolver implements ssolib.UserResolver for Monitor.
//
// ⚠️ THE LIBRARY DOES NOT CALL THIS INTERFACE — the application does, from its
// callback handler. The interface exists to give the decision matrix a stable name
// and to put the documented ordering somewhere a reader of the library will find
// it. That means nothing in the library will catch an implementation that gets the
// order wrong; resolve_test.go is what catches it, and it is the reason that file
// must not be deleted or weakened.
type Resolver struct {
	Engine db.Queryable
}

// NewResolver returns a Resolver over the given engine.
func NewResolver(engine db.Queryable) *Resolver { return &Resolver{Engine: engine} }

// ResolveUser satisfies ssolib.UserResolver.
//
// Monitor's own callback uses ResolveIdentity directly, because it needs the full
// user row to check user.Active before issuing a session — a user id alone cannot
// answer that. This method is the interface-shaped view of the same logic, so the
// two can never diverge.
func (r *Resolver) ResolveUser(_ context.Context, p *ssolib.Provider, id ssolib.Identity) (int64, error) {
	user, err := ResolveIdentity(r.Engine, p, id)
	if err != nil {
		return 0, err
	}
	return user.ID, nil
}

// LinkIdentity satisfies ssolib.UserResolver.
func (r *Resolver) LinkIdentity(_ context.Context, p *ssolib.Provider, userID int64, id ssolib.Identity) error {
	return LinkIdentity(r.Engine, userID, id)
}

// LinkIdentity attaches an identity to an already-authenticated user.
//
// It refuses to move an identity that is already owned by a DIFFERENT user.
// Without that check, linking becomes a way to transfer an identity between
// accounts: sign in as yourself, link the victim's identity, and their next SSO
// login lands in your account.
//
// Re-linking the same identity to the same user is a no-op success, because a user
// double-clicking "connect" is not an error worth showing them.
func LinkIdentity(engine db.Queryable, userID int64, ni ssolib.Identity) error {
	existing, err := query.GetIdentityByProviderSubject(engine, ni.Provider, ni.Subject)
	if err != nil {
		return fmt.Errorf("sso: lookup identity: %w", err)
	}
	if existing != nil {
		if existing.UserID == userID {
			return nil
		}
		return fmt.Errorf("sso: %s:%s is already linked to another user", ni.Provider, ni.Subject)
	}
	if _, err := createIdentityFor(engine, userID, ni); err != nil {
		return fmt.Errorf("sso: link identity onto user %d: %w", userID, err)
	}
	return nil
}

// ResolveIdentity maps a NormalizedIdentity to the Monitor user it should log in
// as, in three strictly-ordered steps. This is where the nOAuth / pre-account-
// takeover defenses live, so the ordering and gates are load-bearing:
//
//  1. KNOWN IDENTITY — the (provider, subject) pair already exists. Load and
//     return its owning user. This is the ONLY identity key; email is never it.
//
//  2. SAFE LINK (link-on-login) — no such identity yet, but the IdP asserts a
//     VERIFIED email that matches an EXISTING user whose OWN email is verified,
//     and the provider permits auto-linking. Only then do we attach the new
//     identity to that user. Requiring BOTH sides verified defeats the
//     pre-account-takeover attack (an attacker plants an unverified native
//     account on the victim's email; the victim's later SSO login must NOT bind
//     onto it). We NEVER link into an unverified user or on an unverified IdP
//     email — such cases fall through to provisioning.
//
//  3. PROVISION — otherwise, if the provider allows auto-provisioning, create a
//     fresh user (role=pending, awaiting admin approval) plus its identity. If
//     provisioning is disabled, the login is rejected.
//
// engine is a db.Queryable so this is unit-testable against a mocked DB.
func ResolveIdentity(engine db.Queryable, p *ssolib.Provider, ni ssolib.Identity) (*structs.User, error) {
	if ni.Subject == "" {
		return nil, fmt.Errorf("sso: cannot resolve identity with empty subject")
	}

	// 1. Known identity → log in as its user.
	identity, err := query.GetIdentityByProviderSubject(engine, ni.Provider, ni.Subject)
	if err != nil {
		return nil, fmt.Errorf("sso: lookup identity: %w", err)
	}
	if identity != nil {
		user, err := query.GetUserByID(engine, identity.UserID)
		if err != nil {
			return nil, fmt.Errorf("sso: load linked user: %w", err)
		}
		if user == nil {
			return nil, fmt.Errorf("sso: identity %d references missing user %d", identity.ID, identity.UserID)
		}
		if err := query.TouchIdentityLogin(engine, identity.ID); err != nil {
			log.Printf("sso: failed to touch identity %d login time: %v", identity.ID, err)
		}
		return user, nil
	}

	// 2. Safe link — verified-both-sides only, and only if the provider allows it.
	if p.AllowAutoLink && ni.EmailVerified && ni.Email != "" {
		existing, err := query.GetUserByEmail(engine, ni.Email)
		if err != nil {
			return nil, fmt.Errorf("sso: lookup user by email: %w", err)
		}
		if existing != nil {
			if existing.EmailVerified {
				if _, err := createIdentityFor(engine, existing.ID, ni); err != nil {
					return nil, fmt.Errorf("sso: link identity onto user %d: %w", existing.ID, err)
				}
				log.Printf("sso: link-on-login: linked %s:%s onto verified user %d (%s)", ni.Provider, ni.Subject, existing.ID, existing.Email)
				return existing, nil
			}
			// Existing user is UNVERIFIED — refuse to link (pre-account-takeover
			// defense) and refuse to provision onto a colliding email.
			return nil, fmt.Errorf("sso: refusing to link %s onto unverified account for %s", ni.Provider, ni.Email)
		}
	}

	// 3. Provision a fresh account (pending approval) + its first identity.
	if !p.AutoProvision {
		return nil, fmt.Errorf("sso: no account for %s:%s and auto-provisioning is disabled", ni.Provider, ni.Subject)
	}
	user, err := provisionUserWithIdentity(engine, ni)
	if err != nil {
		return nil, err
	}
	log.Printf("sso: provisioned pending user %d (%s) from %s:%s", user.ID, user.Email, ni.Provider, ni.Subject)
	return user, nil
}

// txBeginner is satisfied by *sql.DB; a mocked Queryable in unit tests is not.
type txBeginner interface {
	Begin() (*sql.Tx, error)
}

// provisionUserWithIdentity creates a new user and its first identity atomically.
// A failed identity insert must not leave an orphaned UNIQUE(email) user row
// (which could log in via nothing and block re-registration), so when engine is a
// real *sql.DB the two inserts run in one transaction. When it isn't (unit tests
// with a mocked Queryable), it falls back to sequential inserts.
func provisionUserWithIdentity(engine db.Queryable, ni ssolib.Identity) (*structs.User, error) {
	req := query.CreateUserRequest{
		Email:         ni.Email,
		EmailVerified: ni.EmailVerified,
		Name:          ni.Name,
		Role:          "pending",
	}

	beginner, ok := engine.(txBeginner)
	if !ok {
		user, err := query.CreateUser(engine, req)
		if err != nil {
			return nil, fmt.Errorf("sso: provision user: %w", err)
		}
		if _, err := createIdentityFor(engine, user.ID, ni); err != nil {
			return nil, fmt.Errorf("sso: create identity for provisioned user %d: %w", user.ID, err)
		}
		return user, nil
	}

	tx, err := beginner.Begin()
	if err != nil {
		return nil, fmt.Errorf("sso: begin provision tx: %w", err)
	}
	user, err := query.CreateUser(tx, req)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("sso: provision user: %w", err)
	}
	if _, err := createIdentityFor(tx, user.ID, ni); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("sso: create identity for provisioned user %d: %w", user.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sso: commit provision tx: %w", err)
	}
	return user, nil
}

// createIdentityFor attaches ni as a new identity of userID.
func createIdentityFor(engine db.Queryable, userID int64, ni ssolib.Identity) (*structs.Identity, error) {
	var providerEmail *string
	if ni.Email != "" {
		e := ni.Email
		providerEmail = &e
	}
	return query.CreateIdentity(engine, query.CreateIdentityRequest{
		UserID:         userID,
		Provider:       ni.Provider,
		ProviderUserID: ni.Subject,
		ProviderEmail:  providerEmail,
		EmailVerified:  ni.EmailVerified,
		IdentityData:   ni.RawClaims,
	})
}
