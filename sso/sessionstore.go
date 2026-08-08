package sso

import (
	"context"
	"fmt"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/tools"
)

// SessionStore implements ssolib.SessionStore over the sso_sessions table.
//
// ⚠️ THE TOKENS ARE ENCRYPTED AT REST HERE, AND THAT IS THIS TYPE'S ONE
// IRREPLACEABLE JOB. The library hands them over in plaintext deliberately — it
// will not hold a key, because that would mean every adopting service handing its
// key material to a library. AES-256-GCM via tools.Encrypt on the way in,
// tools.Decrypt on the way out. A refresh token is the longest-lived credential in
// the whole flow, and a plaintext copy in this table would make read access to it
// equivalent to holding every user's upstream grant.
type SessionStore struct {
	Engine db.Queryable
}

// NewSessionStore returns a SessionStore over the given engine.
func NewSessionStore(engine db.Queryable) *SessionStore {
	return &SessionStore{Engine: engine}
}

// SaveSession encrypts and upserts the IdP tokens for a user.
func (s *SessionStore) SaveSession(_ context.Context, userID int64, sess ssolib.Session) error {
	encAccess, err := tools.Encrypt(sess.Tokens.AccessToken)
	if err != nil {
		return fmt.Errorf("sso: encrypt access token: %w", err)
	}

	encRefresh := ""
	if sess.Tokens.RefreshToken != "" {
		encRefresh, err = tools.Encrypt(sess.Tokens.RefreshToken)
		if err != nil {
			return fmt.Errorf("sso: encrypt refresh token: %w", err)
		}
	}

	// Subject and SID are persisted even though nothing reads them until a logout
	// token arrives. They CANNOT be added later: `sid` lives only in the id_token
	// of the login that created this row, so a session written without one is
	// unreachable by a session-scoped logout for the rest of its life.
	return query.UpsertSSOSession(s.Engine, userID, sess.Provider, sess.Subject, sess.SID, encAccess, encRefresh)
}

// LoadSession returns the decrypted session, or (nil, nil) when the user has none.
//
// ⚠️ (nil, nil) IS LOAD-BEARING AND MUST NOT BECOME AN ERROR. Monitor has native
// email/password accounts as well as SSO ones, and a native login has no row here.
// The checkpoint reads (nil, nil) as "not an SSO session, pass" — returning an
// error instead would deny every native login on the platform.
func (s *SessionStore) LoadSession(_ context.Context, userID int64) (*ssolib.Session, error) {
	row, err := query.GetSSOSession(s.Engine, userID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	access, err := tools.Decrypt(row.AccessToken)
	if err != nil {
		// A token that cannot be decrypted is unusable, but the SESSION is still real.
		// Returning an error (rather than nil) matters: it must not be mistaken for
		// "this is a native login", which would pass the checkpoint permanently for a
		// row whose key has rotated.
		return nil, fmt.Errorf("sso: decrypt access token for user %d: %w", userID, err)
	}

	refresh := ""
	if row.RefreshToken != nil && *row.RefreshToken != "" {
		refresh, err = tools.Decrypt(*row.RefreshToken)
		if err != nil {
			return nil, fmt.Errorf("sso: decrypt refresh token for user %d: %w", userID, err)
		}
	}

	return &ssolib.Session{
		Provider:      row.Provider,
		Subject:       derefOr(row.Subject),
		SID:           derefOr(row.SID),
		Tokens:        ssolib.TokenSet{AccessToken: access, RefreshToken: refresh},
		LastCheckedAt: row.LastCheckedAt,
	}, nil
}

// derefOr flattens a nullable column to a string. NULL and "" are the same thing
// to every caller here: "this session cannot be addressed that way".
func derefOr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// ─────────────────────────────────────────────────────────────────────────────
// BackchannelLogoutTarget — the receiving half of OIDC Back-Channel Logout 1.0.
//
// Implementing this OPTIONAL interface is what upgrades Monitor from
// poll-speed revocation (the 5-minute introspection checkpoint) to push-speed.
// Without it go-forta's handler answers 501 and says so, rather than answering
// 200 and discarding the notification.
//
// ⚠️ BOTH METHODS RETURN (0, nil) FOR "NOTHING MATCHED", NEVER AN ERROR. A
// duplicate delivery, an already-expired session and a row predating migration
// 109 all land here, and all three are normal. Returning an error would make the
// provider retry a message that had already been applied, then report this
// receiver as broken.
// ─────────────────────────────────────────────────────────────────────────────

// DeleteSessionsBySID ends the single session with this OIDC session id.
func (s *SessionStore) DeleteSessionsBySID(_ context.Context, provider, sid string) (int, error) {
	return query.DeleteSSOSessionsBySID(s.Engine, provider, sid)
}

// DeleteSessionsBySubject ends every session this subject holds with the
// provider — the correct scope for a subject-wide event such as an
// administrator revoking a grant.
func (s *SessionStore) DeleteSessionsBySubject(_ context.Context, provider, subject string) (int, error) {
	return query.DeleteSSOSessionsBySubject(s.Engine, provider, subject)
}

// TouchSession resets the checkpoint interval after a successful check.
func (s *SessionStore) TouchSession(_ context.Context, userID int64) error {
	return query.TouchSSOSession(s.Engine, userID)
}

// DeleteSession ends the SSO session when the upstream grant is definitively gone.
//
// ⚠️ For Monitor this IS sufficient to end access, and the reason is worth
// recording rather than assumed: Monitor's session middleware consults
// sso_sessions on every request through the checkpoint, so a deleted row denies the
// next request. A service whose own token is validated purely locally would need
// ssolib.LocalTokenRevoker in addition — Monitor deliberately does not implement it
// because there is nothing extra to revoke.
func (s *SessionStore) DeleteSession(_ context.Context, userID int64) error {
	return query.DeleteSSOSession(s.Engine, userID)
}
