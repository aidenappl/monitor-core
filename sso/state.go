package sso

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"golang.org/x/oauth2"
)

// statePrefix namespaces the single-use OAuth state records in the settings KV.
const statePrefix = "sso_state:"

// stateTTL bounds how long an in-flight login may sit between /login and
// /callback before its state expires.
const stateTTL = 10 * time.Minute

// StateData is the server-side record for one in-flight SSO login, stored under
// settings key sso_state:<state>. Keeping the nonce and PKCE verifier server-
// side (rather than in a cookie) means the callback validates against data the
// client never held. Single-use: ConsumeState deletes it on read.
type StateData struct {
	Provider  string    `json:"provider"`
	Nonce     string    `json:"nonce"`
	Verifier  string    `json:"verifier"`
	ReturnURL string    `json:"return_url"`
	ExpiresAt time.Time `json:"expires_at"`

	// LinkUserID, when non-zero, marks this as an authenticated account-LINK flow
	// rather than a login: the callback must attach the returned identity to THIS
	// user (skipping resolve/provision) because the active session already proved
	// the step-up. Zero means an ordinary login.
	LinkUserID int64 `json:"link_user_id,omitempty"`
}

// GenerateState creates a fresh {state, nonce, PKCE verifier} for a login,
// persists the record (single-use, 10-minute TTL), and returns the pieces the
// authorize URL needs. returnURL is where the callback should send the browser
// after a successful login (already sanitized by the caller).
func GenerateState(engine db.Queryable, provider, returnURL string) (state, nonce, verifier string, err error) {
	return generateState(engine, provider, returnURL, 0)
}

// GenerateLinkState is GenerateState for the authenticated account-LINK flow: the
// record carries linkUserID so the callback attaches the returned identity to
// that already-signed-in user instead of resolving/provisioning a login.
func GenerateLinkState(engine db.Queryable, provider, returnURL string, linkUserID int64) (state, nonce, verifier string, err error) {
	return generateState(engine, provider, returnURL, linkUserID)
}

func generateState(engine db.Queryable, provider, returnURL string, linkUserID int64) (state, nonce, verifier string, err error) {
	state = randToken()
	nonce = randToken()
	verifier = oauth2.GenerateVerifier()

	payload, err := json.Marshal(StateData{
		Provider:   provider,
		Nonce:      nonce,
		Verifier:   verifier,
		ReturnURL:  returnURL,
		ExpiresAt:  time.Now().Add(stateTTL),
		LinkUserID: linkUserID,
	})
	if err != nil {
		return "", "", "", fmt.Errorf("sso: marshal state: %w", err)
	}

	if err := query.SetSetting(engine, statePrefix+state, string(payload)); err != nil {
		return "", "", "", fmt.Errorf("sso: persist state: %w", err)
	}

	go sweepExpiredStates(engine)
	return state, nonce, verifier, nil
}

// ConsumeState validates and atomically consumes a state record. It is single-
// use: the record is deleted whether it was valid, expired, or malformed, so a
// replayed state can never succeed twice. Returns an error when the state is
// unknown or expired.
func ConsumeState(engine db.Queryable, state string) (*StateData, error) {
	if state == "" {
		return nil, fmt.Errorf("sso: empty state")
	}
	key := statePrefix + state

	raw, err := query.GetSetting(engine, key)
	if err != nil || raw == "" {
		return nil, fmt.Errorf("sso: unknown or expired state")
	}

	// Consume atomically: the DELETE row-lock ensures only one of N concurrent
	// callbacks presenting the same state observes deleted=true. A loser (or a
	// state already consumed between the read above and here) is rejected, so a
	// replayed state can never authenticate twice.
	deleted, err := query.DeleteSettingExisted(engine, key)
	if err != nil {
		return nil, fmt.Errorf("sso: consume state: %w", err)
	}
	if !deleted {
		return nil, fmt.Errorf("sso: state already consumed")
	}

	var sd StateData
	if err := json.Unmarshal([]byte(raw), &sd); err != nil {
		return nil, fmt.Errorf("sso: corrupt state record")
	}
	if time.Now().After(sd.ExpiresAt) {
		return nil, fmt.Errorf("sso: state expired")
	}
	return &sd, nil
}

func randToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// sweepExpiredStates best-effort prunes expired state records so the settings
// table does not accumulate dead rows.
func sweepExpiredStates(engine db.Queryable) {
	states, err := query.GetSettingsByPrefix(engine, statePrefix)
	if err != nil {
		return
	}
	for k, v := range states {
		var sd StateData
		if err := json.Unmarshal([]byte(v), &sd); err != nil {
			_ = query.DeleteSetting(engine, k)
			continue
		}
		if time.Now().After(sd.ExpiresAt) {
			_ = query.DeleteSetting(engine, k)
		}
	}
}
