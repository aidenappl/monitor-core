package sso

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ssolib "github.com/aidenappl/go-forta/sso"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
)

// statePrefix namespaces in-flight login records in the settings KV table.
const statePrefix = "sso_state:"

// StateStore implements ssolib.StateStore over Monitor's settings table.
//
// The settings KV is used rather than a dedicated table because these records
// live for ten minutes and are deleted on read — a table, an index and a
// migration for data that is never queried by anything but its own key would be
// machinery earning nothing.
type StateStore struct {
	Engine db.Queryable
}

// NewStateStore returns a StateStore over the given engine.
func NewStateStore(engine db.Queryable) *StateStore {
	return &StateStore{Engine: engine}
}

// SaveState persists an in-flight login record.
//
// expiresAt is carried inside the payload by the library, so it is not stored as a
// separate column; the sweep below re-reads it. Best-effort sweeping is triggered
// here rather than by a background job because this is the only moment a new dead
// row can be created, and the table is otherwise empty in normal operation.
func (s *StateStore) SaveState(_ context.Context, state string, data []byte, _ time.Time) error {
	if err := query.SetSetting(s.Engine, statePrefix+state, string(data)); err != nil {
		return fmt.Errorf("sso: persist state: %w", err)
	}
	go s.sweepExpired()
	return nil
}

// ConsumeState atomically returns and deletes a state record.
//
// ─────────────────────────────────────────────────────────────────────────────
// ⚠️ THE ATOMICITY HERE IS THE REPLAY DEFENCE. DO NOT RESTRUCTURE IT.
//
// The read comes first, but it is NOT what decides the outcome —
// DeleteSettingExisted does. That helper's DELETE reports whether it removed a
// row, and the database's row lock guarantees exactly one of N concurrent callers
// presenting the same state sees true. Everyone else is refused.
//
// Reordering this into "read, validate, then delete" reintroduces the window where
// two callers both read the record and both proceed, which is precisely what an
// attacker replaying a captured callback URL alongside the real one is racing for.
// The read before the delete is only there to have the payload in hand; the
// authorisation to USE it comes from having won the delete.
// ─────────────────────────────────────────────────────────────────────────────
func (s *StateStore) ConsumeState(_ context.Context, state string) ([]byte, error) {
	key := statePrefix + state

	raw, err := query.GetSetting(s.Engine, key)
	if err != nil || raw == "" {
		return nil, ssolib.ErrNoState
	}

	deleted, err := query.DeleteSettingExisted(s.Engine, key)
	if err != nil {
		return nil, fmt.Errorf("sso: consume state: %w", err)
	}
	if !deleted {
		// Someone else won, or it was consumed between the read and here. Either way
		// this caller is not authorised to use it.
		return nil, ssolib.ErrNoState
	}

	return []byte(raw), nil
}

// sweepExpired best-effort prunes expired records so the settings table does not
// accumulate dead rows.
//
// A record whose payload will not parse is deleted too: it can never be consumed
// successfully, so leaving it is strictly worse than removing it.
func (s *StateStore) sweepExpired() {
	states, err := query.GetSettingsByPrefix(s.Engine, statePrefix)
	if err != nil {
		return
	}
	for k, v := range states {
		var sd ssolib.StateData
		if err := json.Unmarshal([]byte(v), &sd); err != nil {
			_ = query.DeleteSetting(s.Engine, k)
			continue
		}
		if time.Now().After(sd.ExpiresAt) {
			_ = query.DeleteSetting(s.Engine, k)
		}
	}
}
