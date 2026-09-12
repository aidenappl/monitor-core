package bootstrap

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
	"github.com/google/uuid"
)

// InstallIDSetting is the settings key holding this install's identity.
const InstallIDSetting = "install_id"

// EnsureInstallID returns a UUID that identifies THIS DATABASE, minting it on
// first boot and reading it back forever after.
//
// WHY A ZONE NEEDS AN IDENTITY THAT IS NOT ITS SLUG. probe.classify compares the
// slug a box reports on /health against the slug the registry row expected, and
// that check is defeated by the most likely misconfiguration there is: a zone
// that never set MON_ZONE_SLUG inherits "trailblaze", and a zone that never set
// MON_PUBLIC_URL used to inherit the control plane's URL — so the registry row
// pointed at the control plane, the control plane answered, and the slug check
// compared "trailblaze" to "trailblaze" and returned HEALTHY. Two wrong values
// agreed and produced a green tick.
//
// An install id cannot agree by accident. It lives in the SETTINGS table, so it
// is a property of the database rather than of the configuration — two registry
// rows resolving to the same install is then a provable fact rather than an
// inference, and a zone whose row resolves to the control plane's own install is
// detectable no matter how its env is set.
//
// It is stored, not derived. A value derived from the DSN or the hostname would
// change when either did, which is exactly when you least want the identity to
// move.
//
// ⚠️ NOT A SECRET. It is reported on /health so a prober can read it, exactly
// like `zone` and `role`. It identifies a database; it authorises nothing.
func EnsureInstallID(engine db.Queryable) (string, error) {
	// ⚠️ sql.ErrNoRows IS THE FIRST-BOOT CASE, NOT A FAILURE. query.GetSetting
	// returns it for any absent key — its own doc comment says so — and treating
	// it as an error is how the first version of this function failed: on the one
	// boot that was supposed to MINT the id, it reported "failed to read
	// install_id: sql: no rows in result set", logged a warning, and left
	// /version reporting an empty install_id forever after, because every later
	// boot took the same branch.
	existing, err := query.GetSetting(engine, InstallIDSetting)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("failed to read %s: %w", InstallIDSetting, err)
	}
	if id := strings.TrimSpace(existing); id != "" {
		return id, nil
	}

	id := uuid.New().String()
	if err := query.SetSetting(engine, InstallIDSetting, id); err != nil {
		return "", fmt.Errorf("failed to record %s: %w", InstallIDSetting, err)
	}

	// Logged once, on the boot that mints it, because this is the only moment it
	// is new information — and because "this database has never been booted
	// before" is worth seeing in a log when you believed you were restarting an
	// existing install.
	log.Printf("bootstrap: minted install id %s for this database", id)
	return id, nil
}
