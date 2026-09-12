package migrations

import (
	"sort"
	"strings"
)

// Inventory reports what ClickHouse migrations this BINARY carries.
//
// Deliberately "carries" and not "has applied": this runner has no
// applied-tracking table — it replays every embedded file on every boot and
// records nothing — so the honest answer from inside the process is which files
// are compiled in. That is still the number a drift check needs, because it is
// the one that differs between two images.
//
// The MariaDB runner does track (migrations_applied), which is why its half of
// /version can report an applied count and this half cannot. The asymmetry is
// real and reported rather than papered over.
func Inventory() (count int, latest string) {
	entries, err := migrationsFS.ReadDir(".")
	if err != nil {
		return 0, ""
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	if len(names) == 0 {
		return 0, ""
	}

	// Lexical order is the apply order — the files are numerically prefixed, and
	// RunMigrations relies on the same property.
	sort.Strings(names)
	return len(names), names[len(names)-1]
}
