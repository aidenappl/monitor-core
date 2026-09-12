// Package preflight diagnoses the configuration mistakes that this service can
// see but never used to say.
//
// WHY IT EXISTS. Standing up the second zone took hours, and every hour went to
// the same shape of problem: the binary KNEW something was wrong and reported it
// as something else, or not at all.
//
//   - MON_DB_DSN arrived containing a literal "${MARIADB_PASSWORD}" because
//     Lattice substitutes ${VAR} only as a whole value, never mid-string. The
//     process reported "Access denied for user 'monitor' (using password: YES)"
//     — indistinguishable from a wrong password, a stale volume, or a Keyring
//     override, all three of which were investigated first.
//   - Migration 110 does CREATE DATABASE monitor, but the mariadb image grants
//     the DSN user rights only on MARIADB_DATABASE. The boot died with
//     "Error 1044: Access denied ... to database 'monitor'" from inside a
//     migration, naming neither the grant nor the fix. Nothing in the repo
//     executed or documented it.
//   - Keyring's InjectEnv runs BEFORE env.Load(), so a broadly-granted token
//     silently REPLACED this zone's CLICKHOUSE_* with another zone's. Nothing
//     logged which variables it had overridden.
//
// Each of those is a one-line check. This package is those checks, run at boot
// AND available as `monitor-core doctor`, from ONE code path so the two can
// never disagree about what a healthy install looks like.
package preflight

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

// Severity orders the outcomes worst-first.
type Severity int

const (
	// Fatal — the process cannot do its job, and continuing would produce a
	// misleading failure somewhere further along.
	Fatal Severity = iota
	// Warn — it will run, but something is not what the operator meant.
	Warn
	// Info — worth stating so it is on the record, e.g. a value that came from a
	// fallback rather than a choice.
	Info
)

func (s Severity) String() string {
	switch s {
	case Fatal:
		return "FATAL"
	case Warn:
		return "WARN"
	}
	return "INFO"
}

// Check is one finding. Remedy is separate from Detail because the whole point
// is that the operator should not have to work out what to do next: every
// finding tonight was legible only after somebody had already fixed it once.
type Check struct {
	Name     string
	Severity Severity
	Detail   string
	Remedy   string
}

// envPrefixes are the variable families whose values this service resolves.
// Anything outside them belongs to another program and is none of our business.
var envPrefixes = []string{"MON_", "MONITOR_", "CLICKHOUSE_", "KEYRING_"}

func ours(key string) bool {
	for _, prefix := range envPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// Env runs every check that needs no database connection.
//
// Pure and I/O-free on purpose: it is what `monitor-core check-env` runs, so it
// has to work before the stack exists — which is exactly when the operator is
// standing a zone up and most wants to know the config is sane.
func Env() []Check {
	var checks []Check

	checks = append(checks, unresolvedInterpolation()...)

	return checks
}

// unresolvedInterpolation catches a value that still contains a literal "${".
//
// THIS IS THE CHECK THAT WOULD HAVE SAVED THE MOST TIME. Lattice resolves ${VAR}
// only when it is the ENTIRE value; a ${VAR} embedded in a longer string is
// passed through verbatim. Docker Compose does interpolate mid-string, so a
// compose file copied from the repo into a Lattice stack silently changes
// meaning — and the symptom is a credential error that looks like every other
// credential error.
//
// Fatal rather than a warning: a DSN or a key containing "${" cannot be what
// anyone intended, and every downstream failure it causes is misattributed.
func unresolvedInterpolation() []Check {
	var checks []Check

	for _, entry := range os.Environ() {
		key, value, found := strings.Cut(entry, "=")
		if !found || !ours(key) || !strings.Contains(value, "${") {
			continue
		}
		checks = append(checks, Check{
			Name:     "env." + key,
			Severity: Fatal,
			Detail: fmt.Sprintf("%s still contains a literal ${…}, so it was never substituted. "+
				"Lattice replaces ${VAR} only when it is the WHOLE value, never inside a longer "+
				"string — unlike Docker Compose, which is where this syntax usually comes from.", key),
			Remedy: fmt.Sprintf("Set %s to a fully resolved literal, or move the interpolated part "+
				"into its own variable that is referenced whole.", key),
		})
	}

	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	return checks
}

// KeyringOverrides compares the environment before and after Keyring injection
// and reports every variable Keyring REPLACED rather than filled in.
//
// Keyring is authoritative by construction — client.InjectEnv runs before
// env.Load(), and getEnv reads os.Getenv first — which is a reasonable design
// and a terrible one to discover at 1am. A broadly-granted token overrode a
// zone's CLICKHOUSE_ADDR/USERNAME/PASSWORD with the control plane's, and the
// only evidence was an "(override)" marker in a boot table nobody thought to
// read as authoritative.
//
// ⚠️ NAMES ONLY, NEVER VALUES. This runs on the boot path and its output goes
// to container logs.
func KeyringOverrides(before, after map[string]string) []Check {
	var overridden []string
	for key, newValue := range after {
		oldValue, existed := before[key]
		if existed && oldValue != "" && oldValue != newValue {
			overridden = append(overridden, key)
		}
	}
	if len(overridden) == 0 {
		return nil
	}
	sort.Strings(overridden)

	return []Check{{
		Name:     "keyring.override",
		Severity: Warn,
		Detail: fmt.Sprintf("Keyring REPLACED %d variable(s) that this container had already set: %s. "+
			"Keyring injects before env.Load(), so its values win — the container's are only a fallback.",
			len(overridden), strings.Join(overridden, ", ")),
		Remedy: "If any of those should have come from the stack, narrow this zone's Keyring token " +
			"so it does not grant them. A token granting another zone's credentials will silently " +
			"point this process at that zone's stores.",
	}}
}

// Snapshot captures the current environment for the before/after comparison.
func Snapshot() map[string]string {
	snapshot := make(map[string]string)
	for _, entry := range os.Environ() {
		if key, value, found := strings.Cut(entry, "="); found && ours(key) {
			snapshot[key] = value
		}
	}
	return snapshot
}

// Report logs findings worst-first and reports whether any was Fatal.
//
// It does not exit. The caller decides, because the two callers differ: the boot
// path stops, while `doctor` prints everything it found and sets an exit code —
// and a doctor that stopped at the first fatal would send the operator round the
// loop once per problem, which is the loop this package exists to end.
func Report(checks []Check) (fatal bool) {
	sort.SliceStable(checks, func(i, j int) bool { return checks[i].Severity < checks[j].Severity })

	for _, check := range checks {
		if check.Severity == Fatal {
			fatal = true
		}
		log.Printf("preflight %s [%s] %s", check.Severity, check.Name, check.Detail)
		if check.Remedy != "" {
			log.Printf("preflight %s [%s] → %s", check.Severity, check.Name, check.Remedy)
		}
	}
	return fatal
}
