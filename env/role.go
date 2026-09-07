package env

import (
	"fmt"
	"strings"
)

// ROLE_ENV_VAR is the variable that selects the plane. Held as a constant
// because two places have to agree on the spelling — the read in Load and the
// refusal message in RequireValidRole — and the whole value of that message is
// that it names the variable the operator has to go and fix.
const ROLE_ENV_VAR = "MON_ROLE"

// Role names which of Monitor's two planes a process runs.
//
// Monitor is one binary doing two unrelated jobs. It is a CONTROL PLANE:
// accounts, sessions, SSO, API keys, the tenancy registry — the things there is
// exactly one of, ever. And it is a DATA PLANE: it accepts events, writes them
// to ClickHouse, evaluates alerts and tracks issues — the things there will
// eventually be one of per zone, sitting near the data they ingest. Today a
// single process does both, on one host, for one zone.
//
// This type makes that split EXPLICIT before there is a second zone to get it
// wrong against. It is deliberately not a second binary: two `main`s drift, and
// the way they drift is that a route added to one and forgotten in the other
// stays invisible until someone deploys the split topology and finds a 404 in
// production. One binary with one router, gated by role at the line of
// registration, cannot drift that way — the gate and the thing it gates are
// written together.
//
// An invalid Role answers false to BOTH capability predicates, so a value that
// somehow reaches them runs nothing rather than guessing. main() refuses to boot
// on one long before that can matter (RequireValidRole), and the two facts are a
// pair: neither is load-bearing alone.
type Role string

const (
	// RoleApp is the control plane: identity and configuration. It holds NO
	// ClickHouse connection — db.Conn stays nil — and runs none of the ingest
	// machinery (queue, batcher, event hub) nor the alert evaluator.
	RoleApp Role = "app"

	// RoleZone is the data plane: events, ingest, alerts, issues. It mints no
	// sessions and installs no SSO. A zone VERIFIES an access token (the /v1
	// subrouter still falls back to SessionMiddleware); it does not issue one,
	// seed an admin or own a provider row.
	RoleZone Role = "zone"

	// RoleBoth is one process doing both jobs.
	//
	// THIS IS THE DEPLOYED CONFIGURATION AND THE DEFAULT, and both halves of
	// that sentence are the reason it is the default. A role that had to be set
	// would make the very first deploy of this change a behaviour change on a
	// live system ingesting ~53k events/day, gated on someone remembering to add
	// an env var to a stack. Defaulting to `both` makes the deploy a no-op and
	// the split an opt-in, which is the only ordering in which the split can be
	// tried without risking what already works.
	RoleBoth Role = "both"
)

// DEFAULT_ROLE is what an install with MON_ROLE unset (or blank) gets. See
// RoleBoth for why it is this and not something narrower.
const DEFAULT_ROLE = RoleBoth

// MonRole is the resolved role for this process, set by Load and validated by
// RequireValidRole. Read it only after main() has called both.
var MonRole Role

// ParseRole resolves a raw MON_ROLE value.
//
// Whitespace and case are normalised away before matching: the value arrives
// through a Lattice stack's env blob or a copy-pasted shell line, so a trailing
// newline or a capitalised "Zone" is an artefact of transport rather than a
// statement of intent, and refusing to boot on one would be pedantry rather than
// safety. Anything that is not one of the three roles after that is returned
// UNCHANGED — the caller's exact bytes, not the normalised form — so that
// RequireValidRole can quote back what the operator actually set rather than a
// tidied-up version of it that they will not find in their config.
//
// Blank means unset means DEFAULT_ROLE. The default lives here, in one place,
// rather than in the getEnv call in Load, so that tests exercising this function
// exercise the real default.
func ParseRole(raw string) Role {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return DEFAULT_ROLE
	}
	if role := Role(normalized); role.IsValid() {
		return role
	}
	return Role(raw)
}

// IsValid reports whether the role is one of the three known values.
func (r Role) IsValid() bool {
	switch r {
	case RoleApp, RoleZone, RoleBoth:
		return true
	}
	return false
}

// RunsControlPlane reports whether this process owns identity and configuration:
// the /auth surface, the SSO subsystem and its install hook, the admin bootstrap
// and the tenancy registry seeder.
func (r Role) RunsControlPlane() bool {
	return r == RoleApp || r == RoleBoth
}

// RunsDataPlane reports whether this process owns events: the ClickHouse
// connection and its migrations, ingestion, the batcher, both SSE hubs, the
// alert evaluator and the issue tracker.
func (r Role) RunsDataPlane() bool {
	return r == RoleZone || r == RoleBoth
}

// RequireValidRole returns an error when MON_ROLE named something that is not a
// role. main() calls it immediately after Load, before anything reads MonRole.
//
// FAIL FAST RATHER THAN FALL BACK — that distinction is the entire reason this
// function exists rather than ParseRole simply returning DEFAULT_ROLE for junk.
// "zonr", "data", "app,zone", a value that picked up a stray character in a JSON
// env blob: every one of those is a typo an operator makes while standing up the
// split topology, and under a silent default every one of them produces a
// process that runs the CONTROL PLANE on a machine that was meant to be a zone.
// It would seed a zone row, mint sessions, install SSO and answer /auth/login,
// while the operator reads their own config back and sees a data plane. That is
// not a degraded state anybody notices — it is a second control plane. Refusing
// to boot is by far the cheaper outcome, and the message below is written to be
// actionable from a container log with nothing else to hand.
func RequireValidRole() error {
	if MonRole.IsValid() {
		return nil
	}
	return fmt.Errorf(
		"%s=%q is not a recognised role: it must be one of %q (control plane), %q (data plane) or %q (both, the default when unset)",
		ROLE_ENV_VAR, string(MonRole), RoleApp, RoleZone, RoleBoth,
	)
}
