package env

import (
	"strings"
	"testing"
)

// TestParseRole covers the one property that matters most about MON_ROLE: an
// unset variable must resolve to `both`, and a value that is not a role must NOT.
//
// The second half is the reason this file exists. `both` is the deployed
// configuration, so a parser that fell back to it on junk would turn every typo
// into a silently working process — and the process it works as is the control
// plane, on a host the operator believes is a zone.
func TestParseRole(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		want  Role
		valid bool
	}{
		{"unset falls back to the deployed default", "", RoleBoth, true},
		{"blank is the same as unset", "   ", RoleBoth, true},
		{"app", "app", RoleApp, true},
		{"zone", "zone", RoleZone, true},
		{"both", "both", RoleBoth, true},
		{"case is transport noise, not intent", "ZONE", RoleZone, true},
		{"a trailing newline out of an env blob is not a typo", "app\n", RoleApp, true},
		{"a near miss is refused, not rounded to the default", "zonr", Role("zonr"), false},
		{"a plausible synonym is still not a role", "data", Role("data"), false},
		{"two roles at once is not a role", "app,zone", Role("app,zone"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseRole(tt.raw)
			if got != tt.want {
				t.Errorf("ParseRole(%q) = %q, want %q", tt.raw, got, tt.want)
			}
			if got.IsValid() != tt.valid {
				t.Errorf("ParseRole(%q).IsValid() = %t, want %t", tt.raw, got.IsValid(), tt.valid)
			}
		})
	}
}

// TestParseRoleKeepsTheOperatorsBytesOnAReject pins the small deliberate
// asymmetry in ParseRole: a value it accepts comes back normalised, a value it
// rejects comes back exactly as the operator wrote it. The refusal message
// quotes it, and an operator searching their stack config for a tidied-up
// version of their own typo will not find it.
func TestParseRoleKeepsTheOperatorsBytesOnAReject(t *testing.T) {
	const raw = "  Zone-B  "
	if got := ParseRole(raw); string(got) != raw {
		t.Errorf("ParseRole(%q) = %q; a rejected value must be returned unchanged so the error can quote it", raw, string(got))
	}
}

// TestRequireValidRoleNamesTheVariableAndEveryLegalValue asserts the content of
// the refusal, not just that there is one. This message is read from a container
// log by someone whose process will not start, usually with no other context, so
// it has to carry the variable name and all three values it will accept.
func TestRequireValidRoleNamesTheVariableAndEveryLegalValue(t *testing.T) {
	original := MonRole
	t.Cleanup(func() { MonRole = original })

	MonRole = ParseRole("zonr")
	err := RequireValidRole()
	if err == nil {
		t.Fatal("RequireValidRole accepted \"zonr\"; a typo must stop the boot, not degrade to the default")
	}

	msg := err.Error()
	for _, want := range []string{ROLE_ENV_VAR, "zonr", string(RoleApp), string(RoleZone), string(RoleBoth)} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q does not mention %q", msg, want)
		}
	}
}

// TestRequireValidRoleAcceptsEveryRole is the other half: the three real values
// must pass, including the default, or the fail-fast guard would be a fail-always.
func TestRequireValidRoleAcceptsEveryRole(t *testing.T) {
	original := MonRole
	t.Cleanup(func() { MonRole = original })

	for _, role := range []Role{RoleApp, RoleZone, RoleBoth} {
		MonRole = role
		if err := RequireValidRole(); err != nil {
			t.Errorf("RequireValidRole rejected %q: %v", role, err)
		}
	}
}

// TestRoleCapabilities is the table the rest of the process is gated on: which
// plane each role runs.
//
// The `both` row is the one worth reading twice. It answers true to both
// predicates, which is what makes this whole change a no-op in the deployed
// configuration — every `if role.Runs…` in main.go and buildRouter is entered
// exactly as the unconditional code before it was.
//
// The invalid row is not padding either: an unrecognised role runs NEITHER
// plane. main.go refuses to boot on one long before that matters, but if that
// guard were ever moved or lost, the failure this produces is a process that
// serves nothing — loud — rather than one that guesses.
func TestRoleCapabilities(t *testing.T) {
	tests := []struct {
		role         Role
		controlPlane bool
		dataPlane    bool
	}{
		{RoleApp, true, false},
		{RoleZone, false, true},
		{RoleBoth, true, true},
		{Role("nonsense"), false, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			if got := tt.role.RunsControlPlane(); got != tt.controlPlane {
				t.Errorf("%q.RunsControlPlane() = %t, want %t", tt.role, got, tt.controlPlane)
			}
			if got := tt.role.RunsDataPlane(); got != tt.dataPlane {
				t.Errorf("%q.RunsDataPlane() = %t, want %t", tt.role, got, tt.dataPlane)
			}
		})
	}
}

// TestDefaultRoleIsBoth is a one-line test guarding a one-word decision, and it
// is here because that word is the difference between this change deploying as a
// no-op and deploying as an outage. Changing DEFAULT_ROLE to anything else means
// the next deploy of a live install silently stops doing half its job.
func TestDefaultRoleIsBoth(t *testing.T) {
	if DEFAULT_ROLE != RoleBoth {
		t.Fatalf("DEFAULT_ROLE = %q, want %q — an install with no MON_ROLE set must keep running both planes", DEFAULT_ROLE, RoleBoth)
	}
}
