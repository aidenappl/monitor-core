package env

import "testing"

// A defaulted MON_ZONE_SLUG is the one configuration mistake that yields a
// WORKING process serving under another zone's name: it seeds a second zone row
// called "trailblaze" in its own registry, binds every key it mints to that row,
// and answers /health as "trailblaze". Nothing downstream errors.
//
// The guard is scoped to MON_ROLE=zone because that is the only role where the
// default is unambiguously wrong — the deployed control plane runs "both" and
// genuinely is trailblaze, so widening this would fail a correct install.
func TestRequireZoneIdentity(t *testing.T) {
	tests := []struct {
		name     string
		role     Role
		explicit bool
		wantErr  bool
	}{
		{"zone that named itself", RoleZone, true, false},
		{"zone that inherited the control plane's name", RoleZone, false, true},
		{"control plane may default — trailblaze IS its zone", RoleApp, false, false},
		{"both may default, for the same reason", RoleBoth, false, false},
		{"explicit is always fine", RoleBoth, true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prevRole, prevExplicit, prevSlug := MonRole, ZoneSlugExplicit, ZoneSlug
			t.Cleanup(func() {
				MonRole, ZoneSlugExplicit, ZoneSlug = prevRole, prevExplicit, prevSlug
			})

			MonRole, ZoneSlugExplicit, ZoneSlug = tc.role, tc.explicit, "trailblaze"

			err := RequireZoneIdentity()
			if tc.wantErr && err == nil {
				t.Fatal("accepted a data plane that never chose a zone")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("refused a valid configuration: %v", err)
			}
		})
	}
}

// getEnvExplicit is what makes the guard above possible, so the distinction it
// draws is pinned here: a value present in the environment must read as chosen,
// and the fallback must not.
func TestGetEnvExplicit(t *testing.T) {
	t.Setenv("MON_TEST_CHOSEN", "appleby")
	if got, explicit := getEnvExplicit("MON_TEST_CHOSEN", "trailblaze"); got != "appleby" || !explicit {
		t.Errorf("set var: got (%q, %t), want (\"appleby\", true)", got, explicit)
	}

	if got, explicit := getEnvExplicit("MON_TEST_ABSENT", "trailblaze"); got != "trailblaze" || explicit {
		t.Errorf("unset var: got (%q, %t), want (\"trailblaze\", false)", got, explicit)
	}

	// An empty value is NOT a choice — it is how an unset Lattice/Keyring
	// variable arrives, and treating it as chosen would defeat the guard.
	t.Setenv("MON_TEST_EMPTY", "")
	if got, explicit := getEnvExplicit("MON_TEST_EMPTY", "trailblaze"); got != "trailblaze" || explicit {
		t.Errorf("empty var: got (%q, %t), want (\"trailblaze\", false)", got, explicit)
	}
}
