package preflight

import (
	"strings"
	"testing"
)

// The unresolved-${} check is the highest-value one in this package, because the
// failure it prevents is a MISDIAGNOSIS rather than an outage: a DSN containing
// a literal "${MARIADB_PASSWORD}" produces "Access denied for user 'monitor'
// (using password: YES)", which reads exactly like a wrong password. Hours went
// to a stale volume and a Keyring override before the DSN itself was suspected.
func TestUnresolvedInterpolationIsFatal(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantFatal bool
	}{
		{
			name:      "a DSN that was never substituted",
			key:       "MON_DB_DSN",
			value:     "monitor:${MARIADB_PASSWORD}@tcp(mariadb:3306)/monitor_auth",
			wantFatal: true,
		},
		{
			name:      "a whole-value reference that also failed",
			key:       "CLICKHOUSE_PASSWORD",
			value:     "${CLICKHOUSE_PASSWORD}",
			wantFatal: true,
		},
		{
			name:      "a fully resolved DSN",
			key:       "MON_DB_DSN",
			value:     "monitor:s3cret@tcp(mariadb:3306)/monitor_auth",
			wantFatal: false,
		},
		{
			// A dollar sign is legal in a password and must not trip this.
			name:      "a password containing a bare dollar",
			key:       "CLICKHOUSE_PASSWORD",
			value:     "pa$$word{not-a-reference}",
			wantFatal: false,
		},
		{
			// Another program's variable is none of our business.
			name:      "an unrelated variable is ignored",
			key:       "SOME_OTHER_TOOL_DSN",
			value:     "user:${PASSWORD}@host",
			wantFatal: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)

			checks := unresolvedInterpolation()
			var found bool
			for _, check := range checks {
				if check.Name == "env."+tc.key {
					found = true
					if check.Severity != Fatal {
						t.Errorf("severity = %s, want FATAL", check.Severity)
					}
					if check.Remedy == "" {
						t.Error("no remedy — the point of this package is that the operator " +
							"should not have to work out what to do next")
					}
					// The value may be a credential and must never be echoed.
					if strings.Contains(check.Detail, tc.value) || strings.Contains(check.Remedy, tc.value) {
						t.Error("the finding echoed the variable's VALUE; it may be a secret")
					}
				}
			}
			if found != tc.wantFatal {
				t.Errorf("flagged = %t, want %t", found, tc.wantFatal)
			}
		})
	}
}

// Keyring wins over the container by construction — InjectEnv runs before
// env.Load() and getEnv reads os.Getenv first. That is defensible, and it is
// terrible to discover during an incident. Reporting only OVERRIDES (not
// fill-ins) is what makes the output short enough to read.
func TestKeyringOverridesReportsReplacementsOnly(t *testing.T) {
	before := map[string]string{
		"CLICKHOUSE_PASSWORD": "from-the-stack",
		"CLICKHOUSE_ADDR":     "clickhouse:9000",
		"MON_ZONE_SLUG":       "appleby",
	}
	after := map[string]string{
		"CLICKHOUSE_PASSWORD": "from-keyring", // replaced
		"CLICKHOUSE_ADDR":     "clickhouse:9000",
		"MON_ZONE_SLUG":       "appleby",
		"MON_CRYPTO_KEY":      "newly-injected", // filled in, not replaced
	}

	checks := KeyringOverrides(before, after)
	if len(checks) != 1 {
		t.Fatalf("got %d checks, want 1", len(checks))
	}

	detail := checks[0].Detail
	if !strings.Contains(detail, "CLICKHOUSE_PASSWORD") {
		t.Errorf("did not name the overridden variable: %s", detail)
	}
	if strings.Contains(detail, "MON_CRYPTO_KEY") {
		t.Errorf("reported a fill-in as an override, which buries the signal: %s", detail)
	}
	if strings.Contains(detail, "from-keyring") || strings.Contains(detail, "from-the-stack") {
		t.Error("a VALUE reached the finding; this runs on the boot path and its output goes to container logs")
	}
}

func TestKeyringOverridesSilentWhenNothingReplaced(t *testing.T) {
	before := map[string]string{"CLICKHOUSE_ADDR": "clickhouse:9000"}
	after := map[string]string{"CLICKHOUSE_ADDR": "clickhouse:9000", "MON_CRYPTO_KEY": "injected"}

	if checks := KeyringOverrides(before, after); len(checks) != 0 {
		t.Errorf("got %d checks, want none — a clean injection must not add noise to every boot", len(checks))
	}
}

// Report's return value is what the boot path stops on, so a fatal must be
// reported as fatal and a warning must not stop anything.
func TestReportSignalsFatalOnly(t *testing.T) {
	if Report([]Check{{Name: "a", Severity: Warn}, {Name: "b", Severity: Info}}) {
		t.Error("warnings and info reported as fatal; a boot would stop on a diagnostic")
	}
	if !Report([]Check{{Name: "a", Severity: Warn}, {Name: "b", Severity: Fatal}}) {
		t.Error("a fatal was not reported, so the boot would continue into a misdiagnosis")
	}
}
