package query

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

// tableConstPattern matches the `const fooTable = "monitor.foo"` declarations
// this package uses to name the table each query file reads.
var tableConstPattern = regexp.MustCompile(`(?m)^const\s+\w+Table\s*=\s*"([^"]+)"`)

// TestEveryQueriedTableIsClassified is the guard that would have caught the
// defect this whole tenancy phase exists to fix.
//
// SEVEN monitor.* tables shipped with no tenancy column at all — alert_rules,
// notification_channels, notification_policies, service_groups, dashboards,
// saved_views and service_repos. Every list returned every project's rows. The
// way it was eventually noticed was a human running `grep "scope\."` across the
// query package and getting zero hits, months later.
//
// Nothing DECLARED what those tables should have been, so nothing could notice
// they were not it. structs.TableTenancy is that declaration, and this test is
// what makes it binding: a new table whose name reaches this package must be
// classified, or the build fails. The point is not that the classification is
// always right — it is that adding a table becomes a decision somebody made,
// rather than an omission that only reads as one in hindsight.
func TestEveryQueriedTableIsClassified(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	found := map[string]string{} // table name -> the file that names it
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, m := range tableConstPattern.FindAllStringSubmatch(string(src), -1) {
			found[m[1]] = name
		}
	}

	if len(found) == 0 {
		t.Fatal("no table constants found — this test has stopped testing anything, which is worse than failing")
	}

	for table, file := range found {
		if _, ok := structs.TableTenancy[table]; !ok {
			t.Errorf("%s queries %q, which is not classified in structs.TableTenancy.\n"+
				"Decide its tenancy and add it: TenancyProject (needs a `project` column and a "+
				"predicate on every read and mutation), TenancyDerived (scoped through a "+
				"project-bearing parent — say which), TenancyZone, or TenancyInstall.",
				file, table)
		}
	}
}

// TestTenancyClassificationsAreMeaningful keeps the register honest in the other
// direction: a map full of TenancyInstall would satisfy the test above while
// declaring nothing.
//
// The seven tables named in the header of TestEveryQueriedTableIsClassified are
// asserted to be TenancyProject BY NAME. If a future change genuinely demotes
// one of them — the argument would have to be that it is infrastructure shared
// across every tenant, which migration 128 rejects for notification_channels
// with its cross-scope-reference reasoning — that is a deliberate edit here, in
// a test that says what it is giving up.
func TestTenancyClassificationsAreMeaningful(t *testing.T) {
	mustBeProject := []string{
		"monitor.issues",
		"monitor.alert_rules",
		"monitor.notification_channels",
		"monitor.notification_policies",
		"monitor.service_groups",
		"monitor.dashboards",
		"monitor.saved_views",
		"monitor.service_repos",
		"api_keys",
	}

	for _, table := range mustBeProject {
		got, ok := structs.TableTenancy[table]
		if !ok {
			t.Errorf("%s is missing from structs.TableTenancy", table)
			continue
		}
		if got != structs.TenancyProject {
			t.Errorf("%s is classified %s, want project — a row here belongs to exactly one tenant, "+
				"and an unscoped read of it returns every tenant's rows", table, got)
		}
	}

	// The two derived tables are asserted explicitly so that "derived" stays a
	// claim about WHERE the scoping happens rather than a way to opt out of it.
	// Both are keyed by issue id, and every handler resolves the issue with a
	// project-scoped lookup before touching them.
	for _, table := range []string{"monitor.issue_timeline", "monitor.issue_links"} {
		if structs.TableTenancy[table] != structs.TenancyDerived {
			t.Errorf("%s should be TenancyDerived — it carries no project column and is "+
				"scoped through its parent issue", table)
		}
	}
}
