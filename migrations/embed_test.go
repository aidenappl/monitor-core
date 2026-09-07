package migrations

import (
	"strings"
	"testing"
)

// embeddedFiles returns the migration files as the runner sees them, so these
// tests exercise the real DDL rather than a fixture that can drift from it.
func embeddedFiles(t *testing.T) map[string]string {
	t.Helper()

	entries, err := migrationsFS.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read embedded migrations: %v", err)
	}

	files := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		content, err := migrationsFS.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("failed to read migration %s: %v", entry.Name(), err)
		}
		files[entry.Name()] = string(content)
	}
	if len(files) == 0 {
		t.Fatal("no migrations embedded — the go:embed pattern is not matching")
	}
	return files
}

// The deployed configuration is CLICKHOUSE_DATABASE=monitor, which the files are
// already written against. Templating them must therefore change nothing at all:
// this is what makes the change safe to ship to a live zone, and the assertion is
// byte equality rather than "still executes" for exactly that reason.
func TestRewriterIsIdentityForDefaultDatabase(t *testing.T) {
	rewrite := rewriter("monitor")

	for name, content := range embeddedFiles(t) {
		t.Run(name, func(t *testing.T) {
			if got := rewrite.Replace(content); got != content {
				t.Errorf("rewriting %s for database \"monitor\" was not a no-op", name)
			}
		})
	}
}

// Every reference must move together. A file that keeps the literal name after
// the rewrite is the split-brain bug this whole mechanism exists to prevent —
// it would be created in one database and read from another.
func TestRewriterLeavesNoLiteralDatabaseName(t *testing.T) {
	// Deliberately shares no substring with "monitor", so a leftover is
	// unambiguous rather than a fragment of the substituted name.
	rewrite := rewriter("obs_staging")

	for name, content := range embeddedFiles(t) {
		t.Run(name, func(t *testing.T) {
			got := rewrite.Replace(content)
			if strings.Contains(got, "monitor") {
				t.Errorf("%s still references the literal database name after rewriting:\n%s", name, got)
			}
			if !strings.Contains(got, "obs_staging.") && !strings.Contains(got, "CREATE DATABASE IF NOT EXISTS obs_staging") {
				t.Errorf("%s came out with no reference to the target database at all", name)
			}
		})
	}
}

// The runner splits on ';' with no parser, so the statement count is decided by
// the number of semicolons. The rewrite must not move that count — a name is
// constrained to [a-z][a-z0-9_]{2,62} by db.ValidateDatabaseName precisely so it
// cannot smuggle one in, and this pins the guarantee to the real files.
func TestRewriterPreservesStatementBoundaries(t *testing.T) {
	rewrite := rewriter("obs_staging")

	for name, content := range embeddedFiles(t) {
		t.Run(name, func(t *testing.T) {
			before := strings.Count(content, ";")
			after := strings.Count(rewrite.Replace(content), ";")
			if before != after {
				t.Errorf("rewriting %s changed the semicolon count from %d to %d", name, before, after)
			}
		})
	}
}
