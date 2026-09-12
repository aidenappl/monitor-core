package migrations

import (
	"strings"
	"testing"
)

// TestNoSemicolonInsideAComment guards the sharpest edge in this runner.
//
// RunMigrations splits a file into statements with strings.Split(content, ";").
// That is textual: it has no idea what a comment is. So a `;` anywhere inside a
// `--` comment cuts the file at that point, and the remainder of the comment —
// no longer preceded by its `--` — is prepended to the next statement. ClickHouse
// rejects it, RunMigrations returns an error, and main.go calls log.Fatalf.
//
// The result is a CRASH LOOP on a completely healthy system, triggered by
// punctuation in prose. And because this runner has no applied-tracking and
// replays every file on every boot, it does not fail once — it fails forever,
// on every restart, until someone edits the comment.
//
// 005_issue_occurrences_daily.sql already carries a hand-written warning about
// this. A warning in a comment is protection that depends on the next author
// reading the right file first; this test is protection that does not.
//
// ⚠️ THE REAL FIX is a splitter that understands comments and string literals,
// which would let this test be deleted. Until that lands, this is the cheap half
// — and the cheap half is what stops the outage.
func TestNoSemicolonInsideAComment(t *testing.T) {
	entries, err := migrationsFS.ReadDir(".")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}

	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		checked++

		content, err := migrationsFS.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}

		for i, line := range strings.Split(string(content), "\n") {
			marker := strings.Index(line, "--")
			if marker == -1 {
				continue
			}
			if strings.Contains(line[marker:], ";") {
				t.Errorf("%s:%d has a ';' inside a comment:\n    %s\n"+
					"RunMigrations splits statements on ';' textually, so this cuts the file "+
					"mid-comment and crash-loops the service on every boot. Reword the comment.",
					entry.Name(), i+1, strings.TrimSpace(line))
			}
		}
	}

	// A test that silently stops testing is worse than one that fails: if the
	// embed pattern ever stops matching, this would pass on zero files forever.
	if checked == 0 {
		t.Fatal("no migration files were checked — the embed is not matching, and this test is now vacuous")
	}
}
