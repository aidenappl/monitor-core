package scope

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// This file is a structural guard, not a behavioural one, and it exists because
// every other test in this change can only prove that the read paths which
// EXIST today are scoped. None of them can fail when someone adds a tenth one.
//
// That is the actual shape of the risk. A cross-project leak here does not
// arrive as a regression in a covered path — it arrives as a new handler, a new
// aggregate, a new debugging query written against monitor.events by someone who
// had no reason to know a chokepoint existed. It returns rows, it looks correct,
// and nothing anywhere goes red.
//
// So this walks the packages that read events and asserts a syntactic property:
// a function that names the events table must also name a scoping helper. It is
// deliberately dumb — it proves nothing about correctness, only that the author
// was routed to the right place — and it fails with an explanation rather than
// an assertion, because the person it fails for is by definition someone who has
// not read any of this.

// eventsReadPackages are the package directories audited. alerts is included
// specifically so that its known, deliberate exemption stays VISIBLE here rather
// than being invisible by omission — an exemption nobody can see is one nobody
// re-examines.
// The root package is included for main()'s boot probe: leaving it out would
// make this audit's coverage a matter of which directories someone happened to
// list, rather than every package that names the table.
var eventsReadPackages = []string{"..", "../services", "../routes", "../issues", "../alerts"}

// scopingHelpers are the identifiers that count as "this function was routed
// through a chokepoint". Naming any of them is enough: they all terminate in
// scope.ProjectPredicate, which is the only thing that actually builds the
// predicate.
var scopingHelpers = map[string]bool{
	"ProjectPredicate":     true,
	"selectEvents":         true,
	"buildEventWhere":      true,
	"selectIssueEvents":    true,
	"queryEventsByIssueID": true,
}

// exemptFunctions are the functions allowed to name the events table without a
// scoping helper, each with the reason it is safe. Adding an entry here is a
// deliberate act that shows up in review; forgetting to scope a new function is
// not, which is the whole asymmetry this test creates.
var exemptFunctions = map[string]string{
	// Returns the qualified table NAME and issues no query of its own. It is the
	// thing the chokepoints are built on top of, so requiring it to call one
	// would be circular.
	"eventsTable": "returns the table name; reads nothing",

	// The boot probe in main.go, which reads the events table to prove the
	// schema exists before the process starts accepting writes. It carries
	// WHERE 1 = 0 and returns no rows by construction, so there is no data for
	// a project predicate to scope — and it runs before any request exists.
	"main": "boot schema probe; WHERE 1 = 0 returns no rows",

	// queryAggForRange (alerts/evaluator.go) WAS exempt here, on the grounds
	// that alert evaluation runs on a timer with no request context. That was
	// only half true — EvaluateRuleNow reaches it from POST
	// /v1/alert-rules/{id}/test with a real request context — so it now calls
	// scope.ProjectPredicate and no longer needs an exemption. The entry is
	// deleted rather than left to rot, exactly as its own comment demanded.
	//
	// Worth noting what that near-miss says about this list: an exemption is a
	// claim about a function's CALLERS, and this file cannot check one. When you
	// add an entry, name the callers you checked.
}

// TestEveryEventsReadIsRoutedThroughAChokepoint fails when a function reads
// monitor.events without going through a scoping helper.
func TestEveryEventsReadIsRoutedThroughAChokepoint(t *testing.T) {
	fset := token.NewFileSet()

	for _, dir := range eventsReadPackages {
		pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatalf("failed to parse %s: %v", dir, err)
		}

		for _, pkg := range pkgs {
			for filename, file := range pkg.Files {
				ast.Inspect(file, func(n ast.Node) bool {
					fn, ok := n.(*ast.FuncDecl)
					if !ok || fn.Body == nil {
						return true
					}
					if !namesEventsTable(fn.Body) {
						return true
					}
					if reason, exempt := exemptFunctions[fn.Name.Name]; exempt {
						t.Logf("%s: %s is exempt (%s)", filepath.Base(filename), fn.Name.Name, reason)
						return true
					}
					if namesScopingHelper(fn.Body) {
						return true
					}

					t.Errorf(
						"%s: %s reads monitor.events but never calls a scoping helper.\n"+
							"\tEvery read of the events table must carry the project predicate, or it returns\n"+
							"\tevery project's data to whoever asked. Build the query through selectEvents\n"+
							"\t(services/query.go), buildEventWhere (services/analytics.go) or selectIssueEvents\n"+
							"\t(routes/issues.go) — or, if this genuinely cannot be scoped, add it to\n"+
							"\texemptFunctions in scope/chokepoint_test.go with the reason.",
						filepath.Base(filename), fn.Name.Name,
					)
					return true
				})
			}
		}
	}
}

// namesEventsTable reports whether a function body refers to the events table,
// either through the eventsTable() helper or as a literal ".events" in SQL text.
// Both forms are checked because the hand-written string is the one that skips
// every builder — and therefore the one most likely to skip the predicate too.
func namesEventsTable(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.Ident:
			if node.Name == "eventsTable" {
				found = true
			}
		case *ast.BasicLit:
			if node.Kind == token.STRING && strings.Contains(node.Value, ".events") {
				found = true
			}
		}
		return !found
	})
	return found
}

// namesScopingHelper reports whether a function body calls one of the helpers
// that attaches the project predicate.
func namesScopingHelper(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok && scopingHelpers[ident.Name] {
			found = true
		}
		return !found
	})
	return found
}
