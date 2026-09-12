package env

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// envReadPattern finds the variables this package actually reads:
// getEnv("X", …), getEnvInt("X", …), getEnvDuration("X", …), getEnvExplicit("X", …).
var envReadPattern = regexp.MustCompile(`getEnv[A-Za-z]*\("([A-Z_0-9]+)"`)

// exampleAssignPattern finds `NAME=value` lines in .env.example, including ones
// commented out with a leading `# ` (Keyring's block is documented that way).
var exampleAssignPattern = regexp.MustCompile(`(?m)^#?\s*([A-Z_0-9]+)=`)

// varsThisPackageReads scrapes env.go and role.go rather than maintaining a
// second list. A hand-kept list is the thing that drifted in the first place.
func varsThisPackageReads(t *testing.T) map[string]bool {
	t.Helper()

	names := map[string]bool{}
	for _, file := range []string{"env.go", "role.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, m := range envReadPattern.FindAllStringSubmatch(string(src), -1) {
			names[m[1]] = true
		}
	}

	// role.go reads MON_ROLE through the ROLE_ENV_VAR constant rather than a
	// literal, so the regex above cannot see it.
	names[ROLE_ENV_VAR] = true

	if len(names) < 10 {
		t.Fatalf("only found %d env reads — the scraper has stopped working, "+
			"and a test that silently stops testing is worse than one that fails", len(names))
	}
	return names
}

func varsDocumentedInExample(t *testing.T) map[string]bool {
	t.Helper()

	src, err := os.ReadFile("../.env.example")
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}

	names := map[string]bool{}
	for _, m := range exampleAssignPattern.FindAllStringSubmatch(string(src), -1) {
		names[m[1]] = true
	}
	return names
}

// TestEnvExampleDocumentsEveryVariable is the half that catches an OMISSION.
//
// The file this replaced was 25 lines and contained none of the fourteen MON_*
// variables — i.e. none of the ones that fail the boot. Someone standing up a
// second zone therefore had no way to learn what it needed except by starting it
// and reading the fatal, once per variable. That is most of why it took hours.
func TestEnvExampleDocumentsEveryVariable(t *testing.T) {
	read := varsThisPackageReads(t)
	documented := varsDocumentedInExample(t)

	for name := range read {
		if !documented[name] {
			t.Errorf(".env.example does not document %s, which env reads. "+
				"Every variable that can change or stop this process must be discoverable "+
				"without starting it.", name)
		}
	}
}

// TestEnvExampleHasNoPhantomVariables is the half that catches a LIE.
//
// The old file advertised `API_KEY=your-secret-key-here`. Nothing has ever read
// API_KEY. A phantom is worse than an omission: an omission leaves you looking,
// while a phantom has you set a value, watch it do nothing, and conclude the
// problem is somewhere else entirely.
//
// KEYRING_* are the deliberate exception — they are read by the Keyring client
// in main.go, not by this package, and they belong in the example because they
// change what every other value in it means.
func TestEnvExampleHasNoPhantomVariables(t *testing.T) {
	read := varsThisPackageReads(t)
	documented := varsDocumentedInExample(t)

	for name := range documented {
		if strings.HasPrefix(name, "KEYRING_") {
			continue
		}
		if !read[name] {
			t.Errorf(".env.example documents %s, which nothing in package env reads. "+
				"Remove it, or if another package reads it, add that package to "+
				"varsThisPackageReads so this test can see it.", name)
		}
	}
}
