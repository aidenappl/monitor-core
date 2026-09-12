// Package buildinfo answers "which build is this, and which database is it
// talking to" — the two questions nothing in monitor-core could answer.
//
// WHY THIS EXISTS. CI deploys a single container (`?container=monitor-core`
// against one Lattice deploy token), so a push to main redeploys the control
// plane's stack and nothing else. Zone `appleby` runs whatever image it was last
// handed, indefinitely. And no endpoint reported a version: /health returned
// status, queue counters, store booleans, role and zone; /ready returned less.
// So a zone could be an arbitrary number of commits and migrations behind with
// NO query, endpoint or log line that revealed it.
//
// It is a separate package rather than living in main so that routes/ can read
// it without importing the command.
package buildinfo

import (
	"runtime"
	"runtime/debug"
	"sync"
)

// Set at link time:
//
//	go build -ldflags="-X github.com/aidenappl/monitor-core/buildinfo.Commit=$(git rev-parse HEAD)"
//
// The Dockerfile passes GIT_SHA/GIT_REF through as build args. All three fall
// back to values read from the embedded build info, so a plain `go build` — and
// therefore every local run and every test — still reports something true rather
// than an empty string.
var (
	Version   = ""
	Commit    = ""
	BuildTime = ""
)

var (
	once     sync.Once
	resolved Info
)

// Info is the immutable half of what /version reports. The mutable half —
// install id, schema counts, role, zone — is assembled by the caller, because
// this package must not depend on db or env.
type Info struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"built_at"`
	Go        string `json:"go"`
}

// Get resolves the build identity once and caches it. Values are immutable for
// the process lifetime, so recomputing them per request would be pure waste on
// an endpoint that exists to be polled.
func Get() Info {
	once.Do(func() {
		resolved = Info{
			Service:   "monitor-core",
			Version:   Version,
			Commit:    Commit,
			BuildTime: BuildTime,
			Go:        runtime.Version(),
		}

		// A binary built without -ldflags still knows its own revision, because
		// the toolchain stamps VCS data into the build info. This is what makes
		// the endpoint useful for a locally-built image, which is exactly the
		// case where somebody is asking "is this actually my code?".
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if resolved.Commit == "" {
					resolved.Commit = setting.Value
				}
			case "vcs.time":
				if resolved.BuildTime == "" {
					resolved.BuildTime = setting.Value
				}
			case "vcs.modified":
				// A dirty tree is worth saying out loud: it means the commit
				// below does NOT fully describe what is running, which is the
				// single most misleading thing a version endpoint can imply.
				if setting.Value == "true" && resolved.Commit != "" {
					resolved.Commit += "-dirty"
				}
			}
		}
		if resolved.Version == "" && info.Main.Version != "" {
			resolved.Version = info.Main.Version
		}
	})
	return resolved
}

// ShortCommit is the 7-character form for logs and UI columns. Returns whatever
// it has when the commit is shorter or absent, rather than panicking on a slice.
func ShortCommit() string {
	c := Get().Commit
	if len(c) > 7 {
		return c[:7]
	}
	return c
}
