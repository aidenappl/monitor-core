package routes

import (
	"encoding/json"
	"net/http"

	"github.com/aidenappl/monitor-core/buildinfo"
	"github.com/aidenappl/monitor-core/env"
)

// InstallID identifies the database this process is attached to. Set from
// main.go after bootstrap.EnsureInstallID; empty on a process that failed to
// resolve one, which is reported honestly rather than hidden.
var InstallID string

// SchemaState is what the two migration runners applied, captured ONCE at boot.
//
// Computed at boot rather than per request because it cannot change while the
// process runs — both runners execute before the server starts listening — and
// /version exists to be polled by a drift checker.
type SchemaState struct {
	MariaDBApplied   int    `json:"mariadb_applied"`
	MariaDBLatest    string `json:"mariadb_latest"`
	ClickHouseFiles  int    `json:"clickhouse_files"`
	ClickHouseLatest string `json:"clickhouse_latest"`
}

// Schema is populated from main.go. Zero values mean "not established" — on a
// control plane running MON_ROLE=app there is no ClickHouse at all, and saying
// zero is more honest than omitting the field and letting a reader assume.
var Schema SchemaState

// versionResponse is deliberately flat and unenveloped, exactly like /health.
// The probe and any drift checker read it without unwrapping, and monitor-mcp's
// scope resolver already special-cases /health for the same reason.
type versionResponse struct {
	buildinfo.Info
	Role      string      `json:"role"`
	Zone      string      `json:"zone"`
	InstallID string      `json:"install_id"`
	Schema    SchemaState `json:"schema"`
}

// VersionHandler answers "which build is this, and which database is it on".
//
// Nothing in monitor-core could answer that before. /health reported status,
// queue counters, store booleans, role and zone; /ready reported less; there was
// no /version. Combined with CI that redeploys exactly ONE container — a single
// Lattice deploy token with `?container=monitor-core` — a zone could be any
// number of commits and migrations behind the control plane with no query,
// endpoint or log line that revealed it. This is the endpoint that makes fleet
// drift a fact you can poll for.
//
// UNAUTHENTICATED, and registered in every role beside /health. The same
// argument that keeps `zone` public applies: a build sha and a migration count
// are not secrets — the sha is in a public repo — and withholding them would
// mean the one caller that needs them, a control plane checking whether its
// zones are current, cannot ask.
func VersionHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(versionResponse{
		Info:      buildinfo.Get(),
		Role:      string(env.MonRole),
		Zone:      env.ZoneSlug,
		InstallID: InstallID,
		Schema:    Schema,
	})
}
