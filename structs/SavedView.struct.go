package structs

import "time"

// SavedView is a saved query/filter for one dashboard page. Lives in MariaDB
// (monitor.saved_views).
//
// It was `views.View` and moved here with the table (migration 124). The type is
// named SavedView rather than View to match the table and to leave the bare word
// free — `structs.View` next to `structs.Event` reads like a projection of an
// event, which is not what this is.
//
// The json tags are unchanged, so the wire contract is identical: monitor-web
// still sees {id, name, query_params, page, created_at}.
//
// There is no UpdatedAt because there is no update path — a view is created and
// deleted, never edited. QueryParams is an opaque, client-owned blob for the same
// reason Dashboard.Config is.
// Project is the tenant the view belongs to (migration 132). The wire contract
// GAINS this field rather than changing one: monitor-web still sees every key it
// read before, so an older bundle keeps working while a newer one can show which
// project a view came from.
type SavedView struct {
	ID          string    `json:"id"`
	Project     string    `json:"project"`
	Name        string    `json:"name"`
	QueryParams string    `json:"query_params"`
	Page        string    `json:"page"`
	CreatedAt   time.Time `json:"created_at"`
}
