package alerts

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/query"
)

// service_groups moved to MariaDB in migration 122, so InitServiceGroups and the
// CRUD that used to be here are now query/service_groups.query.go against
// structs.ServiceGroup. What is left is the one thing that was never SQL.

// ResolveServiceGroups returns the ids of every group containing a service.
//
// The membership test happens in Go rather than in SQL because `services` is a
// JSON array in a column, and MariaDB cannot index inside one — a
// JSON_CONTAINS predicate would be a full scan wearing a WHERE clause. Over a
// handful of groups the loop is the honest version of the same work.
//
// The ctx argument is now unused, because the read it used to make was a
// ClickHouse call and the replacement is a MariaDB one through db.SQL. It is
// kept because every caller is on a request or evaluation path that already
// holds one, and removing it would churn those call sites for a signature that
// wants the context back the moment this read is given a timeout.
func ResolveServiceGroups(ctx context.Context, service string) ([]string, error) {
	_ = ctx

	groups, err := query.ListServiceGroups(db.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve service groups: %w", err)
	}

	var matchingIDs []string
	for _, sg := range groups {
		var services []string
		// A group whose `services` will not parse is SKIPPED, not reported —
		// preserved from the original. Migration 122 makes it a JSON column so
		// an unparseable value can no longer be written; this branch now only
		// covers rows written before that, and a valid JSON value of the wrong
		// SHAPE (an object where an array is expected), which json_valid still
		// accepts.
		if err := json.Unmarshal([]byte(sg.Services), &services); err != nil {
			continue
		}
		for _, s := range services {
			if s == service {
				matchingIDs = append(matchingIDs, sg.ID)
				break
			}
		}
	}
	return matchingIDs, nil
}
