package structs

import "time"

// ServiceRepo maps a reporting service to the source repository it is built
// from. Lives in MariaDB (monitor.service_repos).
//
// The relationship is many-to-one and deliberately explicit. Monitor's estate
// runs several versions of one service side by side — `auth-service-v1` and
// `auth-service-v2`, `team-service` at v1/v2/v3 — each reporting under its own
// name while sharing a single repo. Deriving the repo by stripping a `-vN`
// suffix would silently mislabel anything not following that convention.
//
// A service with no mapping is simply unmapped: links still work, they just
// cannot be resolved from a bare issue/PR number.
type ServiceRepo struct {
	Service       string    `json:"service"`
	Provider      string    `json:"provider"`
	Owner         string    `json:"owner"`
	Repo          string    `json:"repo"`
	DefaultBranch *string   `json:"default_branch,omitempty"`
	InsertedAt    time.Time `json:"inserted_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// FullName renders the canonical "owner/repo" identifier.
func (s ServiceRepo) FullName() string { return s.Owner + "/" + s.Repo }

// URL renders the repository's web URL.
func (s ServiceRepo) URL() string { return "https://github.com/" + s.Owner + "/" + s.Repo }
