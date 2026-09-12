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
// Project is the tenant the mapping belongs to, and it is HALF THE KEY as of
// migration 133 — the primary key was `service` alone, which is only unique
// inside one project's event stream. Two tenants both running a service called
// `api` could not map it to different repositories at all: the second PUT
// overwrote the first, and every issue in both projects then linked to one
// tenant's repository.
type ServiceRepo struct {
	Project       string    `json:"project"`
	Service       string    `json:"service"`
	Provider      string    `json:"provider"`
	Owner         string    `json:"owner"`
	Repo          string    `json:"repo"`
	DefaultBranch *string   `json:"default_branch,omitempty"`
	InsertedAt    time.Time `json:"inserted_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// MappedService is one (project, service) pair a repository is mapped to.
//
// It exists for the reverse lookup — "which services does this repository build"
// — which the GitHub webhook performs with NO tenant in hand, because a delivery
// names owner/repo and nothing else. That lookup therefore spans every project in
// the zone, and a bare service name coming back out of it is not addressable:
// `api` in project A and `api` in project B are different services that a
// []string cannot tell apart. Returning the pair is what lets the caller scope
// what it does next to the project the mapping actually belongs to.
type MappedService struct {
	Project string `json:"project"`
	Service string `json:"service"`
}

// FullName renders the canonical "owner/repo" identifier.
func (s ServiceRepo) FullName() string { return s.Owner + "/" + s.Repo }

// URL renders the repository's web URL.
func (s ServiceRepo) URL() string { return "https://github.com/" + s.Owner + "/" + s.Repo }
