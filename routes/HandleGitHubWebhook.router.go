package routes

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/github"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/structs"
)

// maxWebhookBody caps the payload read. GitHub's pull_request deliveries run to
// tens of KB; 5 MiB is generous while still bounding an attacker who can reach
// the endpoint (the body must be read in full BEFORE the signature can be
// checked, so this limit applies to unauthenticated input by necessity).
const maxWebhookBody = 5 << 20

// webhookActorLabel attributes webhook-driven timeline entries. Deliveries carry
// a GitHub actor, but the writer here is Monitor reacting to an event, not that
// person acting in Monitor — conflating them would put someone's name against a
// change they never made here.
const webhookActorLabel = "github-webhook"

type githubWebhookPayload struct {
	Action      string `json:"action"`
	PullRequest *struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
		State  string `json:"state"`
		Merged bool   `json:"merged"`
		User   struct {
			Login string `json:"login"`
		} `json:"user"`
		HTMLURL string `json:"html_url"`
		Base    struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"pull_request"`
	Repository *struct {
		Name  string `json:"name"`
		Owner struct {
			Login string `json:"login"`
		} `json:"owner"`
	} `json:"repository"`
}

// HandleGitHubWebhook receives GitHub deliveries and updates linked PR state.
//
// Mounted on the ROOT router, deliberately outside /v1, so QueryAuthMiddleware
// never runs on it — GitHub cannot present an API key. HMAC signature
// verification is the authentication, which is why an unconfigured secret
// rejects rather than allows.
//
// It NEVER changes issue status. A merged PR appends a pr_merged timeline entry
// and refreshes the chip; resolving stays a deliberate human or agent action.
// GitHub itself only honours closing keywords on the default branch, and
// silently resolving someone's issue is a surprising thing for a webhook to do.
//
// ---------------------------------------------------------------------------
// TENANCY: this is the one handler in the service with NO project of its own.
//
// Every /v1 route resolves a tenant with requireProject, because a credential
// names one. A GitHub delivery names owner/repo and nothing else, so there is no
// requireProject here and there cannot be — which makes it worth stating exactly
// which of its three database touches are cross-project and why each is sound.
//
//  1. query.ListIssueIDsForPR and query.UpdateIssueLinkState span every project,
//     by design. monitor.issue_links is TenancyDerived (structs/tenancy.go): a
//     link row exists only because somebody who could already read that issue
//     created it, and the fact being written — this PR is now merged, titled
//     this, by this author — is GitHub's, identical for every project that
//     linked it. Scoping them would mean one delivery refreshing one tenant's
//     chip and leaving the others stale, which is worse and no safer.
//
//  2. query.ListServicesForRepo spans every project because it must: the repo is
//     the only key the delivery carries. It returns (project, service) PAIRS so
//     what happens next can be scoped — see affectedServicesForIssue, which is
//     where the tenant is finally pinned down, one issue at a time.
//
// The rule the whole handler follows: a lookup keyed by something GitHub knows
// may cross projects; anything WRITTEN onto an issue must be narrowed to that
// issue's own project first.
// ---------------------------------------------------------------------------
func HandleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	if err := github.VerifySignature(body, r.Header.Get(github.SignatureHeader)); err != nil {
		// 401 for both an unconfigured secret and a bad signature. The caller is
		// GitHub, which retries on failure and needs no diagnostic detail; a
		// forger gets none either.
		log.Printf("github webhook: rejected delivery: %v", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	event := r.Header.Get("X-GitHub-Event")

	// ping is GitHub's handshake when a webhook is first configured. Answering
	// 200 is what turns the green tick on in the repo settings.
	if event == "ping" {
		writeWebhookOK(w, "pong")
		return
	}
	if event != "pull_request" {
		// Unknown events are acknowledged, not errored: a 4xx makes GitHub retry
		// and eventually disable the webhook for something we simply ignore.
		writeWebhookOK(w, "ignored")
		return
	}

	var payload githubWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		log.Printf("github webhook: malformed pull_request payload: %v", err)
		writeWebhookOK(w, "ignored")
		return
	}
	if payload.PullRequest == nil || payload.Repository == nil {
		writeWebhookOK(w, "ignored")
		return
	}

	owner := payload.Repository.Owner.Login
	repo := payload.Repository.Name
	number := payload.PullRequest.Number

	// Only touch PRs something is actually linked to. Most deliveries from a busy
	// repo concern nothing Monitor tracks, and this is the cheap early exit.
	issueIDs, err := query.ListIssueIDsForPR(db.SQL, owner, repo, number)
	if err != nil {
		log.Printf("github webhook: failed to resolve linked issues for %s/%s#%d: %v", owner, repo, number, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if len(issueIDs) == 0 {
		writeWebhookOK(w, "no linked issues")
		return
	}

	state := payload.PullRequest.State
	merged := payload.PullRequest.Merged
	title := payload.PullRequest.Title
	author := payload.PullRequest.User.Login

	if _, err := query.UpdateIssueLinkState(db.SQL, query.UpdateIssueLinkStateRequest{
		Owner:  owner,
		Repo:   repo,
		Number: number,
		State:  &state,
		Merged: &merged,
		Title:  &title,
		Author: &author,
	}); err != nil {
		log.Printf("github webhook: failed to update link state for %s/%s#%d: %v", owner, repo, number, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	entryType, ok := timelineTypeForPRAction(payload.Action, merged)
	if !ok {
		// State was refreshed above; this action just has no timeline meaning
		// (a `synchronize` is a push to the branch, not a lifecycle change).
		writeWebhookOK(w, "state updated")
		return
	}

	// Keyed on the action and the PR, so GitHub's at-least-once redelivery — and
	// the retries it performs on any non-2xx — collapse to one entry instead of
	// stacking duplicates on the timeline.
	dedupeKey := fmt.Sprintf("gh:%s/%s#%d:%s", owner, repo, number, entryType)
	body_ := prTimelineBody(entryType, owner, repo, number, title)

	// Which services this repo builds, and IN WHICH PROJECT each mapping lives.
	// Best-effort context on the entry: a merge in one repo can affect several
	// services (auth-service-v1 and -v2 share a repo), and the reader of a
	// timeline usually wants to know which. A failure here must not cost the
	// entry itself.
	//
	// This lookup is deliberately CROSS-PROJECT and cannot be otherwise: a
	// delivery names owner/repo and nothing else, so there is no tenant to scope
	// by at this point — which is why migration 133 leaves
	// idx_service_repos_lookup (provider, owner, repo) alone while changing the
	// primary key. The scoping happens one step later, per issue.
	mapped, err := query.ListServicesForRepo(db.SQL, owner, repo)
	if err != nil {
		log.Printf("github webhook: failed to resolve services for %s/%s: %v", owner, repo, err)
		mapped = nil
	}

	// Grouped by project so each issue can be annotated with ITS OWN project's
	// services. Before 133 this was a flat []string, and a repository mapped in
	// two projects put both tenants' service inventories on both tenants'
	// timelines — one project's service names leaking into another's issue
	// history, through a delivery neither of them can see or audit.
	servicesByProject := map[string][]string{}
	for _, m := range mapped {
		servicesByProject[m.Project] = append(servicesByProject[m.Project], m.Service)
	}

	for _, issueID := range issueIDs {
		affected := affectedServicesForIssue(db.SQL, issueID, servicesByProject)

		if _, err := query.AppendTimelineEntry(db.SQL, query.AppendTimelineEntryRequest{
			IssueID: issueID,
			Type:    entryType,
			Actor:   structs.SystemActor(webhookActorLabel),
			Body:    &body_,
			Metadata: map[string]any{
				"owner":             owner,
				"repo":              repo,
				"number":            number,
				"state":             state,
				"merged":            merged,
				"author":            author,
				"base_branch":       payload.PullRequest.Base.Ref,
				"url":               payload.PullRequest.HTMLURL,
				"affected_services": affected,
			},
			DedupeKey: &dedupeKey,
		}); err != nil {
			// Log and continue: one issue's timeline failing must not stop the
			// others, and the link state is already updated either way.
			log.Printf("github webhook: failed to append %s to issue %s: %v", entryType, issueID, err)
		}
	}

	writeWebhookOK(w, fmt.Sprintf("updated %d issue(s)", len(issueIDs)))
}

// affectedServicesForIssue narrows a repository's mapped services to the ones
// belonging to the SAME project as the issue about to be annotated.
//
// HOW THE PROJECT IS FOUND, given the delivery carries none. query.GetIssue
// treats the project as part of the LOOKUP rather than as a check applied after
// it, so an issue that belongs to another project reads as (nil, nil). Asking for
// the issue once per candidate project therefore identifies its project by
// construction: exactly one can return a row, because an issue id exists in
// exactly one project. Map iteration order is unspecified and does not matter for
// the same reason — there is no second candidate that could also match.
//
// The candidates are only the projects this repository is mapped in, which is one
// in every install that has not split its zone and a handful at worst. This runs
// once per linked issue on a webhook delivery, not on a read path.
//
// WHAT IT PREVENTS: `affected_services` is stored in the timeline entry's
// metadata and rendered beside the merge. Passing every mapped service would show
// project B's service names on project A's issue whenever both map the same
// repository — the exact case migration 133's composite primary key was added to
// make expressible.
//
// Returning nil is the normal answer when nothing matches: the repository may be
// mapped in no project at all, or only in projects that own none of these issues.
// The entry is still written — `affected_services` has always been decoration,
// and losing it must not cost the timeline entry it decorates.
func affectedServicesForIssue(engine db.Queryable, issueID string, servicesByProject map[string][]string) []string {
	for project, services := range servicesByProject {
		issue, err := query.GetIssue(engine, project, issueID)
		if err != nil {
			// Best-effort, like every other enrichment on this path: a failed
			// probe costs the annotation, never the entry.
			log.Printf("github webhook: failed to resolve issue %s in project %s: %v", issueID, project, err)
			continue
		}
		if issue != nil {
			return services
		}
	}
	return nil
}

// timelineTypeForPRAction maps a pull_request action to a timeline entry type,
// reporting false when the action has no timeline meaning.
//
// `closed` is two different events: GitHub distinguishes a merge from an
// abandonment only by the `merged` boolean, not by the action.
func timelineTypeForPRAction(action string, merged bool) (structs.TimelineEntryType, bool) {
	switch action {
	case "closed":
		if merged {
			return structs.TimelinePRMerged, true
		}
		return structs.TimelinePRClosed, true
	case "reopened":
		return structs.TimelinePRReopened, true
	}
	return "", false
}

func prTimelineBody(t structs.TimelineEntryType, owner, repo string, number int, title string) string {
	verb := "updated"
	switch t {
	case structs.TimelinePRMerged:
		verb = "merged"
	case structs.TimelinePRClosed:
		verb = "closed without merging"
	case structs.TimelinePRReopened:
		verb = "reopened"
	}
	return fmt.Sprintf("%s/%s#%d (%s) was %s.", owner, repo, number, title, verb)
}

func writeWebhookOK(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": message})
}
