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

	// Which services this repo builds. Best-effort context on the entry: a merge
	// in one repo can affect several services (auth-service-v1 and -v2 share a
	// repo), and the reader of a timeline usually wants to know which. A failure
	// here must not cost the entry itself.
	affected, err := query.ListServicesForRepo(db.SQL, owner, repo)
	if err != nil {
		log.Printf("github webhook: failed to resolve services for %s/%s: %v", owner, repo, err)
		affected = nil
	}

	for _, issueID := range issueIDs {
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
