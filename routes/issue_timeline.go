package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/github"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/aidenappl/monitor-core/telemetry"
	"github.com/gorilla/mux"
)

// requireIssue resolves the {id} path variable to an issue THE CALLER'S PROJECT
// OWNS, writing the response and returning nil when it cannot.
//
// Every handler in this file — the timeline, the occurrence history, all three
// comment verbs and all three link verbs — starts here, which makes this one
// function the tenancy boundary for the whole of an issue's sub-resources. That
// concentration is worth stating plainly, because the same property cuts both
// ways: before the project was threaded through, a single unscoped GetIssue here
// meant eight endpoints served another tenant's issue, and the occurrence
// history among them is per-day event counts read out of ClickHouse.
func requireIssue(w http.ResponseWriter, r *http.Request) *structs.Issue {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return nil
	}

	project, ok := requireProject(w, r)
	if !ok {
		return nil
	}

	issue, err := query.GetIssue(db.SQL, project, id)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to fetch issue", err)
		return nil
	}
	if issue == nil {
		responder.Error(w, http.StatusNotFound, "issue not found")
		return nil
	}
	return issue
}

// requireActor pulls the caller's identity from context.
//
// Every write in this file records who made it, so a missing actor is a hard
// failure rather than something to paper over with an "unknown" label — an
// unattributable entry is worse than no entry.
func requireActor(w http.ResponseWriter, r *http.Request) *structs.Actor {
	actor, ok := middleware.GetActor(r.Context())
	if !ok || actor == nil {
		responder.Error(w, http.StatusUnauthorized, "authentication required")
		return nil
	}
	return actor
}

// canModifyComment reports whether actor may edit or delete a comment.
//
// Until this existed, any authenticated caller could rewrite or delete anyone's
// comment — including the notes agents leave on issues as they work, which are
// the record of what was tried. The author may change their own; an admin
// session and the master key (break-glass) may moderate anything.
func canModifyComment(actor *structs.Actor, user *structs.User, entry *structs.IssueTimelineEntry) bool {
	if actor == nil || entry == nil {
		return false
	}
	if user != nil && user.Role == "admin" {
		return true
	}
	if actor.Kind == structs.ActorKindSystem {
		return true
	}
	if actor.UserID != nil && entry.ActorUserID != nil && *actor.UserID == *entry.ActorUserID {
		return true
	}
	if actor.APIKeyID != nil && entry.ActorAPIKeyID != nil && *actor.APIKeyID == *entry.ActorAPIKeyID {
		return true
	}
	return false
}

// requireOwnComment loads the comment and refuses (403) an actor who did not
// write it. It writes the response and returns false on any refusal.
func requireOwnComment(w http.ResponseWriter, r *http.Request, issueID string, entryID int64, actor *structs.Actor) bool {
	entry, err := query.GetTimelineEntry(db.SQL, issueID, entryID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load comment", err)
		return false
	}
	if entry == nil || entry.Type != structs.TimelineComment || entry.DeletedAt != nil {
		responder.Error(w, http.StatusNotFound, "comment not found")
		return false
	}
	user, _ := middleware.GetUserFromContext(r.Context())
	if !canModifyComment(actor, user, entry) {
		responder.Error(w, http.StatusForbidden, "only the comment's author or an admin can change it")
		return false
	}
	return true
}

// HandleGetIssueTimeline returns one issue's activity feed, oldest first.
func HandleGetIssueTimeline(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}

	req := query.ListTimelineRequest{IssueID: issue.ID}
	q := r.URL.Query()

	if v := q.Get("type"); v != "" {
		entryType := structs.TimelineEntryType(v)
		if !entryType.IsValid() {
			responder.Error(w, http.StatusBadRequest, fmt.Sprintf("invalid timeline type %q", v))
			return
		}
		req.Type = &entryType
	}
	if v := q.Get("include_deleted"); v != "" {
		includeDeleted, err := strconv.ParseBool(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid include_deleted (expected true or false)")
			return
		}
		req.IncludeDeleted = includeDeleted
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid limit")
			return
		}
		req.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, "invalid offset")
			return
		}
		req.Offset = n
	}

	total, err := query.CountTimeline(db.SQL, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to count timeline", err)
		return
	}

	entries, err := query.ListTimeline(db.SQL, req)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list timeline", err)
		return
	}
	if entries == nil {
		entries = []structs.IssueTimelineEntry{}
	}
	responder.NewWithCount(w, entries, total, "", "")
}

// HandleGetIssueHistory returns the per-day occurrence sparkline.
//
// Reads the no-TTL rollup rather than raw events, so an issue that first fired
// months ago still reports when and how often — the events themselves are long
// gone under the 30-day TTL.
func HandleGetIssueHistory(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}

	to := time.Now().UTC()
	from := to.AddDate(0, 0, -defaultHistoryDays)

	if v := r.URL.Query().Get("from"); v != "" {
		t, err := parseTimeParam(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, fmt.Sprintf("invalid from: %v", err))
			return
		}
		from = t
	}
	if v := r.URL.Query().Get("to"); v != "" {
		t, err := parseTimeParam(v)
		if err != nil {
			responder.Error(w, http.StatusBadRequest, fmt.Sprintf("invalid to: %v", err))
			return
		}
		to = t
	}

	history, err := issues.GetOccurrenceHistory(r.Context(), issue.ID, from, to)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to load occurrence history", err)
		return
	}
	responder.New(w, history)
}

// HandleAddIssueComment appends a comment, optionally idempotently.
//
// This is the primary path for an agent leaving notes as it works. A dedupe_key
// makes the write idempotent per (issue, key): reposting the same body is a
// no-op, a changed body edits in place. That is what lets an agent retry a task,
// or revisit an issue across sessions, without leaving five copies of one note.
func HandleAddIssueComment(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}
	actor := requireActor(w, r)
	if actor == nil {
		return
	}

	var body struct {
		Body      string  `json:"body"`
		DedupeKey *string `json:"dedupe_key"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxIssueBody)).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validateCommentBody(body.Body); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	entry, err := query.AppendTimelineEntry(db.SQL, query.AppendTimelineEntryRequest{
		IssueID:   issue.ID,
		Type:      structs.TimelineComment,
		Actor:     actor,
		Body:      &body.Body,
		DedupeKey: body.DedupeKey,
	})
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to add comment", err)
		return
	}
	responder.New(w, entry, "comment added")
}

// MAX_COMMENT_RUNES caps a comment body, counted in characters (runes) rather
// than bytes so the limit monitor-web shows matches what a person typed. Well
// inside issue_timeline.body's TEXT column even at four bytes a rune.
const MAX_COMMENT_RUNES = 10000

// validateCommentBody is the create and edit rule for a comment body: not blank,
// at most MAX_COMMENT_RUNES characters. The body is stored as sent; only the
// blank check trims.
func validateCommentBody(body string) error {
	if strings.TrimSpace(body) == "" {
		return errors.New("body is required")
	}
	if utf8.RuneCountInString(body) > MAX_COMMENT_RUNES {
		return fmt.Errorf("body must be at most %d characters", MAX_COMMENT_RUNES)
	}
	return nil
}

// HandleEditIssueComment replaces a comment's body.
//
// Only comments are editable — a status transition or a PR merge is a historical
// fact, not a draft.
func HandleEditIssueComment(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}
	actor := requireActor(w, r)
	if actor == nil {
		return
	}

	entryID, err := strconv.ParseInt(mux.Vars(r)["commentID"], 10, 64)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid comment id")
		return
	}
	if !requireOwnComment(w, r, issue.ID, entryID, actor) {
		return
	}

	var body struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxIssueBody)).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validateCommentBody(body.Body); err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	entry, err := query.EditComment(db.SQL, issue.ID, entryID, body.Body)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to edit comment", err)
		return
	}
	if entry == nil {
		responder.Error(w, http.StatusNotFound, "comment not found")
		return
	}
	responder.New(w, entry, "comment updated")
}

// HandleDeleteIssueComment soft-deletes a comment, keeping the row so the
// timeline's shape and the audit trail survive.
func HandleDeleteIssueComment(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}
	actor := requireActor(w, r)
	if actor == nil {
		return
	}

	entryID, err := strconv.ParseInt(mux.Vars(r)["commentID"], 10, 64)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid comment id")
		return
	}
	if !requireOwnComment(w, r, issue.ID, entryID, actor) {
		return
	}

	deleted, err := query.SoftDeleteComment(db.SQL, issue.ID, entryID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete comment", err)
		return
	}
	if !deleted {
		responder.Error(w, http.StatusNotFound, "comment not found")
		return
	}
	responder.New(w, nil, "comment deleted")
}

// HandleListIssueLinks returns an issue's external links.
func HandleListIssueLinks(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}

	links, err := query.ListIssueLinks(db.SQL, issue.ID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to list links", err)
		return
	}
	if links == nil {
		links = []structs.IssueLink{}
	}
	responder.New(w, links)
}

// HandleCreateIssueLink links a GitHub PR, issue or commit to this issue.
//
// The input may be a full URL, "owner/repo#42", or a bare "#42" — the last
// resolved against the issue's service→repository mapping, which is what makes
// the shorthand usable at all.
func HandleCreateIssueLink(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}
	actor := requireActor(w, r)
	if actor == nil {
		return
	}

	var body struct {
		// URL and Ref are accepted interchangeably so a caller can paste a link
		// or type a shorthand without picking the right field name.
		URL string `json:"url"`
		Ref string `json:"ref"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxIssueBody)).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	input := body.URL
	if input == "" {
		input = body.Ref
	}
	if strings.TrimSpace(input) == "" {
		responder.Error(w, http.StatusBadRequest, "url (or ref) is required")
		return
	}

	// The service's repository is the fallback for a bare number. Failing to
	// resolve it is not fatal — a full URL needs no fallback at all.
	//
	// Scoped by the ISSUE'S project (migration 133 made it half the mapping's
	// primary key), taken from the row rather than re-read off the context: it is
	// the same value by construction, since requireIssue resolved this issue
	// through a project-scoped lookup, and reading it here keeps the fallback
	// bound to the issue it is a fallback FOR. Unscoped, `#42` typed against a
	// service another tenant also runs would resolve to THEIR repository and mint
	// a link to a pull request in it.
	var fallbackOwner, fallbackRepo string
	if repo, err := query.GetServiceRepo(db.SQL, issue.Project, issue.Service); err == nil && repo != nil {
		fallbackOwner, fallbackRepo = repo.Owner, repo.Repo
	}

	ref, err := github.ParseRef(input, fallbackOwner, fallbackRepo)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	// Live state is best-effort. GitHub being unreachable, rate-limited, or
	// simply unconfigured for this org must never fail the link — it degrades to
	// a bare chip that the webhook can fill in later.
	createReq := query.CreateIssueLinkRequest{
		IssueID:       issue.ID,
		Kind:          ref.Kind,
		URL:           ref.URL(),
		Owner:         &ref.Owner,
		Repo:          &ref.Repo,
		LinkedByLabel: actor.Label,
	}
	if ref.Number > 0 {
		n := ref.Number
		createReq.Number = &n
	}
	if resource, err := github.Fetch(r.Context(), ref); err != nil {
		telemetry.WarnErr(r.Context(), "issue.link.github.fetch.failed", err, map[string]any{
			"issue_id":   issue.ID,
			"kind":       string(ref.Kind),
			"owner":      ref.Owner,
			"repo":       ref.Repo,
			"number":     ref.Number,
			"dependency": "github",
			"outcome":    "linked without live state; the webhook fills it in later",
		})
	} else if resource != nil {
		createReq.Title = &resource.Title
		createReq.State = &resource.State
		createReq.Merged = &resource.Merged
		createReq.Author = &resource.Author
	}

	link, err := query.CreateIssueLink(db.SQL, createReq)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to create link", err)
		return
	}

	entryBody := fmt.Sprintf("Linked %s.", ref.URL())
	if _, err := query.AppendTimelineEntry(db.SQL, query.AppendTimelineEntryRequest{
		IssueID: issue.ID,
		Type:    structs.TimelinePRLinked,
		Actor:   actor,
		Body:    &entryBody,
		Metadata: map[string]any{
			"url": ref.URL(), "owner": ref.Owner, "repo": ref.Repo,
			"number": ref.Number, "kind": string(ref.Kind),
		},
	}); err != nil {
		telemetry.WarnErr(r.Context(), "issue.timeline.append.failed", &db.SQLError{Op: "issues.timeline.append", Err: err}, map[string]any{
			"issue_id":   issue.ID,
			"entry_type": string(structs.TimelinePRLinked),
			"outcome":    "the link was created, but has no timeline entry",
		})
	}

	telemetry.Info(r.Context(), "issue.link.created", map[string]any{
		"issue_id": issue.ID,
		"project":  issue.Project,
		"kind":     string(ref.Kind),
		"owner":    ref.Owner,
		"repo":     ref.Repo,
	})
	responder.New(w, link, "link created")
}

// HandleDeleteIssueLink removes a link.
func HandleDeleteIssueLink(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}
	actor := requireActor(w, r)
	if actor == nil {
		return
	}

	linkID, err := strconv.ParseInt(mux.Vars(r)["linkID"], 10, 64)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid link id")
		return
	}

	deleted, err := query.DeleteIssueLink(db.SQL, issue.ID, linkID)
	if err != nil {
		responder.ErrorWithCause(w, http.StatusInternalServerError, "failed to delete link", err)
		return
	}
	if !deleted {
		responder.Error(w, http.StatusNotFound, "link not found")
		return
	}

	entryBody := "Link removed."
	if _, err := query.AppendTimelineEntry(db.SQL, query.AppendTimelineEntryRequest{
		IssueID:  issue.ID,
		Type:     structs.TimelinePRUnlinked,
		Actor:    actor,
		Body:     &entryBody,
		Metadata: map[string]any{"link_id": linkID},
	}); err != nil {
		telemetry.WarnErr(r.Context(), "issue.timeline.append.failed", &db.SQLError{Op: "issues.timeline.append", Err: err}, map[string]any{
			"issue_id":   issue.ID,
			"entry_type": string(structs.TimelinePRUnlinked),
			"outcome":    "the link was removed, but has no timeline entry",
		})
	}

	telemetry.Info(r.Context(), "issue.link.deleted", map[string]any{
		"issue_id": issue.ID,
		"project":  issue.Project,
		"link_id":  linkID,
	})
	responder.New(w, nil, "link removed")
}
