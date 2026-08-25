package routes

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/github"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/query"
	"github.com/aidenappl/monitor-core/responder"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

// requireIssue resolves the {id} path variable to an existing issue, writing the
// response and returning nil when it cannot.
func requireIssue(w http.ResponseWriter, r *http.Request) *structs.Issue {
	id := mux.Vars(r)["id"]
	if id == "" {
		responder.Error(w, http.StatusBadRequest, "id is required")
		return nil
	}

	issue, err := query.GetIssue(db.SQL, id)
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
	if strings.TrimSpace(body.Body) == "" {
		responder.Error(w, http.StatusBadRequest, "body is required")
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

// HandleEditIssueComment replaces a comment's body.
//
// Only comments are editable — a status transition or a PR merge is a historical
// fact, not a draft.
func HandleEditIssueComment(w http.ResponseWriter, r *http.Request) {
	issue := requireIssue(w, r)
	if issue == nil {
		return
	}
	if actor := requireActor(w, r); actor == nil {
		return
	}

	entryID, err := strconv.ParseInt(mux.Vars(r)["commentID"], 10, 64)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid comment id")
		return
	}

	var body struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxIssueBody)).Decode(&body); err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(body.Body) == "" {
		responder.Error(w, http.StatusBadRequest, "body is required")
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
	if actor := requireActor(w, r); actor == nil {
		return
	}

	entryID, err := strconv.ParseInt(mux.Vars(r)["commentID"], 10, 64)
	if err != nil {
		responder.Error(w, http.StatusBadRequest, "invalid comment id")
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
	var fallbackOwner, fallbackRepo string
	if repo, err := query.GetServiceRepo(db.SQL, issue.Service); err == nil && repo != nil {
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
		log.Printf("issues: failed to fetch %s from github: %v", ref.URL(), err)
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
		log.Printf("issues: failed to record link on %s: %v", issue.ID, err)
	}

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
		log.Printf("issues: failed to record unlink on %s: %v", issue.ID, err)
	}

	responder.New(w, nil, "link removed")
}
