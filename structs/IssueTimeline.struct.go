package structs

import (
	"encoding/json"
	"time"
)

// TimelineEntryType discriminates the polymorphic timeline feed. Adding a new
// kind of event is a new value here plus a value in the ENUM on
// monitor.issue_timeline — never a new table.
type TimelineEntryType string

const (
	TimelineComment         TimelineEntryType = "comment"
	TimelineStatusChanged   TimelineEntryType = "status_changed"
	TimelineRegressed       TimelineEntryType = "regressed"
	TimelineAssigned        TimelineEntryType = "assigned"
	TimelineUnassigned      TimelineEntryType = "unassigned"
	TimelinePriorityChanged TimelineEntryType = "priority_changed"
	TimelineTitleChanged    TimelineEntryType = "title_changed"
	TimelinePRLinked        TimelineEntryType = "pr_linked"
	TimelinePRUnlinked      TimelineEntryType = "pr_unlinked"
	TimelinePRMerged        TimelineEntryType = "pr_merged"
	TimelinePRClosed        TimelineEntryType = "pr_closed"
	TimelinePRReopened      TimelineEntryType = "pr_reopened"
)

// IsValid reports whether the type is one of the known values.
func (t TimelineEntryType) IsValid() bool {
	switch t {
	case TimelineComment, TimelineStatusChanged, TimelineRegressed,
		TimelineAssigned, TimelineUnassigned, TimelinePriorityChanged,
		TimelineTitleChanged, TimelinePRLinked, TimelinePRUnlinked,
		TimelinePRMerged, TimelinePRClosed, TimelinePRReopened:
		return true
	}
	return false
}

// IssueTimelineEntry is one append-only fact about an issue — a comment, a
// status transition, a PR link. Lives in MariaDB (monitor.issue_timeline).
//
// ActorLabel is denormalised and NOT NULL: an API key can be deleted and a user
// deactivated, and the entry must still name who acted. ActorUserID and
// ActorAPIKeyID are weak references that may dangle; ActorLabel never does.
type IssueTimelineEntry struct {
	ID            int64             `json:"id"`
	IssueID       string            `json:"issue_id"`
	Type          TimelineEntryType `json:"type"`
	ActorKind     ActorKind         `json:"actor_kind"`
	ActorUserID   *int64            `json:"actor_user_id,omitempty"`
	ActorAPIKeyID *string           `json:"actor_api_key_id,omitempty"`
	ActorLabel    string            `json:"actor_label"`
	Body          *string           `json:"body,omitempty"`
	Metadata      json.RawMessage   `json:"metadata,omitempty"`
	DedupeKey     *string           `json:"dedupe_key,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	EditedAt      *time.Time        `json:"edited_at,omitempty"`
	DeletedAt     *time.Time        `json:"deleted_at,omitempty"`
}

// IssueLinkKind is what an external link points at.
type IssueLinkKind string

const (
	IssueLinkPullRequest IssueLinkKind = "pull_request"
	IssueLinkIssue       IssueLinkKind = "issue"
	IssueLinkCommit      IssueLinkKind = "commit"
)

// IsValid reports whether the kind is one of the known values.
func (k IssueLinkKind) IsValid() bool {
	switch k {
	case IssueLinkPullRequest, IssueLinkIssue, IssueLinkCommit:
		return true
	}
	return false
}

// IssueLink is a reference to a GitHub PR, issue or commit.
//
// State, Merged, Title and Author are a cache of GitHub's view, refreshed by the
// webhook receiver rather than polled. They are nullable because a link is stored
// even when the GitHub call fails — an unreachable GitHub degrades the chip to a
// bare URL, it never fails the write.
type IssueLink struct {
	ID            int64         `json:"id"`
	IssueID       string        `json:"issue_id"`
	Provider      string        `json:"provider"`
	Kind          IssueLinkKind `json:"kind"`
	URL           string        `json:"url"`
	Owner         *string       `json:"owner,omitempty"`
	Repo          *string       `json:"repo,omitempty"`
	Number        *int          `json:"number,omitempty"`
	Title         *string       `json:"title,omitempty"`
	State         *string       `json:"state,omitempty"`
	Merged        *bool         `json:"merged,omitempty"`
	Author        *string       `json:"author,omitempty"`
	StateSyncedAt *time.Time    `json:"state_synced_at,omitempty"`
	LinkedByLabel string        `json:"linked_by_label"`
	InsertedAt    time.Time     `json:"inserted_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}
