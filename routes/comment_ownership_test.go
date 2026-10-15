package routes

import (
	"testing"

	"github.com/aidenappl/monitor-core/structs"
)

func TestCanModifyComment(t *testing.T) {
	uid := func(v int64) *int64 { return &v }
	kid := func(v string) *string { return &v }

	byUser7 := &structs.IssueTimelineEntry{ActorKind: structs.ActorKindUser, ActorUserID: uid(7)}
	byKeyA := &structs.IssueTimelineEntry{ActorKind: structs.ActorKindAPIKey, ActorAPIKeyID: kid("key-a")}

	tests := []struct {
		name  string
		actor *structs.Actor
		user  *structs.User
		entry *structs.IssueTimelineEntry
		want  bool
	}{
		{"author edits own", &structs.Actor{Kind: structs.ActorKindUser, UserID: uid(7)}, &structs.User{ID: 7, Role: "editor"}, byUser7, true},
		{"other editor refused", &structs.Actor{Kind: structs.ActorKindUser, UserID: uid(8)}, &structs.User{ID: 8, Role: "editor"}, byUser7, false},
		{"admin moderates", &structs.Actor{Kind: structs.ActorKindUser, UserID: uid(1)}, &structs.User{ID: 1, Role: "admin"}, byKeyA, true},
		{"key edits own note", &structs.Actor{Kind: structs.ActorKindAPIKey, APIKeyID: kid("key-a")}, nil, byKeyA, true},
		{"other key refused", &structs.Actor{Kind: structs.ActorKindAPIKey, APIKeyID: kid("key-b")}, nil, byKeyA, false},
		{"user cannot edit a key's note", &structs.Actor{Kind: structs.ActorKindUser, UserID: uid(7)}, &structs.User{ID: 7, Role: "editor"}, byKeyA, false},
		{"master key moderates", structs.SystemActor("master"), nil, byUser7, true},
		{"no actor", nil, nil, byUser7, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canModifyComment(tt.actor, tt.user, tt.entry); got != tt.want {
				t.Errorf("canModifyComment = %v, want %v", got, tt.want)
			}
		})
	}
}
