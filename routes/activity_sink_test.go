package routes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/structs"
	"github.com/gorilla/mux"
)

func TestActivitySinkMiddleware(t *testing.T) {
	prevURL, prevProv, prevZone := env.ActivityReportURL, env.ActivityReportProvider, env.ZoneSlug
	env.ActivityReportURL, env.ActivityReportProvider, env.ZoneSlug = "https://sink.example.test/internal/activity", "forta", "appleby"
	defer func() { env.ActivityReportURL, env.ActivityReportProvider, env.ZoneSlug = prevURL, prevProv, prevZone }()

	type event struct {
		user    int64
		action  string
		details map[string]string
	}
	var got []event
	prev := sinkReport
	sinkReport = func(u int64, a string, d map[string]string) { got = append(got, event{u, a, d}) }
	defer func() { sinkReport = prev }()

	uid, key := int64(7), "key-1"
	userActor := &structs.Actor{Kind: structs.ActorKindUser, UserID: &uid}
	keyActor := &structs.Actor{Kind: structs.ActorKindAPIKey, APIKeyID: &key}

	status := http.StatusOK
	build := func(a *structs.Actor) *mux.Router {
		r := mux.NewRouter()
		v1 := r.PathPrefix("/v1").Subrouter()
		v1.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), middleware.ActorContextKey, a)))
			})
		}, ActivitySinkMiddleware)
		h := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }
		v1.HandleFunc("/issues/{id}", h).Methods(http.MethodPut)
		v1.HandleFunc("/analytics", h).Methods(http.MethodPost)
		return r
	}
	do := func(r *mux.Router, method, path string) {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
	}

	do(build(userActor), http.MethodPut, "/v1/issues/42")
	do(build(userActor), http.MethodPost, "/v1/analytics") // read POST, not named
	do(build(keyActor), http.MethodPut, "/v1/issues/42")   // agent / CI
	status = http.StatusConflict
	do(build(userActor), http.MethodPut, "/v1/issues/42") // not 2xx

	if len(got) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(got), got)
	}
	e := got[0]
	if e.user != uid || e.action != "issue.update" || e.details["id"] != "42" || e.details["zone"] != "appleby" {
		t.Errorf("event = %+v", e)
	}
}
