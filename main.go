package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aidenappl/go-keyring"
	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/bootstrap"
	"github.com/aidenappl/monitor-core/dashboards"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/migrations"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/sso"
	"github.com/aidenappl/monitor-core/views"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func main() {
	// Operator one-off: -backfill-forta <file.json> pre-provisions existing Forta
	// users (see runFortaBackfill / bootstrap.BackfillFortaUsers) then exits
	// WITHOUT starting the HTTP server. Empty (the default) leaves normal startup
	// completely unaffected. Kept in main.go so the single-file `./main.go` Docker
	// build stays intact (no second package-main file).
	backfillFortaPath := flag.String("backfill-forta", "", "path to a JSON array of Forta users to pre-provision, then exit")
	flag.Parse()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Optionally load secrets from Keyring before reading env vars.
	// Requires KEYRING_URL, KEYRING_ACCESS_KEY_ID, and KEYRING_SECRET_ACCESS_KEY
	// to be set in the environment. Silently skipped if they are absent.
	if client, err := keyring.New(); err == nil {
		if err := client.InjectEnv(ctx); err != nil {
			log.Printf("keyring: failed to inject secrets: %v", err)
		}
	}

	// Load configuration (picks up any values injected by Keyring above).
	env.Load()

	if env.IngestKey == "" {
		log.Fatal("FATAL: MONITOR_API_KEY must be set — refusing to start without ingest authentication")
	}

	// Refuse to start in production on the committed dev-default JWT/crypto keys
	// (would allow forged admin sessions / decryptable SSO secrets). Set
	// MON_COOKIE_INSECURE=true for local dev to permit the fallbacks.
	if err := env.RequireProductionSecrets(); err != nil {
		log.Fatalf("FATAL: %v", err)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Connect to ClickHouse
	if err := db.Connect(ctx, env.ClickHouseAddr, env.ClickHouseDatabase, env.ClickHouseUsername, env.ClickHousePassword); err != nil {
		log.Fatalf("❌ failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	// Apply the ClickHouse schema migrations (events + api_keys) at startup.
	// Ingestion and queries depend on monitor.events existing, so this is
	// fail-fast — a fresh deploy no longer needs a manual `dev migrate` step.
	if err := migrations.RunMigrations(ctx); err != nil {
		log.Fatalf("❌ failed to run ClickHouse migrations: %v", err)
	}

	// Connect to MariaDB (relational auth data layer: users, identities,
	// refresh_tokens, sso_providers, sso_sessions, settings, api_keys).
	if err := db.InitSQL(); err != nil {
		log.Fatalf("❌ failed to connect to MariaDB: %v", err)
	}
	defer db.CloseSQL()

	// Apply the MariaDB auth-schema migrations at startup.
	if err := db.RunMigrations(); err != nil {
		log.Fatalf("❌ failed to run MariaDB migrations: %v", err)
	}

	// Seed the first admin user on a fresh database (no-op once any user exists).
	if err := bootstrap.EnsureAdminUser(db.SQL); err != nil {
		log.Fatalf("❌ failed to bootstrap admin user: %v", err)
	}

	// Seed the Forta SSO provider row (migration path) when MON_SSO_FORTA_* is
	// configured. Idempotent and additive — a no-op if the row already exists or
	// Forta is unconfigured. A missing Forta config just means no Forta button, so
	// this logs and continues (never fatal).
	if err := bootstrap.EnsureFortaProvider(db.SQL); err != nil {
		log.Printf("WARNING: failed to seed Forta SSO provider: %v", err)
	}

	// Operator one-off: -backfill-forta <file.json> pre-provisions existing Forta
	// users and exits (no server). Runs after migrations + provider seed so the
	// tables and forta row exist.
	if *backfillFortaPath != "" {
		if err := runFortaBackfill(*backfillFortaPath); err != nil {
			log.Fatalf("❌ forta backfill failed: %v", err)
		}
		os.Exit(0)
	}

	// Wire the SSO revocation checkpoint into SessionMiddleware. Until this runs
	// the hook is nil and the checkpoint is skipped.
	sso.Install()

	// Initialize API key management (loads the key cache from MariaDB)
	if err := apikeys.Init(ctx); err != nil {
		log.Printf("WARNING: failed to initialize api keys: %v", err)
	}

	// Initialize dashboards
	if err := dashboards.Init(ctx); err != nil {
		log.Printf("WARNING: failed to initialize dashboards: %v", err)
	}

	// Initialize saved views
	if err := views.Init(ctx); err != nil {
		log.Printf("WARNING: failed to initialize views: %v", err)
	}

	// Initialize alerts
	if err := alerts.Init(ctx); err != nil {
		log.Printf("WARNING: failed to initialize alerts: %v", err)
	}

	// Initialize issues
	if err := issues.Init(ctx); err != nil {
		log.Printf("WARNING: failed to initialize issues: %v", err)
	}

	// Create SSE hub
	hub := services.NewHub(env.MaxSSESubscribers)
	routes.EventHub = hub

	// Create event queue
	queue := services.NewQueue(env.QueueSize)
	routes.Queue = queue

	// Create and start batcher
	writer := &db.Writer{}
	batcher := services.NewBatcher(queue, writer, env.BatchSize, env.FlushInterval)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in batcher: %v", r)
			}
		}()
		batcher.Run(ctx)
	}()

	// Create alert notification hub for SSE streaming
	alertHub := alerts.NewAlertHub(env.MaxSSESubscribers)
	routes.AlertNotifHub = alertHub

	// Start alert evaluator
	evaluator := alerts.NewEvaluator(alertHub)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC in alert evaluator: %v", r)
			}
		}()
		evaluator.Run(ctx)
	}()

	// Setup router
	r := mux.NewRouter()
	r.Use(middleware.RequestIDMiddleware)
	r.Use(middleware.LoggingMiddleware)
	r.Use(middleware.MuxHeaderMiddleware)
	// Double-submit CSRF for cookie-authenticated browsers. Safe methods, Bearer
	// clients, and X-Api-Key clients (ingestion) are exempt, so this does not
	// affect the go-monitor ingest path or API-key query callers.
	r.Use(middleware.CSRFMiddleware)

	r.HandleFunc("/health", routes.HealthHandler).Methods(http.MethodGet)

	// Native session auth (Monitor-owned JWT). Forta is gone; it is now just one
	// SSO provider row, mounted below via RegisterSSORoutes.
	//   /auth/login, /auth/register — public, CSRF-exempt (no session yet).
	//   /auth/refresh — public, CSRF-exempt (authenticates via the refresh cookie).
	//   /auth/logout, /auth/self* — behind SessionMiddleware.
	r.HandleFunc("/auth/login", routes.HandleLogin).Methods(http.MethodPost)
	if env.AllowRegistration {
		r.HandleFunc("/auth/register", routes.HandleRegister).Methods(http.MethodPost)
	}
	r.HandleFunc("/auth/refresh", routes.HandleRefresh).Methods(http.MethodPost)
	r.HandleFunc("/auth/logout", middleware.Protected(routes.HandleLogout)).Methods(http.MethodPost)

	// Current-user profile + linked sign-in methods (all Protected).
	r.HandleFunc("/auth/self", middleware.Protected(routes.HandleGetSelf)).Methods(http.MethodGet)
	r.HandleFunc("/auth/self", middleware.Protected(routes.HandleUpdateSelf)).Methods(http.MethodPut)
	r.HandleFunc("/auth/self/identities", middleware.Protected(routes.HandleListIdentities)).Methods(http.MethodGet)
	r.HandleFunc("/auth/self/identities/{slug}", middleware.Protected(routes.HandleLinkIdentity)).Methods(http.MethodPost)
	r.HandleFunc("/auth/self/identities/{slug}", middleware.Protected(routes.HandleUnlinkIdentity)).Methods(http.MethodDelete)

	// Pluggable SSO subsystem (Phase 3A): /auth/sso/config, /auth/sso/{slug}/login,
	// /auth/sso/{slug}/callback, and the /admin/sso-providers CRUD.
	routes.RegisterSSORoutes(r)

	// Event ingestion — authenticated by X-Api-Key (used by go-monitor)
	r.HandleFunc("/v1/events", middleware.IngestAuthMiddleware(routes.IngestEventsHandler)).Methods(http.MethodPost)

	// V1 API routes — protected by API key or a Monitor session
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.Use(middleware.QueryAuthMiddleware)

	v1.HandleFunc("/events", routes.QueryEventsHandler).Methods(http.MethodGet)
	v1.HandleFunc("/events/stream", routes.StreamEventsHandler).Methods(http.MethodGet)
	v1.HandleFunc("/labels/{label}/values", routes.GetLabelValuesHandler).Methods(http.MethodGet)
	v1.HandleFunc("/data/keys", routes.GetDataKeysHandler).Methods(http.MethodGet)
	v1.HandleFunc("/data/values", routes.GetDataValuesHandler).Methods(http.MethodGet)

	// Analytics routes (Grafana-compatible)
	v1.HandleFunc("/analytics", routes.AnalyticsHandler).Methods(http.MethodPost)
	v1.HandleFunc("/analytics", routes.AnalyticsQueryHandler).Methods(http.MethodGet)
	v1.HandleFunc("/timeseries", routes.TimeSeriesHandler).Methods(http.MethodPost)
	v1.HandleFunc("/timeseries", routes.TimeSeriesQueryHandler).Methods(http.MethodGet)
	v1.HandleFunc("/topn", routes.TopNHandler).Methods(http.MethodPost)
	v1.HandleFunc("/gauge", routes.GaugeHandler).Methods(http.MethodPost)
	v1.HandleFunc("/compare", routes.CompareHandler).Methods(http.MethodPost)

	// API key management — protected by the session/API-key middleware (admin UI)
	v1.HandleFunc("/api-keys", routes.HandleListAPIKeys).Methods(http.MethodGet)
	v1.HandleFunc("/api-keys", routes.HandleCreateAPIKey).Methods(http.MethodPost)
	v1.HandleFunc("/api-keys/{id}", routes.HandleDeleteAPIKey).Methods(http.MethodDelete)

	// Dashboard persistence
	v1.HandleFunc("/dashboards", routes.HandleListDashboards).Methods(http.MethodGet)
	v1.HandleFunc("/dashboards", routes.HandleCreateDashboard).Methods(http.MethodPost)
	v1.HandleFunc("/dashboards/{id}", routes.HandleGetDashboard).Methods(http.MethodGet)
	v1.HandleFunc("/dashboards/{id}", routes.HandleUpdateDashboard).Methods(http.MethodPut)
	v1.HandleFunc("/dashboards/{id}", routes.HandleDeleteDashboard).Methods(http.MethodDelete)

	// Saved views
	v1.HandleFunc("/views", routes.HandleListViews).Methods(http.MethodGet)
	v1.HandleFunc("/views", routes.HandleCreateView).Methods(http.MethodPost)
	v1.HandleFunc("/views/{id}", routes.HandleDeleteView).Methods(http.MethodDelete)

	// Alert rules
	v1.HandleFunc("/alert-rules", routes.HandleListAlertRules).Methods(http.MethodGet)
	v1.HandleFunc("/alert-rules", routes.HandleCreateAlertRule).Methods(http.MethodPost)
	v1.HandleFunc("/alert-rules/{id}", routes.HandleGetAlertRule).Methods(http.MethodGet)
	v1.HandleFunc("/alert-rules/{id}", routes.HandleUpdateAlertRule).Methods(http.MethodPut)
	v1.HandleFunc("/alert-rules/{id}", routes.HandleDeleteAlertRule).Methods(http.MethodDelete)
	v1.HandleFunc("/alert-rules/{id}/test", routes.HandleTestAlertRule).Methods(http.MethodPost)

	// Alert history
	v1.HandleFunc("/alert-history", routes.HandleListAlertHistory).Methods(http.MethodGet)

	// Notification channels
	v1.HandleFunc("/notification-channels", routes.HandleListNotificationChannels).Methods(http.MethodGet)
	v1.HandleFunc("/notification-channels", routes.HandleCreateNotificationChannel).Methods(http.MethodPost)
	v1.HandleFunc("/notification-channels/{id}", routes.HandleDeleteNotificationChannel).Methods(http.MethodDelete)
	v1.HandleFunc("/notification-channels/{id}/test", routes.HandleTestNotificationChannel).Methods(http.MethodPost)

	// Service groups
	v1.HandleFunc("/service-groups", routes.HandleListServiceGroups).Methods(http.MethodGet)
	v1.HandleFunc("/service-groups", routes.HandleCreateServiceGroup).Methods(http.MethodPost)
	v1.HandleFunc("/service-groups/{id}", routes.HandleUpdateServiceGroup).Methods(http.MethodPut)
	v1.HandleFunc("/service-groups/{id}", routes.HandleDeleteServiceGroup).Methods(http.MethodDelete)

	// Notification policies (routing rules)
	v1.HandleFunc("/notification-policies", routes.HandleListPolicies).Methods(http.MethodGet)
	v1.HandleFunc("/notification-policies", routes.HandleCreatePolicy).Methods(http.MethodPost)
	v1.HandleFunc("/notification-policies/reorder", routes.HandleReorderPolicies).Methods(http.MethodPut)
	v1.HandleFunc("/notification-policies/{id}", routes.HandleGetPolicy).Methods(http.MethodGet)
	v1.HandleFunc("/notification-policies/{id}", routes.HandleUpdatePolicy).Methods(http.MethodPut)
	v1.HandleFunc("/notification-policies/{id}", routes.HandleDeletePolicy).Methods(http.MethodDelete)

	// Alert notification stream (SSE for web/desktop notifications)
	v1.HandleFunc("/alerts/stream", routes.HandleStreamAlerts).Methods(http.MethodGet)

	// Issue tracking
	v1.HandleFunc("/issues", routes.HandleListIssues).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}", routes.HandleGetIssue).Methods(http.MethodGet)
	v1.HandleFunc("/issues/{id}", routes.HandleUpdateIssue).Methods(http.MethodPut)
	v1.HandleFunc("/issues/{id}/events", routes.HandleGetIssueEvents).Methods(http.MethodGet)

	// CORS Middleware
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins: []string{
			"https://monitor.local.appleby.cloud:3020",
			"https://monitor.appleby.cloud",
			"https://trailblaze.to",
			"https://*.trailblaze.to",
			"https://*.appleby.cloud",
			"http://localhost:*",
		},
		AllowCredentials: true,
		AllowedHeaders:   []string{"X-Requested-With", "Content-Type", "Origin", "Authorization", "Accept", "Referer", "Dnt", "User-Agent", "X-Api-Key", "X-CSRF-Token"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
	})

	// Launch Server
	fmt.Printf("✅ monitor-core running on port %s\n", env.Port)
	fmt.Println()

	server := &http.Server{
		Addr:         ":" + env.Port,
		Handler:      corsMiddleware.Handler(r),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("shutting down...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	cancel()
	queue.Close()
	time.Sleep(2 * time.Second)

	log.Println("shutdown complete")
}

// runFortaBackfill reads a JSON array of Forta users from path and pre-provisions
// them as ACTIVE Monitor accounts with linked "forta" identities, so existing
// Forta users are never provisioned as "pending" on their first post-cutover
// "Continue with Forta". Idempotent — a second run reports every user as skipped.
//
// The file is a JSON array of objects:
//
//	[
//	  {"subject": "forta-user-id", "email": "a@b.com", "email_verified": true,
//	   "name": "Ada Lovelace", "role": "admin"},
//	  ...
//	]
//
// where "role" is one of admin|editor|viewer (the operator's mapping of the
// user's Forta grant/role → Monitor role) and "subject" is the Forta subject id
// (the identity key — never the email).
func runFortaBackfill(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	var seeds []bootstrap.FortaUserSeed
	if err := json.Unmarshal(raw, &seeds); err != nil {
		return fmt.Errorf("parse %s (expected a JSON array of {subject,email,email_verified,name,role}): %w", path, err)
	}
	if len(seeds) == 0 {
		return fmt.Errorf("%s contained no users", path)
	}

	log.Printf("forta backfill: processing %d user(s) from %s", len(seeds), path)
	result, err := bootstrap.BackfillFortaUsers(db.SQL, seeds)
	// Always report what happened, even on a partial failure.
	log.Printf("forta backfill: created=%d linked=%d skipped=%d", result.Created, result.Linked, result.Skipped)
	if err != nil {
		return err
	}
	log.Println("forta backfill: done")
	return nil
}
