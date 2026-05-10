package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	forta "github.com/aidenappl/go-forta"
	"github.com/aidenappl/go-keyring"
	"github.com/aidenappl/monitor-core/alerts"
	"github.com/aidenappl/monitor-core/apikeys"
	"github.com/aidenappl/monitor-core/dashboards"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/issues"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/aidenappl/monitor-core/views"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func main() {
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

	// Set up Forta OAuth2 authentication (optional — skipped if credentials are absent).
	if env.FortaClientID != "" && env.FortaClientSecret != "" {
		fmt.Print("Connecting to Forta... ")
		if err := forta.Setup(forta.Config{
			AppDomain:          env.FortaAppDomain,
			APIDomain:          env.FortaAPIDomain,
			LoginDomain:        env.FortaLoginDomain,
			ClientID:           env.FortaClientID,
			ClientSecret:       env.FortaClientSecret,
			CallbackURL:        env.FortaCallbackURL,
			JWTSigningKey:      env.FortaJWTSigningKey,
			PostLoginRedirect:  env.FortaPostLoginRedirect,
			PostLogoutRedirect: env.FortaPostLogoutRedirect,
			CookieDomain:       env.FortaCookieDomain,
			CookieInsecure:     env.FortaCookieInsecure,
			FetchUserOnProtect: env.FortaFetchUserOnProtect,
			DisableAutoRefresh: env.FortaDisableAutoRefresh,
			EnforceGrants:      env.FortaEnforceGrants,
		}); err != nil {
			log.Printf("WARNING: forta setup failed: %v", err)
		} else if err := forta.Ping(); err != nil {
			log.Printf("WARNING: forta unreachable: %v", err)
		} else {
			fmt.Println("✅ Done")
		}
	} else {
		log.Println("WARNING: FORTA_CLIENT_ID / FORTA_CLIENT_SECRET not set, Forta auth disabled")
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Connect to ClickHouse
	if err := db.Connect(ctx, env.ClickHouseAddr, env.ClickHouseDatabase, env.ClickHouseUsername, env.ClickHousePassword); err != nil {
		log.Fatalf("❌ failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	// Initialize API key management (auto-creates table if needed)
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

	// Start alert evaluator
	evaluator := alerts.NewEvaluator()
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

	r.HandleFunc("/health", routes.HealthHandler).Methods(http.MethodGet)

	// Forta OAuth2 routes (unprotected — browser navigates here directly)
	r.HandleFunc("/forta/login", forta.LoginHandler).Methods(http.MethodGet)
	r.HandleFunc("/forta/callback", forta.CallbackHandler).Methods(http.MethodGet)
	r.HandleFunc("/forta/logout", forta.LogoutHandler).Methods(http.MethodGet)

	// Authenticated self endpoint — returns the current Forta user
	r.HandleFunc("/self", forta.Protected(routes.HandleGetSelf)).Methods(http.MethodGet)

	// Event ingestion — authenticated by X-Ingest-Key (used by go-monitor)
	r.HandleFunc("/v1/events", middleware.IngestAuthMiddleware(routes.IngestEventsHandler)).Methods(http.MethodPost)

	// V1 API routes — protected by API key or Forta JWT
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

	// API key management — protected by Forta (admin UI only)
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
			"https://*.trailblaze.to",
			"https://*.appleby.cloud",
			"http://localhost:*",
		},
		AllowCredentials: true,
		AllowedHeaders:   []string{"X-Requested-With", "Content-Type", "Origin", "Authorization", "Accept", "Referer", "Dnt", "User-Agent", "X-Api-Key"},
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
