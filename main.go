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

	"github.com/aidenappl/go-keyring"
	forta "github.com/aidenappl/go-forta"
	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
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

	// Create event queue
	queue := services.NewQueue(env.QueueSize)
	routes.Queue = queue

	// Create and start batcher
	writer := &db.Writer{}
	batcher := services.NewBatcher(queue, writer, env.BatchSize, env.FlushInterval)
	go batcher.Run(ctx)

	// Setup router
	r := mux.NewRouter()
	r.Use(middleware.RequestIDMiddleware)
	r.Use(middleware.LoggingMiddleware)
	r.Use(middleware.MuxHeaderMiddleware)

	r.HandleFunc("/health", routes.HealthHandler).Methods(http.MethodGet)

	// Forta OAuth2 routes (unprotected — browser navigates here directly)
	r.HandleFunc("/forta/login",    forta.LoginHandler).Methods(http.MethodGet)
	r.HandleFunc("/forta/callback", forta.CallbackHandler).Methods(http.MethodGet)
	r.HandleFunc("/forta/logout",   forta.LogoutHandler).Methods(http.MethodGet)

	// Authenticated self endpoint — returns the current Forta user
	r.HandleFunc("/self", forta.Protected(routes.HandleGetSelf)).Methods(http.MethodGet)

	// Event ingestion — authenticated by X-Ingest-Key (used by go-monitor)
	r.HandleFunc("/v1/events", middleware.IngestAuthMiddleware(routes.IngestEventsHandler)).Methods(http.MethodPost)

	// V1 API routes — protected by Forta JWT validation
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.Use(func(next http.Handler) http.Handler {
		return forta.Protected(next.ServeHTTP)
	})

	v1.HandleFunc("/events", routes.QueryEventsHandler).Methods(http.MethodGet)
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

	// CORS Middleware
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		AllowedHeaders:   []string{"X-Requested-With", "Content-Type", "Origin", "Authorization", "Accept", "Referer", "Dnt", "User-Agent"},
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
