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

	"github.com/aidenappl/monitor-core/db"
	"github.com/aidenappl/monitor-core/env"
	"github.com/aidenappl/monitor-core/middleware"
	"github.com/aidenappl/monitor-core/routes"
	"github.com/aidenappl/monitor-core/services"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func main() {
	// Validate configuration
	if env.APIKey == "" {
		log.Println("WARNING: API_KEY is not set, authentication is disabled")
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	r.HandleFunc("/health", routes.HealthHandler).Methods(http.MethodGet)

	// V1 API routes (with auth middleware)
	v1 := r.PathPrefix("/v1").Subrouter()
	v1.Use(middleware.AuthMiddleware)

	v1.HandleFunc("/events", routes.IngestEventsHandler).Methods(http.MethodPost)
	v1.HandleFunc("/events", routes.QueryEventsHandler).Methods(http.MethodGet)
	v1.HandleFunc("/labels/{label}/values", routes.GetLabelValuesHandler).Methods(http.MethodGet)
	v1.HandleFunc("/data/keys", routes.GetDataKeysHandler).Methods(http.MethodGet)
	v1.HandleFunc("/data/values", routes.GetDataValuesHandler).Methods(http.MethodGet)

	// CORS Middleware
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		AllowedHeaders:   []string{"X-Requested-With", "Content-Type", "Origin", "Authorization", "Accept", "X-Api-Key"},
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
