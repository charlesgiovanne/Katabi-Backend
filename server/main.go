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

	"github.com/gorilla/mux"
	"github.com/rs/cors"

	"github.com/charlesgiovanne/Katabi-Backend/server/config"
	"github.com/charlesgiovanne/Katabi-Backend/server/handlers"
	"github.com/charlesgiovanne/Katabi-Backend/server/hub"
	"github.com/charlesgiovanne/Katabi-Backend/server/jobs"
	"github.com/charlesgiovanne/Katabi-Backend/server/store"
)

func main() {
	cfg := config.Load()

	// ── Store (Redis) ──────────────────────────────────────────────────────────
	s, err := store.New(cfg.RedisURL)
	if err != nil {
		log.Fatalf("store init failed: %v", err)
	}
	defer s.Close()
	log.Printf("✓ Redis connected (%s)", cfg.RedisURL)

	// ── Hub (WebSocket broker) ─────────────────────────────────────────────────
	h := hub.New(s)

	// ── Expiry worker ──────────────────────────────────────────────────────────
	jobs.StartExpiryWorker(s, h.BroadcastAll, h.BroadcastRoom)

	// ── HTTP + WebSocket routes ────────────────────────────────────────────────
	api := handlers.New(s, h.BroadcastAll)

	r := mux.NewRouter()

	// REST endpoints
	r.HandleFunc("/api/register", api.Register).Methods(http.MethodPost)
	r.HandleFunc("/api/rooms", api.ListRooms).Methods(http.MethodGet)
	r.HandleFunc("/api/rooms", api.CreateRoom).Methods(http.MethodPost)
	r.HandleFunc("/api/rooms/{id}/validate", api.ValidateKeyword).Methods(http.MethodPost)
	r.HandleFunc("/api/rooms/{id}/messages", api.GetMessages).Methods(http.MethodGet)

	// WebSocket endpoint
	r.HandleFunc("/ws", h.ServeWS)

	// Health check
	r.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	}).Methods(http.MethodGet)

	// ── CORS ───────────────────────────────────────────────────────────────────
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{cfg.CORSOrigin},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true,
	})

	// ── HTTP server ────────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      c.Handler(r),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("✓ PIXELCHAT server listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// ── Graceful shutdown ──────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("stopped.")
}
