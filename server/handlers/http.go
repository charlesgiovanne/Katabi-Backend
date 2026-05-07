package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/charlesgiovanne/Katabi-Backend/server/models"
	"github.com/charlesgiovanne/Katabi-Backend/server/store"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

// ── validation patterns ───────────────────────────────────────────────────────

var (
	reUsername = regexp.MustCompile(`^[A-Z0-9_\-]{2,28}$`)
	reRoomName = regexp.MustCompile(`^[A-Z0-9_\-\s]{2,24}$`)
	reKeyword  = regexp.MustCompile(`^[A-Z0-9_\-]{2,32}$`)
)

// ── handler struct ────────────────────────────────────────────────────────────

type Handlers struct {
	store           *store.Store
	hubBroadcastAll func(msgType string, data any)
}

func New(s *store.Store, broadcastAll func(string, any)) *Handlers {
	return &Handlers{store: s, hubBroadcastAll: broadcastAll}
}

// ── POST /api/register ────────────────────────────────────────────────────────

func (h *Handlers) Register(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "BAD_REQUEST", "Invalid JSON", http.StatusBadRequest)
		return
	}

	username := strings.ToUpper(strings.TrimSpace(body.Username))
	if !reUsername.MatchString(username) {
		jsonError(w, "USERNAME_INVALID", "Alphanumeric + _ - only, 2–28 chars (uppercase)", http.StatusBadRequest)
		return
	}

	user := &models.User{
		ID:       uuid.NewString(),
		Username: username,
	}
	if err := h.store.CreateUser(r.Context(), user); err != nil {
		jsonError(w, "INTERNAL_ERROR", "Could not create user", http.StatusInternalServerError)
		return
	}

	jsonOK(w, user, http.StatusCreated)
}

// ── GET /api/rooms ────────────────────────────────────────────────────────────

func (h *Handlers) ListRooms(w http.ResponseWriter, r *http.Request) {
	rooms, err := h.store.GetAllActiveRooms(r.Context())
	if err != nil {
		jsonError(w, "INTERNAL_ERROR", "Could not fetch rooms", http.StatusInternalServerError)
		return
	}
	if rooms == nil {
		rooms = []*models.Room{}
	}
	jsonOK(w, rooms, http.StatusOK)
}

// ── POST /api/rooms ───────────────────────────────────────────────────────────

func (h *Handlers) CreateRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name      string `json:"name"`
		Keyword   string `json:"keyword"`
		CreatorID string `json:"creatorId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "BAD_REQUEST", "Invalid JSON", http.StatusBadRequest)
		return
	}

	name := strings.ToUpper(strings.TrimSpace(body.Name))
	keyword := strings.ToUpper(strings.TrimSpace(body.Keyword))
	creatorID := strings.TrimSpace(body.CreatorID)

	if !reRoomName.MatchString(name) {
		jsonError(w, "ROOM_NAME_INVALID", "Room name: 2–24 chars, alphanumeric + _ - spaces", http.StatusBadRequest)
		return
	}
	if !reKeyword.MatchString(keyword) {
		jsonError(w, "ROOM_KEYWORD_INVALID", "Keyword: 2–32 chars, alphanumeric + _ -", http.StatusBadRequest)
		return
	}
	if creatorID == "" {
		jsonError(w, "CREATOR_REQUIRED", "creatorId is required", http.StatusBadRequest)
		return
	}

	// Verify user exists
	if _, err := h.store.GetUser(r.Context(), creatorID); err != nil {
		jsonError(w, "USER_NOT_FOUND", "Unknown creatorId", http.StatusBadRequest)
		return
	}

	// Hash the keyword before storing
	hash, err := bcrypt.GenerateFromPassword([]byte(keyword), bcrypt.DefaultCost)
	if err != nil {
		jsonError(w, "INTERNAL_ERROR", "Could not hash keyword", http.StatusInternalServerError)
		return
	}

	now := time.Now().UnixMilli()
	room := &models.Room{
		ID:           uuid.NewString(),
		Name:         name,
		Keyword:      keyword, // plaintext — returned to creator only, once
		CreatorID:    creatorID,
		CreatedAt:    now,
		LastActivity: now,
	}

	if err := h.store.CreateRoom(r.Context(), room, string(hash)); err != nil {
		jsonError(w, "INTERNAL_ERROR", "Could not create room", http.StatusInternalServerError)
		return
	}

	// Notify all lobby clients
	go func() {
		rooms, err := h.store.GetAllActiveRooms(context.Background())
		if err == nil {
			h.hubBroadcastAll("ROOMS_UPDATED", rooms)
		}
	}()

	jsonOK(w, room, http.StatusCreated)
}

// ── POST /api/rooms/{id}/validate ────────────────────────────────────────────

func (h *Handlers) ValidateKeyword(w http.ResponseWriter, r *http.Request) {
	roomID := mux.Vars(r)["id"]

	// 1. Verify room exists before touching the rate-limit counter.
	//    This prevents wasting attempts on phantom room IDs.
	hash, err := h.store.GetRoomKeywordHash(r.Context(), roomID)
	if err != nil {
		jsonError(w, "ROOM_NOT_FOUND", "Room does not exist or has expired", http.StatusNotFound)
		return
	}

	var body struct {
		Keyword string `json:"keyword"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "BAD_REQUEST", "Invalid JSON", http.StatusBadRequest)
		return
	}

	candidate := strings.ToUpper(strings.TrimSpace(body.Keyword))
	matchErr := bcrypt.CompareHashAndPassword([]byte(hash), []byte(candidate))
	if matchErr == nil {
		// Correct — don't count successful attempts against the limit.
		jsonOK(w, map[string]any{"valid": true}, http.StatusOK)
		return
	}

	// 2. Wrong keyword — increment failed-attempt counter (rate limit: 5 per IP per room per 10 min).
	ip := realIP(r)
	attempts, _ := h.store.IncrValidateAttempts(r.Context(), ip, roomID)
	if attempts > 5 {
		jsonOK(w, map[string]any{"valid": false, "locked": true}, http.StatusOK)
		return
	}

	jsonOK(w, map[string]any{"valid": false}, http.StatusOK)
}

// ── GET /api/rooms/{id}/messages ─────────────────────────────────────────────

func (h *Handlers) GetMessages(w http.ResponseWriter, r *http.Request) {
	roomID := mux.Vars(r)["id"]

	// Verify room exists
	if _, err := h.store.GetRoom(r.Context(), roomID); err != nil {
		jsonError(w, "ROOM_NOT_FOUND", "Room does not exist or has expired", http.StatusNotFound)
		return
	}

	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	before := q.Get("before")

	msgs, hasMore, err := h.store.GetMessages(r.Context(), roomID, limit, before)
	if err != nil {
		jsonError(w, "INTERNAL_ERROR", "Could not fetch messages", http.StatusInternalServerError)
		return
	}
	if msgs == nil {
		msgs = []*models.Message{}
	}

	jsonOK(w, map[string]any{
		"messages": msgs,
		"hasMore":  hasMore,
	}, http.StatusOK)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, data any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error":   code,
		"message": message,
	})
}

func realIP(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return strings.Split(ip, ",")[0]
	}
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	// Strip port
	addr := r.RemoteAddr
	if i := strings.LastIndex(addr, ":"); i != -1 {
		return addr[:i]
	}
	return addr
}
