package models

// ── Domain models ─────────────────────────────────────────────────────────────

type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// Room is the shape sent to clients. KeywordHash is never serialised.
type Room struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Keyword      string `json:"keyword"`      // "***" in list responses; plaintext only on create
	KeywordHash  string `json:"-"`            // stored in Redis, never sent over the wire
	UserCount    int    `json:"userCount"`
	CreatedAt    int64  `json:"createdAt"`    // Unix ms
	LastActivity int64  `json:"lastActivity"` // Unix ms
	CreatorID    string `json:"creatorId"`
}

type Message struct {
	ID        string `json:"id"`
	RoomID    string `json:"roomId"`
	UserID    string `json:"userId"`   // "system" for server-generated messages
	Username  string `json:"username"` // "SYSTEM" for server-generated messages
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"` // Unix ms
	Type      string `json:"type"`      // "user" | "system"
}

// ── WebSocket envelope ────────────────────────────────────────────────────────

// WSMessage is the top-level JSON frame for every WS message in both directions.
type WSMessage struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

// ── Inbound client → server payloads ─────────────────────────────────────────

type IdentifyPayload struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

type JoinRoomPayload struct {
	RoomID string `json:"roomId"`
	UserID string `json:"userId"`
}

type LeaveRoomPayload struct {
	RoomID string `json:"roomId"`
	UserID string `json:"userId"`
}

type SendMessagePayload struct {
	RoomID   string `json:"roomId"`
	UserID   string `json:"userId"`
	Username string `json:"username"`
	Content  string `json:"content"`
}

// ── Outbound server → client payloads ────────────────────────────────────────

type UserEventPayload struct {
	RoomID    string `json:"roomId"`
	UserID    string `json:"userId"`
	Username  string `json:"username"`
	UserCount int    `json:"userCount"`
}

type RoomExpiredPayload struct {
	RoomID string `json:"roomId"`
}

type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
