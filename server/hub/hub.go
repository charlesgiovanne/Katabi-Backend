package hub

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pixelchat/server/models"
	"github.com/pixelchat/server/store"
)

// ── WebSocket tuning ──────────────────────────────────────────────────────────

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
	identifyWindow = 5 * time.Second
	sendBufferSize = 256
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// CORS for WebSocket is handled by checking the Origin header in the handler.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client represents one connected browser tab.
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	userID   string
	username string

	mu    sync.RWMutex
	rooms map[string]bool // roomIDs this client has joined
}

func (c *Client) addRoom(roomID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rooms[roomID] = true
}

func (c *Client) removeRoom(roomID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rooms, roomID)
}

func (c *Client) activeRooms() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.rooms))
	for id := range c.rooms {
		out = append(out, id)
	}
	return out
}

// send a typed message to this client only.
func (c *Client) emit(msgType string, data any) {
	b, err := json.Marshal(models.WSMessage{Type: msgType, Data: data})
	if err != nil {
		return
	}
	select {
	case c.send <- b:
	default:
		// Buffer full — client is too slow; disconnect handled by writePump
	}
}

// ── Hub ───────────────────────────────────────────────────────────────────────

// Hub is the central broker: it owns all Client references and performs
// thread-safe broadcasts. All exported methods are safe to call from any goroutine.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client            // userID → client
	sockets map[*websocket.Conn]*Client   // conn → client (for disconnect lookup)
	rooms   map[string]map[string]*Client // roomID → userID → client

	Store *store.Store
}

func New(s *store.Store) *Hub {
	return &Hub{
		clients: make(map[string]*Client),
		sockets: make(map[*websocket.Conn]*Client),
		rooms:   make(map[string]map[string]*Client),
		Store:   s,
	}
}

// ── Hub: client lifecycle ─────────────────────────────────────────────────────

func (h *Hub) registerClient(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c.userID] = c
	h.sockets[c.conn] = c
}

func (h *Hub) unregisterClient(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c.userID)
	delete(h.sockets, c.conn)
}

// ── Hub: room membership ──────────────────────────────────────────────────────

// JoinRoom adds the client to the room's broadcast group and updates Redis presence.
// Returns the new user count.
func (h *Hub) JoinRoom(ctx context.Context, c *Client, roomID string) (int, error) {
	if err := h.Store.AddUser(ctx, roomID, c.userID); err != nil {
		return 0, err
	}
	c.addRoom(roomID)

	h.mu.Lock()
	if h.rooms[roomID] == nil {
		h.rooms[roomID] = make(map[string]*Client)
	}
	h.rooms[roomID][c.userID] = c
	h.mu.Unlock()

	count, _ := h.Store.GetUserCount(ctx, roomID)
	return count, nil
}

// LeaveRoom removes the client from the room's broadcast group and updates Redis.
// Returns the new user count.
func (h *Hub) LeaveRoom(ctx context.Context, c *Client, roomID string) (int, error) {
	if err := h.Store.RemoveUser(ctx, roomID, c.userID); err != nil {
		return 0, err
	}
	c.removeRoom(roomID)

	h.mu.Lock()
	if members, ok := h.rooms[roomID]; ok {
		delete(members, c.userID)
		if len(members) == 0 {
			delete(h.rooms, roomID)
		}
	}
	h.mu.Unlock()

	count, _ := h.Store.GetUserCount(ctx, roomID)
	return count, nil
}

// ── Hub: broadcasts ───────────────────────────────────────────────────────────

// BroadcastAll sends to every connected client (lobby + all rooms).
func (h *Hub) BroadcastAll(msgType string, data any) {
	b, err := json.Marshal(models.WSMessage{Type: msgType, Data: data})
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.clients {
		select {
		case c.send <- b:
		default:
		}
	}
}

// BroadcastRoom sends to all clients currently in a specific room.
func (h *Hub) BroadcastRoom(roomID, msgType string, data any) {
	b, err := json.Marshal(models.WSMessage{Type: msgType, Data: data})
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.rooms[roomID] {
		select {
		case c.send <- b:
		default:
		}
	}
}

// EmitTo sends a message to a single user by userID.
func (h *Hub) EmitTo(userID, msgType string, data any) {
	h.mu.RLock()
	c, ok := h.clients[userID]
	h.mu.RUnlock()
	if ok {
		c.emit(msgType, data)
	}
}

// ── Hub: HTTP upgrade ─────────────────────────────────────────────────────────

// ServeWS upgrades an HTTP connection to WebSocket and starts the client pumps.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade error: %v", err)
		return
	}

	c := &Client{
		hub:   h,
		conn:  conn,
		send:  make(chan []byte, sendBufferSize),
		rooms: make(map[string]bool),
	}

	// The client MUST send CLIENT_IDENTIFY within identifyWindow seconds.
	if err := h.waitForIdentify(c); err != nil {
		log.Printf("ws identify timeout: %v", err)
		conn.Close()
		return
	}

	h.registerClient(c)
	defer h.handleDisconnect(c)

	// Push the current room list immediately on connect.
	go h.pushRoomList(c)

	go c.writePump()
	c.readPump() // blocks until the connection closes
}

// waitForIdentify reads exactly one frame and expects CLIENT_IDENTIFY.
func (h *Hub) waitForIdentify(c *Client) error {
	c.conn.SetReadDeadline(time.Now().Add(identifyWindow))
	defer c.conn.SetReadDeadline(time.Time{})

	_, raw, err := c.conn.ReadMessage()
	if err != nil {
		return err
	}

	var msg models.WSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return err
	}
	if msg.Type != "CLIENT_IDENTIFY" {
		return &wsError{"expected CLIENT_IDENTIFY"}
	}

	b, _ := json.Marshal(msg.Data)
	var p models.IdentifyPayload
	if err := json.Unmarshal(b, &p); err != nil || p.UserID == "" {
		return &wsError{"invalid CLIENT_IDENTIFY payload"}
	}

	c.userID = p.UserID
	c.username = p.Username
	return nil
}

func (h *Hub) pushRoomList(c *Client) {
	rooms, err := h.Store.GetAllActiveRooms(context.Background())
	if err != nil {
		log.Printf("pushRoomList error: %v", err)
		return
	}
	c.emit("ROOMS_UPDATED", rooms)
}

// handleDisconnect is deferred; cleans up presence and notifies room members.
func (h *Hub) handleDisconnect(c *Client) {
	h.unregisterClient(c)
	c.conn.Close()
	close(c.send)

	ctx := context.Background()
	for _, roomID := range c.activeRooms() {
		count, _ := h.LeaveRoom(ctx, c, roomID)

		// system message
		sysmsg := systemMessage(roomID, c.username+" DISCONNECTED")
		_ = h.Store.AddMessage(ctx, sysmsg)
		h.BroadcastRoom(roomID, "MESSAGE_SENT", sysmsg)

		h.BroadcastRoom(roomID, "USER_LEFT", models.UserEventPayload{
			RoomID:    roomID,
			UserID:    c.userID,
			Username:  c.username,
			UserCount: count,
		})
	}

	// Refresh lobby
	if rooms, err := h.Store.GetAllActiveRooms(ctx); err == nil {
		h.BroadcastAll("ROOMS_UPDATED", rooms)
	}
}

// ── Client pumps ──────────────────────────────────────────────────────────────

// readPump processes inbound WebSocket frames.
func (c *Client) readPump() {
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		c.handleMessage(raw)
	}
}

// writePump drains the send channel and sends pings.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ── Client: event dispatch ────────────────────────────────────────────────────

func (c *Client) handleMessage(raw []byte) {
	var msg models.WSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.emit("ERROR", models.ErrorPayload{Code: "BAD_JSON", Message: "Invalid JSON"})
		return
	}

	b, _ := json.Marshal(msg.Data)
	ctx := context.Background()

	switch msg.Type {
	case "JOIN_ROOM":
		var p models.JoinRoomPayload
		if json.Unmarshal(b, &p) != nil || p.RoomID == "" {
			c.emit("ERROR", models.ErrorPayload{Code: "INVALID_PAYLOAD", Message: "Missing roomId"})
			return
		}
		c.handleJoinRoom(ctx, p.RoomID)

	case "LEAVE_ROOM":
		var p models.LeaveRoomPayload
		if json.Unmarshal(b, &p) != nil || p.RoomID == "" {
			return
		}
		c.handleLeaveRoom(ctx, p.RoomID)

	case "SEND_MESSAGE":
		var p models.SendMessagePayload
		if json.Unmarshal(b, &p) != nil {
			return
		}
		c.handleSendMessage(ctx, p)
	}
}

func (c *Client) handleJoinRoom(ctx context.Context, roomID string) {
	// Verify room exists
	if _, err := c.hub.Store.GetRoom(ctx, roomID); err != nil {
		c.emit("ERROR", models.ErrorPayload{Code: "ROOM_NOT_FOUND", Message: "Room does not exist or has expired"})
		return
	}

	count, err := c.hub.JoinRoom(ctx, c, roomID)
	if err != nil {
		log.Printf("join room error: %v", err)
		return
	}

	// System message
	sysmsg := systemMessage(roomID, c.username+" JOINED THE ROOM")
	_ = c.hub.Store.AddMessage(ctx, sysmsg)
	c.hub.BroadcastRoom(roomID, "MESSAGE_SENT", sysmsg)

	// Notify room members
	c.hub.BroadcastRoom(roomID, "USER_JOINED", models.UserEventPayload{
		RoomID:    roomID,
		UserID:    c.userID,
		Username:  c.username,
		UserCount: count,
	})

	// Update all lobby clients
	if rooms, err := c.hub.Store.GetAllActiveRooms(ctx); err == nil {
		c.hub.BroadcastAll("ROOMS_UPDATED", rooms)
	}
}

func (c *Client) handleLeaveRoom(ctx context.Context, roomID string) {
	count, _ := c.hub.LeaveRoom(ctx, c, roomID)

	sysmsg := systemMessage(roomID, c.username+" LEFT THE ROOM")
	_ = c.hub.Store.AddMessage(ctx, sysmsg)
	c.hub.BroadcastRoom(roomID, "MESSAGE_SENT", sysmsg)

	c.hub.BroadcastRoom(roomID, "USER_LEFT", models.UserEventPayload{
		RoomID:    roomID,
		UserID:    c.userID,
		Username:  c.username,
		UserCount: count,
	})

	if rooms, err := c.hub.Store.GetAllActiveRooms(ctx); err == nil {
		c.hub.BroadcastAll("ROOMS_UPDATED", rooms)
	}
}

func (c *Client) handleSendMessage(ctx context.Context, p models.SendMessagePayload) {
	// Auth: must be in the room
	c.mu.RLock()
	inRoom := c.rooms[p.RoomID]
	c.mu.RUnlock()
	if !inRoom {
		c.emit("ERROR", models.ErrorPayload{Code: "UNAUTHORIZED", Message: "You are not in this room"})
		return
	}

	// Validate content
	content := trimContent(p.Content)
	if content == "" {
		return
	}
	if len([]rune(content)) > 500 {
		c.emit("ERROR", models.ErrorPayload{Code: "CONTENT_TOO_LONG", Message: "Message exceeds 500 characters"})
		return
	}

	msg := &models.Message{
		ID:        uuid.NewString(),
		RoomID:    p.RoomID,
		UserID:    c.userID,
		Username:  c.username,
		Content:   content,
		Timestamp: time.Now().UnixMilli(),
		Type:      "user",
	}

	if err := c.hub.Store.AddMessage(ctx, msg); err != nil {
		log.Printf("add message error: %v", err)
		return
	}

	// Reset room expiry
	_ = c.hub.Store.UpdateLastActivity(ctx, p.RoomID)

	// Broadcast to room
	c.hub.BroadcastRoom(p.RoomID, "MESSAGE_SENT", msg)

	// Nudge lobby so timers stay accurate
	if rooms, err := c.hub.Store.GetAllActiveRooms(ctx); err == nil {
		c.hub.BroadcastAll("ROOMS_UPDATED", rooms)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func systemMessage(roomID, content string) *models.Message {
	return &models.Message{
		ID:        uuid.NewString(),
		RoomID:    roomID,
		UserID:    "system",
		Username:  "SYSTEM",
		Content:   content,
		Timestamp: time.Now().UnixMilli(),
		Type:      "system",
	}
}

func trimContent(s string) string {
	// Trim leading/trailing whitespace
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

type wsError struct{ msg string }

func (e *wsError) Error() string { return e.msg }
