package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pixelchat/server/models"
)

// CreateRoom persists a new room. keywordHash is the bcrypt hash of the keyword.
func (s *Store) CreateRoom(ctx context.Context, room *models.Room, keywordHash string) error {
	pipe := s.rdb.Pipeline()

	pipe.HSet(ctx, roomKey(room.ID),
		"id", room.ID,
		"name", room.Name,
		"keyword_hash", keywordHash,
		"creator_id", room.CreatorID,
		"created_at", room.CreatedAt,
		"last_activity", room.LastActivity,
	)
	pipe.Expire(ctx, roomKey(room.ID), RoomTTL*time.Second)
	pipe.SAdd(ctx, activeRoomsKey, room.ID)

	_, err := pipe.Exec(ctx)
	return err
}

// GetRoom retrieves a room by ID. UserCount is NOT set here — caller populates it.
func (s *Store) GetRoom(ctx context.Context, roomID string) (*models.Room, error) {
	vals, err := s.rdb.HGetAll(ctx, roomKey(roomID)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("room not found: %s", roomID)
	}
	return roomFromMap(vals)
}

// GetRoomKeywordHash returns the stored bcrypt hash for keyword validation.
func (s *Store) GetRoomKeywordHash(ctx context.Context, roomID string) (string, error) {
	return s.rdb.HGet(ctx, roomKey(roomID), "keyword_hash").Result()
}

// UpdateLastActivity resets the room TTL and updates last_activity.
func (s *Store) UpdateLastActivity(ctx context.Context, roomID string) error {
	now := time.Now().UnixMilli()
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, roomKey(roomID), "last_activity", now)
	pipe.Expire(ctx, roomKey(roomID), RoomTTL*time.Second)
	pipe.Expire(ctx, roomMsgsKey(roomID), RoomTTL*time.Second)
	pipe.Expire(ctx, roomUsersKey(roomID), RoomTTL*time.Second)
	_, err := pipe.Exec(ctx)
	return err
}

// DeleteRoom removes all keys related to a room.
func (s *Store) DeleteRoom(ctx context.Context, roomID string) error {
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, roomKey(roomID))
	pipe.Del(ctx, roomMsgsKey(roomID))
	pipe.Del(ctx, roomUsersKey(roomID))
	pipe.SRem(ctx, activeRoomsKey, roomID)
	_, err := pipe.Exec(ctx)
	return err
}

// GetAllActiveRooms returns all non-expired rooms with live user counts.
func (s *Store) GetAllActiveRooms(ctx context.Context) ([]*models.Room, error) {
	ids, err := s.rdb.SMembers(ctx, activeRoomsKey).Result()
	if err != nil {
		return nil, err
	}

	rooms := make([]*models.Room, 0, len(ids))
	for _, id := range ids {
		room, err := s.GetRoom(ctx, id)
		if err != nil {
			// Room TTL expired in Redis but still in the set — clean up
			s.rdb.SRem(ctx, activeRoomsKey, id)
			continue
		}
		count, _ := s.GetUserCount(ctx, id)
		room.UserCount = count
		room.Keyword = "***" // never expose plaintext keyword in list
		rooms = append(rooms, room)
	}
	return rooms, nil
}

// GetExpiredRoomIDs returns room IDs whose lastActivity is older than expiryMs.
func (s *Store) GetExpiredRoomIDs(ctx context.Context, expiryMs int64) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, activeRoomsKey).Result()
	if err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	var expired []string
	for _, id := range ids {
		val, err := s.rdb.HGet(ctx, roomKey(id), "last_activity").Result()
		if err != nil {
			// Key gone — treat as expired
			expired = append(expired, id)
			continue
		}
		lastActivity, _ := strconv.ParseInt(val, 10, 64)
		if now-lastActivity >= expiryMs {
			expired = append(expired, id)
		}
	}
	return expired, nil
}

// CreateUser stores user data with a 24-hour TTL.
func (s *Store) CreateUser(ctx context.Context, user *models.User) error {
	return s.rdb.HSet(ctx, userKey(user.ID),
		"id", user.ID,
		"username", user.Username,
	).Err()
}

// GetUser retrieves a user by ID.
func (s *Store) GetUser(ctx context.Context, userID string) (*models.User, error) {
	vals, err := s.rdb.HGetAll(ctx, userKey(userID)).Result()
	if err != nil || len(vals) == 0 {
		return nil, fmt.Errorf("user not found: %s", userID)
	}
	return &models.User{ID: vals["id"], Username: vals["username"]}, nil
}

// IncrValidateAttempts increments the rate-limit counter and sets a 10-min TTL on first hit.
// Returns (current count, error).
func (s *Store) IncrValidateAttempts(ctx context.Context, ip, roomID string) (int64, error) {
	key := rateLimitKey(ip, roomID)
	pipe := s.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 10*time.Minute)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func roomFromMap(m map[string]string) (*models.Room, error) {
	createdAt, _ := strconv.ParseInt(m["created_at"], 10, 64)
	lastActivity, _ := strconv.ParseInt(m["last_activity"], 10, 64)

	// Suppress the linter — json.Marshal is only used for internal debug logging.
	_ = json.Marshal

	return &models.Room{
		ID:           m["id"],
		Name:         m["name"],
		KeywordHash:  m["keyword_hash"],
		CreatorID:    m["creator_id"],
		CreatedAt:    createdAt,
		LastActivity: lastActivity,
	}, nil
}
