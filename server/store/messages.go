package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/charlesgiovanne/Katabi-Backend/server/models"
)

// AddMessage appends a message to the room's list, trims to MaxMessages, and refreshes TTL.
func (s *Store) AddMessage(ctx context.Context, msg *models.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	pipe := s.rdb.Pipeline()
	pipe.RPush(ctx, roomMsgsKey(msg.RoomID), string(data))
	// Keep only the last MaxMessages entries (0-indexed, so -(MaxMessages) to -1)
	pipe.LTrim(ctx, roomMsgsKey(msg.RoomID), int64(-MaxMessages), -1)
	pipe.Expire(ctx, roomMsgsKey(msg.RoomID), RoomTTL*time.Second)
	_, err = pipe.Exec(ctx)
	return err
}

// GetMessages returns up to limit messages for a room, newest-first when before is set.
// Returns (messages oldest→newest, hasMore, error).
func (s *Store) GetMessages(ctx context.Context, roomID string, limit int, before string) ([]*models.Message, bool, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	// Fetch all messages (list is capped at 500 so this is bounded)
	raw, err := s.rdb.LRange(ctx, roomMsgsKey(roomID), 0, -1).Result()
	if err != nil {
		return nil, false, err
	}

	all := make([]*models.Message, 0, len(raw))
	for _, r := range raw {
		var m models.Message
		if err := json.Unmarshal([]byte(r), &m); err == nil {
			all = append(all, &m)
		}
	}

	// Cursor-based pagination: find the position of `before` message ID
	end := len(all)
	if before != "" {
		for i, m := range all {
			if m.ID == before {
				end = i
				break
			}
		}
	}

	start := end - limit
	hasMore := start > 0
	if start < 0 {
		start = 0
	}

	return all[start:end], hasMore, nil
}

// DeleteMessages removes all messages for a room (called on expiry).
func (s *Store) DeleteMessages(ctx context.Context, roomID string) error {
	return s.rdb.Del(ctx, roomMsgsKey(roomID)).Err()
}
