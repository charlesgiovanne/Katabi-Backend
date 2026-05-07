package store

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Store wraps the Redis client and owns all data-access logic.
type Store struct {
	rdb *redis.Client
}

func New(redisURL string) (*Store, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_URL: %w", err)
	}

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Store{rdb: rdb}, nil
}

func (s *Store) Close() error {
	return s.rdb.Close()
}

// ── Redis key helpers ─────────────────────────────────────────────────────────

func roomKey(id string) string         { return "room:" + id }
func roomMsgsKey(id string) string     { return "room:" + id + ":messages" }
func roomUsersKey(id string) string    { return "room:" + id + ":users" }
func userKey(id string) string         { return "user:" + id }
func rateLimitKey(ip, roomID string) string {
	return "ratelimit:validate:" + ip + ":" + roomID
}

const (
	activeRoomsKey = "rooms:active"
	RoomTTL        = 3600 // seconds — 1 hour
	MaxMessages    = 500
)
