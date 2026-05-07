package store

import (
	"context"
	"time"
)

// AddUser adds a user to a room's presence set and refreshes TTL.
func (s *Store) AddUser(ctx context.Context, roomID, userID string) error {
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, roomUsersKey(roomID), userID)
	pipe.Expire(ctx, roomUsersKey(roomID), RoomTTL*time.Second)
	_, err := pipe.Exec(ctx)
	return err
}

// RemoveUser removes a user from a room's presence set.
func (s *Store) RemoveUser(ctx context.Context, roomID, userID string) error {
	return s.rdb.SRem(ctx, roomUsersKey(roomID), userID).Err()
}

// GetUserCount returns the number of users currently in a room.
func (s *Store) GetUserCount(ctx context.Context, roomID string) (int, error) {
	n, err := s.rdb.SCard(ctx, roomUsersKey(roomID)).Result()
	return int(n), err
}

// GetUsers returns all user IDs currently in a room.
func (s *Store) GetUsers(ctx context.Context, roomID string) ([]string, error) {
	return s.rdb.SMembers(ctx, roomUsersKey(roomID)).Result()
}

// DeletePresence removes the presence set for a room (called on expiry).
func (s *Store) DeletePresence(ctx context.Context, roomID string) error {
	return s.rdb.Del(ctx, roomUsersKey(roomID)).Err()
}
