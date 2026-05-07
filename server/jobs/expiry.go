package jobs

import (
	"context"
	"log"
	"time"

	"github.com/charlesgiovanne/Katabi-Backend/server/store"
)

const (
	ExpiryMS      = int64(60 * 60 * 1000) // 1 hour in milliseconds
	CheckInterval = 60 * time.Second
)

// StartExpiryWorker launches a goroutine that periodically deletes inactive rooms
// and notifies connected clients via the provided broadcast functions.
func StartExpiryWorker(
	s *store.Store,
	broadcastAll func(msgType string, data any),
	broadcastRoom func(roomID, msgType string, data any),
) {
	go func() {
		ticker := time.NewTicker(CheckInterval)
		defer ticker.Stop()

		for range ticker.C {
			runExpiryCheck(s, broadcastAll, broadcastRoom)
		}
	}()
	log.Printf("[expiry] worker started (interval=%s, threshold=1h)", CheckInterval)
}

func runExpiryCheck(
	s *store.Store,
	broadcastAll func(string, any),
	broadcastRoom func(string, string, any),
) {
	ctx := context.Background()

	expiredIDs, err := s.GetExpiredRoomIDs(ctx, ExpiryMS)
	if err != nil {
		log.Printf("[expiry] error fetching expired rooms: %v", err)
		return
	}
	if len(expiredIDs) == 0 {
		return
	}

	log.Printf("[expiry] deleting %d expired room(s): %v", len(expiredIDs), expiredIDs)

	for _, roomID := range expiredIDs {
		// Notify clients currently in the room before deleting
		broadcastRoom(roomID, "ROOM_EXPIRED", map[string]string{"roomId": roomID})

		if err := s.DeleteRoom(ctx, roomID); err != nil {
			log.Printf("[expiry] failed to delete room %s: %v", roomID, err)
		}
	}

	// Push updated room list to all lobby clients
	rooms, err := s.GetAllActiveRooms(ctx)
	if err != nil {
		log.Printf("[expiry] error fetching rooms after expiry: %v", err)
		return
	}
	broadcastAll("ROOMS_UPDATED", rooms)
}
