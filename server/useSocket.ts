import { useState, useEffect, useCallback, useRef } from 'react';
import type { Room, Message, ConnectionStatus } from '../types';

// ── config ────────────────────────────────────────────────────────────────────

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:3001';
const WS_URL   = API_BASE.replace(/^http/, 'ws') + '/ws';

export const EXPIRY_MS = 60 * 60 * 1000; // must match server constant

// ── types ─────────────────────────────────────────────────────────────────────

interface WSFrame {
  type: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any;
}

interface UseSocketOptions {
  userId: string;
  username: string;
  currentRoomId: string | null;
}

// ── hook ──────────────────────────────────────────────────────────────────────

export function useSocket({ userId, username, currentRoomId }: UseSocketOptions) {
  const [status, setStatus] = useState<ConnectionStatus>('connecting');
  const [rooms, setRooms]     = useState<Room[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);

  const wsRef      = useRef<WebSocket | null>(null);
  const roomRef    = useRef<string | null>(currentRoomId);
  const reconnectRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const attemptRef = useRef(0);

  // track current room in a ref so callbacks always see the latest value
  useEffect(() => { roomRef.current = currentRoomId; }, [currentRoomId]);

  // ── low-level send ─────────────────────────────────────────────────────────

  const send = useCallback((type: string, data: unknown) => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type, data }));
    }
  }, []);

  // ── REST helpers ───────────────────────────────────────────────────────────

  const apiFetch = useCallback(async (path: string, opts?: RequestInit) => {
    const res = await fetch(API_BASE + path, {
      headers: { 'Content-Type': 'application/json' },
      ...opts,
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message ?? `HTTP ${res.status}`);
    }
    return res.json();
  }, []);

  // ── WebSocket lifecycle ───────────────────────────────────────────────────

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    setStatus('connecting');
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setStatus('connected');
      attemptRef.current = 0;

      // Handshake — must be first message
      ws.send(JSON.stringify({
        type: 'CLIENT_IDENTIFY',
        data: { userId, username },
      }));

      // Re-join active room after a reconnect
      if (roomRef.current) {
        ws.send(JSON.stringify({
          type: 'JOIN_ROOM',
          data: { roomId: roomRef.current, userId },
        }));
      }
    };

    ws.onclose = () => {
      setStatus('disconnected');
      wsRef.current = null;

      // Exponential back-off reconnect (cap at 30 s)
      const delay = Math.min(1000 * 2 ** attemptRef.current, 30_000);
      attemptRef.current += 1;
      reconnectRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };

    ws.onmessage = (e) => {
      try {
        const frame: WSFrame = JSON.parse(e.data);
        handleFrame(frame);
      } catch { /* ignore malformed frames */ }
    };
  // handleFrame intentionally omitted from deps (stable ref pattern below)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userId, username]);

  // stable ref so ws.onmessage always has the latest handler without re-connecting
  const handleFrameRef = useRef<(f: WSFrame) => void>(() => {});

  const handleFrame = useCallback((frame: WSFrame) => {
    switch (frame.type) {
      case 'ROOMS_UPDATED':
        setRooms(frame.data ?? []);
        break;

      case 'MESSAGE_SENT':
        // Only add message if it belongs to the currently open room
        if (frame.data?.roomId === roomRef.current) {
          setMessages(prev => {
            // deduplicate by id
            if (prev.some(m => m.id === frame.data.id)) return prev;
            return [...prev, frame.data as Message];
          });
        }
        break;

      case 'USER_JOINED':
      case 'USER_LEFT':
        // Room user counts are included in the next ROOMS_UPDATED;
        // these events are mainly consumed for future UI additions (typing indicators etc.)
        break;

      case 'ROOM_EXPIRED':
        if (frame.data?.roomId === roomRef.current) {
          // Page will redirect to lobby — handled by ChatRoom.tsx watching rooms list
          setMessages([]);
        }
        break;

      case 'ERROR':
        console.warn('[pixelchat ws error]', frame.data);
        break;
    }
  }, []);

  useEffect(() => { handleFrameRef.current = handleFrame; }, [handleFrame]);

  // patch ws.onmessage every render so it always uses the latest handler
  useEffect(() => {
    if (wsRef.current) {
      wsRef.current.onmessage = (e) => {
        try { handleFrameRef.current(JSON.parse(e.data)); } catch { /* */ }
      };
    }
  });

  // ── connect on mount, cleanup on unmount ──────────────────────────────────

  useEffect(() => {
    connect();
    return () => {
      if (reconnectRef.current) clearTimeout(reconnectRef.current);
      wsRef.current?.close();
    };
  }, [connect]);

  // ── room navigation: join / leave ─────────────────────────────────────────

  useEffect(() => {
    const prev = roomRef.current;

    if (prev && prev !== currentRoomId) {
      send('LEAVE_ROOM', { roomId: prev, userId });
      setMessages([]);
    }

    if (currentRoomId) {
      send('JOIN_ROOM', { roomId: currentRoomId, userId });
      // Load message history via REST
      apiFetch(`/api/rooms/${currentRoomId}/messages?limit=50`)
        .then(res => setMessages(res.messages ?? []))
        .catch(err => console.warn('Failed to load messages:', err));
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentRoomId]);

  // ── public API (mirrors the old localStorage hook exactly) ───────────────

  const createRoom = useCallback(async (name: string, keyword: string): Promise<Room> => {
    const room: Room = await apiFetch('/api/rooms', {
      method: 'POST',
      body: JSON.stringify({ name, keyword, creatorId: userId }),
    });
    return room;
  }, [apiFetch, userId]);

  const sendMessage = useCallback((roomId: string, content: string) => {
    send('SEND_MESSAGE', { roomId, userId, username, content });
  }, [send, userId, username]);

  const postSystemMessage = useCallback((_roomId: string, _content: string) => {
    // System messages are now generated server-side.
    // This is a no-op kept for API compatibility with ChatRoom.tsx.
  }, []);

  const validateKeyword = useCallback(async (roomId: string, keyword: string): Promise<boolean> => {
    try {
      const res = await apiFetch(`/api/rooms/${roomId}/validate`, {
        method: 'POST',
        body: JSON.stringify({ keyword }),
      });
      return res.valid === true;
    } catch {
      return false;
    }
  }, [apiFetch]);

  const getRoomById = useCallback((roomId: string): Room | undefined => {
    return rooms.find(r => r.id === roomId);
  }, [rooms]);

  const getRemainingMs = useCallback((room: Room): number => {
    return Math.max(0, EXPIRY_MS - (Date.now() - room.lastActivity));
  }, []);

  const syncRooms = useCallback(async () => {
    try {
      const data = await apiFetch('/api/rooms');
      setRooms(data ?? []);
    } catch (err) {
      console.warn('syncRooms failed:', err);
    }
  }, [apiFetch]);

  return {
    status,
    rooms,
    messages,
    createRoom,
    sendMessage,
    postSystemMessage,
    validateKeyword,
    getRoomById,
    getRemainingMs,
    syncRooms,
    EXPIRY_MS,
  };
}
