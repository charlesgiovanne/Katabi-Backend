# PIXELCHAT — Go Backend

Real-time chat server for PIXELCHAT. Built with Go, Gorilla WebSocket, and Redis.

---

## Requirements

| Tool | Minimum Version | Purpose |
|---|---|---|
| Go | 1.22 | Runtime & compiler |
| Redis | 7.0 | Room storage, presence, message buffer |
| Git | any | Clone the repo |

> **No Docker required** to run the server itself, but Docker is the easiest way to spin up Redis locally if you don't have it installed.

---

## Dependencies (Go modules)

These are fetched automatically by `go mod tidy`. You do not install them manually.

| Package | Version | Purpose |
|---|---|---|
| `github.com/gorilla/mux` | v1.8.1 | HTTP router with path params (`{id}`) |
| `github.com/gorilla/websocket` | v1.5.1 | WebSocket upgrade & framing |
| `github.com/redis/go-redis/v9` | v9.5.1 | Redis client |
| `github.com/google/uuid` | v1.6.0 | UUID v4 generation for IDs |
| `github.com/rs/cors` | v1.10.1 | CORS middleware for browser requests |
| `golang.org/x/crypto` | v0.22.0 | bcrypt for hashing room keywords |

---

## Step-by-Step Setup

### Step 1 — Install Go

**macOS (Homebrew)**
```bash
brew install go
```

**Ubuntu / Debian**
```bash
sudo apt update
sudo apt install -y golang-go
# If the apt version is below 1.22, install directly from go.dev:
wget https://go.dev/dl/go1.22.3.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.22.3.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
```

**Windows**
Download and run the installer from https://go.dev/dl/

**Verify**
```bash
go version
# Expected: go version go1.22.x ...
```

---

### Step 2 — Install Redis

Pick one option.

**Option A — Docker (recommended, zero config)**
```bash
docker run -d \
  --name pixelchat-redis \
  -p 6379:6379 \
  redis:7-alpine
```

Stop/start later:
```bash
docker stop pixelchat-redis
docker start pixelchat-redis
```

**Option B — macOS (Homebrew)**
```bash
brew install redis
brew services start redis
```

**Option C — Ubuntu / Debian**
```bash
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable --now redis-server
```

**Option D — Windows**
Download the MSI from https://github.com/microsoftarchive/redis/releases
or use WSL2 and follow the Ubuntu steps above.

**Verify Redis is running**
```bash
redis-cli ping
# Expected: PONG
```

---

### Step 3 — Clone the Repo

```bash
git clone https://github.com/YOUR_USERNAME/pixelchat-server.git
cd pixelchat-server
```

---

### Step 4 — Configure Environment

```bash
cp .env.example .env
```

Open `.env` and set your values:

```env
PORT=3001
REDIS_URL=redis://localhost:6379
CORS_ORIGIN=http://localhost:5173
```

| Variable | Description | Default |
|---|---|---|
| `PORT` | Port the server listens on | `3001` |
| `REDIS_URL` | Full Redis connection URL | `redis://localhost:6379` |
| `CORS_ORIGIN` | Frontend origin allowed by CORS | `http://localhost:5173` |

> For Redis with a password: `redis://:yourpassword@localhost:6379`
> For Redis with TLS (e.g. Upstash): `rediss://user:password@host:port`

---

### Step 5 — Install Go Dependencies

```bash
go mod tidy
```

This downloads all packages listed in `go.mod` into your local module cache. It also generates/updates `go.sum`.

---

### Step 6 — Run the Server

**Development (reads `.env` automatically via the shell)**
```bash
export $(cat .env | xargs) && go run .
```

Or set variables inline:
```bash
PORT=3001 REDIS_URL=redis://localhost:6379 CORS_ORIGIN=http://localhost:5173 go run .
```

**Expected output:**
```
✓ Redis connected (redis://localhost:6379)
[expiry] worker started (interval=1m0s, threshold=1h)
✓ PIXELCHAT server listening on :3001
```

**Verify it works:**
```bash
curl http://localhost:3001/health
# Expected: OK

curl http://localhost:3001/api/rooms
# Expected: []
```

---

### Step 7 — Build a Production Binary

```bash
go build -o pixelchat-server .
```

Run the binary:
```bash
PORT=3001 REDIS_URL=redis://localhost:6379 CORS_ORIGIN=https://yourdomain.com ./pixelchat-server
```

Cross-compile for Linux from macOS or Windows:
```bash
GOOS=linux GOARCH=amd64 go build -o pixelchat-server-linux .
```

---

## Connecting the Frontend

In the frontend repo, create or update `.env.local`:

```env
VITE_API_URL=http://localhost:3001
```

For production:
```env
VITE_API_URL=https://api.yourdomain.com
```

Then replace `src/hooks/useSocket.ts` with the `useSocket.ts` file included in this repo.

---

## Project Structure

```
pixelchat-server/
├── main.go               # Entry point — router, server, graceful shutdown
├── go.mod                # Module definition and dependency versions
├── go.sum                # Dependency checksums (commit this)
├── .env.example          # Environment variable template
│
├── config/
│   └── config.go         # Loads PORT, REDIS_URL, CORS_ORIGIN from env
│
├── models/
│   └── models.go         # All shared types: User, Room, Message, WS payloads
│
├── store/
│   ├── store.go          # Redis client init, key helpers, constants
│   ├── rooms.go          # Room CRUD + user registration + rate-limit counters
│   ├── messages.go       # Message list storage (capped at 500, paginated)
│   └── presence.go       # Per-room user presence sets
│
├── hub/
│   └── hub.go            # WebSocket hub: Client structs, read/write pumps,
│                         # JOIN_ROOM / LEAVE_ROOM / SEND_MESSAGE dispatch,
│                         # disconnect cleanup, broadcasts
│
├── handlers/
│   └── http.go           # All 5 REST endpoints with validation + bcrypt
│
├── jobs/
│   └── expiry.go         # Background goroutine: deletes rooms after 1hr inactivity
│
└── useSocket.ts          # Drop-in replacement for the frontend hook
```

---

## API Reference (Quick)

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check — returns `OK` |
| `POST` | `/api/register` | Create a user session |
| `GET` | `/api/rooms` | List all active rooms |
| `POST` | `/api/rooms` | Create a new room |
| `POST` | `/api/rooms/{id}/validate` | Validate a room keyword |
| `GET` | `/api/rooms/{id}/messages` | Fetch message history |
| `WS` | `/ws` | WebSocket connection |

---

## WebSocket Protocol

All frames are JSON: `{ "type": "EVENT_NAME", "data": { ... } }`

**Client → Server**
| Event | When |
|---|---|
| `CLIENT_IDENTIFY` | First frame after connecting (required within 5s) |
| `JOIN_ROOM` | User enters a chat room |
| `LEAVE_ROOM` | User leaves a chat room |
| `SEND_MESSAGE` | User sends a message |

**Server → Client**
| Event | When |
|---|---|
| `ROOMS_UPDATED` | Room list changed (create, join, leave, expire) |
| `MESSAGE_SENT` | New message in a room |
| `USER_JOINED` | Someone joined the room |
| `USER_LEFT` | Someone left the room |
| `ROOM_EXPIRED` | Room was deleted after 1hr inactivity |
| `ERROR` | Something went wrong |

---

## Hosting Guide

### Option A — Railway (easiest, free tier available)

1. Push the repo to GitHub.
2. Go to https://railway.app → **New Project** → **Deploy from GitHub repo**.
3. Add a **Redis** plugin inside the project (Railway provides managed Redis).
4. Set environment variables in the Railway dashboard:
   ```
   REDIS_URL   = (Railway auto-fills this when you add the Redis plugin)
   CORS_ORIGIN = https://your-frontend-domain.com
   PORT        = 3001
   ```
5. Railway auto-detects Go and runs `go build` + the binary. Done.

---

### Option B — Render

1. Push to GitHub.
2. https://render.com → **New Web Service** → connect your repo.
3. Set:
   - **Runtime:** Go
   - **Build Command:** `go build -o server .`
   - **Start Command:** `./server`
4. Add a **Redis** instance under **New → Redis** in Render.
5. Copy the Redis internal URL into the `REDIS_URL` environment variable.
6. Set `CORS_ORIGIN` to your frontend URL.

---

### Option C — Fly.io

1. Install the Fly CLI: https://fly.io/docs/hands-on/install-flyctl/
2. From the repo root:
   ```bash
   fly auth login
   fly launch          # follow prompts, choose region
   fly redis create    # create a managed Redis instance
   fly secrets set REDIS_URL="<url from above>"
   fly secrets set CORS_ORIGIN="https://your-frontend.com"
   fly deploy
   ```

---

### Option D — VPS (DigitalOcean / Linode / Hetzner)

```bash
# On the server — install Go
wget https://go.dev/dl/go1.22.3.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.22.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Install Redis
sudo apt install -y redis-server
sudo systemctl enable --now redis-server

# Clone and build
git clone https://github.com/YOUR_USERNAME/pixelchat-server.git
cd pixelchat-server
go mod tidy
go build -o pixelchat-server .

# Run as a systemd service (keeps it alive after SSH disconnect)
sudo nano /etc/systemd/system/pixelchat.service
```

Paste into the service file:
```ini
[Unit]
Description=PIXELCHAT Server
After=network.target redis.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/pixelchat-server
ExecStart=/home/ubuntu/pixelchat-server/pixelchat-server
Restart=on-failure
Environment=PORT=3001
Environment=REDIS_URL=redis://localhost:6379
Environment=CORS_ORIGIN=https://your-frontend.com

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pixelchat
sudo systemctl status pixelchat

# View live logs
sudo journalctl -u pixelchat -f
```

**Nginx reverse proxy (optional but recommended for HTTPS)**
```bash
sudo apt install -y nginx certbot python3-certbot-nginx

sudo nano /etc/nginx/sites-available/pixelchat
```

```nginx
server {
    server_name api.yourdomain.com;

    location / {
        proxy_pass         http://localhost:3001;
        proxy_http_version 1.1;

        # Required for WebSocket upgrade
        proxy_set_header   Upgrade    $http_upgrade;
        proxy_set_header   Connection "upgrade";
        proxy_set_header   Host       $host;
        proxy_set_header   X-Real-IP  $remote_addr;
        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;

        proxy_read_timeout  86400;  # keep WS connections alive
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/pixelchat /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Free HTTPS via Let's Encrypt
sudo certbot --nginx -d api.yourdomain.com
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `redis ping failed: dial tcp: connection refused` | Redis not running | Run `redis-server` or start the Docker container |
| `bind: address already in use` | Port 3001 taken | Change `PORT` in `.env` or kill the process on 3001 |
| `go: command not found` | Go not in PATH | Re-run `source ~/.bashrc` or restart terminal |
| CORS error in browser | Wrong `CORS_ORIGIN` | Set it to the exact frontend origin including port |
| WebSocket closes immediately | `CLIENT_IDENTIFY` not sent within 5s | Check frontend `useSocket.ts` is sending it on open |
| `invalid REDIS_URL` | Malformed URL | Must start with `redis://` or `rediss://` |
