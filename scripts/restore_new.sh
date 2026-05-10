#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
TIMING_LOG="$IMAGES_DIR/restore-timing.log"

START_MS=$(date +%s%3N)
log_timing() {
  local now=$(date +%s%3N)
  local elapsed=$((now - START_MS))
  local timestamp=$(date '+%H:%M:%S.%3N')
  echo "[$timestamp +${elapsed}ms] $1" | sudo tee -a "$TIMING_LOG" >/dev/null
}

sudo rm -f "$TIMING_LOG"
log_timing "=== CRIU Restore - Replica ==="

# Kill leftover criu processes
log_timing "Killing leftover criu processes..."
sudo pkill -9 -f "criu lazy-pages" 2>/dev/null || true
sudo pkill -9 -f "criu/criu lazy-pages" 2>/dev/null || true
sleep 0.1

# Kill existing valkey
log_timing "Killing valkey-server..."
sudo pkill -9 valkey-server 2>/dev/null || true
sleep 1

# Signal readiness to primary (stdout goes back through SSH)
log_timing "Sending READY"
echo "READY"

# Wait for primary to tell us dump is started
log_timing "Waiting for START..."
if ! read -r -t 60 line || [ "$line" != "START" ]; then
  log_timing "ERROR: did not receive START (got: '$line')"
  exit 1
fi
log_timing "START received"

# Start lazy-pages (it will retry connecting to page server)
log_timing "Starting lazy-pages..."
sudo "$CRIU_BIN" lazy-pages \
  --images-dir "$IMAGES_DIR" \
  --page-server \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --cow-dump \
  --tcp-close \
  -v1 -o "$IMAGES_DIR/lazy-server.log" &
LAZY_PAGES_PID=$!

sleep 1
if ! kill -0 "$LAZY_PAGES_PID" 2>/dev/null; then
  log_timing "ERROR: lazy-pages exited early"
  sudo tail -n 120 "$IMAGES_DIR/lazy-server.log" 2>/dev/null || true
  exit 1
fi
log_timing "Lazy-pages started (PID: $LAZY_PAGES_PID)"

# Start CRIU restore (it will retry connecting to lazy-pages unix socket)
log_timing "Starting CRIU restore..."
if ! sudo "$CRIU_BIN" restore \
  --images-dir "$IMAGES_DIR" \
  --lazy-pages \
  --tcp-close \
  --cow-dump \
  --restore-detached \
  --skip-file-rwx-check \
  --skip-file-size-check \
  --file-validation filesize \
  -v1 -o "$IMAGES_DIR/lazy-restore.log"; then
  log_timing "ERROR: restore failed"
  sudo tail -n 120 "$IMAGES_DIR/lazy-restore.log" 2>/dev/null || true
  exit 1
fi
log_timing "CRIU restore completed"

# Configure replication
log_timing "Configuring replication..."
"$SCRIPT_DIR/wait_and_replicate_new.sh"
log_timing "Replication configured"

log_timing "=== Restore complete ==="
