#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# TLS flag: pass --tls to enable
USE_TLS=false
if [[ "${1:-}" == "--tls" ]]; then
  USE_TLS=true
  shift
fi

export IMAGES_DIR="/dev/shm/criu-migrate"
CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
TLS_CERT="${TLS_CERT:-}"
TLS_KEY="${TLS_KEY:-}"
TLS_CACERT="${TLS_CACERT:-}"
TIMING_LOG="$IMAGES_DIR/restore-timing.log"

START_MS=$(date +%s%3N)
log_timing() {
  local now=$(date +%s%3N)
  local elapsed=$((now - START_MS))
  local timestamp=$(date '+%H:%M:%S.%3N')
  echo "[$timestamp +${elapsed}ms] $1" | sudo tee -a "$TIMING_LOG" >/dev/null
}

sudo mkdir -p "$IMAGES_DIR"
sudo rm -f "$TIMING_LOG"
log_timing "=== CRIU Restore - Replica ==="

# Enable core dumps for debugging
log_timing "Enabling core dumps..."
sudo bash -c 'echo "/tmp/core.%e.%p.%t" > /proc/sys/kernel/core_pattern'
sudo bash -c 'echo 0 > /proc/sys/kernel/core_pipe_limit'
ulimit -c unlimited

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

# Wait for primary to tell us dump is started (format: "START <PID>")
log_timing "Waiting for START..."
if ! read -r -t 60 line; then
  log_timing "ERROR: timeout waiting for START"
  exit 1
fi
TREE_PID="${line#START }"
if [ "$TREE_PID" = "$line" ]; then
  log_timing "ERROR: expected 'START <PID>', got: '$line'"
  exit 1
fi
log_timing "START received (tree PID: $TREE_PID)"

# Start lazy-pages (connects to page server, buffers pages, starts restore)
log_timing "Starting lazy-pages..."

TLS_OPTS=""
if [ "$USE_TLS" = true ] && [ -n "$TLS_CERT" ]; then
  TLS_OPTS="--tls --tls-cert $TLS_CERT --tls-key $TLS_KEY --tls-cacert $TLS_CACERT --tls-no-cn-verify"
  log_timing "TLS enabled: cert=$TLS_CERT"
else
  log_timing "TLS disabled"
fi

# Monitor: wait for restored process to appear then attach strace immediately
(
  while [ ! -d "/proc/$TREE_PID/fd" ]; do sleep 0.01; done
  sudo strace -p "$TREE_PID" -tt -f -e trace=write,exit_group,openat \
    -s 1024 -o /dev/shm/criu-migrate/valkey-strace.log
) &
MONITOR_PID=$!
log_timing "strace monitor launched for PID $TREE_PID"

sudo "$CRIU_BIN" lazy-pages \
  --images-dir "$IMAGES_DIR" \
  --page-server \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --cow-dump \
  --tree "$TREE_PID" \
  --tcp-close \
  $TLS_OPTS \
  -v1 -o "$IMAGES_DIR/lazy-server.log" &
LAZY_PAGES_PID=$!

# Wait for lazy-pages to complete (it starts restore internally)
wait $LAZY_PAGES_PID
LP_EXIT=$?
if [ $LP_EXIT -ne 0 ]; then
  log_timing "ERROR: lazy-pages failed (exit code $LP_EXIT)"
  sudo tail -n 120 "$IMAGES_DIR/lazy-server.log" 2>/dev/null || true
  exit 1
fi
log_timing "Lazy-pages completed (restore done)"

# Give a moment for valkey to stabilize or die
sleep 1

# Check valkey-server state
PROC_STATE=$(cat /proc/$TREE_PID/status 2>/dev/null | grep "^State:" || echo "State: GONE")
log_timing "valkey-server PID=$TREE_PID $PROC_STATE"

if [[ "$PROC_STATE" == *"zombie"* ]] || [[ "$PROC_STATE" == *"GONE"* ]]; then
  log_timing "ERROR: valkey-server is dead!"
  log_timing "--- valkey-strace.log (last 50 lines) ---"
  sudo tail -50 /dev/shm/criu-migrate/valkey-strace.log 2>/dev/null | sudo tee -a "$TIMING_LOG" || true
  sudo dmesg -T | tail -10 | sudo tee -a "$TIMING_LOG"
  kill $MONITOR_PID 2>/dev/null || true
  exit 1
fi

sudo prlimit --pid "$TREE_PID" --core=unlimited:unlimited
log_timing "Core dump limit set to unlimited for PID $TREE_PID"

# Configure replication
log_timing "Configuring replication..."
REPLICATE_TLS_FLAG=""
if [ "$USE_TLS" = true ]; then
  REPLICATE_TLS_FLAG="--tls"
fi
"$SCRIPT_DIR/wait_and_replicate_new.sh" $REPLICATE_TLS_FLAG
log_timing "Replication configured"

# Stop strace monitor
kill $MONITOR_PID 2>/dev/null || true

log_timing "=== Restore complete ==="
