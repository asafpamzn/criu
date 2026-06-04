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

# Auto-detect CLONE thread configuration based on CPU cores
NUM_CORES=$(nproc)
CLONE_PRE_SCAN_FLAG=""
if [ "$NUM_CORES" -ge 64 ]; then
  CLONE_P3_THREADS=15
  CLONE_P3_THREADS_BULK=15
  CLONE_SCANNERS=20
  CLONE_PRE_SCANNERS=1
  CLONE_DRAIN_THREADS=20
elif [ "$NUM_CORES" -ge 8 ]; then
  CLONE_P3_THREADS=15
  CLONE_P3_THREADS_BULK=15
  CLONE_SCANNERS=20
  CLONE_PRE_SCANNERS=1
  CLONE_DRAIN_THREADS=20
elif [ "$NUM_CORES" -ge 4 ]; then
  CLONE_P3_THREADS=4
  CLONE_P3_THREADS_BULK=1
  CLONE_SCANNERS=4
  CLONE_PRE_SCANNERS=1
  CLONE_DRAIN_THREADS=4
  CLONE_PRE_SCAN_FLAG="--clone-pre-scan"
else
  echo "ERROR: machine has only $NUM_CORES cores, need at least 4"
  exit 1
fi

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
log_timing "CLONE config: cores=$NUM_CORES p3=$CLONE_P3_THREADS p3_bulk=$CLONE_P3_THREADS_BULK scanners=$CLONE_SCANNERS pre_scanners=$CLONE_PRE_SCANNERS drain=$CLONE_DRAIN_THREADS pre_scan=${CLONE_PRE_SCAN_FLAG:+yes}"

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

sudo "$CRIU_BIN" lazy-pages \
  --images-dir "$IMAGES_DIR" \
  --page-server \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --clone-dump \
  --clone-p3-threads "$CLONE_P3_THREADS" \
  --clone-p3-threads-bulk "$CLONE_P3_THREADS_BULK" \
  --clone-scanners "$CLONE_SCANNERS" \
  --clone-pre-scanners "$CLONE_PRE_SCANNERS" \
  --clone-drain-threads "$CLONE_DRAIN_THREADS" \
  $CLONE_PRE_SCAN_FLAG \
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
  sudo dmesg -T | tail -10 | sudo tee -a "$TIMING_LOG"
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

log_timing "=== Restore complete ==="
