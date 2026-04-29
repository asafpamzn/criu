#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
LOG_FILE="$IMAGES_DIR/lazy-primary.log"
LOG_FILE_SERVER="$IMAGES_DIR/lazy-server.log"
TIMING_LOG="$IMAGES_DIR/restore-timing.log"

# Timing helper
START_MS=$(date +%s%3N)
log_timing() {
  local now=$(date +%s%3N)
  local elapsed=$((now - START_MS))
  local timestamp=$(date '+%H:%M:%S.%3N')
  echo "[$timestamp +${elapsed}ms] $1" | sudo tee -a "$TIMING_LOG"
}

sudo rm -f "$TIMING_LOG"
log_timing "=== CRIU Restore - Replica ==="

echo "=== CRIU Restore - Replica ==="

# Step 0: Kill leftover criu processes
log_timing "Step 0: Killing leftover criu processes..."
echo "Step 0: Killing leftover criu processes..."
sudo pkill -9 -f "criu lazy-pages" 2>/dev/null || true
sudo pkill -9 -f "criu/criu lazy-pages" 2>/dev/null || true
sleep 0.1
log_timing "Step 0: Done"

# Step 1: Kill existing valkey
log_timing "Step 1: Killing valkey-server..."
echo "Step 1: Killing valkey-server..."
sudo pkill -9 valkey-server 2>/dev/null || true
sleep 1
log_timing "Step 1: Done (after 1s sleep)"

# Step 2: Signal readiness to PRIMARY
log_timing "Step 2: Creating ready signal..."
echo "Step 2: Creating ready signal..."
echo "READY" | sudo tee "$IMAGES_DIR/ready.log" >/dev/null
log_timing "Step 2: Done"

# Step 3: Wait for source page-server
log_timing "Step 3: Waiting for source page-server..."
echo "Step 3: Waiting for source page-server..."
while true; do
  if [ -f "$LOG_FILE" ] && sudo grep -q "PAGE SERVER READY TO SERVE" "$LOG_FILE" 2>/dev/null; then
    log_timing "Step 3: Source page-server ready"
    echo "Source page-server ready"
    break
  fi
  sleep 0.1
done

# Step 4: Start lazy-pages daemon
log_timing "Step 4: Starting lazy-pages daemon..."
echo "Step 4: Starting lazy-pages daemon..."
sudo rm -f "$IMAGES_DIR/lazy-server.log"

sudo "$CRIU_BIN" lazy-pages \
  --images-dir "$IMAGES_DIR" \
  --page-server \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --cow-dump \
  --tcp-close \
  -v1 -o "$IMAGES_DIR/lazy-server.log" &
LAZY_PAGES_PID=$!

log_timing "Step 4: lazy-pages launched, sleeping 1s..."
sleep 1
if ! kill -0 "$LAZY_PAGES_PID" 2>/dev/null; then
  log_timing "Step 4: ERROR - lazy-pages exited early"
  echo "ERROR: lazy-pages exited early"
  sudo tail -n 120 "$LOG_FILE_SERVER" 2>/dev/null || true
  exit 1
fi
log_timing "Step 4: Done - Lazy-pages started (PID: $LAZY_PAGES_PID)"
echo "Lazy-pages started (PID: $LAZY_PAGES_PID)"

# Step 5: Wait for Phase 3 skeleton dump
log_timing "Step 5: Waiting for skeleton dump..."
echo "Step 5: Waiting for skeleton dump..."
SKELETON_READY_PATTERN="PHASE 3 SKELETON DUMP COMPLETE"
PHASE3_READY_PATTERN="COW Phase 3: Waiting for restore to connect"

STEP5_ITER=0
SKELETON_FOUND=0
PHASE3_FOUND=0
while true; do
  STEP5_ITER=$((STEP5_ITER + 1))
  if [ $SKELETON_FOUND -eq 0 ] && [ -f "$LOG_FILE" ] && sudo grep -q "$SKELETON_READY_PATTERN" "$LOG_FILE" 2>/dev/null; then
    SKELETON_FOUND=1
    log_timing "Step 5: SKELETON_READY_PATTERN found (iter $STEP5_ITER)"
  fi
  if [ $PHASE3_FOUND -eq 0 ] && [ -f "$LOG_FILE_SERVER" ] && sudo grep -q "$PHASE3_READY_PATTERN" "$LOG_FILE_SERVER" 2>/dev/null; then
    PHASE3_FOUND=1
    log_timing "Step 5: PHASE3_READY_PATTERN found (iter $STEP5_ITER)"
  fi
  if [ $SKELETON_FOUND -eq 1 ] && [ $PHASE3_FOUND -eq 1 ]; then
    log_timing "Step 5: Both patterns found - skeleton dump ready"
    echo "Skeleton dump ready"
    break
  fi
  if ! kill -0 "$LAZY_PAGES_PID" 2>/dev/null; then
    log_timing "Step 5: ERROR - lazy-pages died"
    echo "ERROR: lazy-pages died"
    sudo tail -n 120 "$LOG_FILE_SERVER" 2>/dev/null || true
    exit 1
  fi
  sleep 0.1
done

# Step 6: CRIU restore
log_timing "Step 6: Starting CRIU restore..."
echo "Step 6: Starting CRIU restore..."
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
  log_timing "Step 6: ERROR - restore failed"
  echo "ERROR: restore failed"
  sudo tail -n 120 "$IMAGES_DIR/lazy-restore.log" 2>/dev/null || true
  exit 1
fi
log_timing "Step 6: CRIU restore command completed"

# Step 7: Call wait_and_replicate
log_timing "Step 7: Configuring replication..."
echo "Step 7: Configuring replication..."
"$SCRIPT_DIR/wait_and_replicate_new.sh"
log_timing "Step 7: Done - replication configured"

log_timing "=== Restore complete ==="
echo "=== Restore complete ==="
