#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA_SSH_HOST="${REPLICA_IP:-$REPLICA_HOST}"
TIMING_LOG="$IMAGES_DIR/migrate-timing.log"

# Timing helper
SCRIPT_START_MS=$(date +%s%3N)
log_timing() {
  local now=$(date +%s%3N)
  local elapsed=$((now - SCRIPT_START_MS))
  echo "[${elapsed}ms] $1" | sudo tee -a "$TIMING_LOG"
}

PID=$(pgrep -x valkey-server | head -n1)

if [ -z "$PID" ]; then
  echo "ERROR: valkey-server not running"
  exit 1
fi

# Step 0: Kill leftover ssh/restore processes
log_timing "Step 0: Killing leftover ssh restore processes..."
echo "Step 0: Killing leftover ssh restore processes..."
sudo pkill -9 -f "ssh.*restore_new.sh" 2>/dev/null || true
sleep 0.1
log_timing "Step 0: Done"

# Step 1: Clean images dir
log_timing "Step 1: Cleaning $IMAGES_DIR..."
echo "Step 1: Cleaning $IMAGES_DIR..."
sudo rm -rf "$IMAGES_DIR"/*
sudo rm -f "$TIMING_LOG"
log_timing "Step 1: Done"

# Step 2: Start restore on replica (it will wait for page server)
log_timing "Step 2: Starting restore on replica..."
echo "Step 2: Starting restore on replica..."
stdbuf -oL $SSH ubuntu@$REPLICA_SSH_HOST "sudo $SCRIPT_DIR/restore_new.sh" 2>&1 | stdbuf -oL sed 's/^/[replica] /' &
REPLICA_PID=$!
log_timing "Step 2: SSH launched (PID: $REPLICA_PID)"

# Step 3: Wait for replica ready signal
log_timing "Step 3: Waiting for replica ready signal..."
echo "Step 3: Waiting for replica ready signal..."
READY_FILE="$IMAGES_DIR/ready.log"
for i in $(seq 1 60); do
  if [ -f "$READY_FILE" ]; then
    log_timing "Step 3: Replica ready (iter $i)"
    echo "Replica ready"
    break
  fi
  sleep 0.5
done
if [ ! -f "$READY_FILE" ]; then
  log_timing "Step 3: ERROR - replica ready signal not found"
  echo "ERROR: replica ready signal not found at $READY_FILE"
  exit 1
fi

# Step 4: CRIU dump
log_timing "Step 4: Starting CRIU dump for PID $PID..."
echo "Step 4: Starting CRIU dump for PID $PID..."
START_TIME=$(date +%s%3N)

sudo "$CRIU_BIN" dump \
  --tree "$PID" \
  --images-dir "$IMAGES_DIR" \
  --cow-dump \
  --lazy-pages \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --tcp-close \
  --skip-in-flight \
  --ext-unix-sk \
  --leave-running \
  --display-stats \
  -v2 -o "$IMAGES_DIR/lazy-primary.log"

DUMP_END_TIME=$(date +%s%3N)
DUMP_ELAPSED=$((DUMP_END_TIME - START_TIME))
log_timing "Step 4: CRIU dump completed (took ${DUMP_ELAPSED}ms)"

# Step 5: Wait for replica master_link_status:up
log_timing "Step 5: Waiting for replica master_link_status:up..."
echo "Step 5: Waiting for replica master_link_status:up..."
REPLICA_PORT="${REPLICA_PORT:-6379}"
for i in $(seq 1 30); do
  STATUS=$($SSH ubuntu@$REPLICA_SSH_HOST "valkey-cli -p $REPLICA_PORT info replication 2>/dev/null | grep master_link_status" || true)
  if [[ "$STATUS" == *"master_link_status:up"* ]]; then
    END_TIME=$(date +%s%3N)
    ELAPSED=$((END_TIME - START_TIME))
    log_timing "Step 5: master_link_status:up (iter $i, ${ELAPSED}ms since dump start)"
    echo "Replica master_link_status:up after ${ELAPSED}ms"
    break
  fi
  sleep 0.5
done

if [[ "$STATUS" != *"master_link_status:up"* ]]; then
  log_timing "Step 5: WARNING - master_link_status:up not reached within 60s"
  echo "WARNING: master_link_status:up not reached within 60s"
fi

log_timing "Migration complete"
echo "Migration complete"

# Print timing summary
echo ""
echo "=== TIMING SUMMARY (primary) ==="
cat "$TIMING_LOG"
