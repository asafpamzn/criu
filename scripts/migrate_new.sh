#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA_SSH_HOST="${REPLICA_IP:-$REPLICA_HOST}"
TIMING_LOG="$IMAGES_DIR/migrate-timing.log"

SCRIPT_START_MS=$(date +%s%3N)
log_timing() {
  local now=$(date +%s%3N)
  local elapsed=$((now - SCRIPT_START_MS))
  local timestamp=$(date '+%H:%M:%S.%3N')
  echo "[$timestamp +${elapsed}ms] $1" | sudo tee -a "$TIMING_LOG" >&2
}

PID=$(pgrep -x valkey-server | head -n1)
if [ -z "$PID" ]; then
  echo "ERROR: valkey-server not running"
  exit 1
fi

# Kill leftover ssh/restore processes
log_timing "Killing leftover processes..."
sudo pkill -9 -f "ssh.*restore_new.sh" 2>/dev/null || true
sleep 0.1

# Clean images dir
log_timing "Cleaning $IMAGES_DIR..."
sudo rm -rf "$IMAGES_DIR"/*
sudo rm -f "$TIMING_LOG"

# Start replica over SSH (bidirectional protocol via coproc)
log_timing "Starting restore on replica..."
coproc REPLICA { $SSH ubuntu@$REPLICA_SSH_HOST "sudo $SCRIPT_DIR/restore_new.sh"; }
REPLICA_SAVED_PID=$REPLICA_PID
log_timing "SSH launched (PID: $REPLICA_SAVED_PID)"

# Wait for replica READY (cleanup done, ready for dump)
log_timing "Waiting for READY from replica..."
if ! read -r -t 60 line <&"${REPLICA[0]}" || [ "$line" != "READY" ]; then
  log_timing "ERROR: replica did not send READY (got: '$line')"
  exit 1
fi
log_timing "READY received"

# Start CRIU dump
log_timing "Starting CRIU dump for PID $PID..."
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
  -v2 -o "$IMAGES_DIR/lazy-primary.log" &
DUMP_PID=$!
log_timing "CRIU dump started (PID: $DUMP_PID)"

# Tell replica to start (dump is running, page server will accept with retry)
echo "START" >&"${REPLICA[1]}"
log_timing "Sent START to replica"

# Poll for replica master_link_status:up
log_timing "Polling for master_link_status:up..."
REPLICA_PORT="${REPLICA_PORT:-6379}"
STATUS=""
for i in $(seq 1 120); do
  STATUS=$($SSH ubuntu@$REPLICA_SSH_HOST "valkey-cli -p $REPLICA_PORT info replication 2>/dev/null | grep master_link_status" || true)
  if [[ "$STATUS" == *"master_link_status:up"* ]]; then
    END_TIME=$(date +%s%3N)
    ELAPSED=$((END_TIME - START_TIME))
    log_timing "master_link_status:up (iter $i, ${ELAPSED}ms since dump start)"
    echo "Replica master_link_status:up after ${ELAPSED}ms"
    break
  fi
  sleep 0.5
done

if [[ "$STATUS" != *"master_link_status:up"* ]]; then
  ELAPSED=0
  log_timing "WARNING: master_link_status:up not reached within 60s"
  echo "WARNING: master_link_status:up not reached within 60s"
fi

# Wait for CRIU dump to finish
log_timing "Waiting for CRIU dump to complete..."
wait $DUMP_PID
DUMP_EXIT_CODE=$?
DUMP_END_TIME=$(date +%s%3N)
DUMP_ELAPSED=$((DUMP_END_TIME - START_TIME))
if [ $DUMP_EXIT_CODE -eq 0 ]; then
  log_timing "CRIU dump completed (took ${DUMP_ELAPSED}ms)"
else
  log_timing "ERROR: CRIU dump failed (exit code $DUMP_EXIT_CODE)"
  echo "ERROR: CRIU dump failed"
  exit 1
fi

wait $REPLICA_SAVED_PID 2>/dev/null || true
log_timing "Migration complete"

MIGRATION_TIME_MS=$ELAPSED
MIGRATION_TIME_SEC=$(echo "scale=2; $MIGRATION_TIME_MS / 1000" | bc)

SUMMARY_LOG="$IMAGES_DIR/migrate_summary.log"
{
echo ""
echo "=================================================================="
echo ""
echo "    MIGRATION TIME:  ${MIGRATION_TIME_SEC}s  (${MIGRATION_TIME_MS}ms)"
echo ""
echo "=================================================================="
echo ""
echo "=== DETAILED TIMING LOG ==="
cat "$TIMING_LOG"
} | sudo tee "$SUMMARY_LOG"
