#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# TLS flag: pass --no-tls to disable
USE_TLS=true
if [[ "${1:-}" == "--no-tls" ]]; then
  USE_TLS=false
  shift
fi

IMAGES_DIR="/dev/shm/criu-migrate"
CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
TLS_CERT="${TLS_CERT:-}"
TLS_KEY="${TLS_KEY:-}"
TLS_CACERT="${TLS_CACERT:-}"
SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA_SSH_HOST="${REPLICA_IP:-$REPLICA_HOST}"
TIMING_LOG="$IMAGES_DIR/migrate-timing.log"

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

SCRIPT_START_MS=$(date +%s%3N)
log_timing() {
  local now=$(date +%s%3N)
  local elapsed=$((now - SCRIPT_START_MS))
  local timestamp=$(date '+%H:%M:%S.%3N')
  echo "[$timestamp +${elapsed}ms] $1" | sudo tee -a "$TIMING_LOG" >&2
}

cleanup() {
  log_timing "Cleaning up..."
  sudo pkill -9 -f "criu dump" 2>/dev/null || true
  sudo pkill -9 -f "ssh.*restore_new.sh" 2>/dev/null || true
  exit 1
}
trap cleanup INT TERM

PID=$(pgrep -x valkey-server | head -n1)
if [ -z "$PID" ]; then
  echo "ERROR: valkey-server not running"
  exit 1
fi

# Quick binary check: compare md5 of valkey binaries on both machines
LOCAL_MD5=$(md5sum /usr/bin/valkey-* /usr/local/bin/valkey-* 2>/dev/null | sort | md5sum | awk '{print $1}')
REMOTE_MD5=$($SSH ubuntu@$REPLICA_SSH_HOST "md5sum /usr/bin/valkey-* /usr/local/bin/valkey-* 2>/dev/null | sort | md5sum | awk '{print \$1}'"
)
if [ "$LOCAL_MD5" != "$REMOTE_MD5" ]; then
  echo "ERROR: valkey binaries differ between primary and replica"
  echo "  Primary: $LOCAL_MD5"
  echo "  Replica: $REMOTE_MD5"
  exit 1
fi

# Kill leftover ssh/restore processes
log_timing "Killing leftover processes..."
sudo pkill -9 -f "ssh.*restore_new.sh" 2>/dev/null || true
sleep 0.1

# Clean images dir
log_timing "Cleaning $IMAGES_DIR..."
sudo mkdir -p "$IMAGES_DIR"
sudo rm -rf "$IMAGES_DIR"/*
log_timing "CLONE config: cores=$NUM_CORES p3=$CLONE_P3_THREADS p3_bulk=$CLONE_P3_THREADS_BULK scanners=$CLONE_SCANNERS pre_scanners=$CLONE_PRE_SCANNERS drain=$CLONE_DRAIN_THREADS pre_scan=${CLONE_PRE_SCAN_FLAG:+yes}"

# Start replica over SSH (bidirectional protocol via coproc)
log_timing "Starting restore on replica..."
RESTORE_TLS_FLAG=""
if [ "$USE_TLS" = true ]; then
  RESTORE_TLS_FLAG="--tls"
fi
coproc REPLICA { $SSH ubuntu@$REPLICA_SSH_HOST "sudo $SCRIPT_DIR/restore_new.sh $RESTORE_TLS_FLAG"; }
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

TLS_OPTS=""
if [ "$USE_TLS" = true ] && [ -n "$TLS_CERT" ]; then
  TLS_OPTS="--tls --tls-cert $TLS_CERT --tls-key $TLS_KEY --tls-cacert $TLS_CACERT --tls-no-cn-verify"
  log_timing "TLS enabled: cert=$TLS_CERT"
else
  log_timing "TLS disabled"
fi

sudo "$CRIU_BIN" dump \
  --tree "$PID" \
  --images-dir "$IMAGES_DIR" \
  --clone-dump \
  --clone-p3-threads "$CLONE_P3_THREADS" \
  --clone-p3-threads-bulk "$CLONE_P3_THREADS_BULK" \
  --clone-scanners "$CLONE_SCANNERS" \
  --clone-pre-scanners "$CLONE_PRE_SCANNERS" \
  --clone-drain-threads "$CLONE_DRAIN_THREADS" \
  $CLONE_PRE_SCAN_FLAG \
  --address "$PRIMARY_IP" \
  --port "$CRIU_PORT" \
  --tcp-close \
  --skip-in-flight \
  --ext-unix-sk \
  --leave-running \
  --display-stats \
  $TLS_OPTS \
  -v2 -o "$IMAGES_DIR/clone-primary.log" &
DUMP_PID=$!
log_timing "CRIU dump started (PID: $DUMP_PID)"

# Tell replica to start (include PID so it knows the task)
echo "START $PID" >&"${REPLICA[1]}"
log_timing "Sent START $PID to replica"

# Poll for replica master_link_status:up
log_timing "Polling for master_link_status:up..."
REPLICA_PORT="${REPLICA_PORT:-6379}"
REMOTE_CLI_TLS=""
if [ "$USE_TLS" = true ] && [ -n "${TLS_CERT:-}" ]; then
  _CLI_CERT="${TLS_CLIENT_CERT:-$TLS_CERT}"
  _CLI_KEY="${TLS_CLIENT_KEY:-$TLS_KEY}"
  REMOTE_CLI_TLS="--tls --cert $_CLI_CERT --key $_CLI_KEY --cacert $TLS_CACERT"
fi
STATUS=""
for i in $(seq 1 120); do
  STATUS=$($SSH ubuntu@$REPLICA_SSH_HOST "valkey-cli -p $REPLICA_PORT $REMOTE_CLI_TLS info replication 2>/dev/null | grep master_link_status" || true)
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
