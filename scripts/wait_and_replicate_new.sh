#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

IMAGES_DIR="/dev/shm/criu-migrate"
MAX_WAIT=${MAX_WAIT:-600}
TIMING_LOG="$IMAGES_DIR/restore-timing.log"

# Build valkey-cli TLS options if TLS is configured
VALKEY_TLS_OPTS=""
if [ -n "${VALKEY_TLS_CERT:-}" ]; then
  VALKEY_TLS_OPTS="--tls --cert $VALKEY_TLS_CERT --key $VALKEY_TLS_KEY --cacert $VALKEY_TLS_CACERT"
fi
CLI="valkey-cli $VALKEY_TLS_OPTS"

# Timing helper - append to same log as restore_new.sh
log_timing() {
  if [ -f "$TIMING_LOG" ]; then
    # Read START_MS from first line if available, otherwise use current time
    local first_line=$(head -1 "$TIMING_LOG" 2>/dev/null || echo "")
    if [[ "$first_line" =~ ^\[([0-9]+)ms\] ]]; then
      # Extract original start time by subtracting elapsed from current
      local orig_elapsed="${BASH_REMATCH[1]}"
      local now=$(date +%s%3N)
      # We need the original START_MS - read it from env or estimate
      local start_ms=${RESTORE_START_MS:-$(date +%s%3N)}
      local elapsed=$((now - start_ms))
      echo "[${elapsed}ms] (wait_replicate) $1" | sudo tee -a "$TIMING_LOG"
    else
      echo "[???ms] (wait_replicate) $1" | sudo tee -a "$TIMING_LOG"
    fi
  else
    echo "(wait_replicate) $1"
  fi
}

WAIT_START=$(date +%s%3N)

echo "Waiting for Valkey to respond to PING..."
for i in $(seq 1 "$MAX_WAIT"); do
  if $CLI ping &>/dev/null; then
    WAIT_END=$(date +%s%3N)
    WAIT_ELAPSED=$((WAIT_END - WAIT_START))
    echo "Valkey is responsive after ${WAIT_ELAPSED}ms (iter $i)"
    echo "[$(date '+%H:%M:%S.%3N') +${WAIT_ELAPSED}ms] (wait_replicate) Valkey responsive after iter $i" | sudo tee -a "$TIMING_LOG"
    break
  fi
  sleep 0.1
done

if ! $CLI ping &>/dev/null; then
  echo "ERROR: Valkey not responsive after ${MAX_WAIT}x0.1s"
  exit 1
fi

echo "Running CLEAN_STATE_FOR_DOLLY_SAVE..."
CLEAN_START=$(date +%s%3N)
$CLI CLEAN_STATE_FOR_DOLLY_SAVE
CLEAN_END=$(date +%s%3N)
CLEAN_ELAPSED=$((CLEAN_END - CLEAN_START))
echo "[$(date '+%H:%M:%S.%3N') +${CLEAN_ELAPSED}ms] (wait_replicate) CLEAN_STATE_FOR_DOLLY_SAVE done" | sudo tee -a "$TIMING_LOG"

echo "Configuring as replica of ${PRIMARY_IP}:${VALKEY_PORT}..."
REPL_START=$(date +%s%3N)
$CLI replicaof "$PRIMARY_IP" "$VALKEY_PORT"
REPL_END=$(date +%s%3N)
REPL_ELAPSED=$((REPL_END - REPL_START))
echo "[$(date '+%H:%M:%S.%3N') +${REPL_ELAPSED}ms] (wait_replicate) replicaof command done" | sudo tee -a "$TIMING_LOG"

echo "Replica configured"
