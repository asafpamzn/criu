#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# TLS flag: pass --tls to enable
USE_TLS=false
if [[ "${1:-}" == "--tls" ]]; then
  USE_TLS=true
  shift
fi

IMAGES_DIR="/dev/shm/criu-migrate"
MAX_WAIT=${MAX_WAIT:-600}
TIMING_LOG="$IMAGES_DIR/restore-timing.log"

# Build valkey-cli TLS options if TLS is enabled
# Uses TLS_CLIENT_CERT/KEY if set, otherwise falls back to TLS_CERT/KEY
CLI_TLS_OPTS=""
if [ "$USE_TLS" = true ] && [ -n "${TLS_CERT:-}" ]; then
  _CLI_CERT="${TLS_CLIENT_CERT:-$TLS_CERT}"
  _CLI_KEY="${TLS_CLIENT_KEY:-$TLS_KEY}"
  CLI_TLS_OPTS="--tls --cert $_CLI_CERT --key $_CLI_KEY --cacert $TLS_CACERT"
  echo "valkey-cli TLS enabled: cert=$_CLI_CERT"
fi
CLI="valkey-cli $CLI_TLS_OPTS"

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

echo "(wait_replicate) USE_TLS=$USE_TLS CLI='$CLI' PRIMARY_IP=$PRIMARY_IP VALKEY_PORT=$VALKEY_PORT" | sudo tee -a "$TIMING_LOG"

echo "Waiting for Valkey to respond to PING..."
for i in $(seq 1 "$MAX_WAIT"); do
  if $CLI ping &>/dev/null; then
    WAIT_END=$(date +%s%3N)
    WAIT_ELAPSED=$((WAIT_END - WAIT_START))
    echo "Valkey is responsive after ${WAIT_ELAPSED}ms (iter $i)"
    echo "[$(date '+%H:%M:%S.%3N') +${WAIT_ELAPSED}ms] (wait_replicate) Valkey responsive after iter $i" | sudo tee -a "$TIMING_LOG"
    break
  fi
  if [ "$i" -le 3 ] || [ $((i % 50)) -eq 0 ]; then
    echo "(wait_replicate) PING attempt $i failed: $($CLI ping 2>&1 || true)" | sudo tee -a "$TIMING_LOG"
  fi
  sleep 0.1
done

if ! $CLI ping &>/dev/null; then
  echo "(wait_replicate) FINAL PING failed: $($CLI ping 2>&1 || true)" | sudo tee -a "$TIMING_LOG"
  echo "ERROR: Valkey not responsive after ${MAX_WAIT}x0.1s"
  exit 1
fi

echo "Running CLEAN_STATE_FOR_DOLLY_SAVE..."
CLEAN_START=$(date +%s%3N)
CLEAN_RESULT=$($CLI CLEAN_STATE_FOR_DOLLY_SAVE 2>&1) || true
CLEAN_END=$(date +%s%3N)
CLEAN_ELAPSED=$((CLEAN_END - CLEAN_START))
echo "[$(date '+%H:%M:%S.%3N') +${CLEAN_ELAPSED}ms] (wait_replicate) CLEAN_STATE_FOR_DOLLY_SAVE: $CLEAN_RESULT" | sudo tee -a "$TIMING_LOG"

echo "Configuring as replica of ${PRIMARY_IP}:${VALKEY_PORT}..."
REPL_START=$(date +%s%3N)
REPL_RESULT=$($CLI replicaof "$PRIMARY_IP" "$VALKEY_PORT" 2>&1) || true
REPL_END=$(date +%s%3N)
REPL_ELAPSED=$((REPL_END - REPL_START))
echo "[$(date '+%H:%M:%S.%3N') +${REPL_ELAPSED}ms] (wait_replicate) replicaof: $REPL_RESULT" | sudo tee -a "$TIMING_LOG"

echo "Replica configured"
