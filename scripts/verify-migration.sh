#!/usr/bin/env bash
set -euo pipefail

# Migration correctness verification.  Run on SOURCE.
#
# Usage:
#   ./verify-migration.sh [SIZE_GB] [--skip-fill] [--no-live] [--benchmark]
#
# Tests (7):
#   1. Migration completed
#   2. Replica PONG
#   3. Key count match
#   4. Memory within 10%
#   5. 500-key spot-check
#   6. BGSAVE (heap consistency)
#   7. RANDOMKEY smoke test

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# ── Args ──
SIZE_GB="${1:-10}"
LIVE=1; SKIP_FILL=0; BENCHMARK=0
for arg in "$@"; do
  case "$arg" in
    --no-live)   LIVE=0 ;;
    --skip-fill) SKIP_FILL=1 ;;
    --benchmark) BENCHMARK=1 ;;
    [0-9]*)      SIZE_GB="$arg" ;;
  esac
done

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
RSSH="$SSH ubuntu@${REPLICA_IP:-$REPLICA_HOST}"
VCLI="valkey-cli -p $VALKEY_PORT"
RUN_DIR="$SCRIPT_DIR/../artifacts/verify-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RUN_DIR"

PASS=0; FAIL=0; TESTS=()
log()  { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RUN_DIR/verify.log"; }
pass() { PASS=$((PASS+1)); TESTS+=("PASS: $1"); log "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); TESTS+=("FAIL: $1"); log "  FAIL: $1"; }

log "================================================================"
log "Migration Correctness Verification"
log "  Size: ${SIZE_GB}GB  Live: $LIVE  Results: $RUN_DIR"
log "================================================================"

# ── 1. Prepare source ──
log "Step 1: Prepare source"

start_valkey() {
  sudo pkill -9 valkey-server 2>/dev/null || true
  for _ in $(seq 1 100); do
    pgrep -x valkey-server >/dev/null 2>&1 || break; sleep 0.1
  done
  sleep 1  # let port fully release
  sudo rm -f /var/lib/valkey/dump.rdb /var/lib/valkey/temp-*.rdb
  sudo /usr/local/sbin/valkey-server --port "$VALKEY_PORT" \
    --daemonize yes --save "" --protected-mode no \
    --io-threads 16 2>&1 || true
  for _ in $(seq 1 50); do
    $VCLI ping >/dev/null 2>&1 && return 0; sleep 0.2
  done
  # Last resort: try without io-threads
  sudo /usr/local/sbin/valkey-server --port "$VALKEY_PORT" \
    --daemonize yes --save "" --protected-mode no 2>&1 || true
  for _ in $(seq 1 50); do
    $VCLI ping >/dev/null 2>&1 && return 0; sleep 0.2
  done
  log "ERROR: cannot start valkey"; exit 1
}

if [ "$SKIP_FILL" = "0" ]; then
  start_valkey
  NUM_KEYS=$(( SIZE_GB * 16000 ))
  log "  Filling $NUM_KEYS keys (~${SIZE_GB}GB)..."
  # Use -d 64000 for fast fill. Real workloads with JSON data
  # compress ~50% with LZ4, doubling effective transfer throughput.
  valkey-benchmark -p "$VALKEY_PORT" -t set -n "$NUM_KEYS" -d 64000 \
    -r 1000000000 -c 64 --threads 4 > /dev/null 2>&1
fi

SRC_KEYS=$($VCLI DBSIZE | sed 's/[^0-9]//g')
SRC_MEM=$($VCLI info memory | grep "^used_memory:" | tr -d '\r' | cut -d: -f2)
SRC_MEM_H=$($VCLI info memory | grep "used_memory_human" | tr -d '\r' | cut -d: -f2)
log "  Source: $SRC_MEM_H, $SRC_KEYS keys"

# ── 2. Sample keys ──
log "Step 2: Sample 500 keys"
python3 "$SCRIPT_DIR/sample-keys.py" "$RUN_DIR/pre-sample.txt" 500 127.0.0.1 "$VALKEY_PORT"
SAMPLE_N=$(wc -l < "$RUN_DIR/pre-sample.txt")
log "  Recorded $SAMPLE_N keys"

# ── 3. Migrate ──
log "Step 3: Run migration"
MENV="SKIP_FILL=1 KEEP_SOURCE_RUNNING=1"
[ "$LIVE" = "1" ] && MENV="$MENV RUN_WORKLOAD_DURING_MIGRATION=1" \
                   || MENV="$MENV RUN_WORKLOAD_DURING_MIGRATION=0"

T0=$(date +%s)
eval "$MENV bash $SCRIPT_DIR/migrate.sh $SIZE_GB" > "$RUN_DIR/migrate.log" 2>&1 || true
DUR=$(( $(date +%s) - T0 ))

# Parse results from migrate.sh output
MIGR_MS=$(grep -oP 'Duration: \K\d+(?=ms)' "$RUN_DIR/migrate.log" 2>/dev/null || echo "?")
SEIZE_T=$(grep -oP 'Seize: \K[0-9.?]+' "$RUN_DIR/migrate.log" 2>/dev/null || echo "?")
CONVERGE_T=$(grep -oP 'Converge: \K[0-9.]+ms' "$RUN_DIR/migrate.log" 2>/dev/null || echo "?")
XFER_LINE=$(grep "^page-recv:.*pages.*MB/s" "$RUN_DIR/migrate.log" 2>/dev/null | tail -1 || true)
XFER_T=$(echo "$XFER_LINE" | grep -oP 'in \K[0-9.]+(?=s)' || echo "?")
XFER_R=$(echo "$XFER_LINE" | grep -oP '\(\K[0-9.]+(?= MB/s)' || echo "?")
XFER_MB=$(echo "$XFER_LINE" | grep -oP '\K[0-9.]+(?= MB\))' || echo "?")
log "  Migration: ${MIGR_MS}ms  transfer: ${XFER_T}s ${XFER_MB}MB @ ${XFER_R} MB/s"
log "  Frozen: seize=${SEIZE_T}s  converge=${CONVERGE_T}"

if grep -q "Migration completed successfully" "$RUN_DIR/migrate.log" 2>/dev/null; then
  pass "migration completed"
else
  fail "migration did not complete"
fi

[ "$BENCHMARK" = "1" ] && {
  log ""; log "BENCHMARK COMPLETE (verification skipped)"; exit "$FAIL"
}

# ── 4. Verify replica ──
log "Step 4: Verify replica"

# 4a. PONG
RPONG=$($RSSH "$VCLI PING" 2>/dev/null | tr -d '\r')
[ "$RPONG" = "PONG" ] && pass "replica PONG" || fail "replica PONG (got: $RPONG)"

# 4b. Key count
RKEYS=$($RSSH "$VCLI DBSIZE" 2>/dev/null | sed 's/[^0-9]//g')
log "  Replica: $RKEYS keys (source: $SRC_KEYS)"
if [ "$LIVE" = "1" ]; then
  [ -n "$RKEYS" ] && [ "$RKEYS" -ge "$SRC_KEYS" ] 2>/dev/null \
    && pass "key count >= source ($RKEYS >= $SRC_KEYS)" \
    || fail "key count (src=$SRC_KEYS rep=$RKEYS)"
else
  [ "$RKEYS" = "$SRC_KEYS" ] && pass "key count match ($SRC_KEYS)" \
    || fail "key count mismatch (src=$SRC_KEYS rep=$RKEYS)"
fi

# 4c. Memory within 10%
RMEM=$($RSSH "$VCLI info memory" 2>/dev/null | grep "^used_memory:" | tr -d '\r' | cut -d: -f2)
if [ -n "$RMEM" ] && [ -n "$SRC_MEM" ]; then
  PCT=$(python3 -c "print(f'{abs($RMEM-$SRC_MEM)/$SRC_MEM*100:.1f}')")
  python3 -c "exit(0 if abs($RMEM-$SRC_MEM)/$SRC_MEM<0.10 else 1)" \
    && pass "memory within 10% (diff: ${PCT}%)" \
    || fail "memory diverged by ${PCT}%"
fi

# 4d. Spot-check keys
log "  Spot-checking keys on replica..."
scp -i "$SSH_KEY" -o StrictHostKeyChecking=no \
  "$RUN_DIR/pre-sample.txt" "$SCRIPT_DIR/verify-keys.py" \
  "ubuntu@${REPLICA_IP}:/tmp/" 2>/dev/null
SPOT=$($RSSH "python3 /tmp/verify-keys.py /tmp/pre-sample.txt 127.0.0.1 $VALKEY_PORT" 2>/dev/null | tr -d '\r')
log "  Spot-check: $SPOT"
echo "$SPOT" | grep -q " 0 failed" \
  && pass "spot-check: all keys match" \
  || fail "spot-check: $SPOT"

# 4e. BGSAVE
log "  Running BGSAVE on replica..."
$RSSH "$VCLI BGSAVE" > /dev/null 2>&1 || true
for _ in $(seq 1 600); do
  BG=$($RSSH "$VCLI info persistence" 2>/dev/null \
    | grep "rdb_bgsave_in_progress" | tr -d '\r' | cut -d: -f2)
  [ "$BG" = "0" ] && break
  sleep 1
done
BGST=$($RSSH "$VCLI info persistence" 2>/dev/null \
  | grep "rdb_last_bgsave_status" | tr -d '\r' | cut -d: -f2)
[ "$BGST" = "ok" ] && pass "BGSAVE success (heap consistent)" \
  || fail "BGSAVE status: $BGST"

# 4f. RANDOMKEY smoke
log "  RANDOMKEY smoke test..."
RK=$($RSSH "$VCLI RANDOMKEY" 2>/dev/null | tr -d '\r')
if [ -n "$RK" ] && [ "$RK" != "(nil)" ]; then
  RT=$($RSSH "$VCLI TYPE $RK" 2>/dev/null | tr -d '\r')
  [ "$RT" = "string" ] && pass "RANDOMKEY type=string" \
    || fail "RANDOMKEY type=$RT (expected string)"
else
  fail "RANDOMKEY returned nil"
fi

# ── 5. Summary ──
log ""
log "================================================================"
log "  RESULTS: $([ "$FAIL" -eq 0 ] && echo "ALL $PASS TESTS PASSED" || echo "$FAIL FAILED, $PASS passed")"
log ""
for t in "${TESTS[@]}"; do log "  $t"; done
log ""
log "  migration: ${MIGR_MS}ms  transfer: ${XFER_T}s @ ${XFER_R} MB/s"
log "  frozen: seize=${SEIZE_T}s  converge=${CONVERGE_T}"
log "  total (incl. scripts): ${DUR}s"
log "  Logs: $RUN_DIR/"
log "================================================================"

exit "$FAIL"
