#!/usr/bin/env bash
set -euo pipefail

# Full migration test with latency/TPS measurement.
# Run on SOURCE. Usage: full-test.sh SIZE_GB [--traffic] [--skip-fill]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

SIZE_GB="${1:-100}"
TRAFFIC=0
SKIP_FILL=0
for arg in "$@"; do
  case "$arg" in
    --traffic) TRAFFIC=1 ;;
    --skip-fill) SKIP_FILL=1 ;;
    [0-9]*) ;; # size already captured
  esac
done

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA="${REPLICA_IP:-$REPLICA_HOST}"
VCLI="valkey-cli -p $VALKEY_PORT"
TAG="${SIZE_GB}gb-$([ "$TRAFFIC" = 1 ] && echo "traffic" || echo "quiesced")"
RUN_DIR="$SCRIPT_DIR/../artifacts/fulltest-$(date +%Y%m%d_%H%M%S)-${TAG}"
mkdir -p "$RUN_DIR"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RUN_DIR/test.log"; }

log "========================================================"
log "Full Test: ${SIZE_GB}GB $([ "$TRAFFIC" = 1 ] && echo "WITH TRAFFIC" || echo "QUIESCED")"
log "========================================================"

cleanup() {
  kill "$LATENCY_PID" 2>/dev/null || true
  kill "$TPS_PID" 2>/dev/null || true
  [ -n "${BENCH_PID:-}" ] && kill "$BENCH_PID" 2>/dev/null || true
  sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
  kill "$LATENCY_PID" 2>/dev/null || true
}

# ── 1. Prepare data ──
if [ "$SKIP_FILL" = "0" ]; then
  log "Restarting valkey-server..."
  sudo systemctl restart valkey-server
  sleep 2
  NUM_KEYS=$(( SIZE_GB * 16000 ))
  log "Filling ~${SIZE_GB}GB ($NUM_KEYS keys × 64KB)..."
  valkey-benchmark -p "$VALKEY_PORT" -t set -n "$NUM_KEYS" -d 64000 \
    -r 1000000000 -c 64 --threads 10 -q 2>&1 | tee -a "$RUN_DIR/test.log"
fi

MEM=$($VCLI info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
KEYS=$($VCLI DBSIZE | sed 's/[^0-9]//g')
log "Source: $MEM, $KEYS keys"

# Sample keys for verification
python3 "$SCRIPT_DIR/sample-keys.py" "$RUN_DIR/pre-sample.txt" 500 127.0.0.1 "$VALKEY_PORT"
log "Sampled 500 keys"

# Disable background persistence
$VCLI CONFIG SET lazyfree-lazy-expire no CONFIG SET save "" >/dev/null 2>&1 || true

# ── 2. Start probes ──
log "Starting latency probe (PING every 100ms)..."
python3 "$SCRIPT_DIR/latency-probe.py" 127.0.0.1 "$VALKEY_PORT" 0.1 \
  > "$RUN_DIR/latency.txt" 2>/dev/null &
LATENCY_PID=$!

log "Starting TPS probe (INFO stats every 1s)..."
(while true; do
  OPS=$($VCLI info stats 2>/dev/null | grep instantaneous_ops_per_sec | cut -d: -f2 | tr -d '\r' || echo 0)
  echo "$(date +%s.%3N) $OPS"
  sleep 1
done) > "$RUN_DIR/tps.txt" 2>/dev/null &
TPS_PID=$!

trap cleanup EXIT

# ── 3. Traffic for baseline (if --traffic) ──
BENCH_PID=""
if [ "$TRAFFIC" = "1" ]; then
  log "Starting benchmark for baseline TPS (80/20 get/set, 512B)..."
  valkey-benchmark -p "$VALKEY_PORT" -t set,get -r 1000000 -c 16 -P 8 \
    -d 512 --ratio 20:80 -n 1000000000 --threads 4 -q > "$RUN_DIR/bench-before.log" 2>&1 &
  BENCH_PID=$!
  sleep 2  # let benchmark stabilize
fi

# ── 4. 30s baseline ──
BASELINE_START=$(date +%s.%3N)
log "Collecting 30s baseline... ($(date +%H:%M:%S))"
sleep 30
BASELINE_END=$(date +%s.%3N)
log "Baseline complete. ($(date +%H:%M:%S))"

# Kill baseline benchmark (migrate.sh will also kill, but be explicit)
if [ -n "$BENCH_PID" ]; then
  kill "$BENCH_PID" 2>/dev/null || true
  sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
  BENCH_PID=""
  sleep 0.5
fi

# ── 5. Migration ──
MIGRATION_START=$(date +%s.%3N)
log "Starting migration... ($(date +%H:%M:%S))"

MARGS="SKIP_FILL=1 KEEP_SOURCE_RUNNING=1"
if [ "$TRAFFIC" = "1" ]; then
  MARGS="$MARGS RUN_WORKLOAD_DURING_MIGRATION=1"
else
  MARGS="$MARGS RUN_WORKLOAD_DURING_MIGRATION=0"
fi

eval "$MARGS bash $SCRIPT_DIR/migrate.sh $SIZE_GB" > "$RUN_DIR/migrate.log" 2>&1 || true
MIGRATION_END=$(date +%s.%3N)
MIGR_DUR=$(python3 -c "print(f'{$MIGRATION_END - $MIGRATION_START:.1f}')")
log "Migration completed in ${MIGR_DUR}s"

# ── 6. Post-migration traffic ──
if [ "$TRAFFIC" = "1" ]; then
  log "Starting benchmark for post-migration TPS (80/20 get/set, 512B)..."
  valkey-benchmark -p "$VALKEY_PORT" -t set,get -r 1000000 -c 16 -P 8 \
    -d 512 --ratio 20:80 -n 1000000000 --threads 4 -q > "$RUN_DIR/bench-after.log" 2>&1 &
  BENCH_PID=$!
  sleep 2
fi

# ── 7. 30s post-migration ──
AFTER_START=$(date +%s.%3N)
log "Collecting 30s post-migration... ($(date +%H:%M:%S))"
sleep 30
AFTER_END=$(date +%s.%3N)
log "Post-migration complete. ($(date +%H:%M:%S))"

# Kill post benchmark
if [ -n "$BENCH_PID" ]; then
  kill "$BENCH_PID" 2>/dev/null || true
  sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
  BENCH_PID=""
fi

# Stop probes
kill "$LATENCY_PID" 2>/dev/null || true
kill "$TPS_PID" 2>/dev/null || true
sleep 0.5

# Save time windows
cat > "$RUN_DIR/windows.txt" <<EOF
BASELINE_START=$BASELINE_START
BASELINE_END=$BASELINE_END
MIGRATION_START=$MIGRATION_START
MIGRATION_END=$MIGRATION_END
AFTER_START=$AFTER_START
AFTER_END=$AFTER_END
EOF

# ── 8. Analyze latency ──
log ""
log "======== LATENCY (microseconds) ========"
python3 - "$RUN_DIR/latency.txt" "$RUN_DIR/windows.txt" <<'PYEOF' 2>&1 | tee -a "$RUN_DIR/test.log"
import sys, statistics

windows = {}
with open(sys.argv[2]) as f:
    for line in f:
        k, v = line.strip().split("=")
        windows[k] = float(v)

samples = []
with open(sys.argv[1]) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 2:
            ts, lat = float(parts[0]), int(parts[1])
            err = len(parts) > 2
            samples.append((ts, lat, err))

def analyze(name, start, end):
    lats = [lat for ts, lat, err in samples if start <= ts <= end]
    errs = sum(1 for ts, lat, err in samples if start <= ts <= end and err)
    if not lats:
        print(f"  {name}: no samples")
        return
    lats.sort()
    n = len(lats)
    p50 = lats[n // 2]
    p99 = lats[int(n * 0.99)]
    mx = max(lats)
    avg = statistics.mean(lats)
    spikes = sum(1 for l in lats if l > 10000)  # >10ms
    line = f"  {name}: n={n:>4}  p50={p50:>6}us  p99={p99:>6}us  max={mx:>8}us  avg={avg:>7.0f}us"
    if spikes > 0:
        line += f"  SPIKES(>10ms)={spikes}"
    if errs > 0:
        line += f"  ERRORS={errs}"
    print(line)

analyze("BEFORE ", windows["BASELINE_START"], windows["BASELINE_END"])
analyze("DURING ", windows["MIGRATION_START"], windows["MIGRATION_END"])
analyze("AFTER  ", windows["AFTER_START"], windows["AFTER_END"])

# Find the top-5 highest latency samples during migration
print("\n  Top-5 latency spikes during migration:")
during = [(ts, lat, err) for ts, lat, err in samples
          if windows["MIGRATION_START"] <= ts <= windows["MIGRATION_END"]]
during.sort(key=lambda x: x[1], reverse=True)
for ts, lat, err in during[:5]:
    e = " ERR" if err else ""
    print(f"    {lat:>8}us at {ts:.3f}{e}")
PYEOF

# ── 9. Analyze TPS ──
log ""
log "======== TPS (ops/sec) ========"
python3 - "$RUN_DIR/tps.txt" "$RUN_DIR/windows.txt" <<'PYEOF' 2>&1 | tee -a "$RUN_DIR/test.log"
import sys, statistics

windows = {}
with open(sys.argv[2]) as f:
    for line in f:
        k, v = line.strip().split("=")
        windows[k] = float(v)

samples = []
with open(sys.argv[1]) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 2:
            try:
                ts, ops = float(parts[0]), int(parts[1])
                samples.append((ts, ops))
            except ValueError:
                pass

def analyze(name, start, end):
    ops_list = [ops for ts, ops in samples if start <= ts <= end]
    if not ops_list:
        print(f"  {name}: no samples")
        return
    avg = statistics.mean(ops_list)
    mn = min(ops_list)
    mx = max(ops_list)
    print(f"  {name}: n={len(ops_list):>3}  avg={avg:>10.0f}  min={mn:>10}  max={mx:>10} ops/s")

analyze("BEFORE ", windows["BASELINE_START"], windows["BASELINE_END"])
analyze("DURING ", windows["MIGRATION_START"], windows["MIGRATION_END"])
analyze("AFTER  ", windows["AFTER_START"], windows["AFTER_END"])
PYEOF

# ── 10. Migration timing from logs ──
log ""
log "======== MIGRATION TIMING ========"
grep -aE "(COW|dump_one_task|cutover|frozen|T3|FREEZE|convergence|bulk|Duration)" \
  "$RUN_DIR/migrate.log" 2>/dev/null | head -30 | tee -a "$RUN_DIR/test.log" || true

# ── 11. Verify replica ──
set +e  # verification must not abort on individual failures
log ""
log "======== VERIFICATION ========"
PASS=0; FAIL=0

# Migration completed?
if grep -q "Migration completed successfully" "$RUN_DIR/migrate.log" 2>/dev/null; then
  log "  PASS: migration completed"; PASS=$((PASS+1))
else
  log "  FAIL: migration did not complete"; FAIL=$((FAIL+1))
  tail -20 "$RUN_DIR/migrate.log" | tee -a "$RUN_DIR/test.log"
fi

# Replica PONG
RPONG=$($SSH ubuntu@"$REPLICA" "$VCLI PING" 2>/dev/null | tr -d '\r')
[ "$RPONG" = "PONG" ] \
  && { log "  PASS: replica PONG"; PASS=$((PASS+1)); } \
  || { log "  FAIL: replica PONG (got: $RPONG)"; FAIL=$((FAIL+1)); }

# Key count
RKEYS=$($SSH ubuntu@"$REPLICA" "$VCLI DBSIZE" 2>/dev/null | sed 's/[^0-9]//g')
log "  Replica: $RKEYS keys (source: $KEYS)"
if [ "$TRAFFIC" = "0" ]; then
  [ "$RKEYS" = "$KEYS" ] \
    && { log "  PASS: key count match ($KEYS)"; PASS=$((PASS+1)); } \
    || { log "  FAIL: key count mismatch (src=$KEYS rep=$RKEYS)"; FAIL=$((FAIL+1)); }
else
  [ -n "$RKEYS" ] && [ "$RKEYS" -ge "$KEYS" ] 2>/dev/null \
    && { log "  PASS: key count >= source ($RKEYS >= $KEYS)"; PASS=$((PASS+1)); } \
    || { log "  FAIL: key count (src=$KEYS rep=$RKEYS)"; FAIL=$((FAIL+1)); }
fi

# Memory within 10%
SRC_MEM=$($VCLI info memory | grep "^used_memory:" | tr -d '\r' | cut -d: -f2)
RMEM=$($SSH ubuntu@"$REPLICA" "$VCLI info memory" 2>/dev/null | grep "^used_memory:" | tr -d '\r' | cut -d: -f2)
if [ -n "$RMEM" ] && [ -n "$SRC_MEM" ]; then
  PCT=$(python3 -c "print(f'{abs($RMEM-$SRC_MEM)/$SRC_MEM*100:.1f}')")
  python3 -c "exit(0 if abs($RMEM-$SRC_MEM)/$SRC_MEM<0.10 else 1)" \
    && { log "  PASS: memory within 10% (diff: ${PCT}%)"; PASS=$((PASS+1)); } \
    || { log "  FAIL: memory diverged by ${PCT}%"; FAIL=$((FAIL+1)); }
fi

# Spot-check keys
log "  Spot-checking keys on replica..."
scp -i "$SSH_KEY" -o StrictHostKeyChecking=no \
  "$RUN_DIR/pre-sample.txt" "$SCRIPT_DIR/verify-keys.py" \
  "ubuntu@${REPLICA}:/tmp/" 2>/dev/null
SPOT=$($SSH ubuntu@"$REPLICA" "python3 /tmp/verify-keys.py /tmp/pre-sample.txt 127.0.0.1 $VALKEY_PORT" 2>/dev/null | tr -d '\r')
log "  Spot-check: $SPOT"
echo "$SPOT" | grep -q " 0 failed" \
  && { log "  PASS: spot-check all keys match"; PASS=$((PASS+1)); } \
  || { log "  FAIL: spot-check: $SPOT"; FAIL=$((FAIL+1)); }

# BGSAVE
log "  Running BGSAVE on replica..."
$SSH ubuntu@"$REPLICA" "$VCLI BGSAVE" > /dev/null 2>&1 || true
for _ in $(seq 1 300); do
  BG=$($SSH ubuntu@"$REPLICA" "$VCLI info persistence" 2>/dev/null | grep "rdb_bgsave_in_progress" | tr -d '\r' | cut -d: -f2)
  [ "$BG" = "0" ] && break; sleep 1
done
BGST=$($SSH ubuntu@"$REPLICA" "$VCLI info persistence" 2>/dev/null | grep "rdb_last_bgsave_status" | tr -d '\r' | cut -d: -f2)
[ "$BGST" = "ok" ] \
  && { log "  PASS: BGSAVE (heap consistent)"; PASS=$((PASS+1)); } \
  || { log "  FAIL: BGSAVE status: $BGST"; FAIL=$((FAIL+1)); }

# RANDOMKEY
RK=$($SSH ubuntu@"$REPLICA" "$VCLI RANDOMKEY" 2>/dev/null | tr -d '\r')
if [ -n "$RK" ] && [ "$RK" != "(nil)" ]; then
  RT=$($SSH ubuntu@"$REPLICA" "$VCLI TYPE $RK" 2>/dev/null | tr -d '\r')
  [ "$RT" = "string" ] \
    && { log "  PASS: RANDOMKEY type=string"; PASS=$((PASS+1)); } \
    || { log "  FAIL: RANDOMKEY type=$RT"; FAIL=$((FAIL+1)); }
else
  log "  FAIL: RANDOMKEY nil"; FAIL=$((FAIL+1))
fi

# ── Summary ──
TOTAL=$((PASS + FAIL))
log ""
log "========================================================"
log "  RESULT: $PASS/$TOTAL tests passed $([ "$FAIL" -eq 0 ] && echo "- ALL PASS" || echo "- $FAIL FAILED")"
log "  Config: ${SIZE_GB}GB $([ "$TRAFFIC" = 1 ] && echo "WITH TRAFFIC" || echo "QUIESCED")"
log "  Artifacts: $RUN_DIR"
log "========================================================"

exit "$FAIL"
