#!/bin/bash
set -euo pipefail

# Full apple-to-apple comparison: 100GB, fuzzy traffic, three approaches
# Usage: sudo ./test/run-full-comparison.sh

VALKEY_BIN="${VALKEY_BIN:-/home/ubuntu/work/valkey/src/valkey-server}"
CRIU_BIN="/usr/local/sbin/criu"
BENCH="/home/ubuntu/work/criu/test/latency-bench"
RESULTS_DIR="/tmp/comparison-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

HOST=127.0.0.1
PORT=6379
BENCH_DURATION=300  # 5 minutes per test
MIGRATION_DELAY=60  # Trigger migration at 60s mark

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULTS_DIR/run.log"; }

start_valkey() {
    sudo pkill -9 valkey 2>/dev/null || true
    sudo fuser -k $PORT/tcp 2>/dev/null || true
    sleep 3
    sudo "$VALKEY_BIN" --daemonize yes --port $PORT --enable-debug-command yes \
        --save "" --appendonly no --logfile "$RESULTS_DIR/$1.valkey.log"
    sleep 2
    valkey-cli -p $PORT ping >/dev/null
    log "Valkey started ($1)"
}

fill_100gb() {
    log "Filling 100GB..."
    valkey-benchmark -h $HOST -p $PORT -t set -n 2530000 -d 65536 -r 2530000 -q 2>&1 | tail -1
    local mem=$(valkey-cli -p $PORT info memory | grep used_memory_human | tr -d '\r')
    local keys=$(valkey-cli -p $PORT dbsize | tr -d '\r')
    log "Filled: $mem, $keys"
}

start_fuzzy_traffic() {
    # Mixed SET/GET with varying sizes, 16 clients, pipelined
    valkey-benchmark -h $HOST -p $PORT -t set,get -n 1000000000 \
        -d 256 -r 1000000 -c 16 -P 4 -q 2>"$RESULTS_DIR/$1.traffic.log" &
    echo $!
}

# ============================================================
# TEST 1: BASELINE (no migration, just traffic + latency)
# ============================================================
log "========== TEST 1: BASELINE =========="
start_valkey "baseline"
fill_100gb

TRAFFIC_PID=$(start_fuzzy_traffic "baseline")
log "Fuzzy traffic started (PID=$TRAFFIC_PID)"

$BENCH $HOST $PORT $BENCH_DURATION 256 > "$RESULTS_DIR/baseline-latency.txt" 2>&1
log "Baseline benchmark complete"
kill $TRAFFIC_PID 2>/dev/null || true

# ============================================================
# TEST 2: CRIU WP_ASYNC migration
# ============================================================
log "========== TEST 2: CRIU WP_ASYNC =========="
start_valkey "criu"
fill_100gb

$BENCH $HOST $PORT $BENCH_DURATION 256 > "$RESULTS_DIR/criu-latency.txt" 2>&1 &
BENCH_PID=$!

TRAFFIC_PID=$(start_fuzzy_traffic "criu")
log "Fuzzy traffic started"

sleep $MIGRATION_DELAY
log "Triggering CRIU migration..."
CRIU_START=$(date +%s%N)

# Direct CRIU dump (no migrate.sh — just dump with COW)
VALKEY_PID=$(pgrep -x valkey-server)
sudo mkdir -p /fsx/lazy
sudo $CRIU_BIN dump -t $VALKEY_PID -D /fsx/lazy --lazy-pages --cow-dump \
    --tcp-close --shell-job -v1 -o /fsx/lazy/criu-comparison.log &
CRIU_PID=$!

wait $BENCH_PID 2>/dev/null
CRIU_END=$(date +%s%N)
sudo kill $CRIU_PID 2>/dev/null || true
kill $TRAFFIC_PID 2>/dev/null || true

CRIU_DURATION=$(( (CRIU_END - CRIU_START) / 1000000 ))
log "CRIU migration ran for ${CRIU_DURATION}ms"

# Extract timing from CRIU log
CRIU_FREEZE=$(sudo strings /fsx/lazy/criu-comparison.log 2>/dev/null | grep "dump_one_task TOTAL" | awk '{print $NF}')
CRIU_WP=$(sudo strings /fsx/lazy/criu-comparison.log 2>/dev/null | grep "cow_dump_apply_writeprotect" | awk '{print $NF}')
log "CRIU freeze: ${CRIU_FREEZE}s, WP: ${CRIU_WP}s"

# ============================================================
# TEST 3: VALKEY WP_ASYNC migration
# ============================================================
log "========== TEST 3: VALKEY WP_ASYNC =========="
start_valkey "valkey-wp"
fill_100gb

$BENCH $HOST $PORT $BENCH_DURATION 256 > "$RESULTS_DIR/valkey-wp-latency.txt" 2>&1 &
BENCH_PID=$!

TRAFFIC_PID=$(start_fuzzy_traffic "valkey-wp")
log "Fuzzy traffic started"

sleep $MIGRATION_DELAY
log "Triggering Valkey WP_ASYNC migration..."
WP_START=$(date +%s%N)
valkey-cli -p $PORT DEBUG wp-migrate-start 2>&1
log "WP_ASYNC migration started"

wait $BENCH_PID 2>/dev/null
WP_END=$(date +%s%N)
kill $TRAFFIC_PID 2>/dev/null || true

WP_DURATION=$(( (WP_END - WP_START) / 1000000 ))
log "Valkey WP_ASYNC ran for ${WP_DURATION}ms"

# Check completion
WP_COMPLETE=$(grep "serialization complete" "$RESULTS_DIR/valkey-wp.valkey.log" 2>/dev/null | head -1)
log "WP_ASYNC result: $WP_COMPLETE"

# ============================================================
# SUMMARY
# ============================================================
log ""
log "========== RESULTS =========="
log "Results dir: $RESULTS_DIR"
log ""
log "=== BASELINE (sec 55-65) ==="
awk '$1+0 >= 55 && $1+0 <= 65' "$RESULTS_DIR/baseline-latency.txt"
log ""
log "=== CRIU (sec 55-75, migration at 60) ==="
awk '$1+0 >= 55 && $1+0 <= 75' "$RESULTS_DIR/criu-latency.txt"
log ""
log "=== VALKEY WP_ASYNC (sec 55-75, migration at 60) ==="
awk '$1+0 >= 55 && $1+0 <= 75' "$RESULTS_DIR/valkey-wp-latency.txt"
log ""
log "Done. Full data in $RESULTS_DIR/"
