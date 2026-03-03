#!/usr/bin/env bash
set -euo pipefail

# Migrate a running Valkey process to a replica machine.
# Run on the SOURCE machine.
#
# Usage: migrate.sh [SIZE_GB]
#   SIZE_GB: used only for fill (default from .env)
#   Set SKIP_FILL=1 to skip filling, KEEP_SOURCE_RUNNING=1 to keep source up.
#   Set RUN_WORKLOAD_DURING_MIGRATION=1 for live traffic during transfer.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# --- Config ---
DATA_SIZE_GB=${1:-$DEFAULT_DATA_SIZE_GB}
CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
[ -x "$CRIU_BIN" ] || CRIU_BIN="criu"
SKIP_FILL=${SKIP_FILL:-0}
KEEP_SOURCE_RUNNING=${KEEP_SOURCE_RUNNING:-0}
RUN_WORKLOAD_DURING_MIGRATION=${RUN_WORKLOAD_DURING_MIGRATION:-0}
CUTOVER_PORT=${CUTOVER_PORT:-9003}
STAGED_PORT=${STAGED_PORT:-9004}
IMAGE_XFER_PORT=${IMAGE_XFER_PORT:-9005}
DUMP_EXIT_TIMEOUT_S=${DUMP_EXIT_TIMEOUT_S:-600}
WORKLOAD_KEYSPACE=${WORKLOAD_KEYSPACE:-1000000}
WORKLOAD_DATA_SIZE=${WORKLOAD_DATA_SIZE:-64000}

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA=${REPLICA_IP:-$REPLICA_HOST}

# Pre-establish SSH ControlMaster for fast cutover signaling
SSH_CTL="/tmp/ssh-migrate-$$"
$SSH -fN -o ControlMaster=yes -o ControlPath="$SSH_CTL" ubuntu@$REPLICA 2>/dev/null
SSH="$SSH -o ControlPath=$SSH_CTL"
trap 'ssh -o ControlPath="$SSH_CTL" -O exit ubuntu@'"$REPLICA"' 2>/dev/null || true' EXIT

# Artifacts dir for this run
RUN_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/../artifacts}/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RUN_DIR"
MARKER="$RUN_DIR/source_markers.log"
mark() { printf "%s %s PRIMARY\n" "$1" "$(date +%s%3N)" >>"$MARKER" 2>/dev/null || true; }
log() { echo "[$(date +%H:%M:%S)] $*"; }

# --- 1. Clean both machines ---
log "Step 1: Clean up..."
[ "$KEEP_SOURCE_RUNNING" = "1" ] || sudo pkill -9 valkey-server 2>/dev/null || true
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sudo pkill -9 criu 2>/dev/null || true
$SSH ubuntu@$REPLICA "sudo pkill -9 valkey-server; sudo pkill -9 criu; sudo pkill -9 -f restore.sh; sudo rm -f /var/lib/valkey/temp-*.rdb" 2>/dev/null || true
sleep 1

# --- 2. Find Valkey ---
PID=$(pgrep -x valkey-server | head -1 || true)
[ -n "$PID" ] || { log "ERROR: valkey-server not running"; exit 1; }
log "Step 2: Valkey PID $PID"

# --- 3. Fill (optional) ---
if [ "$SKIP_FILL" = "1" ]; then
  log "Step 3: Skip fill"
else
  NUM_KEYS=$((DATA_SIZE_GB * 25300))
  log "Step 3: Filling ~${DATA_SIZE_GB}GB ($NUM_KEYS keys)..."
  valkey-benchmark -p "$VALKEY_PORT" -t set -d 64000 -r "$NUM_KEYS" -n $((NUM_KEYS + 50000)) --threads 10 -q
fi
MEM=$(valkey-cli -p "$VALKEY_PORT" info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r' || echo "?")
log "  Memory: $MEM"

# --- 4. Prepare images dir ---
sudo rm -rf "$IMAGES_DIR" && sudo mkdir -p "$IMAGES_DIR" && sudo chmod 777 "$IMAGES_DIR"

# --- 5. Start replica ---
log "Step 5: Starting restore on replica..."
$SSH ubuntu@$REPLICA "sudo $SCRIPT_DIR/restore.sh" &
REPLICA_PID=$!
sleep 1

# --- 6. Quiesce + dump ---
log "Step 6: Quiesce + dump..."
# Kill benchmarks, wait for clients to disconnect
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sleep 0.1
for _ in $(seq 1 50); do
  N=$(valkey-cli -p "$VALKEY_PORT" CLIENT LIST 2>/dev/null | grep -c "^" || echo 99)
  [ "$N" -le 1 ] && break
  sleep 0.1
done

# Disable background operations
valkey-cli -p "$VALKEY_PORT" CONFIG SET lazyfree-lazy-expire no CONFIG SET save "" >/dev/null 2>&1 || true

# Wait for threads to be idle
PID=$(pgrep -x valkey-server)
for _ in $(seq 1 200); do
  SC=$(sudo cat /proc/$PID/syscall 2>/dev/null | awk '{print $1}')
  [ "$SC" = "22" ] || [ "$SC" = "73" ] && break
  sleep 0.01
done

# Find cgroup for freeze
CGROUP_PATH=""
CGROUP_REL=$(grep '^0::' "/proc/$PID/cgroup" 2>/dev/null | cut -d: -f3 || true)
case "$CGROUP_REL" in *.service) CGROUP_PATH="/sys/fs/cgroup${CGROUP_REL}" ;; esac

# CRIU dump
CRIU_ARGS=(
  sudo env COW_PRE_FREEZE_CMD="valkey-cli -p $VALKEY_PORT CLIENT PAUSE 5000 ALL >/dev/null 2>&1" "$CRIU_BIN" dump
  --tree "$PID"
  --images-dir "$IMAGES_DIR"
  --cow-dump --lazy-pages
  --address "$PRIMARY_IP" --port "$CRIU_PORT"
  --tcp-close --skip-in-flight --ext-unix-sk
  --leave-running --display-stats
  -v2 -o "$IMAGES_DIR/lazy-primary.log"
)
[ -n "$CGROUP_PATH" ] && CRIU_ARGS+=(--freeze-cgroup "$CGROUP_PATH")

mark "DUMP_LAUNCH_MS"
MIGRATION_START_MS=${EPOCHREALTIME/./}; MIGRATION_START_MS=${MIGRATION_START_MS:0:13}
"${CRIU_ARGS[@]}" &
DUMP_PID=$!

# Wait for page server ready
for _ in $(seq 1 1200); do
  sudo grep -aq "PAGE SERVER READY TO SERVE" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null && break
  kill -0 "$DUMP_PID" 2>/dev/null || { log "ERROR: dump exited early"; exit 1; }
  sleep 0.025
done
mark "PAGE_SERVER_READY_MS"

# Serve image files to replica
fuser -k "$IMAGE_XFER_PORT"/tcp 2>/dev/null || true
python3 "$SCRIPT_DIR/image-server.py" "$IMAGES_DIR" "$IMAGE_XFER_PORT" &

# --- 7. Live workload (optional) ---
WORKLOAD_PID=""
if [ "$RUN_WORKLOAD_DURING_MIGRATION" = "1" ]; then
  # Wait for bulk transfer to finish before starting writes
  for _ in $(seq 1 600); do [ -f "$IMAGES_DIR/bulk_send_done" ] && break; sleep 0.5; done
  log "Step 7: Starting live workload..."
  valkey-benchmark -p "$VALKEY_PORT" -t set -r "$WORKLOAD_KEYSPACE" -c 13 -P 16 -d "$WORKLOAD_DATA_SIZE" -n 1000000000 -q >/dev/null 2>&1 &
  WORKLOAD_PID=$!
fi

# --- 8. Wait for staged signal from replica ---
log "Step 8: Waiting for replica staged..."
fuser -k "$STAGED_PORT"/tcp 2>/dev/null || true
STAGED_MSG=$(timeout "${DUMP_EXIT_TIMEOUT_S}s" python3 -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', $STAGED_PORT))
s.listen(1)
conn, _ = s.accept()
data = conn.recv(64)
conn.close(); s.close()
sys.stdout.write(data.decode().strip())
" 2>/dev/null || true)
[ -n "$STAGED_MSG" ] || { log "ERROR: staged signal not received"; exit 1; }
log "  Staged signal received"

# --- 9. Cutover (critical path — bash builtins only) ---
_t0=${EPOCHREALTIME/./}; _t0=${_t0:0:13}
kill -STOP "$PID" 2>/dev/null || sudo kill -STOP "$PID"
(echo "GO" > /dev/tcp/"$REPLICA"/"$CUTOVER_PORT") 2>/dev/null || echo "GO" | nc -q 0 -w 1 "$REPLICA" "$CUTOVER_PORT" 2>/dev/null
_t1=${EPOCHREALTIME/./}; _t1=${_t1:0:13}
log "Step 9: Cutover — source frozen $((_t1 - _t0))ms"

MIGRATION_END_MS=${EPOCHREALTIME/./}; MIGRATION_END_MS=${MIGRATION_END_MS:0:13}
MIGRATION_TIME_MS=$((MIGRATION_END_MS - MIGRATION_START_MS))

[ "$KEEP_SOURCE_RUNNING" = "1" ] && sudo pkill -CONT -x valkey-server 2>/dev/null || true

# --- 10. Cleanup ---
[ -n "$WORKLOAD_PID" ] && { kill "$WORKLOAD_PID" 2>/dev/null; sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null; } || true
wait "$REPLICA_PID" 2>/dev/null || true

# Wait for dump to finish
for _ in $(seq 1 $((DUMP_EXIT_TIMEOUT_S * 10))); do
  kill -0 "$DUMP_PID" 2>/dev/null || break; sleep 0.1
done
kill -0 "$DUMP_PID" 2>/dev/null && sudo kill -TERM "$DUMP_PID" 2>/dev/null
wait "$DUMP_PID" 2>/dev/null || true

# --- 11. Report ---
# Extract timing from CRIU logs
LOG="$IMAGES_DIR/lazy-primary.log"
DOT=$(sudo grep -a "dump_one_task TOTAL" "$LOG" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || echo "?")
REPLICA_MEM=$($SSH ubuntu@$REPLICA "valkey-cli info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r'" 2>/dev/null || echo "?")

# Archive logs
sudo cp -f "$IMAGES_DIR/lazy-primary.log" "$RUN_DIR/" 2>/dev/null || true
sudo cp -f "$IMAGES_DIR/stats-dump" "$IMAGES_DIR/stats-restore" "$RUN_DIR/" 2>/dev/null || true

log "================================================================"
log "Migration completed successfully!"
log "  Duration: ${MIGRATION_TIME_MS}ms ($(( MIGRATION_TIME_MS / 1000 ))s)"
log "  Source:   $MEM   Replica: $REPLICA_MEM"
log "  Freeze:   dump_one_task=${DOT}s  cutover=$((_t1 - _t0))ms"
log "  Artifacts: $RUN_DIR"
log "================================================================"
