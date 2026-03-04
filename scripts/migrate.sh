#!/usr/bin/env bash
set -euo pipefail

# Migrate a running Valkey to a replica machine using CRIU COW dump.
# Run on SOURCE. Usage: migrate.sh [SIZE_GB]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

DATA_SIZE_GB=${1:-$DEFAULT_DATA_SIZE_GB}
CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
[ -x "$CRIU_BIN" ] || CRIU_BIN="criu"
SKIP_FILL=${SKIP_FILL:-0}
KEEP_SOURCE_RUNNING=${KEEP_SOURCE_RUNNING:-0}
RUN_WORKLOAD_DURING_MIGRATION=${RUN_WORKLOAD_DURING_MIGRATION:-0}
CUTOVER_PORT=${CUTOVER_PORT:-9003}
STAGED_PORT=${STAGED_PORT:-9004}
IMAGE_XFER_PORT=${IMAGE_XFER_PORT:-9005}

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA=${REPLICA_IP:-$REPLICA_HOST}
SSH_CTL="/tmp/ssh-migrate-$$"
$SSH -fN -o ControlMaster=yes -o ControlPath="$SSH_CTL" ubuntu@$REPLICA 2>/dev/null
SSH="$SSH -o ControlPath=$SSH_CTL"
trap 'ssh -o ControlPath="$SSH_CTL" -O exit ubuntu@'"$REPLICA"' 2>/dev/null || true' EXIT

RUN_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/../artifacts}/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RUN_DIR"
log() { echo "[$(date +%H:%M:%S)] $*"; }

# --- 1. Clean ---
log "Clean up..."
[ "$KEEP_SOURCE_RUNNING" = "1" ] || sudo pkill -9 valkey-server 2>/dev/null || true
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sudo pkill -9 criu 2>/dev/null || true
$SSH ubuntu@$REPLICA "sudo pkill -9 valkey-server; sudo pkill -9 criu; sudo pkill -9 -f restore.sh; sudo rm -f /var/lib/valkey/temp-*.rdb" 2>/dev/null || true
sleep 1

# --- 2. Find Valkey ---
PID=$(pgrep -x valkey-server | head -1 || true)
[ -n "$PID" ] || { log "ERROR: valkey-server not running"; exit 1; }
log "Valkey PID $PID"

# --- 3. Fill (optional) ---
if [ "$SKIP_FILL" != "1" ]; then
  NUM=$((DATA_SIZE_GB * 25300))
  log "Filling ~${DATA_SIZE_GB}GB..."
  valkey-benchmark -p "$VALKEY_PORT" -t set -d 64000 -r "$NUM" -n $((NUM + 50000)) --threads 10 -q
fi
MEM=$(valkey-cli -p "$VALKEY_PORT" info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r' || echo "?")
log "Memory: $MEM"

# --- 4. Prepare ---
sudo rm -rf "$IMAGES_DIR" && sudo mkdir -p "$IMAGES_DIR" && sudo chmod 777 "$IMAGES_DIR"

# --- 5. Start replica ---
log "Starting restore on replica..."
$SSH ubuntu@$REPLICA "sudo env CUTOVER_PORT=$CUTOVER_PORT STAGED_PORT=$STAGED_PORT IMAGE_XFER_PORT=$IMAGE_XFER_PORT $SCRIPT_DIR/restore.sh" &
REPLICA_PID=$!
sleep 1

# --- 6. Quiesce + dump ---
log "Quiesce + dump..."
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sleep 0.1
for _ in $(seq 1 50); do
  N=$(valkey-cli -p "$VALKEY_PORT" CLIENT LIST 2>/dev/null | grep -c "^" || echo 99)
  [ "$N" -le 1 ] && break; sleep 0.1
done
valkey-cli -p "$VALKEY_PORT" CONFIG SET lazyfree-lazy-expire no CONFIG SET save "" >/dev/null 2>&1 || true

# CLIENT PAUSE before dump — freeze client processing so all threads settle.
# Must happen HERE (not in COW_PRE_FREEZE_CMD) so we can verify all threads
# are in idle syscalls AFTER the pause processing (including any logging/printf)
# completes. This prevents catching threads mid-malloc at cgroup freeze time.
# The page server re-quiesces before convergence via COW_PRE_CONVERGE_CMD.
# 200ms: ~10ms settle + 166ms dump freeze + margin
valkey-cli -p "$VALKEY_PORT" CLIENT PAUSE 200 ALL >/dev/null 2>&1 || true

# Wait for main thread to return to epoll_wait (meaning CLIENT PAUSE
# processing including any log output/printf/malloc is complete)
PID=$(pgrep -x valkey-server)
for _ in $(seq 1 200); do
  SC=$(sudo cat /proc/$PID/syscall 2>/dev/null | awk '{print $1}')
  [ "$SC" = "22" ] || [ "$SC" = "73" ] && break; sleep 0.01
done

# Wait for ALL threads to be in idle syscalls (not just main thread).
# Idle: 22=epoll_pwait, 73=ppoll, 98=futex, 101=nanosleep, 115=clock_nanosleep
for _ in $(seq 1 1000); do
  ALL_IDLE=1
  for tid_dir in /proc/$PID/task/*/; do
    SC=$(sudo cat "${tid_dir}syscall" 2>/dev/null | awk '{print $1}')
    case "$SC" in
      22|73|98|101|115) ;;
      *) ALL_IDLE=0; break ;;
    esac
  done
  [ "$ALL_IDLE" = "1" ] && break
  sleep 0.001
done

# Cgroup freeze path
CGROUP=""
REL=$(grep '^0::' "/proc/$PID/cgroup" 2>/dev/null | cut -d: -f3 || true)
case "$REL" in *.service) CGROUP="/sys/fs/cgroup${REL}" ;; esac

CRIU_ARGS=(
  sudo
  COW_PRE_FREEZE_CMD="valkey-cli -p $VALKEY_PORT CLIENT PAUSE 50 ALL"
  "$CRIU_BIN" dump
  --tree "$PID" --images-dir "$IMAGES_DIR"
  --cow-dump --lazy-pages
  --address "$PRIMARY_IP" --port "$CRIU_PORT"
  --serve-images "$IMAGE_XFER_PORT"
  --tcp-close --skip-in-flight --ext-unix-sk
  --leave-running --display-stats
  -v2 -o "$IMAGES_DIR/lazy-primary.log"
)
[ -n "$CGROUP" ] && CRIU_ARGS+=(--freeze-cgroup "$CGROUP")

MIGRATION_START_MS=${EPOCHREALTIME/./}; MIGRATION_START_MS=${MIGRATION_START_MS:0:13}
"${CRIU_ARGS[@]}" &
DUMP_PID=$!

# Wait for page server
for _ in $(seq 1 1200); do
  sudo grep -aq "PAGE SERVER READY TO SERVE" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null && break
  kill -0 "$DUMP_PID" 2>/dev/null || { log "ERROR: dump exited early"; exit 1; }
  sleep 0.025
done

# --- 7. Start staged listener in background ---
fuser -k "$STAGED_PORT"/tcp 2>/dev/null || true
STAGED_FILE=$(mktemp /tmp/staged.XXXXXX)
(timeout 600 python3 -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', $STAGED_PORT)); s.listen(1)
c, _ = s.accept(); d = c.recv(64); c.close(); s.close()
print(d.decode().strip())
" > "$STAGED_FILE" 2>/dev/null) &
STAGED_PID=$!

# --- 7b. Live workload (optional) ---
WORKLOAD_PID=""
if [ "$RUN_WORKLOAD_DURING_MIGRATION" = "1" ]; then
  for _ in $(seq 1 600); do [ -f "$IMAGES_DIR/bulk_send_done" ] && break; sleep 0.5; done
  log "Starting live workload..."
  valkey-benchmark -p "$VALKEY_PORT" -t set -r 1000000 -c 13 -P 16 -d 64000 -n 1000000000 -q >/dev/null 2>&1 &
  WORKLOAD_PID=$!
fi

# --- 8. Wait for staged ---
log "Waiting for staged..."
wait "$STAGED_PID" 2>/dev/null || true
STAGED=$(cat "$STAGED_FILE" 2>/dev/null || true)
rm -f "$STAGED_FILE"
[ -n "$STAGED" ] || { log "ERROR: staged not received"; exit 1; }

# --- 9. Cutover (disable set -e for robustness) ---
set +e
_t0=${EPOCHREALTIME/./}; _t0=${_t0:0:13}
kill -STOP "$PID" 2>/dev/null || sudo kill -STOP "$PID" 2>/dev/null || true
(echo "GO" > /dev/tcp/"$REPLICA"/"$CUTOVER_PORT") 2>/dev/null || echo "GO" | nc -q 0 -w 1 "$REPLICA" "$CUTOVER_PORT" 2>/dev/null || true
_t1=${EPOCHREALTIME/./}; _t1=${_t1:0:13}
MIGRATION_END_MS=${EPOCHREALTIME/./}; MIGRATION_END_MS=${MIGRATION_END_MS:0:13}
log "Cutover: source frozen $((_t1 - _t0))ms"

if [ "$KEEP_SOURCE_RUNNING" = "1" ]; then
  sudo pkill -CONT -x valkey-server 2>/dev/null || true
  valkey-cli -p "$VALKEY_PORT" CLIENT UNPAUSE >/dev/null 2>&1 || true
fi

# --- 10. Cleanup ---
[ -n "$WORKLOAD_PID" ] && { kill "$WORKLOAD_PID" 2>/dev/null; sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null; } || true
wait "$REPLICA_PID" 2>/dev/null || true
for _ in $(seq 1 600); do kill -0 "$DUMP_PID" 2>/dev/null || break; sleep 0.1; done
kill -0 "$DUMP_PID" 2>/dev/null && sudo kill -TERM "$DUMP_PID" 2>/dev/null
wait "$DUMP_PID" 2>/dev/null || true

# --- Report ---
MIGRATION_TIME_MS=$((MIGRATION_END_MS - MIGRATION_START_MS))
DOT=$(sudo grep -a "dump_one_task TOTAL" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || echo "?")
RMEM=$($SSH ubuntu@$REPLICA "valkey-cli info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r'" 2>/dev/null || echo "?")
sudo cp -f "$IMAGES_DIR/lazy-primary.log" "$IMAGES_DIR/stats-dump" "$IMAGES_DIR/stats-restore" "$RUN_DIR/" 2>/dev/null || true

log "================================================================"
log "Migration completed successfully!"
log "  Duration: ${MIGRATION_TIME_MS}ms ($(( MIGRATION_TIME_MS / 1000 ))s)"
log "  Source: $MEM  Replica: $RMEM"
log "  Freeze: dump_one_task=${DOT}s  cutover=$((_t1 - _t0))ms"
log "  Artifacts: $RUN_DIR"
log "================================================================"
