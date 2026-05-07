#!/usr/bin/env bash
set -euo pipefail

# Live-migrate a running Valkey to a replica machine.
#
# Phases:
#   1. SEIZE    — brief freeze, capture process blueprint, resume
#   2. TRACK    — WP + eBPF dirty tracking (source running)
#   3. TRANSFER — 8-stream bulk page transfer (source running)
#   4. CONVERGE — final freeze, send delta, resume source
#   5. HANDOFF  — replica applies state, goes live
#
# Run on SOURCE.  Usage: migrate.sh [SIZE_GB]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

# ── Config ──
DATA_SIZE_GB=${1:-$DEFAULT_DATA_SIZE_GB}
CRIU_BIN="${CRIU_BIN:-$SCRIPT_DIR/../criu/criu}"
[ -x "$CRIU_BIN" ] || CRIU_BIN="criu"
CUTOVER_PORT=${CUTOVER_PORT:-9003}
STAGED_PORT=${STAGED_PORT:-9004}
IMAGE_XFER_PORT=${IMAGE_XFER_PORT:-9005}
SKIP_FILL=${SKIP_FILL:-0}
KEEP_SOURCE=${KEEP_SOURCE_RUNNING:-0}
LIVE_WORKLOAD=${RUN_WORKLOAD_DURING_MIGRATION:-0}

# ── SSH ──
SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA=${REPLICA_IP:-$REPLICA_HOST}
SSH_CTL="/tmp/ssh-migrate-$$"
$SSH -fN -o ControlMaster=yes -o ControlPath="$SSH_CTL" ubuntu@$REPLICA 2>/dev/null
SSH="$SSH -o ControlPath=$SSH_CTL"
trap 'ssh -o ControlPath="$SSH_CTL" -O exit ubuntu@'"$REPLICA"' 2>/dev/null || true' EXIT

RUN_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/../artifacts}/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RUN_DIR"
log() { echo "[$(date +%H:%M:%S)] $*"; }

# ── 1. Clean + deploy ──
log "Clean..."
[ "$KEEP_SOURCE" = "1" ] || sudo pkill -9 valkey-server 2>/dev/null || true
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sudo pkill -9 criu 2>/dev/null || true
$SSH ubuntu@$REPLICA "sudo pkill -9 valkey-server; sudo pkill -9 criu; sudo pkill -9 page-recv; sudo pkill -9 -f restore.sh; sudo rm -rf /tmp/criu-images /var/lib/valkey/temp-*.rdb" 2>/dev/null || true
sleep 1

# Deploy criu + page-recv to replica (ensure binaries match)
PAGE_RECV_BIN_LOCAL="${PAGE_RECV_BIN:-$SCRIPT_DIR/../tools/page-recv}"
if [ -x "$PAGE_RECV_BIN_LOCAL" ]; then
  scp -i "$SSH_KEY" -o StrictHostKeyChecking=no \
    "$CRIU_BIN" "$PAGE_RECV_BIN_LOCAL" \
    ubuntu@$REPLICA:/tmp/ 2>/dev/null || true
  $SSH ubuntu@$REPLICA "sudo cp /tmp/criu /usr/local/sbin/criu 2>/dev/null; cp /tmp/page-recv ~/work/criu/tools/page-recv 2>/dev/null" || true
fi
sleep 1

# ── 2. Valkey ──
PID=$(pgrep -x valkey-server | head -1 || true)
[ -n "$PID" ] || { log "ERROR: valkey-server not running"; exit 1; }
log "Valkey PID=$PID"

if [ "$SKIP_FILL" != "1" ]; then
  NUM=$((DATA_SIZE_GB * 25300))
  log "Filling ~${DATA_SIZE_GB}GB..."
  valkey-benchmark -p "$VALKEY_PORT" -t set -d 64000 -r "$NUM" \
    -n $((NUM + 50000)) --threads 10 -q >/dev/null 2>&1
fi
MEM=$(valkey-cli -p "$VALKEY_PORT" info memory 2>/dev/null \
  | grep used_memory_human | cut -d: -f2 | tr -d '\r' || echo "?")
log "Memory: $MEM"

# ── 3. Prepare images ──
sudo rm -rf "$IMAGES_DIR"
sudo mkdir -p "$IMAGES_DIR"
sudo chmod 777 "$IMAGES_DIR"

# ── 4. Start replica restore ──
log "Starting restore on replica..."
$SSH ubuntu@$REPLICA "sudo env \
  CUTOVER_PORT=$CUTOVER_PORT \
  STAGED_PORT=$STAGED_PORT \
  IMAGE_XFER_PORT=$IMAGE_XFER_PORT \
  $SCRIPT_DIR/restore.sh" &
REPLICA_PID=$!
sleep 1

# ── 5. Seize (CRIU dump with --cow-dump --lazy-pages --leave-running) ──
log "Seize (live_workload=$LIVE_WORKLOAD)..."
valkey-cli -p "$VALKEY_PORT" CONFIG SET lazyfree-lazy-expire no \
  CONFIG SET save "" >/dev/null 2>&1 || true
PID=$(pgrep -x valkey-server)

CGROUP=""
REL=$(grep '^0::' "/proc/$PID/cgroup" 2>/dev/null | cut -d: -f3 || true)
case "$REL" in *.service) CGROUP="/sys/fs/cgroup${REL}" ;; esac

CRIU_ARGS=(
  sudo "$CRIU_BIN" dump
  --tree "$PID" --images-dir "$IMAGES_DIR"
  --cow-dump --lazy-pages
  --address "$PRIMARY_IP" --port "$CRIU_PORT"
  --serve-images "$IMAGE_XFER_PORT"
  --tcp-close --skip-in-flight --ext-unix-sk
  --leave-running --display-stats
  -v2 -o "$IMAGES_DIR/lazy-primary.log"
)
[ -n "$CGROUP" ] && CRIU_ARGS+=(--freeze-cgroup "$CGROUP")

MIGRATION_START=${EPOCHREALTIME/./}; MIGRATION_START=${MIGRATION_START:0:13}
"${CRIU_ARGS[@]}" &
CRIU_PID=$!

# Wait for page server ready
for _ in $(seq 1 1200); do
  sudo grep -aq "PAGE SERVER READY TO SERVE" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null && break
  kill -0 "$CRIU_PID" 2>/dev/null || { log "ERROR: seize exited early"; exit 1; }
  sleep 0.025
done

# ── 6. Staged listener (replica signals when bulk is done) ──
fuser -k "$STAGED_PORT"/tcp 2>/dev/null || true
STAGED_FILE=$(mktemp /tmp/staged.XXXXXX)
(timeout 600 python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', $STAGED_PORT)); s.listen(1)
c, _ = s.accept(); d = c.recv(64); c.close(); s.close()
print(d.decode().strip())
" > "$STAGED_FILE" 2>/dev/null) &
STAGED_PID=$!

# ── 7. Live workload (optional, runs through transfer + converge) ──
WORKLOAD_PID=""
if [ "$LIVE_WORKLOAD" = "1" ]; then
  log "Starting live workload..."
  valkey-benchmark -p "$VALKEY_PORT" -t set,get -r 1000000 \
    -c 16 -P 8 -d 512 --ratio 20:80 -n 1000000000 \
    -q >/dev/null 2>&1 &
  WORKLOAD_PID=$!
fi

# ── 8. Wait for transfer + converge to complete ──
log "Waiting for transfer + converge..."
wait "$STAGED_PID" 2>/dev/null || true
STAGED=$(cat "$STAGED_FILE" 2>/dev/null || true)
rm -f "$STAGED_FILE"
[ -n "$STAGED" ] || { log "ERROR: staged signal not received"; exit 1; }

# ── 9. Handoff ──
set +e
_t0=${EPOCHREALTIME/./}; _t0=${_t0:0:13}
(echo "GO" > /dev/tcp/"$REPLICA"/"$CUTOVER_PORT") 2>/dev/null \
  || echo "GO" | nc -q 0 -w 1 "$REPLICA" "$CUTOVER_PORT" 2>/dev/null || true
_t1=${EPOCHREALTIME/./}; _t1=${_t1:0:13}
MIGRATION_END=${EPOCHREALTIME/./}; MIGRATION_END=${MIGRATION_END:0:13}
log "Handoff: GO sent $((_t1 - _t0))ms"

# ── 10. Cleanup ──
[ -n "$WORKLOAD_PID" ] && kill "$WORKLOAD_PID" 2>/dev/null || true
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
wait "$REPLICA_PID" 2>/dev/null || true
for _ in $(seq 1 300); do kill -0 "$CRIU_PID" 2>/dev/null || break; sleep 0.1; done
kill -0 "$CRIU_PID" 2>/dev/null && sudo kill -TERM "$CRIU_PID" 2>/dev/null
wait "$CRIU_PID" 2>/dev/null || true

# ── Report ──
DURATION_MS=$((MIGRATION_END - MIGRATION_START))
SEIZE_T=$(sudo grep -a "dump_one_task TOTAL" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null \
  | grep -oE '[0-9]+\.[0-9]+' || echo "?")
CONVERGE_T=$(sudo grep -a "COW CONVERGE" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null \
  | grep -oE '[0-9]+\.[0-9]+ms' || echo "?")
RMEM=$($SSH ubuntu@$REPLICA "valkey-cli info memory 2>/dev/null \
  | grep used_memory_human | cut -d: -f2 | tr -d '\r'" 2>/dev/null || echo "?")
sudo cp -f "$IMAGES_DIR/lazy-primary.log" "$RUN_DIR/" 2>/dev/null || true

log "================================================================"
log "Migration completed successfully!"
log "  Duration: ${DURATION_MS}ms ($(( DURATION_MS / 1000 ))s)"
log "  Source: $MEM  Replica: $RMEM"
log "  Seize: ${SEIZE_T}s  Converge: ${CONVERGE_T}  Handoff: $((_t1 - _t0))ms"
log "  Artifacts: $RUN_DIR"
log "================================================================"
