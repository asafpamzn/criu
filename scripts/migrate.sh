#!/usr/bin/env bash
set -euo pipefail

# Master migration script - run on PRIMARY machine

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

DATA_SIZE_GB=${1:-$DEFAULT_DATA_SIZE_GB}
DEFAULT_CRIU_BIN="$SCRIPT_DIR/../criu/criu"
if [ -x "$DEFAULT_CRIU_BIN" ]; then
	CRIU_BIN=${CRIU_BIN:-$DEFAULT_CRIU_BIN}
else
	CRIU_BIN=${CRIU_BIN:-criu}
fi
FAST_CUTOVER=${FAST_CUTOVER:-1}  # Default on: restore in SIGSTOP until all pages transferred
CUTOVER_PAUSE_MS=${CUTOVER_PAUSE_MS:-50}
RUN_WORKLOAD_DURING_MIGRATION=${RUN_WORKLOAD_DURING_MIGRATION:-0}
KEEP_SOURCE_RUNNING=${KEEP_SOURCE_RUNNING:-0}
SKIP_FILL=${SKIP_FILL:-0}
STOP_DUMP_ON_COMPLETE=${STOP_DUMP_ON_COMPLETE:-1}
WORKLOAD_KEYSPACE=${WORKLOAD_KEYSPACE:-1000000}
WORKLOAD_CLIENTS=${WORKLOAD_CLIENTS:-64}
WORKLOAD_PIPELINE=${WORKLOAD_PIPELINE:-16}
# Default to 64KB so the live workload does not shrink the 64KB-filled dataset.
WORKLOAD_DATA_SIZE=${WORKLOAD_DATA_SIZE:-64000}
WORKLOAD_LOG_FILE=${WORKLOAD_LOG_FILE:-}
CRIU_DUMP_STRACE_OUT=${CRIU_DUMP_STRACE_OUT:-}
CUTOVER_MARKER_FILE=${CUTOVER_MARKER_FILE:-}
REPLICA_STAGED_FILE=${REPLICA_STAGED_FILE:-$IMAGES_DIR/replica_staged.log}
CUTOVER_PORT=${CUTOVER_PORT:-9003}
REPLICA_PING_POLL_INTERVAL_S=${REPLICA_PING_POLL_INTERVAL_S:-0.01}
VALKEY_CMD_TIMEOUT_S=${VALKEY_CMD_TIMEOUT_S:-2}
MEASURE_SOURCE_AVAILABILITY=${MEASURE_SOURCE_AVAILABILITY:-1}
SOURCE_PING_INTERVAL_MS=${SOURCE_PING_INTERVAL_MS:-5}
SOURCE_PING_TIMEOUT_MS=${SOURCE_PING_TIMEOUT_MS:-10000}
POST_REPLICA_SYNC_CHECK=${POST_REPLICA_SYNC_CHECK:-0}
CUTOVER_GATE_EVENT_WAIT_S=${CUTOVER_GATE_EVENT_WAIT_S:-15}
DUMP_EXIT_TIMEOUT_S=${DUMP_EXIT_TIMEOUT_S:-600}
SSH_BASE="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"
REPLICA_SSH_HOST="${REPLICA_IP:-$REPLICA_HOST}"

# Pre-establish SSH connection (ControlMaster) for sub-100ms cutover.
# Eliminates ~150ms handshake per SSH call during the critical window.
SSH_CONTROL_PATH="/tmp/ssh-migrate-$$"
$SSH_BASE -fN -o ControlMaster=yes -o ControlPath="$SSH_CONTROL_PATH" ubuntu@$REPLICA_SSH_HOST 2>/dev/null
SSH="$SSH_BASE -o ControlPath=$SSH_CONTROL_PATH"
cleanup_ssh() { ssh -o ControlPath="$SSH_CONTROL_PATH" -O exit ubuntu@$REPLICA_SSH_HOST 2>/dev/null || true; }
trap cleanup_ssh EXIT

log() { echo "[$(date '+%H:%M:%S')] $*"; }

if [ -z "${CUTOVER_MARKER_FILE:-}" ]; then
  CUTOVER_MARKER_FILE="$IMAGES_DIR/cutover_markers.log"
fi

umask 022
ARTIFACTS_DIR=${ARTIFACTS_DIR:-"$SCRIPT_DIR/../artifacts"}
RUN_ID=${RUN_ID:-"$(date '+%Y%m%d_%H%M%S')"}
RUN_DIR="$ARTIFACTS_DIR/$RUN_ID"
mkdir -p "$RUN_DIR" 2>/dev/null || true
chmod 755 "$RUN_DIR" 2>/dev/null || true
LOCAL_MARKER_FILE="$RUN_DIR/source_markers.log"
touch "$LOCAL_MARKER_FILE" 2>/dev/null || true

mark_local_event() {
  local event="$1"
  local ts_ms
  ts_ms=$(date +%s%3N)
  printf "%s %s %s\n" "$event" "$ts_ms" "PRIMARY" >>"$LOCAL_MARKER_FILE" 2>/dev/null || true
}

valkey_cmd()
{
	timeout "${VALKEY_CMD_TIMEOUT_S}s" valkey-cli -h 127.0.0.1 -p "$VALKEY_PORT" "$@"
}

valkey_ping_ok()
{
	valkey_cmd ping &>/dev/null
}

mark_cutover_event() {
  local event="$1"
  local ts_ms

  if [ -z "$CUTOVER_MARKER_FILE" ]; then
    return 0
  fi

  ts_ms=$(date +%s%3N)
  mkdir -p "$(dirname "$CUTOVER_MARKER_FILE")" 2>/dev/null || true
  if ! printf "%s %s %s\n" "$event" "$ts_ms" "PRIMARY" | tee -a "$CUTOVER_MARKER_FILE" >/dev/null 2>&1; then
    printf "%s %s %s\n" "$event" "$ts_ms" "PRIMARY" | sudo tee -a "$CUTOVER_MARKER_FILE" >/dev/null 2>&1 || true
  fi
}

stop_workload() {
	if [ -n "${WORKLOAD_PID:-}" ]; then
		kill "$WORKLOAD_PID" 2>/dev/null || true
		WORKLOAD_PID=""
	fi
	sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
}

SOURCE_PING_PID=""
SOURCE_PING_LOG=""

start_source_ping_monitor() {
  if [ "$MEASURE_SOURCE_AVAILABILITY" != "1" ]; then
    return 0
  fi

  SOURCE_PING_LOG="$RUN_DIR/source-ping.log"
  log "  Source ping monitor: $SOURCE_PING_LOG (interval=${SOURCE_PING_INTERVAL_MS}ms timeout=${SOURCE_PING_TIMEOUT_MS}ms)"
  python3 "$SCRIPT_DIR/valkey_ping_monitor.py" \
    --host 127.0.0.1 \
    --port "$VALKEY_PORT" \
    --interval-ms "$SOURCE_PING_INTERVAL_MS" \
    --timeout-ms "$SOURCE_PING_TIMEOUT_MS" \
    --out "$SOURCE_PING_LOG" &
  SOURCE_PING_PID=$!
  mark_local_event "SOURCE_PING_MONITOR_START_MS"
}

stop_source_ping_monitor() {
  if [ -z "${SOURCE_PING_PID:-}" ]; then
    return 0
  fi
  kill "$SOURCE_PING_PID" 2>/dev/null || true
  wait "$SOURCE_PING_PID" 2>/dev/null || true
  mark_local_event "SOURCE_PING_MONITOR_STOP_MS"
  SOURCE_PING_PID=""
}

# Memory monitoring: track RSS of source process during migration
SOURCE_MEM_PID=""
SOURCE_MEM_LOG=""

start_source_memory_monitor() {
  local pid=$1
  SOURCE_MEM_LOG="$RUN_DIR/source-memory.log"
  log "  Memory monitor: $SOURCE_MEM_LOG (pid=$pid interval=0.5s)"
  (
    while kill -0 "$pid" 2>/dev/null; do
      ts=$(date +%s%N)
      rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null || echo 0)
      echo "$ts $rss"
      sleep 0.5
    done
  ) > "$SOURCE_MEM_LOG" &
  SOURCE_MEM_PID=$!
}

stop_source_memory_monitor() {
  if [ -z "${SOURCE_MEM_PID:-}" ]; then
    return 0
  fi
  kill "$SOURCE_MEM_PID" 2>/dev/null || true
  wait "$SOURCE_MEM_PID" 2>/dev/null || true
  if [ -f "$SOURCE_MEM_LOG" ]; then
    local max_rss
    max_rss=$(awk '{if($2>m)m=$2}END{printf "%.1f", m/1048576}' "$SOURCE_MEM_LOG" 2>/dev/null)
    log "  Peak source RSS: ${max_rss}GB"
  fi
  SOURCE_MEM_PID=""
}

cleanup() {
  stop_source_ping_monitor
}

trap cleanup EXIT

# Step 1: Kill on both
log "Step 1: Kill processes..."
if [ "$KEEP_SOURCE_RUNNING" = "1" ] || [ "$SKIP_FILL" = "1" ]; then
  if [ "$SKIP_FILL" = "1" ] && [ "$KEEP_SOURCE_RUNNING" != "1" ]; then
    log "  SKIP_FILL=1 requires preserving source dataset; keeping source valkey-server running"
  else
    log "  Keeping source valkey-server running"
  fi
else
  sudo pkill -9 valkey-server 2>/dev/null || true
fi
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sudo pkill -9 criu 2>/dev/null || true
$SSH ubuntu@$REPLICA_SSH_HOST "sudo pkill -9 valkey-server || true; sudo pkill -9 criu || true; sudo pkill -9 -f '[/]scripts/restore.sh' || true; sudo pkill -9 -f '[c]riu lazy-pages' || true; while sudo iptables -C INPUT -p tcp --dport $VALKEY_PORT ! -s 127.0.0.1 -j REJECT 2>/dev/null; do sudo iptables -D INPUT -p tcp --dport $VALKEY_PORT ! -s 127.0.0.1 -j REJECT || true; done" 2>/dev/null || true

# Step 1a: Clean replica stale data to prevent disk-full issues
log "Step 1a: Clean replica stale data..."
$SSH ubuntu@$REPLICA_SSH_HOST "
  # Remove leaked CRIU cgroup mounts
  for m in /fsx/lazy/.criu.cgyard.*; do
    sudo umount \"\$m\" 2>/dev/null; sudo rmdir \"\$m\" 2>/dev/null
  done
  # Remove stale RDB dumps (can be 200GB+)
  sudo rm -f /var/lib/valkey/temp-*.rdb 2>/dev/null
  # Report disk usage
  AVAIL=\$(df --output=avail / 2>/dev/null | tail -1)
  echo \"Replica disk available: \${AVAIL}K\"
" 2>/dev/null || true

sleep 1

# Step 2: Wait for valkey to be running and responsive on master
log "Step 2: Check valkey..."
PID=""
for i in $(seq 1 240); do
  PID=$(pgrep -x valkey-server | head -n1 || true)
  if [ -n "$PID" ]; then
    break
  fi
  sleep 0.5
done
if [ -z "$PID" ]; then
  log "ERROR: valkey-server not running"
  exit 1
fi
log "  PID: $PID"
for i in $(seq 1 40); do
  if valkey_ping_ok; then
    break
  fi
  sleep 0.25
done
if ! valkey_ping_ok; then
  log "ERROR: valkey-server not responding on port $VALKEY_PORT"
  exit 1
fi

# Step 3: Fill using valkey-benchmark
# Empirical: ~25300 keys per GB with 64KB values (includes overhead)
NUM_KEYS=$((DATA_SIZE_GB * 25300))
NUM_OPS=$((NUM_KEYS + 50000))
if [ "$SKIP_FILL" = "1" ]; then
  log "Step 3: Skip fill (SKIP_FILL=1)"
else
  log "Step 3: Fill ~${DATA_SIZE_GB}GB using valkey-benchmark..."
  log "  Keys: $NUM_KEYS, Ops: $NUM_OPS, Value size: 64KB"
  valkey-benchmark -h 127.0.0.1 -p "$VALKEY_PORT" -t set -d 64000 -r $NUM_KEYS -n $NUM_OPS --threads 10 -q
fi
MEM=$(valkey_cmd info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r')
if [ -z "${MEM:-}" ]; then
  MEM="unknown"
fi
log "  Memory: $MEM"

# Step 4: Clean images dir
log "Step 4: Clean $IMAGES_DIR..."
sudo rm -rf "$IMAGES_DIR"/*

# Best-effort: ensure shared marker file is writable/empty for this run.
if [ -n "${CUTOVER_MARKER_FILE:-}" ]; then
  sudo rm -f "$CUTOVER_MARKER_FILE" 2>/dev/null || true
fi

# Step 5: Start replica FIRST (it will create ready signal and wait)
log "Step 5: Start replica (will wait for page server)..."
REMOTE_RESTORE_ENV=("CUTOVER_MARKER_FILE='$CUTOVER_MARKER_FILE'")
if [ "$FAST_CUTOVER" = "1" ]; then
  REMOTE_RESTORE_ENV+=("FAST_CUTOVER=1" "CUTOVER_PAUSE_MS='$CUTOVER_PAUSE_MS'")
fi
for env_key in \
  WAIT_REPLICA_ROLE_ACTIVE \
  WAIT_REPLICA_LINK_UP \
  REPLICA_POLL_INTERVAL_S \
  RESTORE_MAX_PING_ATTEMPTS \
  RESTORE_PING_INTERVAL_S \
  RESTORE_WRITE_GUARD_ATTEMPTS \
  RESTORE_WRITE_GUARD_INTERVAL_S; do
  env_val="${!env_key:-}"
  if [ -n "$env_val" ]; then
    REMOTE_RESTORE_ENV+=("$env_key='$env_val'")
  fi
done
$SSH ubuntu@$REPLICA_SSH_HOST "sudo env ${REMOTE_RESTORE_ENV[*]} $SCRIPT_DIR/restore.sh" &
REPLICA_PID=$!

# Step 5b: Wait for replica ready signal
log "Step 5b: Wait for replica ready signal..."
READY_FILE="$IMAGES_DIR/ready.log"
for i in $(seq 1 60); do
  if [ -f "$READY_FILE" ]; then
    log "  Replica ready"
    break
  fi
  sleep 0.5
done
if [ ! -f "$READY_FILE" ]; then
  log "ERROR: replica ready signal not found at $READY_FILE"
  exit 1
fi

# Step 6: NOW start CRIU dump (replica is waiting for page server)
log "Step 6: CRIU dump..."
PID=""
for i in $(seq 1 40); do
  PID=$(pgrep -x valkey-server | head -n1 || true)
  if [ -n "$PID" ] && valkey_ping_ok; then
    break
  fi
  sleep 0.25
done
if [ -z "$PID" ]; then
  log "ERROR: valkey-server PID not found before dump"
  exit 1
fi
log "  Valkey PID: $PID"
log "  Artifacts dir: $RUN_DIR"
# Pre-create CRIU image files on FSx to avoid O_CREAT metadata latency
# during the frozen window.  open() on an existing file is fast even on FSx.
LOCAL_IMGS=""
sudo touch "$IMAGES_DIR/lazy-primary.log"
sudo chmod 644 "$IMAGES_DIR/lazy-primary.log"
mark_local_event "DUMP_PREP_MS"

log "Step 6a: Start source availability monitor..."
start_source_ping_monitor
start_source_memory_monitor "$PID"

# Pre-dump preparation: quiesce Valkey using CLIENT PAUSE ALL.
#
# CLIENT PAUSE ALL waits for all in-flight commands to complete, then
# freezes all client processing.  Data structures are fully consistent:
# no mid-write output buffers, no half-freed clients, no pending async
# frees.  After CRIU restore with --tcp-close, the paused clients get
# EOF on their sockets and Valkey frees them through the normal event
# loop path.
#
# Sequence:
#   1. Kill external benchmarks (their TCP connections close naturally)
#   2. Wait for Valkey to detect disconnections and free client structs
#   3. Disable lazyfree, saves, and set repl-backlog-size to max
#   4. CLIENT PAUSE ALL — freeze remaining client processing
#   5. DEBUG SLEEP — put main thread in a clean usleep() syscall
# Stop the source ping monitor — its connections would be captured in the
# dump and cause stale client state after restore with --tcp-close.
# Memory monitor reads /proc (no TCP), keeps running through migration.
stop_source_ping_monitor

# Kill benchmark processes. Use bracket trick to prevent self-match.
sudo pkill -9 -f "[b]ench_loop" 2>/dev/null || true
sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
sleep 1

# Wait until all benchmark clients have disconnected.
# After pkill, the benchmark's TCP connections close (FIN). Valkey's event
# loop detects EOF and frees client structures.  Poll until CLIENT LIST
# shows only our monitoring connection (≤1 client).
log "  Waiting for benchmark clients to disconnect..."
for i in $(seq 1 100); do
  NCLIENTS=$(valkey_cmd CLIENT LIST 2>/dev/null | grep -c "^" || echo 99)
  if [ "$NCLIENTS" -le 1 ]; then
    log "  All clients gone (${i}00ms)"
    break
  fi
  sleep 0.1
done
NCLIENTS=$(valkey_cmd CLIENT LIST 2>/dev/null | grep -c "^" || echo 99)
if [ "$NCLIENTS" -gt 1 ]; then
  log "  WARN: $NCLIENTS clients still connected, waiting longer..."
  sleep 3
  NCLIENTS=$(valkey_cmd CLIENT LIST 2>/dev/null | grep -c "^" || echo 99)
  log "  Clients after extra wait: $NCLIENTS"
fi
# Do NOT use CLIENT KILL — it corrupts the async free queue (adlist.c:198).
# Remaining clients (1-2 from our valkey_cmd calls) are short-lived and
# harmless; they close naturally when the script's TCP connections end.

# All CONFIG SET commands in one batch via a single connection,
# then close the connection and let the server settle.
valkey-cli -p "$VALKEY_PORT" <<'EOF'
CONFIG SET lazyfree-lazy-expire no
CONFIG SET lazyfree-lazy-server-del no
CONFIG SET lazyfree-lazy-user-del no
CONFIG SET lazyfree-lazy-user-flush no
CONFIG SET save ""
CONFIG SET repl-backlog-size 9223372036854775807
CONFIG SET repl-backlog-ttl 1
EOF

# CLIENT PAUSE ALL: freeze client command processing so the dump
# captures a clean allocator state (no glibc arena locks held).
# Must fire BEFORE the epoll_wait poll — the pause stops all client
# processing, then we wait for the main thread to settle back into
# epoll_wait (meaning all in-flight mallocs have completed).
# Wait for the event loop to settle before dump.
log "  Waiting for main thread to reach epoll_wait..."
PID=$(pgrep -x valkey-server)
for attempt in $(seq 1 200); do
  SYSCALL=$(sudo cat /proc/$PID/syscall 2>/dev/null | awk '{print $1}')
  # ARM64: epoll_pwait=22, nanosleep=101, ppoll=73, clock_nanosleep=115
  if [ "$SYSCALL" = "22" ] || [ "$SYSCALL" = "73" ]; then
    log "  Main thread in epoll_wait (syscall=$SYSCALL) after ${attempt} checks"
    break
  fi
  sleep 0.01
done

# Poll ALL threads for idle state (not just main).
# Worker threads must be in futex_wait (98) = pthread_cond_wait.
# This ensures no thread holds allocator locks at dump time.
log "  Waiting for all threads to be idle..."
ALL_IDLE_ATTEMPTS=0
for _a in $(seq 1 1000); do
  ALL_IDLE=1
  for tid_dir in /proc/$PID/task/*/; do
    SC=$(sudo cat "${tid_dir}syscall" 2>/dev/null | awk '{print $1}')
    # Idle syscalls: 22=epoll_pwait, 73=ppoll, 98=futex, 101=nanosleep, 115=clock_nanosleep
    case "$SC" in
      22|73|98|101|115) ;;
      *) ALL_IDLE=0; break ;;
    esac
  done
  ALL_IDLE_ATTEMPTS=$((_a))
  [ "$ALL_IDLE" = "1" ] && break
  sleep 0.001
done
log "  All threads idle after $ALL_IDLE_ATTEMPTS checks"

# Run dump - cow-dump keeps running, we'll kill it after restore.
# Optional syscall profiling can be enabled via CRIU_DUMP_STRACE_OUT.
# Discover cgroup path for --freeze-cgroup (ensures clean thread freeze).
# Only use it if the cgroup is a dedicated service scope (*.service),
# NOT a session scope (*.scope) which would freeze CRIU itself.
CGROUP_PATH=""
if [ -f "/proc/$PID/cgroup" ]; then
  CGROUP_REL=$(grep '^0::' "/proc/$PID/cgroup" | cut -d: -f3)
  if [ -n "$CGROUP_REL" ] && [ -d "/sys/fs/cgroup${CGROUP_REL}" ]; then
    case "$CGROUP_REL" in
      *.service) CGROUP_PATH="/sys/fs/cgroup${CGROUP_REL}" ;;
      *) log "  Skipping --freeze-cgroup (shared cgroup: $CGROUP_REL)" ;;
    esac
  fi
fi

CRIU_DUMP_CMD=(
  sudo env COW_PRE_FREEZE_CMD="valkey-cli -p $VALKEY_PORT CLIENT PAUSE 5000 ALL >/dev/null 2>&1" "$CRIU_BIN" dump
  --tree "$PID"
  --images-dir "$IMAGES_DIR"
  --cow-dump
  --lazy-pages
  --address "$PRIMARY_IP"
  --port "$CRIU_PORT"
  --tcp-close
  # We don't restore active TCP sessions (we use --tcp-close), and the ping
  # monitor/workload can have sockets mid-teardown. Don't fail the dump on
  # transient in-flight connections.
  --skip-in-flight
  --ext-unix-sk
  --leave-running
  --display-stats
  -v2 -o "$IMAGES_DIR/lazy-primary.log"
)
if [ -n "$CGROUP_PATH" ]; then
  CRIU_DUMP_CMD+=(--freeze-cgroup "$CGROUP_PATH")
  log "  Using --freeze-cgroup $CGROUP_PATH"
fi

if [ -n "$CRIU_DUMP_STRACE_OUT" ]; then
  log "  Enabling dump strace: $CRIU_DUMP_STRACE_OUT"
  CRIU_DUMP_CMD=(
    sudo env strace -ff -yy -tt -T -o "$CRIU_DUMP_STRACE_OUT"
    "$CRIU_BIN" dump
    --tree "$PID"
    --images-dir "$IMAGES_DIR"
    --cow-dump
    --lazy-pages
    --address "$PRIMARY_IP"
    --port "$CRIU_PORT"
    --tcp-close
    --skip-in-flight
    --ext-unix-sk
    --leave-running
    --display-stats
    -v2 -o "$IMAGES_DIR/lazy-primary.log"
  )
  if [ -n "$CGROUP_PATH" ]; then
    CRIU_DUMP_CMD+=(--freeze-cgroup "$CGROUP_PATH")
  fi
fi

mark_local_event "DUMP_LAUNCH_MS"
MIGRATION_START_MS=$(date +%s%3N)
"${CRIU_DUMP_CMD[@]}" &
DUMP_PID=$!

sleep 2
if ! kill -0 "$DUMP_PID" 2>/dev/null; then
  # Dump process exited — check if page server was ready (success) or not (failure)
  if sudo grep -a -q "PAGE SERVER READY TO SERVE" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null; then
    log "  Dump completed quickly (small dataset)"
  else
    log "ERROR: criu dump exited early"
    sudo tail -n 120 "$IMAGES_DIR/lazy-primary.log" || true
    exit 1
  fi
fi
PAGE_SERVER_READY=0
for _ in $(seq 1 600); do
  if sudo grep -a -q "PAGE SERVER READY TO SERVE" "$IMAGES_DIR/lazy-primary.log" 2>/dev/null; then
    PAGE_SERVER_READY=1
    mark_local_event "PAGE_SERVER_READY_MS"
    break
  fi
  sleep 0.05
done
if [ "$PAGE_SERVER_READY" -ne 1 ]; then
  log "WARN: did not observe 'PAGE SERVER READY TO SERVE' in lazy-primary.log within 30s"
fi


WORKLOAD_PID=""
if [ "$RUN_WORKLOAD_DURING_MIGRATION" = "1" ]; then
  # Wait for bulk transfer to complete before starting workload.
  # This ensures bulk pages are from a consistent (pre-write) snapshot.
  # The workload's dirty pages are then captured by the fork snapshot.
  log "Step 6b: Waiting for bulk transfer to complete..."
  BULK_DONE_MARKER="$IMAGES_DIR/bulk_send_done"
  for _bw in $(seq 1 600); do
    [ -f "$BULK_DONE_MARKER" ] && break
    sleep 0.5
  done
  if [ -f "$BULK_DONE_MARKER" ]; then
    log "  Bulk transfer done, starting workload"
  else
    log "  WARN: bulk_send_done not detected in 300s, starting workload anyway"
  fi
  log "Step 6c: Start workload traffic..."
  if [ -z "$WORKLOAD_LOG_FILE" ]; then
    WORKLOAD_LOG_FILE=$(mktemp /tmp/migrate_workload_live.XXXXXX.log 2>/dev/null || echo "/tmp/migrate_workload_live.log")
  fi
  if ! touch "$WORKLOAD_LOG_FILE" 2>/dev/null; then
    WORKLOAD_LOG_FILE="/dev/null"
  fi
  # 80/20 read/write: SET-only at ~20% of full throughput.
  # With 13 clients (vs 64) and pipeline 16, target ~8-9K writes/s.
  WORKLOAD_WRITE_CLIENTS=${WORKLOAD_WRITE_CLIENTS:-13}
  valkey-benchmark -h 127.0.0.1 -p "$VALKEY_PORT" \
    -t set \
    -r "$WORKLOAD_KEYSPACE" \
    -c "$WORKLOAD_WRITE_CLIENTS" \
    -P "$WORKLOAD_PIPELINE" \
    -d "$WORKLOAD_DATA_SIZE" \
    -n 1000000000 \
    -q >"$WORKLOAD_LOG_FILE" 2>&1 &
  WORKLOAD_PID=$!
  log "  Workload PID: $WORKLOAD_PID"
  log "  Workload log: $WORKLOAD_LOG_FILE"
  sleep 1
  if ! kill -0 "$WORKLOAD_PID" 2>/dev/null; then
    log "ERROR: workload process exited early"
    if [ "$WORKLOAD_LOG_FILE" != "/dev/null" ]; then
      tail -n 40 "$WORKLOAD_LOG_FILE" 2>/dev/null || true
    fi
    exit 1
  fi
fi

# Step 7: Wait for replica readiness without blocking on ssh wrapper process
log "Step 7: Wait for replica readiness..."

# In normal mode, count cutover only after the replica gate is lifted
# (shared marker file path required so both hosts can write/read it).
if [ "$FAST_CUTOVER" != "1" ] && [ -n "$CUTOVER_MARKER_FILE" ] && [[ "$CUTOVER_MARKER_FILE" == "$IMAGES_DIR/"* ]]; then
  log "Step 7a: Waiting for replica gate removal marker..."
  GATE_REMOVED=0
  for _ in $(seq 1 $((CUTOVER_GATE_EVENT_WAIT_S * 20))); do
    if grep -q "^REPLICA_GATE_REMOVED " "$CUTOVER_MARKER_FILE" 2>/dev/null; then
      GATE_REMOVED=1
      break
    fi
    sleep 0.05
  done
  if [ "$GATE_REMOVED" -eq 1 ]; then
    log "  Replica gate removed marker detected"
  else
    log "  WARN: replica gate removal marker not observed before cutover timing"
  fi
fi

if [ "$FAST_CUTOVER" = "1" ]; then
  log "Step 7b: Waiting for replica staged marker ($REPLICA_STAGED_FILE)..."
  STAGED_OK=0
  for _ in $(seq 1 $((DUMP_EXIT_TIMEOUT_S * 20))); do
    if [ -f "$REPLICA_STAGED_FILE" ]; then
      STAGED_OK=1
      break
    fi
    sleep 0.05
  done
  if [ "$STAGED_OK" -ne 1 ]; then
    log "ERROR: replica staged marker not found at $REPLICA_STAGED_FILE"
    exit 1
  fi
  log "  Replica staged marker detected"
fi

# Step 8: Cutover/check
REPLICA_UP=0
mark_cutover_event "CUTOVER_START_MS"
mark_local_event "CUTOVER_START_MS"
if [ "$FAST_CUTOVER" = "1" ]; then
  valkey_cmd CLIENT PAUSE "$CUTOVER_PAUSE_MS" WRITE >/dev/null 2>&1 || true
  # Use bash builtins to avoid fork+exec overhead in the critical path:
  # - EPOCHREALTIME instead of $(date) — no fork
  # - kill instead of pkill — no /proc scan
  # - /dev/tcp instead of nc — no fork+exec
  _t0=${EPOCHREALTIME/./}; _t0=${_t0:0:13}  # epoch ms, no fork
  kill -STOP "$PID" 2>/dev/null || sudo kill -STOP "$PID" 2>/dev/null || true
  SOURCE_FROZEN=1
  if (echo "GO" > /dev/tcp/"$REPLICA_SSH_HOST"/"$CUTOVER_PORT") 2>/dev/null; then
    _t1=${EPOCHREALTIME/./}; _t1=${_t1:0:13}
    log "Step 8: TCP cutover — source frozen for $((_t1 - _t0))ms"
  elif echo "GO" | nc -q 0 -w 1 "$REPLICA_SSH_HOST" "$CUTOVER_PORT" 2>/dev/null; then
    _t1=${EPOCHREALTIME/./}; _t1=${_t1:0:13}
    log "Step 8: TCP/nc cutover — source frozen for $((_t1 - _t0))ms"
  else
    log "  WARN: TCP cutover failed, falling back to SSH"
    $SSH ubuntu@$REPLICA_SSH_HOST \
      "sudo kill -CONT \$(pgrep -x valkey-server)" 2>/dev/null
    _t1=${EPOCHREALTIME/./}; _t1=${_t1:0:13}
    log "Step 8: SSH cutover (fallback) — source frozen for $((_t1 - _t0))ms"
  fi
  mark_cutover_event "CUTOVER_END_MS"
  mark_local_event "CUTOVER_END_MS"
  REPLICA_UP=1
else
  log "Step 8: Check replica..."
  for i in $(seq 1 120); do
    if $SSH ubuntu@$REPLICA_SSH_HOST "timeout ${VALKEY_CMD_TIMEOUT_S}s valkey-cli ping >/dev/null 2>&1"; then
      REPLICA_UP=1
      mark_cutover_event "CUTOVER_END_MS"
      mark_local_event "CUTOVER_END_MS"
      break
    fi
    sleep "$REPLICA_PING_POLL_INTERVAL_S"
  done
fi
if [ "$REPLICA_UP" -ne 1 ]; then
  log "ERROR: replica valkey is not responding"
  log "Step 8b: Stop dump process..."
  sudo pkill -9 -f "[c]riu dump" 2>/dev/null || true
  sleep 1
  if [ -n "$WORKLOAD_PID" ]; then
    log "Step 8c: Stop workload traffic..."
    kill "$WORKLOAD_PID" 2>/dev/null || true
    sudo pkill -9 -f "[v]alkey-benchmark" 2>/dev/null || true
  fi
  if kill -0 "$REPLICA_PID" 2>/dev/null; then
    kill "$REPLICA_PID" 2>/dev/null || true
    wait "$REPLICA_PID" 2>/dev/null || true
  fi
  if [ "$FAST_CUTOVER" = "1" ]; then
    log "Recovering source from STOP state"
    sudo pkill -CONT -x valkey-server 2>/dev/null || true
  fi
  exit 1
fi

if [ "$FAST_CUTOVER" = "1" ] && [ "$KEEP_SOURCE_RUNNING" = "1" ]; then
  log "  Resuming source valkey-server (KEEP_SOURCE_RUNNING=1)"
  sudo pkill -CONT -x valkey-server 2>/dev/null || true
  SOURCE_FROZEN=0
fi

MIGRATION_END_MS=$(date +%s%3N)
MIGRATION_TIME_MS=$((MIGRATION_END_MS - MIGRATION_START_MS))

REPLICA_SYNCED=0
if [ "$POST_REPLICA_SYNC_CHECK" = "1" ]; then
  for i in $(seq 1 120); do
    if $SSH ubuntu@$REPLICA_SSH_HOST "timeout ${VALKEY_CMD_TIMEOUT_S}s valkey-cli info replication | awk -F: '/^role:/ {role=\$2} /^master_link_status:/ {link=\$2} END {gsub(/\r/, \"\", role); gsub(/\r/, \"\", link); if ((role == \"slave\" || role == \"replica\") && link == \"up\") exit 0; exit 1}'" >/dev/null 2>&1; then
      REPLICA_SYNCED=1
      break
    fi
    sleep 0.25
  done
if [ "$REPLICA_SYNCED" -ne 1 ]; then
    log "ERROR: replica did not reach role=replica/slave with master_link_status=up"
    log "Step 8b: Stop dump process..."
    sudo pkill -9 -f "[c]riu dump" 2>/dev/null || true
    sleep 1
    stop_workload
    if kill -0 "$REPLICA_PID" 2>/dev/null; then
      kill "$REPLICA_PID" 2>/dev/null || true
      wait "$REPLICA_PID" 2>/dev/null || true
    fi
    if [ "$FAST_CUTOVER" = "1" ]; then
      log "Recovering source from STOP state"
      sudo pkill -CONT -x valkey-server 2>/dev/null || true
    fi
    exit 1
  fi
else
  log "Step 8a: Skip post-cutover replication-link check (POST_REPLICA_SYNC_CHECK=0)"
fi

# Step 8b: Optionally stop dump/page-server process
if [ -n "$WORKLOAD_PID" ]; then
  log "Step 8b: Stop workload traffic..."
  stop_workload
fi

wait "$REPLICA_PID" 2>/dev/null || true

# Step 8c: Optionally stop dump/page-server process.
# Keep it alive until the replica's lazy-pages daemon finishes transferring
# all lazy pages, otherwise the replica can hang on unresolved faults.
if [ "$STOP_DUMP_ON_COMPLETE" = "1" ]; then
  log "Step 8c: Wait for dump process to finish (write stats)..."
  mark_local_event "DUMP_WAIT_FOR_EXIT_START_MS"

  DUMP_EXITED=0
  for _i in $(seq 1 $((DUMP_EXIT_TIMEOUT_S * 10))); do
    if ! kill -0 "$DUMP_PID" 2>/dev/null; then
      DUMP_EXITED=1
      break
    fi
    sleep 0.1
  done

  if [ "$DUMP_EXITED" -ne 1 ]; then
    log "  WARN: dump still running after ${DUMP_EXIT_TIMEOUT_S}s; sending SIGTERM"
    sudo kill -TERM "$DUMP_PID" 2>/dev/null || true
    for _i in $(seq 1 100); do
      if ! kill -0 "$DUMP_PID" 2>/dev/null; then
        DUMP_EXITED=1
        break
      fi
      sleep 0.1
    done
  fi

  if [ "$DUMP_EXITED" -ne 1 ]; then
    log "  WARN: dump did not exit; sending SIGKILL"
    sudo kill -KILL "$DUMP_PID" 2>/dev/null || true
  fi

  wait "$DUMP_PID" 2>/dev/null || true
  mark_local_event "DUMP_EXIT_MS"
else
  log "Step 8c: Keep dump process running (STOP_DUMP_ON_COMPLETE=0)"
fi

stop_source_ping_monitor
stop_source_memory_monitor

REPLICA_MEM=$($SSH ubuntu@$REPLICA_SSH_HOST "timeout ${VALKEY_CMD_TIMEOUT_S}s valkey-cli info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r'" 2>/dev/null || echo "?")

# Extract CRIU timing from logs
LOG_FILE="$IMAGES_DIR/lazy-primary.log"
DUMP_TOTAL=$(sudo grep -a "dump_one_task TOTAL" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || echo "?")
COW_INIT=$(sudo grep -a "cow_dump_init took" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || true)
COW_WRITEPROTECT=$(sudo grep -a "cow_dump_writeprotect took" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || true)
PARSE_MAPS=$(sudo grep -a "parse_maps took" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || true)
PARSE_SMAPS=$(sudo grep -a "parse_smaps took" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || true)
if [ -n "${PARSE_MAPS:-}" ]; then
  PARSE_MAPPINGS="$PARSE_MAPS"
  PARSE_MAPPINGS_LABEL="parse_maps"
elif [ -n "${PARSE_SMAPS:-}" ]; then
  PARSE_MAPPINGS="$PARSE_SMAPS"
  PARSE_MAPPINGS_LABEL="parse_smaps"
else
  PARSE_MAPPINGS="?"
  PARSE_MAPPINGS_LABEL="parse_mappings"
fi
DUMP_PAGES=$(sudo grep -a "parasite_dump_pages_seized took" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || echo "?")
GEN_IOVS=$(sudo grep -a "generate_vma_iovs loop" "$LOG_FILE" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || echo "?")

STATS_DUMP_FILE="$IMAGES_DIR/stats-dump"
FROZEN_US=""
FREEZING_US=""
MEMDUMP_US=""
MEMWRITE_US=""
PAGES_SCANNED=""
PAGES_WRITTEN=""
PAGES_LAZY=""
if [ -f "$STATS_DUMP_FILE" ]; then
  while IFS='=' read -r k v; do
    case "$k" in
      freezing_time) FREEZING_US="$v" ;;
      frozen_time) FROZEN_US="$v" ;;
      memdump_time) MEMDUMP_US="$v" ;;
      memwrite_time) MEMWRITE_US="$v" ;;
      pages_scanned) PAGES_SCANNED="$v" ;;
      pages_written) PAGES_WRITTEN="$v" ;;
      pages_lazy) PAGES_LAZY="$v" ;;
    esac
  done < <(python3 - "$STATS_DUMP_FILE" <<'PY' 2>/dev/null || true
import json
import subprocess
import sys

path = sys.argv[1]
try:
    raw = subprocess.check_output([sys.executable, "-m", "crit", "decode", "-i", path])
    data = json.loads(raw)
    entries = data.get("entries") or []
    entry = entries[0] if entries else {}
    dump = entry.get("dump") or {}
except Exception:
    sys.exit(0)

for key in (
    "freezing_time",
    "frozen_time",
    "memdump_time",
    "memwrite_time",
    "pages_scanned",
    "pages_written",
    "pages_lazy",
):
    val = dump.get(key)
    if val is not None:
        print(f"{key}={val}")
PY
)
fi

STATS_RESTORE_FILE="$IMAGES_DIR/stats-restore"
RESTORE_US=""
RESTORED_PAGES=""
if [ -f "$STATS_RESTORE_FILE" ]; then
  while IFS='=' read -r k v; do
    case "$k" in
      restore_time) RESTORE_US="$v" ;;
      pages_restored) RESTORED_PAGES="$v" ;;
    esac
  done < <(python3 - "$STATS_RESTORE_FILE" <<'PY' 2>/dev/null || true
import json
import subprocess
import sys

path = sys.argv[1]
try:
    raw = subprocess.check_output([sys.executable, "-m", "crit", "decode", "-i", path])
    data = json.loads(raw)
    entries = data.get("entries") or []
    entry = entries[0] if entries else {}
    restore = entry.get("restore") or {}
except Exception:
    sys.exit(0)

for key in ("restore_time", "pages_restored"):
    val = restore.get(key)
    if val is not None:
        print(f"{key}={val}")
PY
)
fi

SOURCE_PING_SUMMARY=""
if [ -n "${SOURCE_PING_LOG:-}" ] && [ -f "$SOURCE_PING_LOG" ]; then
  SOURCE_PING_SUMMARY=$(python3 - "$SOURCE_PING_LOG" <<'PY' 2>/dev/null || true
import sys, math

path = sys.argv[1]
ok = 0
timeouts = 0
errors = 0
rtts_us = []
max_rtt_us = 0
max_status = ""

with open(path, "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) != 4:
            continue
        try:
            send_ns = int(parts[0])
            recv_ns = int(parts[1])
            rtt_us = int(parts[2])
        except ValueError:
            continue
        status = parts[3]

        if status == "OK":
            ok += 1
            rtts_us.append(rtt_us)
        elif status == "TIMEOUT":
            timeouts += 1
        else:
            errors += 1

        if rtt_us >= max_rtt_us:
            max_rtt_us = rtt_us
            max_status = status

def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    idx = max(0, min(len(xs) - 1, int(math.ceil((p/100.0) * len(xs))) - 1))
    return xs[idx]

def fmt_us(v):
    if v is None:
        return "?"
    return f"{v/1000.0:.3f}ms"

p50 = pct(rtts_us, 50)
p95 = pct(rtts_us, 95)
p99 = pct(rtts_us, 99)

print(
    f"samples_ok={ok} samples_timeout={timeouts} samples_error={errors} "
    f"rtt_p50={fmt_us(p50)} rtt_p95={fmt_us(p95)} rtt_p99={fmt_us(p99)} "
    f"rtt_max={max_rtt_us/1000.0:.3f}ms max_status={max_status}"
)
PY
)
fi

# Archive key logs under the run artifacts dir so the next run's FSX cleanup won't destroy them.
sudo cp -f "$IMAGES_DIR/lazy-primary.log" "$RUN_DIR/lazy-primary.log" 2>/dev/null || true
sudo cp -f "$IMAGES_DIR/lazy-restore.log" "$RUN_DIR/lazy-restore.log" 2>/dev/null || true
sudo cp -f "$IMAGES_DIR/lazy-server.log" "$RUN_DIR/lazy-server.log" 2>/dev/null || true
sudo cp -f "$IMAGES_DIR/stats-dump" "$RUN_DIR/stats-dump" 2>/dev/null || true
sudo cp -f "$IMAGES_DIR/stats-restore" "$RUN_DIR/stats-restore" 2>/dev/null || true
if [ -f "$RUN_DIR/stats-dump" ]; then
  python3 -m crit decode --pretty -i "$RUN_DIR/stats-dump" >"$RUN_DIR/stats-dump.json" 2>/dev/null || true
fi
if [ -f "$RUN_DIR/stats-restore" ]; then
  python3 -m crit decode --pretty -i "$RUN_DIR/stats-restore" >"$RUN_DIR/stats-restore.json" 2>/dev/null || true
fi
if [ -n "${CUTOVER_MARKER_FILE:-}" ] && [ -f "$CUTOVER_MARKER_FILE" ]; then
  sudo cp -f "$CUTOVER_MARKER_FILE" "$RUN_DIR/cutover_markers.log" 2>/dev/null || true
fi
if [ -n "${WORKLOAD_LOG_FILE:-}" ] && [ "$WORKLOAD_LOG_FILE" != "/dev/null" ] && [ -f "$WORKLOAD_LOG_FILE" ]; then
  sudo cp -f "$WORKLOAD_LOG_FILE" "$RUN_DIR/workload.log" 2>/dev/null || true
fi

log "================================================================"
log "Migration completed successfully!"
log "  Duration: ${MIGRATION_TIME_MS}ms ($(( MIGRATION_TIME_MS / 1000 ))s)"
log "  Source Memory:   $MEM"
log "  Replica Memory:  $REPLICA_MEM"
log "----------------------------------------------------------------"
log "  CRIU Timing (dump):"
log "    dump_one_task TOTAL:   ${DUMP_TOTAL}s"
log "    ${PARSE_MAPPINGS_LABEL}:           ${PARSE_MAPPINGS}s"
if [ -n "${COW_INIT:-}" ]; then
  log "    cow_dump_init:         ${COW_INIT}s"
fi
if [ -n "${COW_WRITEPROTECT:-}" ]; then
  log "    cow_writeprotect:      ${COW_WRITEPROTECT}s"
fi
log "    dump_pages_seized:     ${DUMP_PAGES}s"
log "    generate_vma_iovs:     ${GEN_IOVS}s"
if [ -n "${FREEZING_US:-}" ] || [ -n "${FROZEN_US:-}" ]; then
  log "    freezing_time:         ${FREEZING_US:-?}us"
  log "    frozen_time:           ${FROZEN_US:-?}us"
fi
if [ -n "${MEMDUMP_US:-}" ] || [ -n "${MEMWRITE_US:-}" ]; then
  log "    memdump_time:          ${MEMDUMP_US:-?}us"
  log "    memwrite_time:         ${MEMWRITE_US:-?}us"
fi
if [ -n "${PAGES_SCANNED:-}" ] || [ -n "${PAGES_WRITTEN:-}" ] || [ -n "${PAGES_LAZY:-}" ]; then
  log "    pages_scanned:         ${PAGES_SCANNED:-?}"
  log "    pages_written:         ${PAGES_WRITTEN:-?}"
  log "    pages_lazy:            ${PAGES_LAZY:-?}"
fi
if [ -n "${RESTORE_US:-}" ] || [ -n "${RESTORED_PAGES:-}" ]; then
  log "----------------------------------------------------------------"
  log "  CRIU Timing (restore):"
  log "    restore_time:          ${RESTORE_US:-?}us"
  log "    pages_restored:        ${RESTORED_PAGES:-?}"
fi
if [ -n "${SOURCE_PING_SUMMARY:-}" ]; then
  log "----------------------------------------------------------------"
  log "  Source availability (Valkey PING):"
  log "    $SOURCE_PING_SUMMARY"
fi
if [ -f "$RUN_DIR/source_markers.log" ] && [ -f "$RUN_DIR/source-ping.log" ]; then
  log "----------------------------------------------------------------"
  log "  Source availability by phase (from artifacts markers):"
  python3 "$SCRIPT_DIR/analyze_phase_latency.py" "$RUN_DIR" 2>/dev/null | sed 's/^/    /' || true
fi
# Replica health check
REPLICA_STATS=$($SSH ubuntu@$REPLICA_SSH_HOST "
  # Disk
  DISK_USED=\$(df --output=pcent / 2>/dev/null | tail -1 | tr -d ' %')
  DISK_AVAIL=\$(df -h --output=avail / 2>/dev/null | tail -1 | tr -d ' ')
  # Memory
  MEM=\$(free -m 2>/dev/null | awk '/^Mem:/{printf \"%dM/%dM (%d%%)\", \$3, \$2, \$3*100/\$2}')
  # Valkey
  VPID=\$(pgrep -x valkey-server 2>/dev/null | head -1)
  if [ -n \"\$VPID\" ]; then
    VSTATE=\$(cat /proc/\$VPID/status 2>/dev/null | awk '/^State:/{print \$2}')
    VRSS=\$(awk '{printf \"%.1fG\", \$2*4/1024/1024}' /proc/\$VPID/statm 2>/dev/null)
    echo \"valkey=\$VSTATE(rss=\$VRSS) mem=\$MEM disk=\${DISK_USED}%(\${DISK_AVAIL}free)\"
  else
    echo \"valkey=NOT_RUNNING mem=\$MEM disk=\${DISK_USED}%(\${DISK_AVAIL}free)\"
  fi
" 2>/dev/null || echo "unreachable")
log "----------------------------------------------------------------"
log "  Replica health: $REPLICA_STATS"
log "  Artifacts: $RUN_DIR"
log "================================================================"
