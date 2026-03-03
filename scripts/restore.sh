#!/usr/bin/env bash
set -euo pipefail

# Restore script — run on REPLICA machine.
# Receives CRIU images from source, restores process, waits for cutover.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-/usr/local/sbin/criu}"
[ -x "$CRIU_BIN" ] || CRIU_BIN="$SCRIPT_DIR/../criu/criu"
CUTOVER_PORT=${CUTOVER_PORT:-9003}
STAGED_PORT=${STAGED_PORT:-9004}
IMAGE_XFER_PORT=${IMAGE_XFER_PORT:-9005}
CUTOVER_MARKER_FILE=${CUTOVER_MARKER_FILE:-$IMAGES_DIR/cutover_markers.log}

mark() { printf "%s %s REPLICA\n" "$1" "$(date +%s%3N)" >>"$CUTOVER_MARKER_FILE" 2>/dev/null || true; }

# --- 1. Clean slate ---
sudo pkill -9 valkey-server 2>/dev/null || true
for _ in $(seq 1 200); do pgrep -x valkey-server >/dev/null 2>&1 || break; sleep 0.01; done
sudo truncate -s 0 /var/log/valkey/stderr.log 2>/dev/null || true
sudo truncate -s 0 /var/log/valkey/stdout.log 2>/dev/null || true

# Block external clients until replication is configured
sudo iptables -I INPUT 1 -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true
trap 'sudo iptables -D INPUT -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true' EXIT

# --- 2. Download CRIU images from source ---
sudo rm -rf "$IMAGES_DIR" && sudo mkdir -p "$IMAGES_DIR" && sudo chmod 777 "$IMAGES_DIR"
echo "Downloading images from $PRIMARY_IP:$IMAGE_XFER_PORT..."
python3 "$SCRIPT_DIR/image-client.py" "$PRIMARY_IP" "$IMAGE_XFER_PORT" "$IMAGES_DIR"

# --- 3. CRIU restore (process starts in SIGSTOP) ---
export PAGE_RECV_BIN="${PAGE_RECV_BIN:-$SCRIPT_DIR/../tools/page-recv}"
export PAGE_RECV_ADDR="$PRIMARY_IP"
export PAGE_RECV_PORT="$CRIU_PORT"

echo "Restoring..."
sudo env PAGE_RECV_BIN="$PAGE_RECV_BIN" PAGE_RECV_ADDR="$PRIMARY_IP" PAGE_RECV_PORT="$CRIU_PORT" \
  "$CRIU_BIN" restore \
    --images-dir "$IMAGES_DIR" \
    --lazy-pages \
    --tcp-close \
    --cow-dump \
    --restore-detached \
    --leave-stopped \
    --skip-file-rwx-check \
    --file-validation filesize \
    -v1 -o "$IMAGES_DIR/lazy-restore.log"

mark "REPLICA_CRIU_RESTORE_STARTED"

# --- 4. Wait for restored Valkey process ---
VALKEY_PID=""
for _ in $(seq 1 120); do
  VALKEY_PID=$(pgrep -x valkey-server || true)
  [ -n "$VALKEY_PID" ] && break
  sleep 0.1
done
[ -n "$VALKEY_PID" ] || { echo "ERROR: Valkey not restored"; exit 1; }
echo "Restored PID $VALKEY_PID (stopped)"
mark "REPLICA_PAGES_INSTALLED"

# --- 5. Signal staged, wait for cutover ---
(timeout 600 nc -l -p "$CUTOVER_PORT" 2>/dev/null || true) > /tmp/cutover_msg &
CUTOVER_PID=$!
sleep 0.05

# Tell source we're ready for cutover (retry until source listener is up)
for _ in $(seq 1 100); do
  (echo "STAGED" > /dev/tcp/"$PRIMARY_IP"/"$STAGED_PORT") 2>/dev/null && break
  sleep 0.1
done
mark "REPLICA_STAGED_FOR_CUTOVER"

# Wait for "GO" from source
wait "$CUTOVER_PID" 2>/dev/null || true
rm -f /tmp/cutover_msg

# --- 6. Resume Valkey ---
sudo kill -CONT "$VALKEY_PID" 2>/dev/null || true

for _ in $(seq 1 6000); do
  timeout 1s valkey-cli ping &>/dev/null && break
  sleep 0.05
done
timeout 1s valkey-cli ping &>/dev/null || { echo "ERROR: Valkey not responsive"; exit 1; }
echo "Valkey is up"
mark "REPLICA_VALKEY_PING_READY"

# --- 7. Configure as replica ---
"$SCRIPT_DIR/wait_and_replicate.sh"
mark "REPLICA_REPLICATE_TASK_DONE"

# Verify READONLY
for _ in $(seq 1 2000); do
  valkey-cli -p "$VALKEY_PORT" set __probe__ 1 2>&1 | grep -qi "READONLY" && break
  sleep 0.01
done
mark "REPLICA_WRITE_GUARD_OK"

# Open to external clients
sudo iptables -D INPUT -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true
mark "REPLICA_GATE_REMOVED"

echo "================================================================"
echo "Migration complete on replica"
echo "  PID:    $VALKEY_PID"
echo "  Memory: $(valkey-cli info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')"
echo "  Keys:   $(valkey-cli dbsize | sed 's/[^0-9]//g')"
echo "================================================================"
mark "REPLICA_RESTORE_SCRIPT_DONE"
