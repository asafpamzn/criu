#!/usr/bin/env bash
set -euo pipefail

# Restore script — run on REPLICA.
# CRIU fetches images from source, restores process, receives pages,
# applies converge state, then waits for handoff GO signal.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-/usr/local/sbin/criu}"
[ -x "$CRIU_BIN" ] || CRIU_BIN="$SCRIPT_DIR/../criu/criu"
CUTOVER_PORT=${CUTOVER_PORT:-9003}
STAGED_PORT=${STAGED_PORT:-9004}
IMAGE_XFER_PORT=${IMAGE_XFER_PORT:-9005}

# ── 1. Clean ──
sudo pkill -9 valkey-server 2>/dev/null || true
for _ in $(seq 1 200); do
  pgrep -x valkey-server >/dev/null 2>&1 || break
  sleep 0.01
done
sudo truncate -s 0 /var/log/valkey/stderr.log 2>/dev/null || true
sudo truncate -s 0 /var/log/valkey/stdout.log 2>/dev/null || true

# Block external clients until restore is complete
sudo iptables -I INPUT 1 -p tcp --dport "$VALKEY_PORT" \
  ! -s 127.0.0.1 -j REJECT 2>/dev/null || true
trap 'sudo iptables -D INPUT -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true' EXIT

# ── 2. Prepare ──
sudo rm -rf "$IMAGES_DIR"
sudo mkdir -p "$IMAGES_DIR"
sudo chmod 777 "$IMAGES_DIR"

# Start handoff listener early (before restore)
(timeout 600 nc -l -p "$CUTOVER_PORT" 2>/dev/null || true) > /tmp/cutover_msg &
CUTOVER_PID=$!
sleep 0.05

# ── 3. CRIU restore ──
export PAGE_RECV_BIN="${PAGE_RECV_BIN:-$SCRIPT_DIR/../tools/page-recv}"
export PAGE_RECV_ADDR="$PRIMARY_IP"
export PAGE_RECV_PORT="$CRIU_PORT"
export PAGE_RECV_STAGED_ADDR="$PRIMARY_IP"
export PAGE_RECV_STAGED_PORT="$STAGED_PORT"

echo "Restoring (images from $PRIMARY_IP:$IMAGE_XFER_PORT)..."
sudo env \
  PAGE_RECV_BIN="$PAGE_RECV_BIN" \
  PAGE_RECV_ADDR="$PRIMARY_IP" \
  PAGE_RECV_PORT="$CRIU_PORT" \
  PAGE_RECV_STAGED_ADDR="$PRIMARY_IP" \
  PAGE_RECV_STAGED_PORT="$STAGED_PORT" \
  "$CRIU_BIN" restore \
    --images-dir "$IMAGES_DIR" \
    --fetch-images "$PRIMARY_IP:$IMAGE_XFER_PORT" \
    --lazy-pages --tcp-close --cow-dump \
    --restore-detached --leave-stopped \
    --skip-file-rwx-check --file-validation filesize \
    -v1 -o "$IMAGES_DIR/lazy-restore.log"

# ── 4. Wait for restored process ──
VALKEY_PID=""
for _ in $(seq 1 120); do
  VALKEY_PID=$(pgrep -x valkey-server || true)
  [ -n "$VALKEY_PID" ] && break
  sleep 0.1
done
[ -n "$VALKEY_PID" ] || { echo "ERROR: Valkey not restored"; exit 1; }
echo "Restored PID $VALKEY_PID (stopped, waiting for handoff)"

# ── 5. Wait for handoff GO ──
wait "$CUTOVER_PID" 2>/dev/null || true
rm -f /tmp/cutover_msg

# ── 6. Resume ──
sudo kill -CONT "$VALKEY_PID" 2>/dev/null || true

for _ in $(seq 1 6000); do
  timeout 1s valkey-cli ping &>/dev/null && break
  sleep 0.05
done
timeout 1s valkey-cli ping &>/dev/null || { echo "ERROR: Valkey not responsive"; exit 1; }
echo "Valkey is up"

# ── 7. Post-restore config ──
valkey-cli config set save "" >/dev/null 2>&1 || true
valkey-cli config set appendonly no >/dev/null 2>&1 || true
echo "Replica ready (standalone, data from CRIU migration)"

# Open to external clients
sudo iptables -D INPUT -p tcp --dport "$VALKEY_PORT" \
  ! -s 127.0.0.1 -j REJECT 2>/dev/null || true

MEM=$(valkey-cli info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
KEYS=$(valkey-cli dbsize | sed 's/[^0-9]//g')
echo "Replica ready: PID=$VALKEY_PID mem=$MEM keys=$KEYS"
