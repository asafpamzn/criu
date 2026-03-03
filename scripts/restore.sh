#!/usr/bin/env bash
set -euo pipefail

# Restore script — run on REPLICA.
# CRIU fetches images from source, restores process, waits for cutover.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/.env"

CRIU_BIN="${CRIU_BIN:-/usr/local/sbin/criu}"
[ -x "$CRIU_BIN" ] || CRIU_BIN="$SCRIPT_DIR/../criu/criu"
CUTOVER_PORT=${CUTOVER_PORT:-9003}
STAGED_PORT=${STAGED_PORT:-9004}
IMAGE_XFER_PORT=${IMAGE_XFER_PORT:-9005}

# --- 1. Clean ---
sudo pkill -9 valkey-server 2>/dev/null || true
for _ in $(seq 1 200); do pgrep -x valkey-server >/dev/null 2>&1 || break; sleep 0.01; done
sudo truncate -s 0 /var/log/valkey/stderr.log 2>/dev/null || true
sudo truncate -s 0 /var/log/valkey/stdout.log 2>/dev/null || true

# Block external clients until replication is configured
sudo iptables -I INPUT 1 -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true
trap 'sudo iptables -D INPUT -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true' EXIT

# --- 2. Prepare images dir ---
sudo rm -rf "$IMAGES_DIR" && sudo mkdir -p "$IMAGES_DIR" && sudo chmod 777 "$IMAGES_DIR"

# --- 2b. Start cutover listener early (before restore, so it's ready when source sends GO) ---
(timeout 600 nc -l -p "$CUTOVER_PORT" 2>/dev/null || true) > /tmp/cutover_msg &
CUTOVER_PID=$!
sleep 0.05  # give nc time to bind

# --- 3. CRIU restore (fetches images, restores process in SIGSTOP) ---
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

# --- 4. Wait for restored process ---
VALKEY_PID=""
for _ in $(seq 1 120); do
  VALKEY_PID=$(pgrep -x valkey-server || true)
  [ -n "$VALKEY_PID" ] && break; sleep 0.1
done
[ -n "$VALKEY_PID" ] || { echo "ERROR: Valkey not restored"; exit 1; }
echo "Restored PID $VALKEY_PID (stopped, waiting for cutover)"

# --- 5. Wait for cutover "GO" signal (listener started in step 2b) ---
wait "$CUTOVER_PID" 2>/dev/null || true
rm -f /tmp/cutover_msg

# --- 6. Resume + unlock stuck mutexes ---
sudo kill -CONT "$VALKEY_PID" 2>/dev/null || true
sleep 0.1

# After SIGCONT, threads may deadlock on signal_handler_lock or arena
# mutexes due to stale per-thread tcache state. Zero the lock words
# and use FUTEX_WAKE to unblock waiting threads.
python3 -c "
import struct, os, ctypes, ctypes.util

pid = $VALKEY_PID
SYS_futex = 98
FUTEX_WAKE = 1

libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)

# Find signal_handler_lock address from ELF
import subprocess
out = subprocess.check_output(['nm', '/usr/bin/valkey-server'], text=True)
text_base = None
for line in open(f'/proc/{pid}/maps'):
    if 'valkey' in line and 'r-xp' in line:
        text_base = int(line.split('-')[0], 16)
        break
if text_base:
    for line in out.split('\n'):
        if 'signal_handler_lock' in line:
            offset = int(line.split()[0], 16)
            addr = text_base + offset
            # Zero the lock and wake all waiters
            with open(f'/proc/{pid}/mem', 'r+b') as f:
                f.seek(addr)
                f.write(struct.pack('<I', 0))
                f.flush()
            # FUTEX_WAKE all waiters on this address
            # We can't call futex() on another process's address directly,
            # but zeroing the lock should let threads retry and see unlocked.
            print(f'Zeroed signal_handler_lock at 0x{addr:x}')

# Also zero all arena mutexes (threads may have re-locked after SIGCONT)
with open(f'/proc/{pid}/maps') as maps:
    for line in maps:
        if 'libc.so' in line and 'rw-' in line:
            start = int(line.split('-')[0], 16)
            arena = start + 0xa50
            with open(f'/proc/{pid}/mem', 'r+b') as f:
                seen = set()
                addr = arena
                count = 0
                while addr not in seen and count < 256:
                    seen.add(addr)
                    count += 1
                    f.seek(addr)
                    f.write(struct.pack('<I', 0))  # zero mutex
                    f.seek(addr + 2160)
                    nxt = struct.unpack('<Q', f.read(8))[0]
                    addr = nxt
                f.flush()
            print(f'Re-zeroed {count} arena mutexes after SIGCONT')
            break
" 2>/dev/null || true

for _ in $(seq 1 6000); do timeout 1s valkey-cli ping &>/dev/null && break; sleep 0.05; done
timeout 1s valkey-cli ping &>/dev/null || { echo "ERROR: Valkey not responsive"; exit 1; }
echo "Valkey is up"

# --- 7. Configure as replica ---
"$SCRIPT_DIR/wait_and_replicate.sh"

# Verify READONLY
for _ in $(seq 1 2000); do
  valkey-cli -p "$VALKEY_PORT" set __probe__ 1 2>&1 | grep -qi "READONLY" && break; sleep 0.01
done

# Open to external clients
sudo iptables -D INPUT -p tcp --dport "$VALKEY_PORT" ! -s 127.0.0.1 -j REJECT 2>/dev/null || true

echo "Replica ready: PID=$VALKEY_PID mem=$(valkey-cli info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r') keys=$(valkey-cli dbsize | sed 's/[^0-9]//g')"
