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

# --- 6. Resume + unblock stuck mutexes via ptrace futex_wake ---
sudo kill -CONT "$VALKEY_PID" 2>/dev/null || true
sleep 0.2

# After SIGCONT, the main thread may be stuck in futex_wait on
# main_arena.mutex (stale tcache triggered malloc corruption path).
# Zero the mutex and inject futex(FUTEX_WAKE) via ptrace to unblock.
sudo python3 -c "
import struct, os, ctypes, ctypes.util, time

pid = $VALKEY_PID
libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
PTRACE_ATTACH = 16
PTRACE_DETACH = 17
SYS_futex = 98

# Find main_arena address
arena = None
with open(f'/proc/{pid}/maps') as maps:
    for line in maps:
        if 'libc.so' in line and 'rw-' in line:
            arena = int(line.split('-')[0], 16) + 0xa50
            break

if arena:
    # Zero main_arena lock+count+owner
    with open(f'/proc/{pid}/mem', 'r+b') as f:
        f.seek(arena)
        f.write(struct.pack('<III', 0, 0, 0))
        f.flush()

    # Ptrace attach to main thread, inject futex(FUTEX_WAKE)
    libc.ptrace(PTRACE_ATTACH, pid, 0, 0)
    os.waitpid(pid, 0)

    # Read registers
    import array
    regs = array.array('Q', [0]*34)  # user_regs_struct on aarch64
    class iovec(ctypes.Structure):
        _fields_ = [('iov_base', ctypes.c_void_p), ('iov_len', ctypes.c_size_t)]
    iov = iovec(regs.buffer_info()[0], regs.buffer_info()[1] * 8)
    libc.ptrace(0x4204, pid, 1, ctypes.byref(iov))  # PTRACE_GETREGSET, NT_PRSTATUS
    orig_regs = array.array('Q', regs)

    # Save original code at PC
    pc = regs[32]  # pc is at index 32 in user_regs_struct
    orig_code = (ctypes.c_char * 8)()
    libc.ptrace(0x4203, pid, 1, ctypes.byref(iov))  # re-read for safety

    # Inject SVC #0; BRK #0
    import mmap
    with open(f'/proc/{pid}/mem', 'r+b') as f:
        f.seek(pc)
        old_code = f.read(8)
        f.seek(pc)
        f.write(bytes([0x01, 0x00, 0x00, 0xd4, 0x00, 0x00, 0x20, 0xd4]))
        f.flush()

        # Set regs for futex(arena, FUTEX_WAKE, INT_MAX)
        regs[8] = SYS_futex  # x8 = syscall number
        regs[0] = arena       # x0 = uaddr
        regs[1] = 1           # x1 = FUTEX_WAKE
        regs[2] = 0x7fffffff  # x2 = INT_MAX
        regs[32] = pc         # pc
        iov = iovec(regs.buffer_info()[0], regs.buffer_info()[1] * 8)
        libc.ptrace(0x4205, pid, 1, ctypes.byref(iov))  # PTRACE_SETREGSET

        libc.ptrace(7, pid, 0, 0)  # PTRACE_CONT
        os.waitpid(pid, 0)  # wait for BRK (SIGTRAP)

        # Read result
        libc.ptrace(0x4204, pid, 1, ctypes.byref(iov))
        woken = regs[0]
        print(f'futex_wake({arena:#x}): woke {woken} threads')

        # Restore original code + regs
        f.seek(pc)
        f.write(old_code)
        f.flush()

    iov = iovec(orig_regs.buffer_info()[0], orig_regs.buffer_info()[1] * 8)
    libc.ptrace(0x4205, pid, 1, ctypes.byref(iov))
    libc.ptrace(PTRACE_DETACH, pid, 0, 0)
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
