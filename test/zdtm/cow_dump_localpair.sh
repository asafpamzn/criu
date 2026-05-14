#!/bin/bash
#
# Two-netns harness for --cow-dump ZDTM tests on a single machine.
#
# Sets up two network namespaces + two independent pid-ns shells (primary
# and replica), so --cow-dump's two-endpoint + PID-reuse model can be
# exercised without a second machine.
#
# Usage: sudo $0 <test-binary-name>
#
# Approach: each role runs as a long-lived bash inside
# `unshare --net=NS --pid --mount --fork`. That bash mounts a fresh /proc
# inside its own mount ns, then idles waiting for commands on an IPC fifo
# in the shared images dir. The host driver script sends command lines
# to each role via the fifo and reads exit status via a second fifo.
#
# This avoids nsenter-into-existing-pidns entirely — every criu/test
# child is forked by the in-pidns bash, so it naturally inherits the
# pidns-local PID view + the mounted /proc.

set -u

TEST_NAME="${1:?usage: $0 <test_name>}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CRIU_BIN="$REPO_ROOT/criu/criu"
TEST_BIN="$REPO_ROOT/test/zdtm/static/$TEST_NAME"
IMAGES_DIR="/tmp/cow_localpair_$TEST_NAME"
PRIMARY_NS="cow_primary"
REPLICA_NS="cow_replica"
PRIMARY_IP="10.200.0.1"
REPLICA_IP="10.200.0.2"
PRIMARY_VETH="vcow_p"
REPLICA_VETH="vcow_r"
PORT=27123

die() { echo "FAIL: $TEST_NAME: $*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "must run as root"
[ -x "$CRIU_BIN" ] || die "criu binary not found at $CRIU_BIN"
[ -x "$TEST_BIN" ] || die "test binary not found at $TEST_BIN"

PRIMARY_WORKER=""
REPLICA_WORKER=""

cleanup() {
	for p in "$PRIMARY_WORKER" "$REPLICA_WORKER"; do
		[ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
	done
	ip netns pids "$PRIMARY_NS" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
	ip netns pids "$REPLICA_NS" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
	ip netns del "$PRIMARY_NS" 2>/dev/null || true
	ip netns del "$REPLICA_NS" 2>/dev/null || true
	ip link del "$PRIMARY_VETH" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# --- Fresh state ---
rm -rf "$IMAGES_DIR"
mkdir -p "$IMAGES_DIR"
chmod 777 "$IMAGES_DIR"
ip netns del "$PRIMARY_NS" 2>/dev/null || true
ip netns del "$REPLICA_NS" 2>/dev/null || true
ip link del "$PRIMARY_VETH" 2>/dev/null || true

# --- netns + veth ---
ip netns add "$PRIMARY_NS"
ip netns add "$REPLICA_NS"
ip link add "$PRIMARY_VETH" type veth peer name "$REPLICA_VETH"
ip link set "$PRIMARY_VETH" netns "$PRIMARY_NS"
ip link set "$REPLICA_VETH" netns "$REPLICA_NS"
ip -n "$PRIMARY_NS" addr add "$PRIMARY_IP/24" dev "$PRIMARY_VETH"
ip -n "$REPLICA_NS" addr add "$REPLICA_IP/24" dev "$REPLICA_VETH"
ip -n "$PRIMARY_NS" link set "$PRIMARY_VETH" up
ip -n "$REPLICA_NS" link set "$REPLICA_VETH" up
ip -n "$PRIMARY_NS" link set lo up
ip -n "$REPLICA_NS" link set lo up
ip netns exec "$PRIMARY_NS" ping -c1 -W1 "$REPLICA_IP" >/dev/null 2>&1 \
	|| die "primary cannot reach replica"
echo "=== Setup OK: primary=$PRIMARY_IP replica=$REPLICA_IP images=$IMAGES_DIR ==="

# --- Start role workers ---
# Each worker runs one bash script inside its netns + fresh pidns+mntns
# with /proc mounted. The worker reads commands from CMD_FIFO and writes
# exit status + stdout line-tagged to OUT_LOG.
mkfifo "$IMAGES_DIR/cmd_primary.fifo"
mkfifo "$IMAGES_DIR/cmd_replica.fifo"
cat > "$IMAGES_DIR/worker.sh" <<'WORKER_EOF'
#!/bin/bash
# $1 = role name
# $2 = cmd fifo
role="$1"; cmd_fifo="$2"
mount -t proc proc /proc 2>/dev/null  # safe to ignore if already mounted
while IFS= read -r line < "$cmd_fifo"; do
	if [ "$line" = "QUIT" ]; then break; fi
	# Each line is a shell command to eval. We run it in a subshell so
	# failures don't kill the worker. Output goes to stdout as-is.
	( eval "$line" )
	echo "WORKER_DONE $role rc=$?"
done
WORKER_EOF
chmod +x "$IMAGES_DIR/worker.sh"

ip netns exec "$PRIMARY_NS" unshare --pid --mount --fork -- \
	"$IMAGES_DIR/worker.sh" primary "$IMAGES_DIR/cmd_primary.fifo" \
	>"$IMAGES_DIR/primary.out" 2>&1 &
PRIMARY_WORKER=$!
ip netns exec "$REPLICA_NS" unshare --pid --mount --fork -- \
	"$IMAGES_DIR/worker.sh" replica "$IMAGES_DIR/cmd_replica.fifo" \
	>"$IMAGES_DIR/replica.out" 2>&1 &
REPLICA_WORKER=$!
sleep 0.5

# Helper: send a shell command to a worker, wait for a NEW WORKER_DONE
# line (one more than existed before the send), return rc in $WORKER_RC.
send_and_wait() {
	local role="$1"; shift
	local cmd="$*"
	local fifo="$IMAGES_DIR/cmd_${role}.fifo"
	local outf="$IMAGES_DIR/${role}.out"
	local marker="^WORKER_DONE $role rc="
	local before_count now_count
	before_count=$(grep -c "$marker" "$outf" 2>/dev/null)
	[ -z "$before_count" ] && before_count=0
	# Send command
	echo "$cmd" > "$fifo"
	local deadline=$(( $(date +%s) + ${SEND_AND_WAIT_TIMEOUT:-900} ))
	while :; do
		now_count=$(grep -c "$marker" "$outf" 2>/dev/null)
		[ -z "$now_count" ] && now_count=0
		if [ "$now_count" -gt "$before_count" ]; then
			# Grab the last matching line for rc
			local last
			last=$(grep "$marker" "$outf" | tail -n1)
			WORKER_RC="${last##*rc=}"
			return 0
		fi
		if [ "$(date +%s)" -ge "$deadline" ]; then
			echo "=== TIMEOUT waiting for $role to finish: $cmd ==="
			return 1
		fi
		sleep 0.1
	done
}

# Sanity: verify each worker has pidns-local /proc (pid 1 should be
# the worker.sh process itself → comm 'bash' or 'worker.sh').
for role in primary replica; do
	send_and_wait "$role" "cat /proc/1/comm" || die "$role worker silent"
	# Check the stdout tail before WORKER_DONE
	comm=$(tail -n2 "$IMAGES_DIR/${role}.out" | head -n1)
	if [ "$comm" != "bash" ] && [ "$comm" != "worker.sh" ]; then
		die "$role pidns /proc/1/comm='$comm' (expected bash/worker.sh)"
	fi
done
echo "=== Workers ready (pidns /proc OK) ==="

# --- Start victim inside primary worker ---
PIDFILE="$IMAGES_DIR/$TEST_NAME.pid"
OUTFILE="$IMAGES_DIR/$TEST_NAME.out"
rm -f "$PIDFILE" "$OUTFILE" "$OUTFILE.inprogress"

# The test binary backgrounds itself in test_init(), so the foreground
# command returns when the daemon is ready. The daemon keeps running
# in the worker's pidns.
# Some tests (e.g. cow_dump_file_backed) declare a mandatory --test_dir
# via TEST_OPTION(). Detect it by grepping the binary's strings and
# pass --test_dir only when the test recognises it; ZDTM's parseargs
# rejects unknown options, so we can't pass it unconditionally.
VICTIM_EXTRA=""
if "$TEST_BIN" --help 2>&1 | grep -q -- "--test_dir"; then
	VICTIM_EXTRA="--test_dir='$IMAGES_DIR'"
fi
VICTIM_CMD="$TEST_BIN --pidfile='$PIDFILE' --outfile='$OUTFILE' $VICTIM_EXTRA 2>&1"
echo "=== dispatching victim cmd: $VICTIM_CMD ==="
send_and_wait primary "$VICTIM_CMD" \
	|| die "victim launch timed out"
[ "$WORKER_RC" -eq 0 ] || die "victim exited rc=$WORKER_RC"
# Small retry: the daemon parent writes the pidfile via an atomic
# rename just before exiting, and we've occasionally seen
# send_and_wait return before the rename lands.
for i in $(seq 1 30); do
	[ -s "$PIDFILE" ] && break
	sleep 0.1
done
[ -s "$PIDFILE" ] || { ls -la "$PIDFILE" 2>&1; die "no pidfile"; }
VPID=$(cat "$PIDFILE")
echo "=== Victim daemonized: pidns-pid=$VPID ==="

# --- Dump (background) ---
# Kick off dump in the primary worker. Because send_and_wait is
# synchronous we fire it into the background here.
DUMP_CMD="$CRIU_BIN dump --tree $VPID --images-dir '$IMAGES_DIR' \
	--cow-dump --lazy-pages \
	--address $PRIMARY_IP --port $PORT \
	--leave-running --shell-job \
	-v4 -o dump.log"
PRIMARY_LOG="$IMAGES_DIR/dump.log"
PRIMARY_DONE_COUNT_BEFORE=$(grep -c "^WORKER_DONE primary" "$IMAGES_DIR/primary.out" 2>/dev/null)
[ -z "$PRIMARY_DONE_COUNT_BEFORE" ] && PRIMARY_DONE_COUNT_BEFORE=0
echo "$DUMP_CMD" > "$IMAGES_DIR/cmd_primary.fifo"
echo "=== Dump command dispatched to primary ==="

# Wait for page server TCP port to be listening.
echo "=== Waiting for page server port $PORT ==="
READY=0
for i in $(seq 1 600); do
	if ip netns exec "$PRIMARY_NS" ss -tln | grep -q ":$PORT "; then
		echo "=== Page server port open after $((i*100))ms ==="
		READY=1
		break
	fi
	now=$(grep -c "^WORKER_DONE primary" "$IMAGES_DIR/primary.out" 2>/dev/null)
	[ -z "$now" ] && now=0
	if [ "$now" -gt "$PRIMARY_DONE_COUNT_BEFORE" ]; then
		echo "=== dump exited before port ready; log tail: ==="
		tail -40 "$PRIMARY_LOG" 2>/dev/null
		die "dump exited before port ready"
	fi
	sleep 0.1
done
[ "$READY" -eq 1 ] || die "timeout waiting for page server port"

# --- Start lazy-pages on replica ---
# lazy-pages connects to primary, receives skeleton files + pages over TCP,
# then auto-triggers restore via cow_start_restore(). Backgrounded inside
# the worker so WORKER_DONE fires immediately.
LAZY_CMD="$CRIU_BIN lazy-pages --images-dir '$IMAGES_DIR' \
	--page-server --cow-dump \
	--address $PRIMARY_IP --port $PORT \
	-v4 -o lazy-pages.log &"
send_and_wait replica "$LAZY_CMD" || die "lazy-pages dispatch timed out"
[ "$WORKER_RC" -eq 0 ] || die "lazy-pages dispatch rc=$WORKER_RC"
echo "=== Lazy-pages launched (auto-restores after receiving skeletons) ==="

# --- Wait for dump to finish ---
echo "=== Waiting for dump to complete ==="
for i in $(seq 1 ${SEND_AND_WAIT_TIMEOUT:-900}); do
	now=$(grep -c "^WORKER_DONE primary" "$IMAGES_DIR/primary.out" 2>/dev/null)
	[ -z "$now" ] && now=0
	if [ "$now" -gt "$PRIMARY_DONE_COUNT_BEFORE" ]; then
		rc_line=$(grep "^WORKER_DONE primary" "$IMAGES_DIR/primary.out" | tail -n1)
		RC_DUMP="${rc_line##*rc=}"
		echo "=== Dump finished rc=$RC_DUMP ==="
		[ "$RC_DUMP" -eq 0 ] || { tail -40 "$PRIMARY_LOG"; die "dump rc=$RC_DUMP"; }
		break
	fi
	sleep 1
done

# --- Start criu restore on replica ---
# lazy-pages is waiting for restore to connect via the lazy-pages socket.
# We launch criu restore in the replica worker (backgrounded) so it connects
# to the lazy-pages daemon and restores the process tree.
RESTORE_CMD="$CRIU_BIN restore --images-dir '$IMAGES_DIR' \
	--lazy-pages --cow-dump \
	--shell-job \
	-v4 -o restore.log &"
send_and_wait replica "$RESTORE_CMD" || die "restore dispatch timed out"
[ "$WORKER_RC" -eq 0 ] || die "restore dispatch rc=$WORKER_RC"
echo "=== Restore launched ==="

# --- Signal restored victim to run verification ---
# After restore, the victim is blocked in test_waitsig() waiting for SIGTERM.
# Retry signaling until process appears (restore may still be in progress).
echo "=== Signaling restored victim (pid $VPID) ==="
for i in $(seq 1 60); do
	send_and_wait replica "kill -0 $VPID 2>/dev/null"
	if [ "$WORKER_RC" -eq 0 ]; then
		send_and_wait replica "kill -TERM $VPID"
		echo "=== Victim signaled ==="
		break
	fi
	sleep 0.5
done

# --- Wait for outfile ---
# ZDTM test outfile format: "HH:MM:SS.mmm: PID: {PASS|FAIL: ...}"
# We match PASS/FAIL anywhere on a line. Tunable via OUTFILE_TIMEOUT
# (default 60s — needs raising for 10+ GB workloads whose verify loop
# reads every page).
OUTFILE_TIMEOUT_MS="${OUTFILE_TIMEOUT_MS:-1200000}"
OUTFILE_ITERS=$(( OUTFILE_TIMEOUT_MS / 100 ))
for i in $(seq 1 "$OUTFILE_ITERS"); do
	for candidate in "$OUTFILE" "$OUTFILE.inprogress"; do
		if [ -s "$candidate" ] && grep -qE "(PASS$|FAIL:)" "$candidate"; then
			RESULT="$candidate"; break 2
		fi
	done
	sleep 0.1
done
RESULT="${RESULT:-}"
if [ -z "$RESULT" ]; then
	ls -la "$OUTFILE" "$OUTFILE.inprogress" 2>&1 | head -5
	die "no PASS/FAIL in outfile"
fi

# If we matched on .inprogress but the test just finished and renamed
# it to .out between grep and cat, fall back to .out. Either file has
# the same content for the one line we care about.
if [ ! -s "$RESULT" ] && [ -s "$OUTFILE" ]; then
	RESULT="$OUTFILE"
fi

echo "=== Test output ==="
cat "$RESULT" 2>/dev/null
echo "=== end ==="
# Re-check against the (possibly final) result file, tolerating the rename.
if grep -qE "PASS$" "$RESULT" 2>/dev/null || \
   grep -qE "PASS$" "$OUTFILE" 2>/dev/null; then
	echo "PASS: $TEST_NAME"
	exit 0
fi
die "verification failed (see $IMAGES_DIR)"
