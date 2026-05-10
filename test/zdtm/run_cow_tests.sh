#!/bin/bash
#
# One-stop runner for the COW system tests.
#
# Usage:
#   sudo ./test/zdtm/run_cow_tests.sh                # all tests, 1 run each
#   sudo ./test/zdtm/run_cow_tests.sh -n 10          # all tests, 10 runs each
#   sudo ./test/zdtm/run_cow_tests.sh cow_dump_grow  # just one test, 1 run
#   sudo ./test/zdtm/run_cow_tests.sh -n 5 cow_dump_basic cow_dump_grow
#
# Env overrides (only needed for very large workloads):
#   SEND_AND_WAIT_TIMEOUT=900         # default 600
#   OUTFILE_TIMEOUT_MS=1200000        # default 300000
#
# Exit 0 iff every run passed. On any failure, /tmp/cow_localpair_<test>/
# and /tmp/cow_localpair_<test>.run.log are preserved for post-mortem.

set -u

RUNS=1
TESTS_ALL=(
	cow_dump_basic
	cow_dump_mmap_munmap
	cow_dump_write_storm
	cow_dump_large_memory
	cow_dump_multi_thread
	cow_dump_file_backed
	cow_dump_new_vma
	cow_dump_grow
	cow_dump_munmap_half
)

usage() { sed -n '3,17p' "$0"; }

while [ $# -gt 0 ]; do
	case "$1" in
		-n|--runs) RUNS="$2"; shift 2 ;;
		-h|--help) usage; exit 0 ;;
		--)        shift; break ;;
		-*)        usage; exit 2 ;;
		*)         break ;;
	esac
done
TESTS=("$@")
[ ${#TESTS[@]} -eq 0 ] && TESTS=("${TESTS_ALL[@]}")

[ "$(id -u)" -eq 0 ] || { echo "ERROR: must run as root (sudo $0 ...)" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HARNESS="$REPO_ROOT/test/zdtm/cow_dump_localpair.sh"
[ -x "$HARNESS" ] || { echo "ERROR: harness not found: $HARNESS" >&2; exit 1; }

# userfaultfd knob (idempotent)
echo 1 > /proc/sys/vm/unprivileged_userfaultfd 2>/dev/null || true

cleanup_between_runs() {
	pkill -9 criu 2>/dev/null || true
	pkill -9 worker.sh 2>/dev/null || true
	local t
	for t in "${TESTS_ALL[@]}"; do
		pkill -9 "$t" 2>/dev/null || true
	done
	sleep 1
	ip netns del cow_primary 2>/dev/null || true
	ip netns del cow_replica 2>/dev/null || true
	ip link del vcow_p 2>/dev/null || true
}
trap cleanup_between_runs EXIT

export SEND_AND_WAIT_TIMEOUT="${SEND_AND_WAIT_TIMEOUT:-600}"
export OUTFILE_TIMEOUT_MS="${OUTFILE_TIMEOUT_MS:-900000}"

echo "=== Running ${#TESTS[@]} test(s), $RUNS run(s) each ==="
printf '%-30s %6s  %s\n' "TEST" "RESULT" "DETAILS"
echo "--------------------------------------------------------------"

declare -A PASS FAIL AVG
total_pass=0
total_runs=0

for t in "${TESTS[@]}"; do
	bin="$REPO_ROOT/test/zdtm/static/$t"
	if [ ! -x "$bin" ]; then
		printf '%-30s %6s  build first: sudo make -C test/zdtm/static %s\n' \
		       "$t" "SKIP" "$t"
		PASS[$t]=0; FAIL[$t]=0; AVG[$t]=0
		continue
	fi

	pass=0; fail=0; total=0; last_reason=""
	for i in $(seq 1 "$RUNS"); do
		cleanup_between_runs
		rm -rf "/tmp/cow_localpair_$t"
		runlog="/tmp/cow_localpair_${t}.run.log"
		start=$(date +%s)
		timeout 600 "$HARNESS" "$t" >"$runlog" 2>&1
		rc=$?
		total=$(( total + $(date +%s) - start ))
		if [ "$rc" -eq 0 ]; then
			pass=$(( pass + 1 ))
		else
			fail=$(( fail + 1 ))
			last_reason=$(grep -E 'BUG at|crashed with signal|FAIL:|dump rc=' \
			              "$runlog" 2>/dev/null | head -1 | sed 's/^[[:space:]]*//')
		fi
	done
	avg=$(( total / RUNS ))
	PASS[$t]=$pass; FAIL[$t]=$fail; AVG[$t]=$avg
	total_pass=$(( total_pass + pass ))
	total_runs=$(( total_runs + pass + fail ))
	if [ "$fail" -eq 0 ]; then
		printf '%-30s %6s  %d/%d runs, avg %ds\n' \
		       "$t" "PASS" "$pass" "$RUNS" "$avg"
	else
		printf '%-30s %6s  %d/%d runs, avg %ds — %.100s\n' \
		       "$t" "FAIL" "$pass" "$RUNS" "$avg" "$last_reason"
		echo "    logs: /tmp/cow_localpair_${t}/  and  /tmp/cow_localpair_${t}.run.log"
	fi
done

echo "--------------------------------------------------------------"
echo "TOTAL: $total_pass / $total_runs passed"
[ "$total_pass" -eq "$total_runs" ]
