#!/bin/bash
#
# perf-freeze.sh - Profile CRIU COW dump during the Phase 3 freeze window only.
#
# Usage:
#   Shell A:  sudo ./perf-freeze.sh
#   Shell B:  run your criu dump as usual.
#
# The script tails /fsx/lazy/lazy-primary.log, starts `perf record` when the
# freeze-start marker appears, stops it on the freeze-end marker, and writes
# /tmp/criu-freeze.data. It then prints the next commands to inspect results.
#

set -u

PERF=/usr/lib/linux-tools/6.17.0-1012-aws/perf
LOG=/fsx/lazy/lazy-primary.log
OUT=/tmp/criu-freeze.data
PIDFILE=/tmp/criu-freeze-perf.pid

# Log-line patterns that bracket the freeze window.
# VERIFY these against a prior dump log before trusting the script:
#   sudo grep -nE 'freeze|cow_wait_p3|Phase 3' /fsx/lazy/lazy-primary.log | head -40
# Adjust the regexes below if the exact wording differs.
START_RE='Phase 3 freeze started|cow_wait_p3_threads starting|TIMING: cow_wait_p3_threads'
END_RE='cow_wait_p3_threads done|Phase 3 freeze done|P3 threads done|TIMING: P3 complete'

if [ "$(id -u)" -ne 0 ]; then
	echo "perf-freeze: must run as root (sudo $0)" >&2
	exit 1
fi

if [ ! -x "$PERF" ]; then
	echo "perf-freeze: $PERF not found or not executable" >&2
	exit 1
fi

if [ ! -r "$LOG" ]; then
	echo "perf-freeze: cannot read $LOG" >&2
	exit 1
fi

rm -f "$OUT" "$PIDFILE"

cleanup() {
	if [ -f "$PIDFILE" ]; then
		local pid
		pid=$(cat "$PIDFILE" 2>/dev/null || true)
		if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
			echo "perf-freeze: cleanup - stopping perf (pid $pid)" >&2
			kill -INT "$pid" 2>/dev/null
			wait "$pid" 2>/dev/null
		fi
		rm -f "$PIDFILE"
	fi
}
trap cleanup EXIT INT TERM

echo "perf-freeze: watching $LOG" >&2
echo "perf-freeze: start regex: $START_RE" >&2
echo "perf-freeze: end   regex: $END_RE" >&2
echo "perf-freeze: waiting for start marker..." >&2

# -n0: start from end of file (ignore history)
# -F : follow across rotation/truncation
sudo tail -n0 -F "$LOG" | while IFS= read -r line; do
	if [ ! -f "$PIDFILE" ]; then
		if echo "$line" | grep -qE "$START_RE"; then
			echo "perf-freeze: START matched: $line" >&2
			# -F 999  : ~1kHz sampling
			# -g      : call-graph
			# -a      : system-wide (criu has 20 P3 threads + kernel softirq work)
			"$PERF" record -F 999 -g -a -o "$OUT" >/dev/null 2>&1 &
			echo $! > "$PIDFILE"
			echo "perf-freeze: perf record started (pid $(cat "$PIDFILE"))" >&2
		fi
	else
		if echo "$line" | grep -qE "$END_RE"; then
			echo "perf-freeze: END matched: $line" >&2
			pid=$(cat "$PIDFILE")
			kill -INT "$pid" 2>/dev/null
			wait "$pid" 2>/dev/null
			rm -f "$PIDFILE"
			echo "perf-freeze: done. Output: $OUT" >&2
			echo "perf-freeze: next steps ->" >&2
			echo "  sudo $PERF report -i $OUT -g graph,0.5,caller --stdio | head -100" >&2
			echo "  # or flamegraph:" >&2
			echo "  sudo $PERF script -i $OUT | ~/FlameGraph/stackcollapse-perf.pl | ~/FlameGraph/flamegraph.pl > /tmp/freeze.svg" >&2
			break
		fi
	fi
done
