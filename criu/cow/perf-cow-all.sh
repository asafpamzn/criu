#!/bin/bash
#
# perf-cow-all.sh - Profile entire CRIU COW migration (dump or restore side).
#
# Usage:
#   PRIMARY (dump) side:
#     sudo ./perf-cow-all.sh -s dump -l /dev/shm/criu-migrate/lazy-primary.log
#
#   REPLICA (restore) side:
#     sudo ./perf-cow-all.sh -s restore -l /dev/shm/criu-migrate/lazy-server.log
#
#   Options:
#     -s <side>      Required: "dump" or "restore"
#     -l <logfile>   Required: path to CRIU log file
#     -d <seconds>   Fixed duration (ignore END marker)
#     -P             Per-pid mode (default: system-wide)
#     -o <outdir>    Output directory (default: /tmp/perf-cow)
#
# The script profiles the entire COW migration and generates:
#   - perf.data for detailed analysis
#   - Hardware counters (IPC, cache misses)
#   - FlameGraph SVG
#   - Thread balance analysis (for drain phase)
#

set -u

# Find perf binary
PERF=/usr/lib/linux-tools-6.8.0-117/perf
if [ ! -x "$PERF" ]; then
    for p in /usr/lib/linux-tools/*/perf /usr/bin/perf perf; do
        if [ -x "$p" ] 2>/dev/null; then
            PERF="$p"
            break
        fi
    done
fi

SIDE=""
LOG=""
OUTDIR=/tmp/perf-cow
FIXED_DURATION=0
PER_PID_MODE=0
MAX_SECONDS=300  # 5 minutes safety cap
CRIU_PROCNAME=criu

while getopts "s:l:d:Po:h" opt; do
    case "$opt" in
    s) SIDE=$OPTARG ;;
    l) LOG=$OPTARG ;;
    d) FIXED_DURATION=$OPTARG ;;
    P) PER_PID_MODE=1 ;;
    o) OUTDIR=$OPTARG ;;
    h)
        sed -n '2,22p' "$0"
        exit 0
        ;;
    esac
done

# Validate inputs
if [ -z "$SIDE" ]; then
    echo "perf-cow-all: -s <dump|restore> is required" >&2
    exit 1
fi
if [ "$SIDE" != "dump" ] && [ "$SIDE" != "restore" ]; then
    echo "perf-cow-all: -s must be 'dump' or 'restore'" >&2
    exit 1
fi
if [ -z "$LOG" ]; then
    echo "perf-cow-all: -l <logfile> is required" >&2
    exit 1
fi
if [ "$(id -u)" -ne 0 ]; then
    echo "perf-cow-all: must run as root (sudo $0)" >&2
    exit 1
fi
if [ -z "$PERF" ] || [ ! -x "$PERF" ]; then
    echo "perf-cow-all: perf not found or not executable" >&2
    exit 1
fi

# Set up markers based on side
if [ "$SIDE" = "dump" ]; then
    # Dump side: profile from bulk transfer start to completion
    START_RE='Starting P3 threads|cow_start_p3_threads|P3 thread .* started|Starting .* scanner threads'
    END_RE='Dump finished|cow_dump_finish|Phase 4 complete|TIMING: total dump'
else
    # Restore side: profile from connection to drain completion
    START_RE='P3 receiver|cow-lazy-pages.*started|Starting lazy-pages|REPLICA PHASE'
    END_RE='TIMING: drain took|Restore complete|lazy-pages completed|All pages drained'
fi

# Create output directory
mkdir -p "$OUTDIR"
OUT="$OUTDIR/perf-$SIDE.data"
STAT_OUT="$OUTDIR/perf-$SIDE-stat.txt"
THREAD_BALANCE="$OUTDIR/thread-balance.txt"
FLAMEGRAPH="$OUTDIR/flamegraph-$SIDE.svg"
PIDFILE="$OUTDIR/perf-$SIDE.pid"
SUMMARY="$OUTDIR/summary-$SIDE.txt"

rm -f "$OUT" "$STAT_OUT" "$THREAD_BALANCE" "$PIDFILE" "$SUMMARY"

PERF_PID=0
STAT_PID=0
TAIL_PID=0
START_EPOCH=0

cleanup() {
    if [ -f "$PIDFILE" ]; then
        local pid
        pid=$(cat "$PIDFILE" 2>/dev/null || true)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "perf-cow-all: cleanup - stopping perf (pid $pid)" >&2
            kill -INT "$pid" 2>/dev/null
            for _ in 1 2 3 4 5; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -KILL "$pid" 2>/dev/null || true
        fi
        rm -f "$PIDFILE"
    fi
    if [ "$STAT_PID" -gt 0 ] && kill -0 "$STAT_PID" 2>/dev/null; then
        kill -INT "$STAT_PID" 2>/dev/null || true
    fi
    if [ "$TAIL_PID" -gt 0 ] && kill -0 "$TAIL_PID" 2>/dev/null; then
        kill "$TAIL_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

resolve_criu_pids() {
    pgrep -d, -x "$CRIU_PROCNAME" 2>/dev/null || true
}

start_perf() {
    local pids
    pids=$(resolve_criu_pids)

    if [ -z "$pids" ]; then
        echo "perf-cow-all: WARNING - no '$CRIU_PROCNAME' process found yet, using system-wide" >&2
        PER_PID_MODE=0
    else
        echo "perf-cow-all: criu PIDs: $pids" >&2
        ps -o pid,ppid,cmd -p "$(echo "$pids" | tr , ' ')" >&2 || true
    fi

    if [ "$PER_PID_MODE" -eq 1 ] && [ -n "$pids" ]; then
        echo "perf-cow-all: mode: per-pid (-p $pids)" >&2
        "$PERF" record -F 999 -g -p "$pids" -o "$OUT" >/tmp/perf-cow-all.stderr 2>&1 &
    else
        echo "perf-cow-all: mode: system-wide (-a)" >&2
        "$PERF" record -F 999 -g -a -o "$OUT" >/tmp/perf-cow-all.stderr 2>&1 &
    fi
    PERF_PID=$!
    echo "$PERF_PID" > "$PIDFILE"
    echo "$pids" > "$OUTDIR/criu_pids.txt"
    echo "perf-cow-all: perf record started (pid $PERF_PID)" >&2

    # Also run perf stat for hardware counters
    if [ -n "$pids" ]; then
        "$PERF" stat -e cycles,instructions,cache-references,cache-misses,LLC-loads,LLC-load-misses,L1-dcache-loads,L1-dcache-load-misses -p "$pids" -o "$STAT_OUT" 2>/dev/null &
        STAT_PID=$!
        echo "perf-cow-all: perf stat started (pid $STAT_PID)" >&2
    fi
}

verify_criu_in_capture() {
    local expected=""
    [ -r "$OUTDIR/criu_pids.txt" ] && expected=$(cat "$OUTDIR/criu_pids.txt")

    echo "" >> "$SUMMARY"
    echo "=== Top processes in capture ===" >> "$SUMMARY"
    "$PERF" report -i "$OUT" --stdio -s comm,pid --no-children 2>/dev/null | \
        awk '/^[[:space:]]*[0-9]/{print} /^#/{next}' | head -15 >> "$SUMMARY" 2>&1 || true
}

analyze_thread_balance() {
    echo "" >> "$SUMMARY"
    echo "=== Drain thread balance ===" >> "$SUMMARY"

    grep -E 'Drain thread [0-9]+.*(drained|finished)' "$LOG" 2>/dev/null | \
        grep -oE 'Drain thread [0-9]+[^0-9]*(drained|finished)[^0-9]*[0-9]+' | \
        sed 's/Drain thread \([0-9]*\).*[^0-9]\([0-9]*\)$/\1 \2/' | \
        sort -t' ' -k1,1n | \
        awk '
        {
            thread[$1] = $2
        }
        END {
            if (length(thread) == 0) {
                print "No drain thread data found"
                exit
            }

            total = 0; min = -1; max = 0; count = 0
            for (t in thread) {
                v = thread[t]; total += v; count++
                if (min < 0 || v < min) min = v
                if (v > max) max = v
            }
            avg = total / count
            imbalance = (max > 0) ? ((max - min) / max * 100) : 0

            printf "Threads: %d, Total: %d pages, Avg: %.0f\n", count, total, avg
            printf "Min: %d, Max: %d, Imbalance: %.1f%%\n", min, max, imbalance

            for (t = 0; t < 50; t++) {
                if (t in thread) {
                    printf "  Thread %2d: %8d pages\n", t, thread[t]
                }
            }
        }
        ' >> "$SUMMARY" 2>&1
}

generate_flamegraph() {
    local stackcollapse=""
    local flamegraph=""

    # Try to find FlameGraph scripts
    for dir in ~/FlameGraph /opt/FlameGraph /usr/local/FlameGraph; do
        if [ -x "$dir/stackcollapse-perf.pl" ] && [ -x "$dir/flamegraph.pl" ]; then
            stackcollapse="$dir/stackcollapse-perf.pl"
            flamegraph="$dir/flamegraph.pl"
            break
        fi
    done

    if [ -n "$stackcollapse" ] && [ -n "$flamegraph" ]; then
        echo "perf-cow-all: generating flamegraph..." >&2
        "$PERF" script -i "$OUT" 2>/dev/null | "$stackcollapse" | "$flamegraph" > "$FLAMEGRAPH" 2>/dev/null
        if [ -s "$FLAMEGRAPH" ]; then
            echo "perf-cow-all: flamegraph: $FLAMEGRAPH" >&2
            echo "" >> "$SUMMARY"
            echo "FlameGraph: $FLAMEGRAPH" >> "$SUMMARY"
        fi
    else
        echo "perf-cow-all: FlameGraph not found, skipping SVG generation" >&2
        echo "  To generate manually:" >&2
        echo "  $PERF script -i $OUT | stackcollapse-perf.pl | flamegraph.pl > $FLAMEGRAPH" >&2
    fi
}

stop_perf() {
    local end_epoch
    end_epoch=$(date +%s)
    local duration=$((end_epoch - START_EPOCH))

    if [ "$PERF_PID" -gt 0 ] && kill -0 "$PERF_PID" 2>/dev/null; then
        kill -INT "$PERF_PID" 2>/dev/null
        wait "$PERF_PID" 2>/dev/null
    fi
    if [ "$STAT_PID" -gt 0 ] && kill -0 "$STAT_PID" 2>/dev/null; then
        kill -INT "$STAT_PID" 2>/dev/null
        wait "$STAT_PID" 2>/dev/null
    fi
    rm -f "$PIDFILE"

    echo "perf-cow-all: stopped after ${duration}s" >&2
    echo "perf-cow-all: output: $OUT" >&2

    # Build summary
    {
        echo "=== CRIU COW Profiling Summary ($SIDE side) ==="
        echo "Duration: ${duration}s"
        echo "Log: $LOG"
        echo "Perf data: $OUT"
        echo ""
    } > "$SUMMARY"

    if [ -s "$OUT" ]; then
        verify_criu_in_capture
    fi

    if [ -s "$STAT_OUT" ]; then
        echo "" >> "$SUMMARY"
        echo "=== Hardware Counters ===" >> "$SUMMARY"
        cat "$STAT_OUT" >> "$SUMMARY"
    fi

    if [ "$SIDE" = "restore" ]; then
        analyze_thread_balance
    fi

    generate_flamegraph

    # Print summary
    echo "" >&2
    echo "========================================" >&2
    cat "$SUMMARY" >&2
    echo "========================================" >&2
    echo "" >&2
    echo "Inspect with:" >&2
    echo "  sudo $PERF report -i $OUT -g graph,0.5,caller --stdio | head -100" >&2
    echo "  sudo $PERF report -i $OUT -g graph,0.5,caller" >&2
}

# ---- MAIN ----
echo "perf-cow-all: profiling $SIDE side" >&2
echo "perf-cow-all: log: $LOG" >&2
echo "perf-cow-all: output dir: $OUTDIR" >&2
echo "perf-cow-all: START regex: $START_RE" >&2
echo "perf-cow-all: END regex: $END_RE" >&2
echo "perf-cow-all: MAX_SECONDS: $MAX_SECONDS" >&2
[ "$FIXED_DURATION" -gt 0 ] && echo "perf-cow-all: FIXED_DURATION: ${FIXED_DURATION}s" >&2
echo "" >&2

# Wait for log file to exist
echo "perf-cow-all: waiting for log file $LOG..." >&2
while [ ! -r "$LOG" ]; do
    sleep 0.5
done
echo "perf-cow-all: log file ready, waiting for START marker..." >&2

# Start tailing the log
exec 3< <(exec tail -n0 -F "$LOG" 2>/dev/null)
TAIL_PID=$!

while :; do
    # If perf is running, enforce MAX_SECONDS cap
    if [ "$PERF_PID" -gt 0 ]; then
        now=$(date +%s)
        elapsed=$((now - START_EPOCH))
        if [ "$FIXED_DURATION" -gt 0 ] && [ "$elapsed" -ge "$FIXED_DURATION" ]; then
            echo "perf-cow-all: fixed ${FIXED_DURATION}s duration reached" >&2
            stop_perf
            break
        fi
        if [ "$elapsed" -ge "$MAX_SECONDS" ]; then
            echo "perf-cow-all: MAX_SECONDS ($MAX_SECONDS) reached - stopping" >&2
            stop_perf
            break
        fi
    fi

    # Read with 1s timeout
    if ! IFS= read -r -t 1 -u 3 line; then
        continue
    fi

    if [ "$PERF_PID" -eq 0 ]; then
        if echo "$line" | grep -qE "$START_RE"; then
            echo "perf-cow-all: START matched: $line" >&2
            START_EPOCH=$(date +%s)
            start_perf
        fi
    else
        if [ "$FIXED_DURATION" -eq 0 ] && echo "$line" | grep -qE "$END_RE"; then
            echo "perf-cow-all: END matched: $line" >&2
            stop_perf
            break
        fi
    fi
done

exec 3<&-
