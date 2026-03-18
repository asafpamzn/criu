#!/bin/bash
# Analyze COW_TRACE logs to debug EEXIST errors
# Usage: ./analyze-cow-trace.sh <lazy-primary.log> <lazy-server.log>

PRIMARY_LOG="${1:-lazy-primary.log}"
SERVER_LOG="${2:-lazy-server.log}"

echo "=== COW TRACE Analysis ==="
echo "Primary log: $PRIMARY_LOG"
echo "Server log:  $SERVER_LOG"
echo ""

# Check files exist
if [[ ! -f "$PRIMARY_LOG" ]]; then
    echo "ERROR: Primary log not found: $PRIMARY_LOG"
    exit 1
fi

echo "=== 1. SUMMARY COUNTS ==="
echo ""

echo "--- Primary Log ---"
echo "Pages added to buffer:     $(grep -c 'COW_TRACE ADD:' "$PRIMARY_LOG" 2>/dev/null || echo 0)"
echo "PF lookups (found):        $(grep 'COW_TRACE PF_LOOKUP:' "$PRIMARY_LOG" 2>/dev/null | grep -c 'found=YES')"
echo "PF lookups (not found):    $(grep 'COW_TRACE PF_LOOKUP:' "$PRIMARY_LOG" 2>/dev/null | grep -c 'found=NO')"
echo "PF copy EEXIST:            $(grep 'COW_TRACE PF_COPY:' "$PRIMARY_LOG" 2>/dev/null | grep -c 'EEXIST')"
echo "Drain removes:             $(grep -c 'COW_TRACE DRAIN_REMOVE:' "$PRIMARY_LOG" 2>/dev/null || echo 0)"
echo "Drain copy EEXIST:         $(grep 'COW_TRACE DRAIN_COPY:' "$PRIMARY_LOG" 2>/dev/null | grep -c 'EEXIST')"
echo "Drain copy ENOENT:         $(grep 'COW_TRACE DRAIN_COPY:' "$PRIMARY_LOG" 2>/dev/null | grep -c 'ENOENT')"
echo "Unmap events:              $(grep -c 'COW_TRACE UNMAP:' "$PRIMARY_LOG" 2>/dev/null || echo 0)"
echo "G_BUFFER drains:           $(grep -c 'COW_TRACE G_BUFFER_DRAIN:' "$PRIMARY_LOG" 2>/dev/null || echo 0)"
echo ""

if [[ -f "$SERVER_LOG" ]]; then
    echo "--- Server Log ---"
    echo "Pages added to buffer:     $(grep -c 'COW_TRACE ADD:' "$SERVER_LOG" 2>/dev/null || echo 0)"
    echo "Drain removes:             $(grep -c 'COW_TRACE DRAIN_REMOVE:' "$SERVER_LOG" 2>/dev/null || echo 0)"
    echo "Drain copy EEXIST:         $(grep 'COW_TRACE DRAIN_COPY:' "$SERVER_LOG" 2>/dev/null | grep -c 'EEXIST')"
    echo ""
fi

echo "=== 2. G_BUFFER STATUS (potential dual-buffer conflict) ==="
grep 'COW_TRACE G_BUFFER_DRAIN:' "$PRIMARY_LOG" 2>/dev/null || echo "(none)"
echo ""

echo "=== 3. UNMAP EVENTS (pages unmapped by app) ==="
grep 'COW_TRACE UNMAP:' "$PRIMARY_LOG" 2>/dev/null | head -20
UNMAP_COUNT=$(grep -c 'COW_TRACE UNMAP:' "$PRIMARY_LOG" 2>/dev/null || echo 0)
if [[ $UNMAP_COUNT -gt 20 ]]; then
    echo "... ($UNMAP_COUNT total, showing first 20)"
fi
echo ""

echo "=== 4. SAMPLE EEXIST ADDRESSES (first 20) ==="
grep 'EEXIST' "$PRIMARY_LOG" 2>/dev/null | grep -oE '0x[0-9a-fA-F]+' | head -20
echo ""

echo "=== 5. CROSS-REFERENCE: EEXIST vs UNMAP ranges ==="
# Extract EEXIST addresses and UNMAP ranges, check overlap
EEXIST_ADDRS=$(grep 'EEXIST' "$PRIMARY_LOG" 2>/dev/null | grep -oE '0x[0-9a-fA-F]+' | sort -u)
UNMAP_RANGES=$(grep 'COW_TRACE UNMAP:' "$PRIMARY_LOG" 2>/dev/null | grep -oE '[0-9a-fA-F]+-[0-9a-fA-F]+')

if [[ -n "$EEXIST_ADDRS" && -n "$UNMAP_RANGES" ]]; then
    echo "Checking if EEXIST addresses fall within UNMAP ranges..."
    OVERLAP_COUNT=0
    for range in $UNMAP_RANGES; do
        START=$(echo "$range" | cut -d'-' -f1)
        END=$(echo "$range" | cut -d'-' -f2)
        START_DEC=$((16#$START))
        END_DEC=$((16#$END))
        for addr in $EEXIST_ADDRS; do
            ADDR_DEC=$((16#${addr#0x}))
            if [[ $ADDR_DEC -ge $START_DEC && $ADDR_DEC -lt $END_DEC ]]; then
                OVERLAP_COUNT=$((OVERLAP_COUNT + 1))
                if [[ $OVERLAP_COUNT -le 10 ]]; then
                    echo "  OVERLAP: $addr in range $range"
                fi
            fi
        done
    done
    echo "Total EEXIST addresses in UNMAP ranges: $OVERLAP_COUNT"
else
    echo "(No data to cross-reference)"
fi
echo ""

echo "=== 6. TRACE SPECIFIC PAGE (set PAGE_ADDR env var) ==="
if [[ -n "$PAGE_ADDR" ]]; then
    echo "Tracing page: $PAGE_ADDR"
    echo "--- Timeline ---"
    grep "$PAGE_ADDR" "$PRIMARY_LOG" 2>/dev/null | head -20
else
    echo "Set PAGE_ADDR=0x... to trace a specific page"
    echo "Example: PAGE_ADDR=0x7f1234000 ./analyze-cow-trace.sh lazy-primary.log"
fi
echo ""

echo "=== 7. RACE DETECTION: Pages removed then faulted ==="
# Look for pattern: DRAIN_REMOVE followed by PF_LOOKUP found=NO for same address
echo "Checking for drain-then-fault races (first 10)..."
DRAIN_ADDRS=$(grep 'COW_TRACE DRAIN_REMOVE:' "$PRIMARY_LOG" 2>/dev/null | grep -oE '0x[0-9a-fA-F]+' | sort -u | head -1000)
PF_MISS_ADDRS=$(grep 'PF_LOOKUP:.*found=NO' "$PRIMARY_LOG" 2>/dev/null | grep -oE '0x[0-9a-fA-F]+' | sort -u)
RACE_COUNT=0
for addr in $DRAIN_ADDRS; do
    if echo "$PF_MISS_ADDRS" | grep -q "^$addr$"; then
        RACE_COUNT=$((RACE_COUNT + 1))
        if [[ $RACE_COUNT -le 10 ]]; then
            echo "  POTENTIAL RACE: $addr (drained, then PF miss)"
        fi
    fi
done
echo "Total potential races: $RACE_COUNT"
echo ""

echo "=== Analysis Complete ==="
