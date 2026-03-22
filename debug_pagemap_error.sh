#!/bin/bash
# Debug script for COW phased migration issues
# Usage: ./debug_pagemap_error.sh <lazy-primary.log> <lazy-server.log> [failing_address]

PRIMARY_LOG="${1:-lazy-primary.log}"
SERVER_LOG="${2:-lazy-server.log}"
FAIL_ADDR="${3:-}"

echo "=============================================="
echo "DEBUG: COW Phased Migration Error Analysis"
echo "=============================================="
echo "Primary log: $PRIMARY_LOG"
echo "Server log: $SERVER_LOG"
echo ""

# ============================================
# SECTION A: UNPROTECT FAILURES (Primary side)
# ============================================
echo "========================================================"
echo "=== A. UNPROTECT FAILURES (Primary) ==="
echo "========================================================"
echo ""

echo "=== A1. Failed to unprotect errors ==="
grep -E "Failed to unprotect|No such file or directory" "$PRIMARY_LOG" 2>/dev/null | head -20 || echo "No unprotect failures found"
echo ""

echo "=== A2. Page requests that failed ==="
grep -B2 "Failed to unprotect" "$PRIMARY_LOG" 2>/dev/null | grep -E "SEND_PAGE|vaddr=" | head -20 || echo "No context found"
echo ""

echo "=== A3. New VMAs on primary (check if failing addr is in range) ==="
grep -E "Added lazy VMA for new region" "$PRIMARY_LOG" 2>/dev/null | head -10 || echo "No new VMAs"
echo ""

echo "=== A4. COW uffd registrations ==="
grep -E "uffd.*register|WP.*protect" "$PRIMARY_LOG" 2>/dev/null | head -20 || echo "No uffd registration logs"
echo ""

echo "=== A5. New VMA WP_SYNC registration ==="
grep -E "Registering new VMA.*WP_SYNC" "$PRIMARY_LOG" 2>/dev/null | head -10 || echo "No new VMA WP_SYNC registrations"
echo ""

echo "=== A6. All tracked VMAs ==="
grep -E "tracked VMA|nr_tracked_vmas" "$PRIMARY_LOG" 2>/dev/null | tail -20 || echo "No tracked VMA info"
echo ""

# ============================================
# SECTION B: PAGE STATE TRACKER ISSUES (Replica)
# ============================================
echo "========================================================"
echo "=== B. PAGE STATE TRACKER ISSUES (Replica) ==="
echo "========================================================"
echo ""

echo "=== B1. Illegal transitions ==="
grep -E "ILLEGAL_TRANSITION" "$SERVER_LOG" 2>/dev/null | head -20 || echo "No illegal transitions"
echo ""

echo "=== B2. Page history for illegal transitions ==="
grep -B5 "ILLEGAL_TRANSITION" "$SERVER_LOG" 2>/dev/null | head -40 || echo "No history found"
echo ""

echo "=== B3. DIRTY pages that got DISCARDED ==="
grep -E "DIRTY.*DISCARDED|DISCARDED.*DIRTY" "$SERVER_LOG" 2>/dev/null | head -10 || echo "None found"
echo ""

echo "=== B4. Convergence copy failures ==="
grep -E "convergence copy failed|Direct convergence" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No convergence failures"
echo ""

echo "=== B5. Final tracker stats ==="
grep -A20 "PAGE STATE TRACKER STATS" "$SERVER_LOG" 2>/dev/null | tail -25 || echo "No stats found"
echo ""

# ============================================
# SECTION C: IOV AND PAGEMAP ISSUES
# ============================================
echo "========================================================"
echo "=== C. IOV AND PAGEMAP ISSUES ==="
echo "========================================================"
echo ""

echo "=== C1. IOVs created for new VMAs ==="
grep -E "Created IOV for new VMA|is_new_vma=true" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No new VMA IOVs"
echo ""

echo "=== C2. No pagemap covers errors ==="
grep -E "no pagemap covers" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No pagemap errors"
echo ""

echo "=== C3. IOV not found errors ==="
grep -E "IOV not found" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No IOV not found errors"
echo ""

echo "=== C4. Zero-fill operations ==="
grep -E "zero.fill|uffd_zero" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No zero-fill logs"
echo ""

# ============================================
# SECTION D: DIRTY BITMAP AND CONVERGENCE
# ============================================
echo "========================================================"
echo "=== D. DIRTY BITMAP AND CONVERGENCE ==="
echo "========================================================"
echo ""

echo "=== D1. Dirty bitmap received ==="
grep -E "Dirty bitmap received|dirty.*ranges" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No dirty bitmap info"
echo ""

echo "=== D2. Convergence mode entry ==="
grep -E "entering convergence|convergence callback" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No convergence entry"
echo ""

echo "=== D3. Pages discarded from buffer ==="
grep -E "discard.*dirty|buffer.*discard" "$SERVER_LOG" 2>/dev/null | head -10 || echo "No discard logs"
echo ""

echo "=== D4. COW phase transitions ==="
grep -E "COW_PHASE|phase.*SYNC|phase.*ASYNC|phase.*CONVERGE" "$PRIMARY_LOG" 2>/dev/null | tail -10 || echo "No phase info"
echo ""

echo "=== D5. WP_SYNC setup ==="
grep -E "WP_SYNC|setup_sync_for_dirty" "$PRIMARY_LOG" 2>/dev/null | tail -10 || echo "No WP_SYNC setup info"
echo ""

# ============================================
# SECTION E: AUTOMATIC ADDRESS ANALYSIS
# ============================================
echo "========================================================"
echo "=== E. AUTOMATIC FAILING ADDRESS ANALYSIS ==="
echo "========================================================"
echo ""

# Extract new VMA ranges from primary log
echo "=== E1. New VMA ranges detected ==="
NEW_VMAS=$(grep -oE "Added lazy VMA for new region 0x[0-9a-f]+-0x[0-9a-f]+" "$PRIMARY_LOG" 2>/dev/null | \
           sed 's/Added lazy VMA for new region //')
if [ -n "$NEW_VMAS" ]; then
    echo "$NEW_VMAS"
else
    echo "No new VMAs found"
fi
echo ""

# Function to check if address is in a VMA range
check_in_vma() {
    local addr=$1
    local addr_dec=$((16#${addr#0x}))
    while IFS='-' read -r start end; do
        local start_dec=$((16#${start#0x}))
        local end_dec=$((16#${end#0x}))
        if [ "$addr_dec" -ge "$start_dec" ] && [ "$addr_dec" -lt "$end_dec" ]; then
            echo "YES (in $start-$end)"
            return 0
        fi
    done <<< "$NEW_VMAS"
    echo "NO"
    return 1
}

# Extract and analyze "Failed to unprotect" addresses
echo "=== E2. Failed to unprotect addresses ==="
UNPROTECT_ADDRS=$(grep -oE "Failed to unprotect page at 0x[0-9a-f]+" "$PRIMARY_LOG" 2>/dev/null | \
                  grep -oE "0x[0-9a-f]+" | sort -u)
if [ -n "$UNPROTECT_ADDRS" ]; then
    while read -r addr; do
        in_new=$(check_in_vma "$addr")
        echo "  $addr - In new VMA: $in_new"
        # Show context from primary log
        echo "    Context from primary:"
        grep -B2 -A2 "$addr" "$PRIMARY_LOG" 2>/dev/null | head -10 | sed 's/^/      /'
        echo ""
    done <<< "$UNPROTECT_ADDRS"
else
    echo "  No unprotect failures found"
fi
echo ""

# Extract and analyze ILLEGAL_TRANSITION addresses
echo "=== E3. ILLEGAL_TRANSITION addresses ==="
ILLEGAL_ADDRS=$(grep -oE "ILLEGAL_TRANSITION: 0x[0-9a-f]+" "$SERVER_LOG" 2>/dev/null | \
                grep -oE "0x[0-9a-f]+" | sort -u)
if [ -n "$ILLEGAL_ADDRS" ]; then
    while read -r addr; do
        in_new=$(check_in_vma "$addr")
        echo "  $addr - In new VMA: $in_new"
        # Show page history
        echo "    Page history:"
        grep -A10 "PAGE_HISTORY $addr" "$SERVER_LOG" 2>/dev/null | head -12 | sed 's/^/      /'
        echo ""
    done <<< "$ILLEGAL_ADDRS"
else
    echo "  No illegal transitions found"
fi
echo ""

# Extract and analyze UFFDIO_COPY error addresses
echo "=== E4. UFFDIO_COPY error addresses ==="
COPY_ERR_ADDRS=$(grep -B5 "UFFDIO_COPY got error\|UFFDIO_COPY err" "$SERVER_LOG" 2>/dev/null | \
                 grep -oE "0x[0-9a-f]{10,}" | sort -u | head -10)
if [ -n "$COPY_ERR_ADDRS" ]; then
    while read -r addr; do
        in_new=$(check_in_vma "$addr")
        echo "  $addr - In new VMA: $in_new"
        # Show context
        echo "    Context:"
        grep -B3 -A3 "$addr" "$SERVER_LOG" 2>/dev/null | grep -E "COPY|HISTORY|transition" | head -8 | sed 's/^/      /'
        echo ""
    done <<< "$COPY_ERR_ADDRS"
else
    echo "  No UFFDIO_COPY errors found"
fi
echo ""

# Extract and analyze "no uffd found" addresses
echo "=== E5. 'no uffd found' addresses (drain thread) ==="
NO_UFFD_ADDRS=$(grep -oE "DRAIN_COPY: 0x[0-9a-f]+ no uffd" "$SERVER_LOG" 2>/dev/null | \
                grep -oE "0x[0-9a-f]+" | sort -u | head -10)
if [ -n "$NO_UFFD_ADDRS" ]; then
    while read -r addr; do
        in_new=$(check_in_vma "$addr")
        echo "  $addr - In new VMA: $in_new"
    done <<< "$NO_UFFD_ADDRS"
else
    echo "  No 'no uffd found' errors"
fi
echo ""

# Manual address analysis if provided
if [ -n "$FAIL_ADDR" ]; then
    echo "=== E6. Manual address analysis: $FAIL_ADDR ==="
    in_new=$(check_in_vma "$FAIL_ADDR")
    echo "  In new VMA: $in_new"
    echo ""
    echo "  In primary log:"
    grep -i "$FAIL_ADDR" "$PRIMARY_LOG" 2>/dev/null | head -10 | sed 's/^/    /'
    echo ""
    echo "  In server log:"
    grep -i "$FAIL_ADDR" "$SERVER_LOG" 2>/dev/null | head -10 | sed 's/^/    /'
    echo ""
fi

# ============================================
# SECTION F: ERROR SUMMARY
# ============================================
echo "========================================================"
echo "=== F. ALL ERRORS (last 50) ==="
echo "========================================================"
echo ""

echo "=== F1. Primary errors ==="
grep -E "^.*Error" "$PRIMARY_LOG" 2>/dev/null | tail -25
echo ""

echo "=== F2. Server errors ==="
grep -E "^.*Error" "$SERVER_LOG" 2>/dev/null | tail -25
echo ""

# ============================================
# SUMMARY
# ============================================
echo "========================================================"
echo "=== SUMMARY ==="
echo "========================================================"
echo ""
echo "ISSUE 1: 'Failed to unprotect page: No such file or directory'"
echo "  - Check section A1-A4"
echo "  - Cause: Trying to unprotect a page in a VMA that's not registered with uffd"
echo "  - New VMAs may not have uffd write-protection registered"
echo ""
echo "ISSUE 2: 'ILLEGAL_TRANSITION: DIRTY -> DISCARDED'"
echo "  - Check section B1-B5"
echo "  - Cause: A dirty page is being discarded when it should be re-sent"
echo "  - Dirty pages should go DIRTY -> IN_BUFFER (re-sent) -> COPIED"
echo ""
echo "ISSUE 3: 'no pagemap covers' / 'IOV not found'"
echo "  - Check section C1-C4"
echo "  - New VMA IOVs should have is_new_vma=true"
echo "  - After dirty bitmap received, missing IOV should zero-fill"
echo ""
