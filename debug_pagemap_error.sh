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

echo "=== A6a. VMA mappings from parse_maps (cr-dump.c) ==="
grep -E "^.*0x[0-9a-f]+-0x[0-9a-f]+.*prot" "$PRIMARY_LOG" 2>/dev/null | head -50 || echo "No VMA mappings found"
echo ""

echo "=== A6b. VMAs being checked for new regions ==="
grep -E "Checking VMA 0x" "$PRIMARY_LOG" 2>/dev/null | head -50 || echo "No VMA check logs"
echo ""

echo "=== A6c. PAGEMAP_SCAN results (dirty ranges per VMA) ==="
grep -E "PAGEMAP_SCAN.*VMA|scanning VMA|returned.*regions" "$PRIMARY_LOG" 2>/dev/null | head -50 || echo "No PAGEMAP_SCAN logs"
echo ""

echo "=== A6d. Dirty ranges summary ==="
grep -E "Scanned.*dirty ranges|Found.*dirty ranges|dirty pages total" "$PRIMARY_LOG" 2>/dev/null | head -10 || echo "No dirty range summary"
echo ""

echo "=== A7. WP_SYNC registration stats (CRITICAL) ==="
grep -E "WP_SYNC registration:|registered_ok|register_skip" "$PRIMARY_LOG" 2>/dev/null | head -10 || echo "No WP_SYNC stats"
echo ""

echo "=== A8. WP_SYNC registration skipped (VMA changed) ==="
grep -E "skipped.*VMA changed|ENOMEM|EINVAL" "$PRIMARY_LOG" 2>/dev/null | head -20 || echo "No skipped registrations"
echo ""

echo "=== A9. All VMAs registered with uffd ==="
grep -E "UFFDIO_REGISTER WP" "$PRIMARY_LOG" 2>/dev/null | head -30 || echo "No uffd register logs"
echo ""

echo "=== A10. Dirty ranges count vs registration count ==="
echo "Dirty ranges sent:"
grep -E "dirty.*ranges|nr_dirty_ranges" "$PRIMARY_LOG" 2>/dev/null | tail -5
echo "Registration summary:"
grep -E "registration:.*ok.*skipped" "$PRIMARY_LOG" 2>/dev/null | tail -5
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

echo "=== B4a. ESRCH (errno=3) errors - process died ==="
grep -E "errno=3|ESRCH|uffd_copy1:ERROR" "$SERVER_LOG" 2>/dev/null | head -20 || echo "No ESRCH errors"
echo ""

echo "=== B4b. Convergence failure address analysis ==="
CONV_FAIL_ADDRS=$(grep -oE "convergence copy failed at 0x[0-9a-f]+" "$SERVER_LOG" 2>/dev/null | \
                  grep -oE "0x[0-9a-f]+" | sort -u)
if [ -n "$CONV_FAIL_ADDRS" ]; then
    while read -r addr; do
        [ -z "$addr" ] && continue
        in_new=$(check_in_vma "$addr")
        echo "  $addr - In new VMA: $in_new"
        echo "    Page history:"
        grep -A10 "PAGE_HISTORY $addr" "$SERVER_LOG" 2>/dev/null | head -12 | sed 's/^/      /'
        echo ""
    done <<< "$CONV_FAIL_ADDRS"
else
    echo "  No convergence failures found"
fi
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

echo "=== D6. Page requests from replica (PS_IOV_GET) ==="
grep -E "PS_IOV_GET|page.*request|#PF req" "$PRIMARY_LOG" 2>/dev/null | head -20 || echo "No page request logs"
echo ""

echo "=== D6a. Processing VMA logs (page-xfer.c) ==="
grep -E "processing VMA|Processing VMA" "$PRIMARY_LOG" 2>/dev/null | head -30 || echo "No processing VMA logs"
echo ""

echo "=== D7. Bulk transfer completion ==="
grep -E "bulk.*complete|BULK_COMPLETE|all.*pages.*sent" "$PRIMARY_LOG" 2>/dev/null | head -10 || echo "No bulk complete logs"
grep -E "bulk.*complete|BULK_COMPLETE|all.*pages.*sent" "$SERVER_LOG" 2>/dev/null | head -10 || echo ""
echo ""

echo "=== D8. Pages discarded from buffer (replica) ==="
grep -E "discard|DIRTY.*IN_BUFFER|IN_BUFFER.*DIRTY" "$SERVER_LOG" 2>/dev/null | head -20 || echo "No discard info"
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

# Check if failing addresses are in tracked VMAs (Phase 1 VMAs)
echo "=== E2a. Check failing addresses against all tracked VMAs ==="
# Extract all tracked VMA ranges from primary log
TRACKED_VMAS=$(grep -oE "tracked VMA.*0x[0-9a-f]+-0x[0-9a-f]+" "$PRIMARY_LOG" 2>/dev/null | \
               grep -oE "0x[0-9a-f]+-0x[0-9a-f]+")
if [ -z "$TRACKED_VMAS" ]; then
    # Try alternative format
    TRACKED_VMAS=$(grep -oE "Adding tracked VMA: 0x[0-9a-f]+-0x[0-9a-f]+" "$PRIMARY_LOG" 2>/dev/null | \
                   grep -oE "0x[0-9a-f]+-0x[0-9a-f]+")
fi
if [ -n "$TRACKED_VMAS" ] && [ -n "$UNPROTECT_ADDRS" ]; then
    echo "  Tracked VMA ranges:"
    echo "$TRACKED_VMAS" | head -50 | sed 's/^/    /'
    echo ""
    echo "  Checking failing addresses:"
    check_in_tracked_vma() {
        local addr=$1
        local addr_dec=$((16#${addr#0x}))
        while IFS='-' read -r start end; do
            local start_dec=$((16#${start#0x}))
            local end_dec=$((16#${end#0x}))
            if [ "$addr_dec" -ge "$start_dec" ] && [ "$addr_dec" -lt "$end_dec" ]; then
                echo "YES (in $start-$end)"
                return 0
            fi
        done <<< "$TRACKED_VMAS"
        echo "NO - not in any tracked VMA!"
        return 1
    }
    while read -r addr; do
        [ -z "$addr" ] && continue
        in_tracked=$(check_in_tracked_vma "$addr")
        echo "    $addr - In tracked VMA: $in_tracked"
    done <<< "$UNPROTECT_ADDRS"
else
    echo "  Could not extract tracked VMAs or no failing addresses"
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
    echo "  In primary log (full lifecycle):"
    grep -i "${FAIL_ADDR#0x}" "$PRIMARY_LOG" 2>/dev/null | head -30 | sed 's/^/    /'
    echo ""
    echo "  In server log (full lifecycle):"
    grep -i "${FAIL_ADDR#0x}" "$SERVER_LOG" 2>/dev/null | head -30 | sed 's/^/    /'
    echo ""
fi

# Auto-analyze first unprotect failure address
FIRST_UNPROTECT=$(grep -oE "Failed to unprotect page at 0x[0-9a-f]+" "$PRIMARY_LOG" 2>/dev/null | head -1 | grep -oE "0x[0-9a-f]+")
if [ -n "$FIRST_UNPROTECT" ] && [ -z "$FAIL_ADDR" ]; then
    echo "=== E7. Auto-analysis of first unprotect failure: $FIRST_UNPROTECT ==="
    ADDR_SHORT="${FIRST_UNPROTECT#0x}"
    in_new=$(check_in_vma "$FIRST_UNPROTECT")
    echo "  In new VMA: $in_new"
    echo ""
    echo "  Primary log (full lifecycle):"
    grep -i "$ADDR_SHORT" "$PRIMARY_LOG" 2>/dev/null | head -30 | sed 's/^/    /'
    echo ""
    echo "  Server log (full lifecycle):"
    grep -i "$ADDR_SHORT" "$SERVER_LOG" 2>/dev/null | head -30 | sed 's/^/    /'
    echo ""
fi

# Find which VMA contains failing addresses
echo "=== E8. Find which VMA contains failing addresses ==="
find_containing_vma() {
    local addr=$1
    local addr_dec=$((16#${addr#0x}))
    # Extract VMA ranges from parse_maps output
    grep -oE "0x[0-9a-f]+-0x[0-9a-f]+.*prot" "$PRIMARY_LOG" 2>/dev/null | \
    while IFS= read -r line; do
        local range=$(echo "$line" | grep -oE "0x[0-9a-f]+-0x[0-9a-f]+")
        local start=$(echo "$range" | cut -d'-' -f1)
        local end=$(echo "$range" | cut -d'-' -f2)
        local start_dec=$((16#${start#0x}))
        local end_dec=$((16#${end#0x}))
        if [ "$addr_dec" -ge "$start_dec" ] && [ "$addr_dec" -lt "$end_dec" ]; then
            echo "$line"
            return 0
        fi
    done
    echo "NOT FOUND in any VMA mapping!"
    return 1
}

if [ -n "$UNPROTECT_ADDRS" ]; then
    while read -r addr; do
        [ -z "$addr" ] && continue
        echo "  Address $addr is in VMA:"
        find_containing_vma "$addr" | sed 's/^/    /'
        echo ""
    done <<< "$(echo "$UNPROTECT_ADDRS" | head -5)"
fi

# Also check if failing addresses are in PAGEMAP_SCAN dirty ranges
echo "=== E9. Check if failing addresses are in dirty ranges ==="
if [ -n "$UNPROTECT_ADDRS" ]; then
    while read -r addr; do
        [ -z "$addr" ] && continue
        ADDR_SHORT="${addr#0x}"
        # Look for PAGEMAP_SCAN entries that contain this address
        echo "  Checking if $addr is in any scanned dirty range..."
        # Get the VMA that contains this address first
        containing_vma=$(find_containing_vma "$addr" | grep -oE "0x[0-9a-f]+-0x[0-9a-f]+")
        if [ -n "$containing_vma" ]; then
            echo "    Containing VMA: $containing_vma"
            vma_start=$(echo "$containing_vma" | cut -d'-' -f1)
            # Check PAGEMAP_SCAN for this VMA
            grep "PAGEMAP_SCAN.*$vma_start" "$PRIMARY_LOG" 2>/dev/null | head -5 | sed 's/^/    /'
        fi
        echo ""
    done <<< "$(echo "$UNPROTECT_ADDRS" | head -3)"
fi
echo ""

# ============================================
# SECTION F: TIMING AND SEQUENCE
# ============================================
echo "========================================================"
echo "=== F. TIMING AND SEQUENCE ==="
echo "========================================================"
echo ""

echo "=== F1. Key events timeline (PRIMARY) ==="
grep -E "Phase|WP_SYNC|dirty bitmap|bulk complete|CONVERGE|unfreeze|all pages sent" "$PRIMARY_LOG" 2>/dev/null | head -30
echo ""

echo "=== F2. Key events timeline (REPLICA) ==="
grep -E "Dirty bitmap|convergence|restore connected|all pages sent|bulk.*done" "$SERVER_LOG" 2>/dev/null | head -30
echo ""

echo "=== F3. First error timestamp vs key events ==="
FIRST_ERROR=$(grep -E "Error.*Failed to unprotect|Error.*errno=3|Error.*ESRCH" "$PRIMARY_LOG" "$SERVER_LOG" 2>/dev/null | head -1)
echo "First critical error: $FIRST_ERROR"
echo ""

# ============================================
# SECTION G: ERROR SUMMARY
# ============================================
echo "========================================================"
echo "=== G. ALL ERRORS (last 50) ==="
echo "========================================================"
echo ""

echo "=== G1. Primary errors ==="
grep -E "^.*Error" "$PRIMARY_LOG" 2>/dev/null | tail -25
echo ""

echo "=== G2. Server errors ==="
grep -E "^.*Error" "$SERVER_LOG" 2>/dev/null | tail -25
echo ""

# ============================================
# SUMMARY
# ============================================
echo "========================================================"
echo "=== SUMMARY ==="
echo "========================================================"
echo ""
echo "ISSUE 1: 'Failed to unprotect page: No such file or directory' (ENOENT)"
echo "  - Check sections A1-A10"
echo "  - ROOT CAUSE: Page is NOT in dirty range, so not registered with WP_SYNC"
echo "  - After WP_ASYNC disabled, only dirty ranges are registered"
echo "  - Non-dirty Phase 1 pages are no longer protected by uffd"
echo "  - FIX NEEDED: Don't unprotect pages that aren't in dirty ranges"
echo "  - Check A7 for WP_SYNC registration stats (ok vs skipped)"
echo "  - Check E2a to see if address is in any tracked VMA"
echo ""
echo "ISSUE 2: 'ILLEGAL_TRANSITION: DIRTY -> DISCARDED'"
echo "  - Check section B1-B5"
echo "  - FIX IMPLEMENTED: page_state_get() check before setting DISCARDED"
echo "  - DIRTY pages are now skipped (not set to DISCARDED)"
echo "  - If still failing: check which code path is setting DISCARDED"
echo ""
echo "ISSUE 3: 'no pagemap covers' / 'IOV not found'"
echo "  - Check section C1-C4"
echo "  - FIX IMPLEMENTED: New VMA IOVs have is_new_vma=true"
echo "  - FIX IMPLEMENTED: Zero-fill after dirty bitmap received for missing IOVs"
echo ""
echo "ISSUE 4: EEXIST (duplicate page copy)"
echo "  - Check for 'BUG: ... EEXIST' messages"
echo "  - This indicates a race between copy paths"
echo "  - FIX IMPLEMENTED: EEXIST now reports bug and fails instead of masking"
echo ""
echo "ISSUE 5: ESRCH (errno=3) - Process died during convergence"
echo "  - Check section B4a-B4b"
echo "  - ROOT CAUSE: Process crashed/exited, uffd became invalid"
echo "  - WHY: Likely due to zero-filling pages that needed real data"
echo "  - Check F1-F3 for timeline of events"
echo "  - Look for page faults followed by ESRCH"
echo ""
echo "ISSUE 6: Replica requests pages that should be in buffer"
echo "  - Check section D6-D8"
echo "  - Pages may have been discarded when dirty bitmap arrived"
echo "  - But re-transmission hasn't happened yet"
echo "  - Race between page fault and convergence page arrival"
echo ""
