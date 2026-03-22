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

# ============================================
# SECTION E: SPECIFIC ADDRESS ANALYSIS
# ============================================
if [ -n "$FAIL_ADDR" ]; then
    echo "========================================================"
    echo "=== E. SPECIFIC ADDRESS ANALYSIS: $FAIL_ADDR ==="
    echo "========================================================"
    echo ""

    echo "=== E1. Address in primary log ==="
    grep -i "$FAIL_ADDR" "$PRIMARY_LOG" 2>/dev/null | head -20 || echo "Not found in primary"
    echo ""

    echo "=== E2. Address in server log ==="
    grep -i "$FAIL_ADDR" "$SERVER_LOG" 2>/dev/null | head -20 || echo "Not found in server"
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
