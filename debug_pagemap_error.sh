#!/bin/bash
# Debug script for "no pagemap covers" error
# Usage: ./debug_pagemap_error.sh <lazy-primary.log> <lazy-server.log>

PRIMARY_LOG="${1:-lazy-primary.log}"
SERVER_LOG="${2:-lazy-server.log}"

echo "=============================================="
echo "DEBUG: No pagemap covers error analysis"
echo "=============================================="
echo ""

# The failing address from the error
FAIL_ADDR="f533a55f0000"

echo "=== 1. ERROR CONTEXT ==="
echo "Failing address: 0x$FAIL_ADDR"
echo ""

echo "=== 2. IOVs CREATED FOR NEW VMAs (Server/Replica) ==="
grep -E "(Created IOV for new VMA|Creating IOVs for.*pending)" "$SERVER_LOG" 2>/dev/null || echo "No new VMA IOVs found"
echo ""

echo "=== 3. ALL IOVs DUMP (Server/Replica) ==="
grep -E "IOV\[|=== IOV DUMP" "$SERVER_LOG" 2>/dev/null | head -50 || echo "No IOV dump found"
echo ""

echo "=== 4. DIRTY BITMAP INFO (Server/Replica) ==="
grep -E "(Dirty bitmap|dirty.*ranges|nr_dirty)" "$SERVER_LOG" 2>/dev/null | head -20 || echo "No dirty bitmap info"
echo ""

echo "=== 5. NEW VMAs DETECTED (Primary) ==="
grep -E "(Added new tracked VMA|Added lazy VMA for new region|cow_detect_new_vmas|cow_extend_tracked_vmas)" "$PRIMARY_LOG" 2>/dev/null | head -30 || echo "No new VMA detection logs"
echo ""

echo "=== 6. PAGE FAULT AT FAILING ADDRESS ==="
grep -i "$FAIL_ADDR" "$SERVER_LOG" 2>/dev/null || echo "No logs for failing address in server log"
grep -i "$FAIL_ADDR" "$PRIMARY_LOG" 2>/dev/null || echo "No logs for failing address in primary log"
echo ""

echo "=== 7. PAGEMAP ENTRIES LOADED (Server) ==="
grep -E "(Processing lazy pagemap entry|pagemap covers|seek_pagemap)" "$SERVER_LOG" 2>/dev/null | head -30 || echo "No pagemap loading logs"
echo ""

echo "=== 8. VMA RANGES FROM MM.IMG (Server) ==="
grep -E "(Found.*VMAs|VMA.*0x)" "$SERVER_LOG" 2>/dev/null | head -30 || echo "No VMA range logs"
echo ""

echo "=== 9. CONVERGENCE PHASE PAGES ==="
grep -E "(Convergence:|convergence_io_complete|buffering)" "$SERVER_LOG" 2>/dev/null | head -30 || echo "No convergence logs"
echo ""

echo "=== 10. PAGE BUFFER STATE ==="
grep -E "(page.*buffer|buffered|nr_pages)" "$SERVER_LOG" 2>/dev/null | head -20 || echo "No page buffer logs"
echo ""

echo "=== 11. ERRORS AND WARNINGS ==="
grep -E "^.*Error|^.*Warn" "$SERVER_LOG" 2>/dev/null | tail -30
echo ""

echo "=== 12. CHECK: Is failing addr in any IOV range? ==="
# Extract IOV ranges and check if failing address falls within
echo "Manual check needed: Compare 0x$FAIL_ADDR against IOV ranges above"
echo ""

echo "=== 13. CHECK: Was this address sent during convergence? ==="
grep -E "0x.*$FAIL_ADDR|vaddr.*$FAIL_ADDR" "$PRIMARY_LOG" 2>/dev/null | head -10 || echo "Address not found in primary log sends"
echo ""

echo "=============================================="
echo "SUMMARY"
echo "=============================================="
echo ""
echo "Key questions to answer:"
echo "1. Was an IOV created for address 0x$FAIL_ADDR? (check section 2-3)"
echo "2. Was this address part of dirty/new VMA ranges? (check section 4-5)"
echo "3. Is there a pagemap entry covering this address? (check section 7)"
echo "4. Was this page sent during convergence? (check section 9, 13)"
echo ""
echo "Likely root cause: IOV was created but pagemap has no entry."
echo "New VMAs don't have pagemap entries from Phase 1 dump."
echo "Fix needed: Handle page faults for new VMAs differently - "
echo "read from page buffer or request from page server instead of pagemap."
