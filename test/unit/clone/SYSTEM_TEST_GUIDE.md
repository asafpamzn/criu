# CLONE System Test Implementation Guide

## Overview

This guide is for implementing end-to-end system tests that exercise the full
CLONE dump/restore pipeline. These tests require:
- Linux with userfaultfd support (kernel 5.7+)
- Root privileges (CRIU needs ptrace, /proc access)
- Two processes OR two machines (source + replica)

## Target Machine

```
ssh -i ~/.ssh/mac.pem ubuntu@ec2-54-160-129-85.compute-1.amazonaws.com
Working directory: /home/ubuntu/work/criu
```

This is an ARM64 (aarch64) r7g.16xlarge instance with 64 vCPUs.
The code is at `/home/ubuntu/work/criu` and can be built with `make -j$(nproc)`.

The unit tests (which don't need root or userfaultfd) can run on a separate machine:
```
ssh -i ~/.ssh/mac.pem ubuntu@ec2-98-84-173-10.compute-1.amazonaws.com
Working directory: /home/ubuntu/work/criu
```

## How CLONE Dump Works (E2E Flow)

The CLONE dump is a multi-phase live migration:

1. **Phase 1 (Skeleton):** CRIU dumps metadata (process tree, FDs, VMAs, etc.)
   while the process runs. Write-protection is applied to all private VMAs via
   userfaultfd WP_ASYNC.

2. **Phase 2 (Bulk Transfer):** The process continues running. Pages are
   streamed to the replica via page-server. When the process writes to a
   WP-protected page, it generates a WP fault — CRIU's uffd monitor snapshots
   the page before the write takes effect.

3. **Phase 3 (Convergence + Freeze):** Scanner threads detect remaining dirty
   pages via PAGEMAP_SCAN. When dirty count is low, the process is frozen.
   Final dirty pages are sent. Replica connects and receives remaining pages.

4. **Restore:** Replica restores from skeleton images + lazy-pages. Page faults
   on the restored process are served from the buffered pages or fetched from
   the primary.

## CLI Flags

```bash
# Source side (dump):
sudo ./criu/criu dump \
  --tree $PID \
  --images-dir /tmp/clone-test-images \
  --clone-dump \
  --lazy-pages \
  --address 127.0.0.1 \
  --port 27000 \
  --leave-running \     # Process keeps running during CLONE
  --tcp-close \
  -v4 -o /tmp/clone-test-images/dump.log

# Replica side (lazy-pages daemon):
sudo ./criu/criu lazy-pages \
  --images-dir /tmp/clone-test-images \
  --page-server \
  --address 127.0.0.1 \
  --port 27000 \
  --clone-dump \
  -v4 -o /tmp/clone-test-images/lazy-pages.log &

# Replica side (restore):
sudo ./criu/criu restore \
  --images-dir /tmp/clone-test-images \
  --lazy-pages \
  --tcp-close \
  --clone-dump \
  --restore-detached \
  -v4 -o /tmp/clone-test-images/restore.log
```

For local (same-machine) testing, source and replica share the images directory.

## Test Structure

Each system test is a self-contained script + test binary:

```
test/clone-system/
├── run_all.sh              # Run all system tests
├── lib/
│   └── clone_test_lib.sh    # Shared shell helpers
├── test_basic_clone.sh       # Test wrapper script
├── test_basic_clone.c        # Test process binary
├── test_write_storm.sh
├── test_write_storm.c
├── test_mmap_munmap.sh
├── test_mmap_munmap.c
├── test_multi_thread.sh
├── test_multi_thread.c
├── test_large_memory.sh
├── test_large_memory.c
├── test_file_backed.sh
├── test_file_backed.c
└── Makefile
```

## Shared Test Library (clone_test_lib.sh)

```bash
#!/bin/bash
# Common helpers for CLONE system tests

CRIU_BIN="${CRIU_BIN:-$(dirname $0)/../../criu/criu}"
IMAGES_DIR=""
CRIU_PORT=27000
TEST_PID=""
LAZY_PID=""

setup_images_dir() {
    IMAGES_DIR=$(mktemp -d /tmp/clone-test-XXXXXX)
    echo "Images dir: $IMAGES_DIR"
}

cleanup() {
    # Kill test process
    [ -n "$TEST_PID" ] && kill -9 $TEST_PID 2>/dev/null || true
    # Kill lazy-pages
    [ -n "$LAZY_PID" ] && kill -9 $LAZY_PID 2>/dev/null || true
    # Kill any leftover criu
    pkill -9 -f "criu.*$IMAGES_DIR" 2>/dev/null || true
    # Cleanup images
    [ -n "$IMAGES_DIR" ] && rm -rf "$IMAGES_DIR"
}

trap cleanup EXIT

# Start the test process binary and return its PID
start_test_process() {
    local binary="$1"
    shift
    "$binary" "$@" &
    TEST_PID=$!
    # Wait for it to write its ready file
    local ready_file="/tmp/clone-test-ready-$TEST_PID"
    for i in $(seq 1 30); do
        [ -f "$ready_file" ] && return 0
        sleep 0.1
    done
    echo "ERROR: test process never became ready"
    return 1
}

# Run the full CLONE dump + restore cycle (local, same machine)
clone_dump_restore_local() {
    local pid=$1
    local verify_cmd="${2:-}"

    # Start lazy-pages daemon
    sudo "$CRIU_BIN" lazy-pages \
        --images-dir "$IMAGES_DIR" \
        --page-server \
        --address 127.0.0.1 \
        --port $CRIU_PORT \
        --clone-dump \
        -v4 -o "$IMAGES_DIR/lazy-pages.log" &
    LAZY_PID=$!
    sleep 0.5

    # Dump with CLONE
    if ! sudo "$CRIU_BIN" dump \
        --tree $pid \
        --images-dir "$IMAGES_DIR" \
        --clone-dump \
        --lazy-pages \
        --address 127.0.0.1 \
        --port $CRIU_PORT \
        --leave-running \
        --tcp-close \
        --shell-job \
        -v4 -o "$IMAGES_DIR/dump.log"; then
        echo "FAIL: criu dump failed"
        cat "$IMAGES_DIR/dump.log" | tail -50
        return 1
    fi

    # Wait for Phase 3 completion signal in log
    for i in $(seq 1 60); do
        if grep -q "PHASE 3 SKELETON DUMP COMPLETE" "$IMAGES_DIR/dump.log" 2>/dev/null; then
            break
        fi
        sleep 0.5
    done

    # Kill the original process (it was left running for CLONE)
    kill -9 $pid 2>/dev/null || true
    sleep 0.5

    # Restore
    if ! sudo "$CRIU_BIN" restore \
        --images-dir "$IMAGES_DIR" \
        --lazy-pages \
        --tcp-close \
        --clone-dump \
        --restore-detached \
        --shell-job \
        -v4 -o "$IMAGES_DIR/restore.log"; then
        echo "FAIL: criu restore failed"
        cat "$IMAGES_DIR/restore.log" | tail -50
        return 1
    fi

    # Wait for lazy-pages to finish serving pages
    wait $LAZY_PID 2>/dev/null || true

    return 0
}

# Simpler approach: dump+kill+restore (process doesn't keep running)
clone_dump_kill_restore() {
    local pid=$1

    # Dump with CLONE (no --leave-running, process is killed after dump)
    if ! sudo "$CRIU_BIN" dump \
        --tree $pid \
        --images-dir "$IMAGES_DIR" \
        --clone-dump \
        --lazy-pages \
        --address 127.0.0.1 \
        --port $CRIU_PORT \
        --tcp-close \
        --shell-job \
        -v4 -o "$IMAGES_DIR/dump.log"; then
        echo "FAIL: criu dump failed"
        return 1
    fi

    # Start lazy-pages
    sudo "$CRIU_BIN" lazy-pages \
        --images-dir "$IMAGES_DIR" \
        --page-server \
        --address 127.0.0.1 \
        --port $CRIU_PORT \
        --clone-dump \
        -v4 -o "$IMAGES_DIR/lazy-pages.log" &
    LAZY_PID=$!
    sleep 0.5

    # Restore
    if ! sudo "$CRIU_BIN" restore \
        --images-dir "$IMAGES_DIR" \
        --lazy-pages \
        --tcp-close \
        --clone-dump \
        --restore-detached \
        --shell-job \
        -v4 -o "$IMAGES_DIR/restore.log"; then
        echo "FAIL: criu restore failed"
        return 1
    fi

    wait $LAZY_PID 2>/dev/null || true
    return 0
}

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; exit 1; }
```

## Test 1: test_basic_clone.c — Basic Write Tracking

Tests that pages written during CLONE phase are correctly captured.

```c
/*
 * Test: Basic CLONE write tracking
 *
 * 1. Allocate N pages, fill with known pattern
 * 2. Signal ready (write ready file)
 * 3. Sleep (CRIU dumps us here, applies WP)
 * 4. Write to specific pages (triggers WP faults)
 * 5. Sleep (CRIU freezes + transfers)
 * 6. After restore: verify BOTH written and unwritten pages
 */

#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>

#define NUM_PAGES    1024
#define PAGE_SZ      4096
#define PATTERN_INIT 0xAA
#define PATTERN_CLONE  0xBB

static volatile int got_signal = 0;
static void sighandler(int sig) { got_signal = 1; }

int main(int argc, char **argv)
{
    char ready_file[256];
    char *mem;
    int i;

    snprintf(ready_file, sizeof(ready_file), "/tmp/clone-test-ready-%d", getpid());

    /* Allocate and fill with initial pattern */
    mem = mmap(NULL, NUM_PAGES * PAGE_SZ, PROT_READ | PROT_WRITE,
               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) { perror("mmap"); return 1; }

    memset(mem, PATTERN_INIT, NUM_PAGES * PAGE_SZ);

    /* Signal ready */
    FILE *f = fopen(ready_file, "w");
    fprintf(f, "%d\n", getpid());
    fclose(f);

    /* Wait for CRIU to dump us and apply WP (test script sends SIGUSR1) */
    signal(SIGUSR1, sighandler);
    while (!got_signal) sleep(1);

    /* Write to every 4th page (CLONE writes) */
    for (i = 0; i < NUM_PAGES; i += 4) {
        memset(mem + i * PAGE_SZ, PATTERN_CLONE, PAGE_SZ);
    }

    /* Wait for freeze + restore (will be killed & restored) */
    signal(SIGUSR2, sighandler);
    got_signal = 0;
    while (!got_signal) sleep(1);

    /* === AFTER RESTORE === */
    /* Verify data integrity */
    int errors = 0;
    for (i = 0; i < NUM_PAGES; i++) {
        unsigned char expected = (i % 4 == 0) ? PATTERN_CLONE : PATTERN_INIT;
        unsigned char *page = (unsigned char *)(mem + i * PAGE_SZ);
        for (int j = 0; j < PAGE_SZ; j++) {
            if (page[j] != expected) {
                fprintf(stderr, "ERROR: page %d offset %d: got 0x%02x expected 0x%02x\n",
                        i, j, page[j], expected);
                errors++;
                break; /* one error per page is enough */
            }
        }
    }

    unlink(ready_file);
    if (errors == 0) {
        printf("PASS: all %d pages verified correctly\n", NUM_PAGES);
        return 0;
    } else {
        printf("FAIL: %d pages have wrong data\n", errors);
        return 1;
    }
}
```

Wrapper script (`test_basic_clone.sh`):
```bash
#!/bin/bash
source "$(dirname $0)/lib/clone_test_lib.sh"
setup_images_dir

# Build if needed
make -C "$(dirname $0)" test_basic_clone 2>/dev/null

# Start test process
./test_basic_clone &
TEST_PID=$!
sleep 1  # Let it fill memory and write ready file

# Dump
clone_dump_restore_local $TEST_PID

# After restore, send SIGUSR2 to trigger verification
NEW_PID=$(pgrep -f test_basic_clone | grep -v $$)
if [ -z "$NEW_PID" ]; then
    fail "restored process not found"
fi
kill -SIGUSR2 $NEW_PID
wait $NEW_PID
RC=$?
[ $RC -eq 0 ] && pass "basic_clone" || fail "basic_clone (exit=$RC)"
```

## Test 2: test_write_storm.c — High Write Rate During CLONE

```c
/*
 * Test: Multiple threads writing rapidly during CLONE phase.
 * Validates convergence under high dirty rate.
 *
 * - 4 writer threads, each writing to its own 64MB region
 * - Writers active during Phase 2 (bulk transfer)
 * - After restore: verify each thread's region has correct final state
 */

#include <sys/mman.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>

#define NUM_THREADS  4
#define REGION_PAGES 16384   /* 64MB per thread */
#define PAGE_SZ      4096
#define ITERATIONS   100     /* Each thread writes all its pages N times */

static atomic_int phase = 0;  /* 0=init, 1=writing, 2=done */
static char *regions[NUM_THREADS];
static uint32_t final_pattern[NUM_THREADS];

void *writer_thread(void *arg)
{
    int id = (int)(long)arg;
    char *region = regions[id];

    /* Wait for phase 1 (CRIU has dumped, WP is active) */
    while (atomic_load(&phase) < 1)
        usleep(1000);

    /* Write storm */
    for (int iter = 0; iter < ITERATIONS; iter++) {
        uint8_t pattern = (uint8_t)(id * 64 + iter);
        for (int p = 0; p < REGION_PAGES; p++) {
            memset(region + p * PAGE_SZ, pattern, PAGE_SZ);
        }
    }

    /* Record final pattern */
    final_pattern[id] = (uint8_t)(id * 64 + ITERATIONS - 1);
    return NULL;
}

int main(void)
{
    pthread_t threads[NUM_THREADS];
    char ready_file[256];

    snprintf(ready_file, sizeof(ready_file), "/tmp/clone-test-ready-%d", getpid());

    /* Allocate regions */
    for (int i = 0; i < NUM_THREADS; i++) {
        regions[i] = mmap(NULL, REGION_PAGES * PAGE_SZ,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        memset(regions[i], 0, REGION_PAGES * PAGE_SZ);
    }

    /* Signal ready */
    FILE *f = fopen(ready_file, "w");
    fprintf(f, "%d\n", getpid());
    fclose(f);

    /* Create writer threads (they wait for phase=1) */
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_create(&threads[i], NULL, writer_thread, (void *)(long)i);

    /* CRIU dumps us here. Script sends SIGUSR1 after WP is active. */
    pause(); /* Wait for signal */

    /* Start writers */
    atomic_store(&phase, 1);

    /* Wait for writers to finish */
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);

    /* Wait for freeze + restore */
    pause();

    /* === AFTER RESTORE: verify === */
    int errors = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        uint8_t expected = final_pattern[i];
        for (int p = 0; p < REGION_PAGES; p++) {
            if (((uint8_t *)regions[i])[p * PAGE_SZ] != expected) {
                errors++;
                break;
            }
        }
    }

    unlink(ready_file);
    if (errors == 0) {
        printf("PASS: write_storm - all %d regions correct\n", NUM_THREADS);
        return 0;
    }
    printf("FAIL: write_storm - %d regions corrupted\n", errors);
    return 1;
}
```

## Test 3: test_mmap_munmap.c — Dynamic Memory During CLONE

```c
/*
 * Test: mmap/munmap during CLONE phase.
 *
 * After CRIU applies WP:
 * 1. munmap some existing regions (generates REMOVE events)
 * 2. mmap new regions (not WP-protected, detected in Phase 3)
 * 3. Write to new regions
 * After restore: verify new regions exist with correct data,
 *                old unmapped regions are gone
 */

#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>

#define PAGE_SZ       4096
#define INITIAL_PAGES 256
#define NEW_PAGES     128

static char *initial_region;
static char *new_region;
static volatile int phase = 0;
static void handler(int s) { phase++; }

int main(void)
{
    char ready_file[256];
    snprintf(ready_file, sizeof(ready_file), "/tmp/clone-test-ready-%d", getpid());

    signal(SIGUSR1, handler);
    signal(SIGUSR2, handler);

    /* Initial allocation */
    initial_region = mmap(NULL, INITIAL_PAGES * PAGE_SZ,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(initial_region, 0x11, INITIAL_PAGES * PAGE_SZ);

    /* Signal ready */
    FILE *f = fopen(ready_file, "w");
    fprintf(f, "%d\n", getpid());
    fclose(f);

    /* Wait for CRIU to dump + apply WP */
    while (phase < 1) sleep(1);

    /* Unmap second half of initial region */
    munmap(initial_region + (INITIAL_PAGES / 2) * PAGE_SZ,
           (INITIAL_PAGES / 2) * PAGE_SZ);

    /* Map new region */
    new_region = mmap(NULL, NEW_PAGES * PAGE_SZ,
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(new_region, 0x22, NEW_PAGES * PAGE_SZ);

    /* Wait for restore */
    while (phase < 2) sleep(1);

    /* === AFTER RESTORE === */
    int errors = 0;

    /* First half should still have 0x11 */
    for (int i = 0; i < INITIAL_PAGES / 2; i++) {
        if (((unsigned char *)initial_region)[i * PAGE_SZ] != 0x11) {
            fprintf(stderr, "ERROR: initial page %d corrupted\n", i);
            errors++;
        }
    }

    /* New region should have 0x22 */
    for (int i = 0; i < NEW_PAGES; i++) {
        if (((unsigned char *)new_region)[i * PAGE_SZ] != 0x22) {
            fprintf(stderr, "ERROR: new page %d corrupted\n", i);
            errors++;
        }
    }

    unlink(ready_file);
    printf("%s: mmap_munmap (%d errors)\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
```

## Test 4: test_large_memory.c — Scale Test

```c
/*
 * Test: Large memory (1GB+) CLONE dump.
 * Validates page pool, batch transfer, and memory pressure handling.
 *
 * Allocates 1GB, fills with page-index-based patterns.
 * During CLONE: writes to random 10% of pages.
 * After restore: verifies all pages (written and unwritten).
 */

#define TOTAL_SIZE_MB  1024
#define NUM_PAGES      (TOTAL_SIZE_MB * 256)  /* 262144 pages */
#define WRITE_PERCENT  10
```

## Test 5: test_multi_thread.c — Multi-threaded Process

```c
/*
 * Test: Multi-threaded process with per-thread TLS + stack.
 * Validates that all threads are restored with correct state.
 *
 * - 8 threads, each with unique TLS value and stack data
 * - During CLONE: threads increment counters
 * - After restore: each thread verifies its own state
 */
```

## Test 6: test_file_backed.c — File-backed Private Mappings

```c
/*
 * Test: MAP_PRIVATE file-backed mappings with CLONE.
 * When a process writes to a MAP_PRIVATE file mapping, the kernel
 * creates a private copy. CRIU must capture these private pages.
 *
 * - Create temp file, mmap MAP_PRIVATE
 * - Write to some pages (creating private copies)
 * - After restore: verify private pages differ from file content
 */
```

## Building

```makefile
CC := gcc
CFLAGS := -Wall -g -O2 -pthread

TESTS := test_basic_clone test_write_storm test_mmap_munmap \
         test_large_memory test_multi_thread test_file_backed

all: $(TESTS)

%: %.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TESTS)

.PHONY: all clean
```

## Running Tests

```bash
# Build CRIU first
cd /home/ubuntu/work/criu
make -j$(nproc)

# Build tests
cd test/clone-system
make

# Run all tests (as root)
sudo ./run_all.sh

# Run single test
sudo ./test_basic_clone.sh
```

## run_all.sh

```bash
#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

make -q all || make all

TESTS=(
    test_basic_clone.sh
    test_write_storm.sh
    test_mmap_munmap.sh
    test_large_memory.sh
    test_multi_thread.sh
    test_file_backed.sh
)

PASSED=0
FAILED=0

for test in "${TESTS[@]}"; do
    echo "=== Running $test ==="
    if ./"$test"; then
        PASSED=$((PASSED + 1))
    else
        FAILED=$((FAILED + 1))
        echo "^^^ FAILED ^^^"
    fi
    echo ""
done

echo "================================"
echo "Results: $PASSED passed, $FAILED failed"
echo "================================"
[ $FAILED -eq 0 ] || exit 1
```

## Important Design Considerations

### Signal-based Phasing

The test processes use signals (SIGUSR1/SIGUSR2) to synchronize with the test
script. The script monitors CRIU logs for phase transitions:
- "WP applied" or similar → send SIGUSR1 to start writes
- After restore → send SIGUSR2 to trigger verification

### Alternative: Poll-based Phasing

Instead of signals, the test process can poll a file:
```c
while (!access("/tmp/clone-test-phase1", F_OK)) sleep_us(1000);
```
This is simpler but slightly less precise.

### ZDTM Integration (Optional)

If you want tests to run via `zdtm.py`, the test binary must follow the ZDTM
conventions:
- Call `test_init(argc, argv)` → `test_daemon()` → `test_waitsig()`
- Verification happens after `test_waitsig()` returns
- Call `pass()` or `fail()` at the end

For CLONE tests in ZDTM style, you'd add to `.desc`:
```
{'flags': 'clone-dump lazy'}
```

But since `zdtm.py` may not support `--clone-dump` yet, the standalone script
approach above is more practical initially.

### What to Verify

For each test, verify:
1. **Data integrity** — all memory pages have expected content
2. **CRIU exit code** — dump and restore succeeded (exit 0)
3. **No errors in logs** — grep for "Error" / "BUG" / "SIGABRT" in CRIU logs
4. **Process state** — PID, threads, FDs are correct after restore

### Debugging Failures

When a test fails:
```bash
# Check dump log
sudo cat /tmp/clone-test-*/dump.log | grep -i error

# Check lazy-pages log
sudo cat /tmp/clone-test-*/lazy-pages.log | grep -i error

# Check restore log
sudo cat /tmp/clone-test-*/restore.log | grep -i error

# Enable page state tracker for detailed debugging
# Edit criu/include/clone/clone-conf.h:
#   #define CONFIG_PAGE_STATE_TRACKER
# Rebuild and re-run
```

## Priority Order

1. `test_basic_clone` — simplest E2E, validates core WP → snapshot → restore
2. `test_mmap_munmap` — validates REMOVE event handling + Phase 3 new VMAs
3. `test_write_storm` — validates convergence under pressure
4. `test_large_memory` — validates scale (page pool, batch transfer)
5. `test_multi_thread` — validates thread state preservation
6. `test_file_backed` — validates file-backed VMA handling
