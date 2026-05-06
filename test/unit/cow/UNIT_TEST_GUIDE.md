# COW Unit Test Implementation Guide

## Overview

This guide is for implementing standalone unit tests for the COW dump components.
These tests run without root privileges, without CRIU, and without kernel
userfaultfd — they test pure logic in isolation.

## Target Machine

```
ssh -i ~/.ssh/mac.pem ubuntu@ec2-98-84-173-10.compute-1.amazonaws.com
Working directory: /home/ubuntu/work/criu
```

Build with `make -j$(nproc)` from the repo root. Unit tests live in
`test/unit/cow/` and are built/run separately from the main CRIU build.

## Directory Structure

```
test/unit/cow/
├── Makefile
├── test_batch_bitmap.c
├── test_spsc_queue.c
├── test_mpsc_queue.c
├── test_page_pool.c
├── test_unmapped_tracker.c
├── test_page_state_tracker.c
├── test_hung_page_tracker.c
└── run_all.sh
```

## Build System

Create `test/unit/cow/Makefile` that:
- Compiles each test as a standalone binary
- Links against pthread (needed for spinlocks, atomics)
- Includes CRIU headers: `-I../../../criu/include -I../../../include`
- Defines `PAGE_SIZE` and `PAGE_SHIFT` if not available from system headers
- Each test binary returns 0 on success, non-zero on failure

Key challenge: The COW code uses CRIU internal helpers (`xmalloc`, `xzalloc`,
`xfree`, `pr_err`, `pr_debug`, `BUG()`, hlist macros, etc.). The unit tests
must provide stubs or shims for these. Create a `test_harness.h` that:

```c
#ifndef __TEST_HARNESS_H__
#define __TEST_HARNESS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Shims for CRIU internals */
#define xmalloc(size)    malloc(size)
#define xzalloc(size)    calloc(1, size)
#define xfree(ptr)       free(ptr)
#define pr_err(fmt, ...) fprintf(stderr, "ERR: " fmt, ##__VA_ARGS__)
#define pr_debug(fmt, ...)  /* silent in tests */
#define pr_info(fmt, ...)   fprintf(stdout, fmt, ##__VA_ARGS__)
#define pr_perror(fmt, ...) fprintf(stderr, "ERR: " fmt ": %m\n", ##__VA_ARGS__)
#define BUG()            do { fprintf(stderr, "BUG at %s:%d\n", __FILE__, __LINE__); abort(); } while(0)
#define BUG_ON(cond)     do { if (cond) BUG(); } while(0)

/* Test assertion macros */
static int __test_failures = 0;
static int __test_passes = 0;

#define TEST_ASSERT(cond, msg) do { \
	if (!(cond)) { \
		fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
		__test_failures++; \
	} else { \
		__test_passes++; \
	} \
} while(0)

#define TEST_ASSERT_EQ(a, b, msg) do { \
	if ((a) != (b)) { \
		fprintf(stderr, "FAIL [%s:%d]: %s (got %ld, expected %ld)\n", \
			__FILE__, __LINE__, msg, (long)(a), (long)(b)); \
		__test_failures++; \
	} else { \
		__test_passes++; \
	} \
} while(0)

#define TEST_SUMMARY() do { \
	printf("\n=== %d passed, %d failed ===\n", __test_passes, __test_failures); \
	return __test_failures > 0 ? 1 : 0; \
} while(0)

/* Page constants (for aarch64/x86 with 4KB pages) */
#ifndef PAGE_SIZE
#define PAGE_SIZE  4096
#endif
#ifndef PAGE_SHIFT
#define PAGE_SHIFT 12
#endif

/* hlist shims - simplified from include/common/list.h */
#include "common/list.h"

#endif /* __TEST_HARNESS_H__ */
```

## Test 1: test_batch_bitmap.c

**File under test:** `criu/include/cow/cow-batch-bitmap.h` (header-only, inline)

**No dependencies** — just include the header directly.

```c
#include "test_harness.h"
#include "cow/cow-batch-bitmap.h"

/* Tests to implement: */

void test_zero_and_fill(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_zero(&bm);
	TEST_ASSERT(cow_batch_bitmap_is_empty(&bm), "zero produces empty");
	TEST_ASSERT(!cow_batch_bitmap_is_full(&bm), "zero is not full");

	cow_batch_bitmap_fill(&bm);
	TEST_ASSERT(cow_batch_bitmap_is_full(&bm), "fill produces full");
	TEST_ASSERT(!cow_batch_bitmap_is_empty(&bm), "fill is not empty");
}

void test_set_clear_test(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_zero(&bm);

	/* Test each word boundary: bits 0, 63, 64, 127, 128, 191, 192, 255 */
	int boundaries[] = {0, 63, 64, 127, 128, 191, 192, 255};
	for (int i = 0; i < 8; i++) {
		cow_batch_bitmap_set(&bm, boundaries[i]);
		TEST_ASSERT(cow_batch_bitmap_test(&bm, boundaries[i]), "set bit visible");
	}
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 8, "8 bits set");

	for (int i = 0; i < 8; i++) {
		cow_batch_bitmap_clear(&bm, boundaries[i]);
		TEST_ASSERT(!cow_batch_bitmap_test(&bm, boundaries[i]), "clear bit gone");
	}
	TEST_ASSERT(cow_batch_bitmap_is_empty(&bm), "all cleared = empty");
}

void test_popcount(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_zero(&bm);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 0, "empty popcount=0");

	for (int i = 0; i < 256; i++) {
		cow_batch_bitmap_set(&bm, i);
		TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), i + 1, "popcount increments");
	}
}

void test_next_set(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_zero(&bm);

	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 0), -1, "empty has no bits");

	cow_batch_bitmap_set(&bm, 5);
	cow_batch_bitmap_set(&bm, 100);
	cow_batch_bitmap_set(&bm, 200);

	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 0), 5, "first set at 5");
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 6), 100, "next set at 100");
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 101), 200, "next set at 200");
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 201), -1, "no more bits");
}

void test_set_range(void)
{
	cow_batch_bitmap_t bm;

	/* Range within one word */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 10, 20);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 20, "range of 20");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 10), "range start set");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 29), "range end set");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 9), "before range clear");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 30), "after range clear");

	/* Range spanning word boundaries */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 60, 10); /* spans word 0-1 boundary */
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 10, "cross-word range");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 60), "cross start");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 69), "cross end");

	/* Full range */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 0, 256);
	TEST_ASSERT(cow_batch_bitmap_is_full(&bm), "full range = full");
}

void test_clear_range(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_fill(&bm);
	cow_batch_bitmap_clear_range(&bm, 64, 64);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 192, "cleared 64 bits");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 64), "cleared start");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 127), "cleared end");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 63), "before range untouched");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 128), "after range untouched");
}

void test_and_or_not(void)
{
	cow_batch_bitmap_t a, b, result;

	cow_batch_bitmap_zero(&a);
	cow_batch_bitmap_zero(&b);
	cow_batch_bitmap_set_range(&a, 0, 128);
	cow_batch_bitmap_set_range(&b, 64, 128);

	cow_batch_bitmap_and(&result, &a, &b);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&result), 64, "AND overlap");
	TEST_ASSERT(cow_batch_bitmap_test(&result, 64), "AND bit 64");
	TEST_ASSERT(cow_batch_bitmap_test(&result, 127), "AND bit 127");
	TEST_ASSERT(!cow_batch_bitmap_test(&result, 63), "AND not bit 63");

	cow_batch_bitmap_or(&result, &a, &b);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&result), 192, "OR union");

	cow_batch_bitmap_not(&result, &a);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&result), 128, "NOT flips");
}

void test_mask(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_fill(&bm);
	cow_batch_bitmap_mask(&bm, 100);
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 100, "mask to 100");
	TEST_ASSERT(cow_batch_bitmap_test(&bm, 99), "bit 99 still set");
	TEST_ASSERT(!cow_batch_bitmap_test(&bm, 100), "bit 100 cleared");
}

void test_is_full_upto(void)
{
	cow_batch_bitmap_t bm;
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 0, 64);
	TEST_ASSERT(cow_batch_bitmap_is_full_upto(&bm, 64), "full up to 64");
	TEST_ASSERT(!cow_batch_bitmap_is_full_upto(&bm, 65), "not full to 65");
	TEST_ASSERT(cow_batch_bitmap_is_full_upto(&bm, 1), "full up to 1");
}

void test_for_each_set_macro(void)
{
	cow_batch_bitmap_t bm;
	int idx, count = 0;
	int expected[] = {3, 77, 200};

	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set(&bm, 3);
	cow_batch_bitmap_set(&bm, 77);
	cow_batch_bitmap_set(&bm, 200);

	COW_BATCH_BITMAP_FOR_EACH_SET(&bm, idx) {
		TEST_ASSERT(count < 3, "not too many iterations");
		TEST_ASSERT_EQ(idx, expected[count], "correct iteration order");
		count++;
	}
	TEST_ASSERT_EQ(count, 3, "iterated all set bits");
}

void test_edge_cases(void)
{
	cow_batch_bitmap_t bm;

	/* set_range with count=0 should be no-op */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 50, 0);
	TEST_ASSERT(cow_batch_bitmap_is_empty(&bm), "zero-count range is nop");

	/* set_range beyond limit */
	cow_batch_bitmap_zero(&bm);
	cow_batch_bitmap_set_range(&bm, 250, 100); /* goes past 256 */
	TEST_ASSERT_EQ(cow_batch_bitmap_popcount(&bm), 6, "clamped to 256");

	/* next_set from 256+ */
	TEST_ASSERT_EQ(cow_batch_bitmap_next_set(&bm, 256), -1, "past end = -1");
}

int main(void)
{
	printf("=== COW Batch Bitmap Tests ===\n");
	test_zero_and_fill();
	test_set_clear_test();
	test_popcount();
	test_next_set();
	test_set_range();
	test_clear_range();
	test_and_or_not();
	test_mask();
	test_is_full_upto();
	test_for_each_set_macro();
	test_edge_cases();
	TEST_SUMMARY();
}
```

## Test 2: test_spsc_queue.c

**File under test:** `criu/include/cow/spsc-queue.h` (macro-based)

The queue uses `xmalloc`/`xfree` — our harness shims cover that.

```c
#include "test_harness.h"
#include "cow/spsc-queue.h"

struct payload {
	int value;
};

DECLARE_SPSC_NODE(test, struct payload);

static struct test_spsc_node *q_head;
static struct test_spsc_node *q_tail;
static unsigned long q_size;

void test_init_and_empty(void)
{
	int rc = spsc_init(q_head, q_tail, q_size, struct test_spsc_node);
	TEST_ASSERT_EQ(rc, 0, "init succeeds");
	TEST_ASSERT_EQ(spsc_size(q_size), 0, "initial size = 0");
	TEST_ASSERT(!spsc_peek(q_head), "empty queue peek = false");

	struct payload *p = spsc_dequeue(q_head, q_size);
	TEST_ASSERT(p == NULL, "dequeue from empty = NULL");
}

void test_enqueue_dequeue_fifo(void)
{
	struct payload items[5];
	for (int i = 0; i < 5; i++) {
		items[i].value = i * 10;
		int rc = spsc_enqueue(q_tail, q_size, &items[i], struct test_spsc_node);
		TEST_ASSERT_EQ(rc, 0, "enqueue succeeds");
	}
	TEST_ASSERT_EQ(spsc_size(q_size), 5, "size = 5 after enqueue");
	TEST_ASSERT(spsc_peek(q_head), "non-empty peek = true");

	for (int i = 0; i < 5; i++) {
		struct payload *p = spsc_dequeue(q_head, q_size);
		TEST_ASSERT(p != NULL, "dequeue non-null");
		TEST_ASSERT_EQ(p->value, i * 10, "FIFO order preserved");
	}
	TEST_ASSERT_EQ(spsc_size(q_size), 0, "size = 0 after drain");
	TEST_ASSERT(!spsc_peek(q_head), "empty after drain");
}

/* Thread stress test: 1 producer, 1 consumer */
#include <pthread.h>
#define STRESS_COUNT 100000

static struct payload stress_items[STRESS_COUNT];
static int consumer_results[STRESS_COUNT];
static int consumer_count;

void *producer_thread(void *arg)
{
	for (int i = 0; i < STRESS_COUNT; i++) {
		stress_items[i].value = i;
		while (spsc_enqueue(q_tail, q_size, &stress_items[i],
				    struct test_spsc_node) != 0) {
			/* allocation failure - shouldn't happen with malloc */
		}
	}
	return NULL;
}

void *consumer_thread(void *arg)
{
	consumer_count = 0;
	while (consumer_count < STRESS_COUNT) {
		struct payload *p = spsc_dequeue(q_head, q_size);
		if (p) {
			consumer_results[consumer_count++] = p->value;
		}
	}
	return NULL;
}

void test_threaded_stress(void)
{
	/* Re-init queue */
	spsc_init(q_head, q_tail, q_size, struct test_spsc_node);

	pthread_t prod, cons;
	pthread_create(&prod, NULL, producer_thread, NULL);
	pthread_create(&cons, NULL, consumer_thread, NULL);
	pthread_join(prod, NULL);
	pthread_join(cons, NULL);

	/* Verify FIFO ordering */
	int ordered = 1;
	for (int i = 0; i < STRESS_COUNT; i++) {
		if (consumer_results[i] != i) {
			ordered = 0;
			break;
		}
	}
	TEST_ASSERT(ordered, "threaded: FIFO ordering preserved across 100K items");
	TEST_ASSERT_EQ(spsc_size(q_size), 0, "threaded: queue empty at end");
}

void test_drain(void)
{
	spsc_init(q_head, q_tail, q_size, struct test_spsc_node);
	struct payload *items = malloc(3 * sizeof(struct payload));
	for (int i = 0; i < 3; i++) {
		items[i].value = i;
		spsc_enqueue(q_tail, q_size, &items[i], struct test_spsc_node);
	}

	/* drain frees nodes but calls free_entry_fn on entries */
	int freed_count = 0;
	/* We can't easily test drain without a custom free_fn,
	 * but we can at least verify it doesn't crash */
	spsc_drain(q_head, free); /* free is a no-op for stack items, but tests the walk */
	TEST_ASSERT(q_head == NULL, "drain sets head to NULL");
	free(items);
}

int main(void)
{
	printf("=== SPSC Queue Tests ===\n");
	test_init_and_empty();
	test_enqueue_dequeue_fifo();
	test_threaded_stress();
	test_drain();
	TEST_SUMMARY();
}
```

## Test 3: test_mpsc_queue.c

**File under test:** `criu/include/cow/mpsc-queue.h`

Same structure as SPSC but with N producer threads:

```c
#include "test_harness.h"
#include "cow/mpsc-queue.h"
#include <pthread.h>

struct payload { int value; int producer_id; };
DECLARE_MPSC_NODE(test, struct payload);

static struct test_mpsc_node *q_head;
static struct test_mpsc_node *q_tail;
static unsigned long q_size;

/* Tests: */
/* 1. test_init_and_empty - same as SPSC */
/* 2. test_single_producer_fifo - verify ordering with 1 producer */
/* 3. test_multi_producer_completeness:
 *    - 8 producers, each enqueuing 10000 items tagged with producer_id
 *    - Single consumer drains all
 *    - Verify: all 80000 items received, per-producer ordering preserved
 *      (items from same producer arrive in order, items from different
 *       producers may interleave)
 */
/* 4. test_size_accuracy - approximate counter matches total in/out */
/* 5. test_drain - after producers stop, drain frees everything */

#define NUM_PRODUCERS    8
#define ITEMS_PER_PROD   10000
#define TOTAL_ITEMS      (NUM_PRODUCERS * ITEMS_PER_PROD)

static struct payload all_items[TOTAL_ITEMS];

void *mpsc_producer(void *arg)
{
	int id = (int)(long)arg;
	for (int i = 0; i < ITEMS_PER_PROD; i++) {
		int idx = id * ITEMS_PER_PROD + i;
		all_items[idx].value = i;
		all_items[idx].producer_id = id;
		while (mpsc_enqueue(q_tail, q_size, &all_items[idx],
				    struct test_mpsc_node) != 0) {}
	}
	return NULL;
}

void test_multi_producer(void)
{
	mpsc_init(q_head, q_tail, q_size, struct test_mpsc_node);

	pthread_t threads[NUM_PRODUCERS];
	for (int i = 0; i < NUM_PRODUCERS; i++)
		pthread_create(&threads[i], NULL, mpsc_producer, (void *)(long)i);
	for (int i = 0; i < NUM_PRODUCERS; i++)
		pthread_join(threads[i], NULL);

	/* Consume all */
	int per_prod_last[NUM_PRODUCERS];
	memset(per_prod_last, -1, sizeof(per_prod_last));
	int total = 0;
	int ordering_ok = 1;

	struct payload *p;
	while ((p = mpsc_dequeue(q_head, q_size)) != NULL) {
		int pid = p->producer_id;
		if (p->value <= per_prod_last[pid]) {
			ordering_ok = 0; /* per-producer ordering violated */
		}
		per_prod_last[pid] = p->value;
		total++;
	}

	TEST_ASSERT_EQ(total, TOTAL_ITEMS, "all items received");
	TEST_ASSERT(ordering_ok, "per-producer FIFO ordering preserved");
	TEST_ASSERT_EQ(spsc_size(q_size), 0, "queue empty at end");
}
```

## Test 4: test_page_pool.c

**File under test:** `criu/cow/page-pool.c`

This one requires the actual page-pool.c to be compiled (it uses mmap).
Link against the real source file.

```c
/* Key tests: */
/* 1. thread_init + get returns PAGE_SIZE-aligned, non-NULL pointer */
/* 2. get_pages(N) returns N contiguous pages */
/* 3. put decrements refcount; after all puts, chunk is freed */
/* 4. multi-threaded: 8 threads each allocating 10000 pages - no overlaps */
/* 5. chunk_id lookup: get a page, get_chunk_id matches expectations */
/* 6. get_chunk returns COW_ALLOC_BATCH (256) pages */
```

Build note: compile with `-DCOW_CHUNK_SIZE=(4*1024*1024)` for tests to use
smaller 4MB chunks so tests run fast without using 256MB.

Alternatively, the test can just exercise the normal 64MB chunks but limit
the number of allocations.

## Test 5: test_unmapped_tracker.c

**File under test:** `criu/cow/unmapped-tracker.c`

Needs: list.h, xmalloc shims, pthread.

```c
/* Key tests: */
/* 1. init/destroy lifecycle - no leaks (valgrind) */
/* 2. mark single page, is_unmapped returns true */
/* 3. mark range of 100 pages, verify each is_unmapped */
/* 4. clear removes page from tracking */
/* 5. page not marked returns false */
/* 6. double-init is safe (returns 0) */
/* 7. operations before init are safe (no crash) */
/* 8. multi-threaded: 4 threads marking different ranges concurrently */
/* 9. hash collision: multiple pages hashing to same bucket */
```

## Test 6: test_page_state_tracker.c

**File under test:** `criu/cow/page-state-tracker.c`

Must compile with `-DCONFIG_PAGE_STATE_TRACKER` to enable the code.

```c
/* Key tests: */
/* 1. init/destroy lifecycle */
/* 2. set/get basic state transitions */
/* 3. history tracking: set multiple states, verify history */
/* 4. CRC storage: set_with_crc, verify check_crc matches */
/* 5. mark_range_unmapped: marks all pages in range */
/* 6. verify_all_terminal: passes when all in terminal states */
/* 7. verify_all_terminal: fails when non-terminal state exists */
/* 8. concurrent: multiple threads updating different addresses */
```

## Makefile

```makefile
CRIU_SRC := ../../../criu
CRIU_INC := $(CRIU_SRC)/include
COMMON_INC := ../../../include

CFLAGS := -Wall -Wextra -g -O2 -pthread \
           -I$(CRIU_INC) -I$(COMMON_INC) -I. \
           -DPAGE_SIZE=4096 -DPAGE_SHIFT=12

TESTS := test_batch_bitmap test_spsc_queue test_mpsc_queue \
         test_page_pool test_unmapped_tracker

# Header-only tests (no extra .c files needed)
HEADER_ONLY_TESTS := test_batch_bitmap test_spsc_queue test_mpsc_queue

# Tests that link against COW source files
test_page_pool: test_page_pool.c $(CRIU_SRC)/cow/page-pool.c
	$(CC) $(CFLAGS) -o $@ $< $(CRIU_SRC)/cow/page-pool.c -lpthread

test_unmapped_tracker: test_unmapped_tracker.c $(CRIU_SRC)/cow/unmapped-tracker.c
	$(CC) $(CFLAGS) -o $@ $< $(CRIU_SRC)/cow/unmapped-tracker.c -lpthread

# Header-only tests
$(HEADER_ONLY_TESTS): %: %.c
	$(CC) $(CFLAGS) -o $@ $< -lpthread

all: $(TESTS)

check: all
	@failures=0; \
	for t in $(TESTS); do \
		echo "--- Running $$t ---"; \
		./$$t || failures=$$((failures + 1)); \
	done; \
	echo ""; \
	if [ $$failures -eq 0 ]; then \
		echo "ALL TESTS PASSED"; \
	else \
		echo "$$failures TEST(S) FAILED"; \
		exit 1; \
	fi

clean:
	rm -f $(TESTS)

.PHONY: all check clean
```

## Implementation Notes

1. **Include path issues:** The COW headers include other CRIU headers like
   `"page.h"`, `"int.h"`, `"criu-log.h"`, `"xmalloc.h"`, `"common/bug.h"`,
   `"common/list.h"`. The test harness must either:
   - Add include paths so these resolve (`-I$(CRIU_INC) -I$(COMMON_INC)`)
   - Or create local shim headers for the ones that don't compile standalone

2. **For page-pool tests:** The code uses `mmap` with large alignment. This
   works on Linux without root. On macOS (development), these tests may need
   `#ifdef __linux__` guards or be skipped.

3. **For queue tests:** The queues use `typeof()` (GCC extension) — compile
   with `-std=gnu11` or just `gcc` defaults.

4. **Valgrind:** Run `valgrind --leak-check=full ./test_*` to catch leaks,
   especially in page pool and unmapped tracker.

5. **Thread sanitizer:** Compile with `-fsanitize=thread` to detect data races
   in the lock-free queue tests and multi-threaded pool tests.

## Priority Order

1. `test_batch_bitmap` — zero dependencies, fast to implement, catches bit-twiddling bugs
2. `test_spsc_queue` — critical lock-free code, catches ABA/ordering bugs
3. `test_mpsc_queue` — same but multi-producer
4. `test_page_pool` — catches mmap/refcount bugs
5. `test_unmapped_tracker` — catches hash table + locking bugs
6. `test_page_state_tracker` — catches state machine bugs (needs CONFIG flag)
