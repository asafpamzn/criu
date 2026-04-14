/*
 * COW Configuration Constants
 *
 * This header consolidates all COW (Copy-on-Write) configuration
 * constants from the phased migration implementation. Constants
 * are organized by category for maintainability.
 *
 * All COW source files should include this header and use these
 * constants instead of defining their own or using magic numbers.
 *
 * Note: Source files must include "page.h" before this header
 * for PAGE_SIZE-dependent constants (COW_BATCH_SIZE, COW_PAGES_PER_CHUNK).
 */

#ifndef __CR_COW_CONF_H__
#define __CR_COW_CONF_H__

/* ================================================================
 * SECTION 1: Batch Transfer Configuration
 * ================================================================ */

/* Pages per batch for bulk transfer (256KB when PAGE_SIZE=4KB) */
#define COW_BATCH_PAGES			64
#define COW_BATCH_SIZE			(COW_BATCH_PAGES * PAGE_SIZE)

/* ================================================================
 * SECTION 2: Thread Count Configuration
 * ================================================================ */

/* Number of parallel P3 sender/receiver threads */
#define COW_NUM_P3_THREADS		20

/* Number of parallel scanner threads */
#define COW_NUM_SCANNERS		4

/* Number of background drain threads */
#define COW_NUM_DRAIN_THREADS		20

/* Number of fault worker threads */
#define COW_FAULT_WORKERS		4

/* Maximum threads for page pool */
#define COW_MAX_THREADS			32

/* ================================================================
 * SECTION 3: Memory Pool Configuration
 * ================================================================ */

/* Chunk sizes for page pool */
#define COW_CHUNK_SIZE			(256UL * 1024 * 1024)  /* 256MB per chunk */
#define COW_CHUNK_ALIGN			COW_CHUNK_SIZE
#define COW_CHUNK_ALIGN_MASK		(~(COW_CHUNK_ALIGN - 1))

/* Allocation batching */
#define COW_ALLOC_BATCH			64  /* Pages per allocation batch (256KB) */

/* Maximum chunks (2048 * 256MB = 512GB max memory) */
#define COW_MAX_POOL_CHUNKS		2048

/* Per-worker page pool size */
#define COW_PAGE_POOL_SIZE		256  /* 256 x 4KB = 1MB per worker */

/* Derived: pages per chunk */
#define COW_PAGES_PER_CHUNK		(COW_CHUNK_SIZE / PAGE_SIZE)

/* Write-protect chunk size for parallel application */
#define COW_WP_CHUNK_SIZE		(64UL * 1024 * 1024)  /* 64MB */

/* ================================================================
 * SECTION 4: Hash Table Configuration
 * ================================================================ */

/* Page buffer hash table (cow-uffd.c) */
#define COW_PAGE_BUFFER_HASH_BITS	20
#define COW_PAGE_BUFFER_HASH_SIZE	(1 << COW_PAGE_BUFFER_HASH_BITS)  /* 1M buckets */

/* Fine-grained locking for page buffer */
#define COW_NUM_HASH_LOCKS		8192
#define COW_BUCKETS_PER_LOCK		128  /* 1M / 8K = 128 buckets per lock */

/* Unrolled linked list node entry count */
#define COW_PAGE_NODE_ENTRIES		32

/* Page state tracker hash table */
#define COW_PAGE_STATE_HASH_BITS	18
#define COW_PAGE_STATE_HASH_SIZE	(1 << COW_PAGE_STATE_HASH_BITS)
#define COW_PAGE_STATE_MAX		10
#define COW_PAGE_STATE_HISTORY_SIZE	16  /* Max history entries per page */

/* Unmapped tracker hash table */
#define COW_NUM_UNMAPPED_LOCKS		512
#define COW_UNMAPPED_BUCKETS_PER_LOCK	128

/* ================================================================
 * SECTION 5: Convergence Thresholds
 * ================================================================ */

/* Dirty page scan freeze threshold (request freeze when below this) */
#define COW_DIRTY_SCAN_FREEZE_THRESHOLD	300000

/* Legacy per-thread convergence threshold */
#define COW_DIRTY_CONVERGENCE_THRESHOLD	50000

/* Low dirty pages threshold for extended sleep */
#define COW_LOW_DIRTY_THRESHOLD		1000

/* Max iterations before extended sleep */
#define COW_MAX_DIRTY_ITERATIONS	3

/* ================================================================
 * SECTION 6: Pre-read Configuration
 * ================================================================ */

/* Pre-read window: 8 pages before + faulting page + 7 pages after = 16 pages */
#define COW_PREREAD_BEFORE		8
#define COW_PREREAD_AFTER		7
#define COW_PREREAD_TOTAL		(COW_PREREAD_BEFORE + 1 + COW_PREREAD_AFTER)

/* ================================================================
 * SECTION 7: Timing Configuration (microseconds unless noted)
 * ================================================================ */

/* Short yield intervals */
#define COW_USLEEP_100US		100	/* 100us - queue empty, drain yield */
#define COW_USLEEP_1MS			1000	/* 1ms - UFFD unregister, dirty scan poll */
#define COW_USLEEP_10MS			10000	/* 10ms - bulk transfer poll */
#define COW_USLEEP_CONVERGENCE		30000	/* 30ms - convergence sleep */

/* Timeout values (milliseconds) */
#define COW_P3_ACCEPT_TIMEOUT_MS	5000	/* 5 seconds */

/* Stats/logging intervals (seconds) */
#define COW_STATS_PRINT_SEC		30	/* UFFD stats */
#define COW_DRAIN_PROGRESS_SEC		10	/* Drain progress */

/* ================================================================
 * SECTION 8: VMA Processing
 * ================================================================ */

/* Minimum VMA size for thread splitting (smaller VMAs go to thread 0) */
#define COW_MIN_VMA_SIZE_FOR_SPLIT	(256 * 1024)  /* 256KB */

/* PAGEMAP_SCAN max regions per call */
#define COW_PAGEMAP_SCAN_VEC_LEN	1000

/* Initial capacity for ranges arrays */
#define COW_INITIAL_RANGES_CAPACITY	64

/* UFFD unregister yield interval (every N VMAs) */
#define COW_UFFD_UNREGISTER_YIELD	10

/* Drain batch size in VMA processing */
#define COW_DRAIN_BATCH_SIZE		100

/* ================================================================
 * SECTION 9: Logging/Debug Thresholds
 * ================================================================ */

/* Sample rates for high-frequency logging (modulo values) */
#define COW_LOG_SAMPLE_1M		1000000	/* Every 1M operations */
#define COW_LOG_SAMPLE_100K		100000	/* Every 100K operations */
#define COW_LOG_SAMPLE_10K		10000	/* Every 10K operations */
#define COW_LOG_SAMPLE_1K		1000	/* Every 1K operations */

/* Progress logging intervals */
#define COW_PROGRESS_LOG_INTERVAL	1000

/* Debug exit frequency */
#define COW_EXIT_DEBUG_FREQUENCY	100

/* ================================================================
 * SECTION 10: Refcount Warning Thresholds (page-pool.c)
 * ================================================================ */

#define COW_REFCOUNT_LOW		1000
#define COW_REFCOUNT_MID		30000

/* ================================================================
 * SECTION 11: Cache Line Padding
 * ================================================================ */

/* Cache line size for struct padding to avoid false sharing */
#define COW_CACHE_LINE_SIZE		64

/* SPSC queue padding (2 cache lines) */
#define COW_SPSC_PADDING		128

#endif /* __CR_COW_CONF_H__ */
