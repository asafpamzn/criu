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
 * SECTION 0: Compile-time Feature Flags
 * ================================================================
 * These flags enable/disable optional COW features at compile time.
 * Uncomment to enable, comment out to disable.
 */

/*
 * CONFIG_PAGE_STATE_TRACKER - Enable page state tracking for debugging.
 * Tracks all state transitions and validates them to detect bugs.
 * Adds overhead, use only for debugging.
 */
// #define CONFIG_PAGE_STATE_TRACKER

/*
 * SCAN_COMPARE - Debug mode to compare BPF vs PAGEMAP_SCAN at freeze time.
 * When enabled, after freeze the code will:
 * 1. Drain BPF ring buffer and count unique dirty pages
 * 2. Run PAGEMAP_SCAN and count dirty pages
 * 3. Report pages that SCAN found but BPF missed
 * 4. Exit immediately (no page transfer)
 * Use this to debug why BPF might be missing pages.
 */
// #define SCAN_COMPARE

/*
 * COW_PRE_SCAN - Enable iterative dirty scanning before freeze.
 * When enabled: Scanners do iterative PAGEMAP_SCAN while process runs,
 *               waiting for dirty pages to converge below threshold before
 *               requesting freeze.
 * When disabled (default): Scanners wait for freeze signal immediately after
 *               bulk transfer completes, then do a single final PAGEMAP_SCAN
 *               on the frozen process. This is faster and simpler.
 */
// #define COW_PRE_SCAN

/*
 * CONFIG_HUNG_PAGE_TRACKER - Enable hung page detection.
 * Tracks pages that take too long to be processed.
 * Adds overhead, use only for debugging.
 */
// #define CONFIG_HUNG_PAGE_TRACKER

/*
 * CONFIG_COW_COMPARE - Enable process comparison during COW dump.
 * When enabled, PRIMARY and REPLICA compare process state after freeze.
 * Useful for debugging memory divergence issues.
 */
// #define CONFIG_COW_COMPARE

/*
 * CONFIG_COW_COMPARE_PAGES - Enable page hash comparison (slow).
 * When enabled, PRIMARY sends page hashes and REPLICA compares them.
 * This is very slow for large processes. Disable to only compare VMAs.
 * Requires CONFIG_COW_COMPARE to be enabled.
 */
// #define CONFIG_COW_COMPARE_PAGES

/*
 * CONFIG_COW_WAIT_REPLICA_TOUCH - Wait for touch file before proceeding.
 * When enabled, PRIMARY waits for /tmp/continue_replica file to exist
 * before unfreezing. Useful for manual debugging/inspection.
 */
// #define CONFIG_COW_WAIT_REPLICA_TOUCH

/*
 * COW_CONF_TODO_ASK_AVI_cow_seize_stop_parasite - Use fast parasite stop.
 * When enabled, uses compel_stop_daemon_fast() which skips rt_sigreturn
 * single-stepping. Measured at ~126us - not worth optimizing.
 * TODO: Ask Avi if this is actually needed.
 */
// #define COW_CONF_TODO_ASK_AVI_cow_seize_stop_parasite

/*
 * COW_CONF_TODO_ASK_AVI_network_lock - Enable network locking for COW mode.
 * Currently COW mode skips network_lock() entirely. This means:
 * 1. Process can do network I/O while running during Phase 2
 * 2. TCP connections are not locked/checkpointed in the traditional way
 *
 * Questions for Avi:
 * - Is this intentional? Process keeps running, so locking would block I/O.
 * - How does COW handle TCP connection state consistency?
 * - If process does network I/O between T1 and T3, does REPLICA get correct state?
 *
 * Timing shows network_lock takes ~0us, so perf is not the reason to skip it.
 * TODO: Ask Avi why we skip network lock in COW mode.
 */
// #define COW_CONF_TODO_ASK_AVI_network_lock

/*
 * COW_CONF_TODO_ASK_AVI_cow_seize_cure_parasite - Use local-only parasite cure.
 * When enabled, uses compel_cure_local() which skips remote munmap.
 * The theory was that restorer handles parasite cleanup anyway.
 *
 * However, actual timing shows compel_cure() only takes ~9.6ms.
 * The optimization may not be worth the complexity/risk.
 *
 * Questions for Avi:
 * - Is skipping remote munmap actually needed for COW correctness?
 * - Or was it just a perf optimization that's not worth ~9.6ms?
 * - Does the restorer actually clean up the parasite mapping?
 *
 * TODO: Ask Avi if this is actually needed.
 */
// #define COW_CONF_TODO_ASK_AVI_cow_seize_cure_parasite

/* ================================================================
 * SECTION 1: Batch Transfer Configuration
 * ================================================================ */

/* Pages per batch for bulk transfer (1MB when PAGE_SIZE=4KB) */
#define COW_BATCH_PAGES			256
#define COW_BATCH_SIZE			(COW_BATCH_PAGES * PAGE_SIZE)

/* ================================================================
 * SECTION 2: Thread Count Configuration
 * ================================================================ */

/*
 * Machine profiles - uncomment ONE to select thread counts.
 * SMALL: 4-core machines (4 scanners, 4 drain, 4 P3 but 1 active in bulk)
 * LARGE: 32+ core machines (20 scanners, 10 drain, 15 P3)
 */
/* #define COW_PROFILE_LARGE */
#define COW_PROFILE_SMALL

#ifdef COW_PROFILE_SMALL
#define COW_NUM_P3_THREADS		4
#define COW_NUM_P3_THREADS_BULK		2   /* Active P3 threads during bulk transfer */
#define COW_P3_SENDER_CPU		40   /* CPU to pin bulk senders to (share 1 core) */
#define COW_NUM_SCANNERS		4
#define COW_NUM_DRAIN_THREADS		4
#define COW_MAX_THREADS			16
#else /* COW_PROFILE_LARGE (default) */
#define COW_NUM_P3_THREADS		15
#define COW_NUM_P3_THREADS_BULK		15
#define COW_NUM_SCANNERS		20
#define COW_NUM_DRAIN_THREADS		10
#define COW_MAX_THREADS			33
#endif

/*
 * Number of queues per scanner (producer). Scanner i owns queues
 * [i*COW_QUEUES_PER_THREAD, (i+1)*COW_QUEUES_PER_THREAD). Total is
 * driven by the producer count so every queue has exactly one producer;
 * consumers (P3 threads) round-robin over the full set.
 */
#define COW_QUEUES_PER_THREAD		5
#define COW_TOTAL_QUEUES		(COW_NUM_SCANNERS * COW_QUEUES_PER_THREAD)

/* Maximum epoll fds for COW lazy-pages */
#define COW_MAX_EPOLL_FDS		128

/* ================================================================
 * SECTION 3: Memory Pool Configuration
 * ================================================================ */

/* Chunk sizes for page pool */
#define COW_CHUNK_SIZE			(64UL * 1024 * 1024)   /* 64MB per chunk */
#define COW_CHUNK_ALIGN			COW_CHUNK_SIZE
#define COW_CHUNK_ALIGN_MASK		(~(COW_CHUNK_ALIGN - 1))

/* Allocation batching */
#define COW_ALLOC_BATCH			256  /* Pages per allocation batch (1MB) */

/* Maximum chunks (8192 * 64MB = 512GB max memory) */
#define COW_MAX_POOL_CHUNKS		8192

/* Per-worker page pool size */
#define COW_PAGE_POOL_SIZE		256  /* 256 x 4KB = 1MB per worker */

/* Derived: pages per chunk */
#define COW_PAGES_PER_CHUNK		(COW_CHUNK_SIZE / PAGE_SIZE)

/* Write-protect chunk size for parallel application */
#define COW_WP_CHUNK_SIZE		(64UL * 1024 * 1024)  /* 64MB */

/* ================================================================
 * SECTION 4: Hash Table Configuration
 * ================================================================ */

/*
 * Batch buffer: hash table stores 1MB-aligned entries.
 * Each entry holds 256 contiguous pages with a bitmap tracking validity.
 * Drain can UFFDIO_COPY 1MB at once instead of per-page.
 */
#define COW_BATCH_SHIFT			20  /* log2(COW_BATCH_SIZE) = log2(1MB) */
#define COW_BATCH_ALIGN_MASK		(~((1UL << COW_BATCH_SHIFT) - 1))

/* 256K buckets: ~5 entries/bucket at 300GB (1.2M entries) */
#define COW_BATCH_BUFFER_HASH_BITS	18
#define COW_BATCH_BUFFER_HASH_SIZE	(1 << COW_BATCH_BUFFER_HASH_BITS)

/* Fine-grained locking for batch buffer - 1:1 lock per bucket to minimize contention */
#define COW_BATCH_NUM_HASH_LOCKS	COW_BATCH_BUFFER_HASH_SIZE
#define COW_BATCH_BUCKETS_PER_LOCK	1

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
#define COW_DIRTY_SCAN_FREEZE_THRESHOLD	10000000

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
 * SECTION 9: Work-Stealing Configuration
 * ================================================================ */

/* Work chunk size for bulk transfer load balancing (32MB) */
#define COW_WORK_CHUNK_SIZE		(32UL * 1024 * 1024)
#define COW_WORK_CHUNK_PAGES		(COW_WORK_CHUNK_SIZE / PAGE_SIZE)

/* Maximum work queue items for bulk transfer */
#define COW_MAX_WORK_ITEMS		16384

/* ================================================================
 * SECTION 10: Logging/Debug Thresholds
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
 * SECTION 11: Refcount Warning Thresholds (page-pool.c)
 * ================================================================ */

#define COW_REFCOUNT_LOW		1000
#define COW_REFCOUNT_MID		30000

/* ================================================================
 * SECTION 12: Cache Line Padding
 * ================================================================ */

/* Cache line size for struct padding to avoid false sharing */
#define COW_CACHE_LINE_SIZE		64

/* SPSC queue padding (2 cache lines) */
#define COW_SPSC_PADDING		128

/* ================================================================
 * SECTION 13: eBPF Dirty Page Tracker Configuration
 * ================================================================ */

/*
 * BPF ring buffer size for dirty page addresses.
 * Each entry is 8 bytes (u64 address), so 64MB = 8M entries.
 * If this fills up, we fall back to PAGEMAP_SCAN.
 */
#define COW_BPF_RING_SIZE		(64UL * 1024 * 1024)  /* 64MB */

/*
 * Initial capacity for BPF drain address array.
 * Will be dynamically grown if needed.
 */
#define COW_BPF_DRAIN_INITIAL_CAP	65536

#endif /* __CR_COW_CONF_H__ */
