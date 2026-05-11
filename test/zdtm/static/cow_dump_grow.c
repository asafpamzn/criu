#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "zdtmtst.h"
#include "cow_dump_util.h"

const char *test_doc = "--cow-dump growth test: start with a large (30 GB by "
		       "default) anon-private region, fill it, then during "
		       "Phase 2 a background thread both (a) dirties pages "
		       "in the original region (exercises WP-fault) and "
		       "(b) mmaps a second equally-large region and fills "
		       "it (exercises new-VMA + large-VMA growth). After "
		       "restore, both regions must be resident and every "
		       "page must carry one of the expected markers with no "
		       "torn writes.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

/*
 * Tunable via build-time -DCOW_GROW_INITIAL_MB=N.
 * Default 30 GB initial + 30 GB grown-during-Phase-2 = 60 GB peak.
 * At 60 GB the test takes ~10 minutes wall on r7g.16xlarge, dominated
 * by mmap/memset setup and post-restore byte-walk verification; the
 * actual CRIU pipeline is a small fraction of that.
 */
#ifndef COW_GROW_INITIAL_MB
#define COW_GROW_INITIAL_MB	30720		/* 30 GB */
#endif
#define INITIAL_MB		COW_GROW_INITIAL_MB
#define GROW_MB			COW_GROW_INITIAL_MB
#define INITIAL_BYTES		((size_t)INITIAL_MB << 20)
#define GROW_BYTES		((size_t)GROW_MB << 20)
#define INITIAL_PAGES		(INITIAL_BYTES / PAGE_SIZE)
#define GROW_PAGES		(GROW_BYTES / PAGE_SIZE)

#define MARKER_INITIAL		0x11
#define MARKER_GROW		0x22
#define MARKER_DIRTY		0x33

#define DRAIN_TIMEOUT_MS_PER_GB	10000UL

static unsigned char *initial_region;
static unsigned char *grown_region;			/* published by grower */
static atomic_int    stop_workers;

/*
 * Grower thread: runs for the whole post-test_daemon period. It does
 * two independent jobs:
 *
 *   - Allocate a second region of GROW_BYTES and fill it with
 *     MARKER_GROW. Published to `grown_region` only after fill completes.
 *
 *   - Repeatedly dirty a subset of pages in the initial region to
 *     MARKER_DIRTY, so the COW WP-fault path is exercised on pages
 *     that were already in the Phase-1 VMA snapshot.
 *
 * The thread is frozen along with the rest of the process at the start
 * of Phase 1, unfrozen at the start of Phase 2, and re-frozen at
 * Phase 3; then restored, and runs again briefly until main() flips
 * stop_workers.
 */
static void *grower_thread(void *arg)
{
	unsigned char *buf;
	size_t i;

	/* Let Phase 1 seize/unfreeze settle before we do large allocations. */
	usleep(50 * 1000);

	buf = mmap(NULL, GROW_BYTES, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (buf == MAP_FAILED) {
		pr_perror("grower mmap %zu bytes", GROW_BYTES);
		return (void *)(intptr_t)-1;
	}

	/*
	 * Fill in page-sized chunks so CRIU's Phase 2 has plenty of WP
	 * fault activity to track. Bail out early if someone stopped us
	 * (post-restore teardown).
	 */
	for (i = 0; i < GROW_BYTES; i += PAGE_SIZE) {
		if (atomic_load(&stop_workers))
			break;
		memset(buf + i, MARKER_GROW, PAGE_SIZE);
	}

	/* Publish only after fill — readers see a fully-initialized region. */
	grown_region = buf;

	/*
	 * Keep churning the initial region until told to stop: dirties
	 * every 256th page to MARKER_DIRTY. Bounded rate so we don't
	 * starve CRIU's scanners.
	 */
	while (!atomic_load(&stop_workers)) {
		for (i = 0; i < INITIAL_BYTES; i += PAGE_SIZE * 256) {
			if (atomic_load(&stop_workers))
				break;
			memset(initial_region + i, MARKER_DIRTY, PAGE_SIZE);
		}
		usleep(1000);
	}
	return NULL;
}

static int verify_region(const char *name, unsigned char *base,
			 size_t nr_pages, const unsigned char *ok_markers,
			 size_t nr_ok_markers)
{
	size_t p, j, k;
	unsigned int bad_marker = 0, torn = 0;

	for (p = 0; p < nr_pages; p++) {
		unsigned char *page = base + p * PAGE_SIZE;
		unsigned char m = page[0];
		int ok = 0;

		for (k = 0; k < nr_ok_markers; k++) {
			if (m == ok_markers[k]) {
				ok = 1;
				break;
			}
		}
		if (!ok) {
			if (bad_marker < 8)
				test_msg("%s page %zu: marker 0x%02x not in allowed set\n",
					 name, p, m);
			bad_marker++;
			continue;
		}
		for (j = 1; j < PAGE_SIZE; j++) {
			if (page[j] != m) {
				if (torn < 8)
					test_msg("%s page %zu: torn at offset %zu (0x%02x != 0x%02x)\n",
						 name, p, j, page[j], m);
				torn++;
				break;
			}
		}
	}

	if (bad_marker || torn) {
		test_msg("%s summary: bad_marker=%u torn=%u pages=%zu\n",
			 name, bad_marker, torn, nr_pages);
		return -1;
	}
	test_msg("%s OK: all %zu pages have a valid marker and are uniform\n",
		 name, nr_pages);
	return 0;
}

int main(int argc, char **argv)
{
	pthread_t th;
	size_t i;
	int initial_errors = 0, grown_errors = 0;
	unsigned long drain_ms;
	unsigned char initial_ok[] = { MARKER_INITIAL, MARKER_DIRTY };
	unsigned char grown_ok[]   = { MARKER_GROW };

	test_init(argc, argv);

	test_msg("cow_dump_grow: INITIAL=%u MB, GROW=%u MB, peak=%u MB\n",
		 (unsigned)INITIAL_MB, (unsigned)GROW_MB,
		 (unsigned)(INITIAL_MB + GROW_MB));

	initial_region = mmap(NULL, INITIAL_BYTES, PROT_READ | PROT_WRITE,
			      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (initial_region == MAP_FAILED) {
		pr_perror("initial mmap %zu bytes", INITIAL_BYTES);
		return 1;
	}
	for (i = 0; i < INITIAL_BYTES; i += PAGE_SIZE)
		memset(initial_region + i, MARKER_INITIAL, PAGE_SIZE);

	atomic_init(&stop_workers, 0);
	grown_region = NULL;

	if (pthread_create(&th, NULL, grower_thread, NULL)) {
		pr_perror("pthread_create grower");
		return 1;
	}

	test_daemon();
	test_waitsig();

	/* Wait for the grower to finish filling before stopping it. */
	while (!grown_region)
		usleep(10 * 1000);

	atomic_store(&stop_workers, 1);
	pthread_join(th, NULL);

	/* Drain gate for both regions. */
	drain_ms = DRAIN_TIMEOUT_MS_PER_GB *
		   ((INITIAL_MB + GROW_MB) / 1024UL + 1);
	if (cow_wait_for_drain(initial_region, INITIAL_BYTES,
			       (unsigned int)drain_ms) < 0) {
		fail("initial region drain did not complete within %lu ms",
		     drain_ms);
		return 1;
	}
	if (cow_wait_for_drain(grown_region, GROW_BYTES,
			       (unsigned int)drain_ms) < 0) {
		fail("grown region drain did not complete within %lu ms",
		     drain_ms);
		return 1;
	}

	if (verify_region("initial", initial_region, INITIAL_PAGES,
			  initial_ok, sizeof(initial_ok)) < 0)
		initial_errors = 1;
	if (verify_region("grown", grown_region, GROW_PAGES,
			  grown_ok, sizeof(grown_ok)) < 0)
		grown_errors = 1;

	if (initial_errors || grown_errors) {
		fail("region verification failed (initial=%d grown=%d)",
		     initial_errors, grown_errors);
		return 1;
	}

	pass();
	return 0;
}
