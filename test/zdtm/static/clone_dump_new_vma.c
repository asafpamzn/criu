#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "zdtmtst.h"
#include "clone_dump_util.h"

const char *test_doc = "--clone-dump new-VMA handling: a background thread "
		       "mmaps new anon-private regions throughout Phase 2 "
		       "(no munmap). After restore the test classifies each "
		       "tracked VMA as present+correct, present+corrupt, or "
		       "missing (mincore=ENOMEM). CRIU's own dump.log warns "
		       "about Phase-2-era VMA metadata being missing from "
		       "the skeleton (see criu/cr-dump.c:~2892); this test "
		       "pins down the observable effect on the restored "
		       "process. PASS criteria: stable region intact, no "
		       "data corruption on surviving VMAs. Missing entries "
		       "are reported but not fatal (documented limitation).";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define STABLE_PAGES	256	/* 1 MB stable region */
#define NEW_VMA_PAGES	4	/* 16 KB per new VMA */
#define TRACKED_MAX	1024	/* upper bound on tracked entries */
#define DRAIN_TIMEOUT	10000	/* ms */

/*
 * Force every new mmap to land at its own hinted address far from the
 * others so the kernel can't merge adjacent VMAs. Without this, 230
 * consecutive 16 KB mmaps typically coalesce into one big anon VMA,
 * and CRIU's "new VMA region" detection sees just one thing, not 230.
 * We stride through a 1 TB virtual window in 2 MB steps — no two
 * tracked entries land within an even-distant page of each other.
 */
#define HINT_BASE	((unsigned long)0x500000000000UL)	/* 80 TB */
#define HINT_STRIDE	((unsigned long)(2UL << 20))		/* 2 MB */

struct tracked_vma {
	void *ptr;
	size_t len;
	unsigned char marker;
};

/*
 * The tracked-list array lives inside the stable region so that it
 * itself is guaranteed to be in the Phase-1 skeleton and therefore
 * restored. Each entry in the array then points at a VMA that may or
 * may not survive, which is what we're measuring.
 */
static struct tracked_vma *g_tracked;
static atomic_uint g_next;
static atomic_int g_stop;

static void *mapper_thread(void *arg)
{
	while (!atomic_load(&g_stop)) {
		unsigned int idx;
		void *p;
		unsigned char m;

		idx = atomic_fetch_add(&g_next, 1);
		if (idx >= TRACKED_MAX) {
			/* Bounded: stop claiming slots, idle. */
			atomic_fetch_sub(&g_next, 1);
			usleep(10 * 1000);
			continue;
		}

		{
			void *hint = (void *)(HINT_BASE + (unsigned long)idx * HINT_STRIDE);
			p = mmap(hint, NEW_VMA_PAGES * PAGE_SIZE,
				 PROT_READ | PROT_WRITE,
				 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			if (p == MAP_FAILED) {
				atomic_fetch_sub(&g_next, 1);
				usleep(1000);
				continue;
			}
		}

		/* Marker: bias high bit to 1 so the zero-page and the
		 * stable-region's low-index pattern bytes don't accidentally
		 * match a VMA marker during verification. */
		m = (unsigned char)((idx & 0x7f) | 0x80);
		memset(p, m, NEW_VMA_PAGES * PAGE_SIZE);

		/* Publish AFTER memset. A reader that sees ptr != NULL is
		 * guaranteed to see the fully-initialized region. */
		g_tracked[idx].len = NEW_VMA_PAGES * PAGE_SIZE;
		g_tracked[idx].marker = m;
		g_tracked[idx].ptr = p;

		/* Throttle to ~2000 mmap/s so Phase 2 always sees some. */
		usleep(500);
	}
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t th;
	unsigned char *stable;
	unsigned long stable_sz = STABLE_PAGES * PAGE_SIZE;
	unsigned int nr_tracked;
	unsigned int i, j;
	unsigned int stable_errors = 0;
	unsigned int new_present_ok = 0;
	unsigned int new_present_corrupt = 0;
	unsigned int new_missing = 0;
	unsigned int new_uninitialized = 0;
	size_t tlist_bytes;
	size_t tlist_pages;

	test_init(argc, argv);

	stable = mmap(NULL, stable_sz, PROT_READ | PROT_WRITE,
		      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (stable == MAP_FAILED) {
		pr_perror("mmap stable");
		return 1;
	}

	/* Fill the whole stable region with an index-derived pattern, then
	 * lay the tracked-list array at offset 0. The verify loop skips
	 * the pages that overlap the array (the mapper writes there, so
	 * the pattern is deliberately overwritten). */
	for (i = 0; i < STABLE_PAGES; i++)
		memset(stable + i * PAGE_SIZE, (unsigned char)(i & 0xff), PAGE_SIZE);

	g_tracked = (struct tracked_vma *)stable;
	memset(g_tracked, 0, TRACKED_MAX * sizeof(*g_tracked));
	atomic_init(&g_next, 0);
	atomic_init(&g_stop, 0);

	tlist_bytes = (size_t)TRACKED_MAX * sizeof(*g_tracked);
	tlist_pages = (tlist_bytes + PAGE_SIZE - 1) / PAGE_SIZE;

	if (pthread_create(&th, NULL, mapper_thread, NULL)) {
		pr_perror("pthread_create");
		return 1;
	}

	test_daemon();
	test_waitsig();

	atomic_store(&g_stop, 1);
	pthread_join(th, NULL);

	nr_tracked = atomic_load(&g_next);
	if (nr_tracked > TRACKED_MAX)
		nr_tracked = TRACKED_MAX;

	/* Drain gate for the stable region (which holds the tracked list). */
	if (clone_wait_for_drain(stable, stable_sz, DRAIN_TIMEOUT) < 0) {
		fail("stable-region drain did not complete within %d ms", DRAIN_TIMEOUT);
		return 1;
	}

	/* Stable-region bytewise check, skipping the tracked-list pages. */
	for (i = tlist_pages; i < STABLE_PAGES; i++) {
		unsigned char expected = (unsigned char)(i & 0xff);
		unsigned char *page = stable + i * PAGE_SIZE;

		for (j = 0; j < PAGE_SIZE; j++) {
			if (page[j] != expected) {
				test_msg("stable page %u offset %u: 0x%02x != 0x%02x\n",
					 i, j, page[j], expected);
				stable_errors++;
				break;
			}
		}
	}

	/* Classify each tracked VMA. */
	for (i = 0; i < nr_tracked; i++) {
		struct tracked_vma *tv = &g_tracked[i];
		size_t npages;
		unsigned char vec[NEW_VMA_PAGES];
		unsigned char *base;
		int corrupt = 0;
		size_t p;

		/* Entry may be still-uninitialized if the mapper was pre-empted
		 * between claiming the index and the publishing store. These
		 * are not failures — we just didn't finish recording them. */
		if (tv->ptr == NULL) {
			new_uninitialized++;
			continue;
		}

		npages = tv->len / PAGE_SIZE;
		if (mincore(tv->ptr, tv->len, (void *)vec) == -1) {
			if (errno == ENOMEM) {
				/* Not mapped in the restored process. */
				new_missing++;
				continue;
			}
			pr_perror("mincore idx=%u", i);
			continue;
		}

		base = tv->ptr;
		for (p = 0; p < npages; p++) {
			if (base[p * PAGE_SIZE] != tv->marker) {
				corrupt = 1;
				break;
			}
		}
		if (corrupt)
			new_present_corrupt++;
		else
			new_present_ok++;
	}

	test_msg("RESULT stable_errors=%u tracked=%u present_ok=%u "
		 "present_corrupt=%u missing=%u uninitialized=%u\n",
		 stable_errors, nr_tracked, new_present_ok,
		 new_present_corrupt, new_missing, new_uninitialized);

	/*
	 * Pass/fail policy:
	 *
	 *   stable_errors        → FAIL (CRIU broke baseline data integrity)
	 *   nr_tracked == 0      → FAIL (test didn't exercise anything)
	 *   new_present_corrupt  → FAIL (CRIU sent wrong data for a surviving VMA)
	 *   new_missing > 0      → PASS (documented limitation — see dump.log
	 *                          for "Found N new VMA regions since Phase 1")
	 */

	if (stable_errors) {
		fail("%u stable-region pages corrupted", stable_errors);
		return 1;
	}
	if (nr_tracked == 0) {
		fail("no tracked VMAs recorded — test did not exercise Phase 2");
		return 1;
	}
	if (new_present_corrupt) {
		fail("%u surviving new VMAs have corrupt data", new_present_corrupt);
		return 1;
	}

	pass();
	return 0;
}
