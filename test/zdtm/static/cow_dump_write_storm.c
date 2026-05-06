#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "--cow-dump under a write storm: N writer threads fill "
		       "their regions with monotonically changing markers "
		       "throughout dump. After restore, each page must be "
		       "uniformly filled with a marker from the writer's "
		       "valid range (catches torn writes / lost pages).";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define NR_WRITERS	4
#define REGION_PAGES	1024	/* 4MB per writer — keeps RAM small on CI */
#define MARKER_MIN	1
#define MARKER_MAX	32

struct writer {
	int id;
	unsigned char *region;
};

static atomic_int stop_writers;

static void *writer_thread(void *arg)
{
	struct writer *w = arg;
	unsigned long sz = REGION_PAGES * PAGE_SIZE;
	unsigned char marker = MARKER_MIN;

	while (!atomic_load(&stop_writers)) {
		memset(w->region, marker, sz);
		marker++;
		if (marker > MARKER_MAX)
			marker = MARKER_MIN;
	}
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t threads[NR_WRITERS];
	struct writer writers[NR_WRITERS];
	unsigned long sz = REGION_PAGES * PAGE_SIZE;
	int i, p, j, errors = 0;

	test_init(argc, argv);

	for (i = 0; i < NR_WRITERS; i++) {
		writers[i].id = i;
		writers[i].region = mmap(NULL, sz, PROT_READ | PROT_WRITE,
					 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (writers[i].region == MAP_FAILED) {
			pr_perror("mmap writer %d", i);
			return 1;
		}
		memset(writers[i].region, MARKER_MIN, sz);
	}

	atomic_init(&stop_writers, 0);
	for (i = 0; i < NR_WRITERS; i++) {
		if (pthread_create(&threads[i], NULL, writer_thread, &writers[i])) {
			pr_perror("pthread_create %d", i);
			return 1;
		}
	}

	test_daemon();
	test_waitsig();

	atomic_store(&stop_writers, 1);
	for (i = 0; i < NR_WRITERS; i++)
		pthread_join(threads[i], NULL);

	/*
	 * Invariant verification. We cannot predict which marker each
	 * page carries (writers ran freely before the freeze, and again
	 * after restore before we signalled stop). But every page must:
	 *   1. Carry a marker in [MARKER_MIN, MARKER_MAX] — else data
	 *      was lost or corrupted.
	 *   2. Be uniformly filled with that marker — else CRIU captured
	 *      a torn write at the WP boundary.
	 */
	for (i = 0; i < NR_WRITERS; i++) {
		for (p = 0; p < REGION_PAGES; p++) {
			unsigned char *page = writers[i].region + p * PAGE_SIZE;
			unsigned char m = page[0];

			if (m < MARKER_MIN || m > MARKER_MAX) {
				test_msg("writer %d page %d: bad marker 0x%02x\n",
					 i, p, m);
				errors++;
				continue;
			}
			for (j = 1; j < PAGE_SIZE; j++) {
				if (page[j] != m) {
					test_msg("writer %d page %d: torn at offset %d "
						 "(0x%02x != 0x%02x)\n",
						 i, p, j, page[j], m);
					errors++;
					break;
				}
			}
		}
	}

	if (errors) {
		fail("%d bad pages after write-storm --cow-dump restore", errors);
		return 1;
	}

	pass();
	return 0;
}
