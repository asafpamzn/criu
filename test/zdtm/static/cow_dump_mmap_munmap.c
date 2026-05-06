#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "--cow-dump with a background thread doing mmap/munmap "
		       "cycles throughout dump (exercises REMOVE events and "
		       "Phase 3 new-VMA detection). The stable mapping must "
		       "survive byte-exact.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define STABLE_PAGES	512
#define CYCLE_PAGES	16

static atomic_int stop_mapper;

static void *mapper_thread(void *arg)
{
	/*
	 * Repeatedly mmap a small region, fill it, munmap it. Runs
	 * continuously; most iterations will happen during COW Phase 2
	 * (process running with WP active). Nothing is verified about
	 * these transient regions — we only want to exercise the
	 * mmap/munmap paths during the dump.
	 */
	while (!atomic_load(&stop_mapper)) {
		unsigned char *p;
		p = mmap(NULL, CYCLE_PAGES * PAGE_SIZE,
			 PROT_READ | PROT_WRITE,
			 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (p == MAP_FAILED)
			continue;
		memset(p, 0x5a, CYCLE_PAGES * PAGE_SIZE);
		munmap(p, CYCLE_PAGES * PAGE_SIZE);
	}
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t th;
	unsigned char *stable;
	unsigned long sz = STABLE_PAGES * PAGE_SIZE;
	int i, j, errors = 0;

	test_init(argc, argv);

	stable = mmap(NULL, sz, PROT_READ | PROT_WRITE,
		      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (stable == MAP_FAILED) {
		pr_perror("mmap stable");
		return 1;
	}
	for (i = 0; i < STABLE_PAGES; i++)
		memset(stable + i * PAGE_SIZE, (unsigned char)(i & 0xff), PAGE_SIZE);

	atomic_init(&stop_mapper, 0);
	if (pthread_create(&th, NULL, mapper_thread, NULL)) {
		pr_perror("pthread_create");
		return 1;
	}

	test_daemon();
	test_waitsig();

	atomic_store(&stop_mapper, 1);
	pthread_join(th, NULL);

	for (i = 0; i < STABLE_PAGES; i++) {
		unsigned char expected = (unsigned char)(i & 0xff);
		unsigned char *page = stable + i * PAGE_SIZE;

		for (j = 0; j < PAGE_SIZE; j++) {
			if (page[j] != expected) {
				test_msg("stable page %d offset %d: 0x%02x != 0x%02x\n",
					 i, j, page[j], expected);
				errors++;
				break;
			}
		}
	}

	if (errors) {
		fail("%d stable pages corrupted after --cow-dump restore", errors);
		return 1;
	}

	pass();
	return 0;
}
