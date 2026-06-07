#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "--clone-dump concurrent VMA-churn stress test. Several "
		       "background threads continuously create, mprotect-flip and "
		       "destroy small VMAs (mmap/mprotect/munmap) for the whole "
		       "duration of the dump, while a large stable mapping must "
		       "survive byte-exact across the clone restore. This stresses "
		       "two clone paths under a realistic, constantly-changing VMA "
		       "layout (the kind of workload quiet single-threaded tests "
		       "miss): (1) the Phase-2 -> Phase-3 re-seize, which re-infects "
		       "the same pid (cr-dump.c reseize_pstree) reusing the parasite "
		       "abstract transport socket -- the bug that needed close() in "
		       "accept_tsock() (compel/src/lib/infect.c); and (2) VMA "
		       "teardown during the dump, which drives the buffer "
		       "remove_range() free path in clone-uffd.c. A correctness "
		       "regression in either surfaces as corrupted stable pages or a "
		       "failed dump.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define STABLE_PAGES	1024
#define CHURN_THREADS	4
#define CHURN_PAGES	8

static atomic_int stop_churn;

static void *churn_thread(void *arg)
{
	/*
	 * Continuously create and destroy small VMAs and flip their
	 * protections. The intent is to keep the VMA layout in constant
	 * flux across the entire dump so that the Phase 2 -> Phase 3
	 * re-seize (which re-infects the same pid, reusing the parasite
	 * abstract transport socket name) is exercised under load. None
	 * of these transient regions are verified.
	 */
	while (!atomic_load(&stop_churn)) {
		unsigned char *p;

		p = mmap(NULL, CHURN_PAGES * PAGE_SIZE, PROT_READ | PROT_WRITE,
			 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (p == MAP_FAILED)
			continue;
		memset(p, 0xa5, CHURN_PAGES * PAGE_SIZE);
		mprotect(p, PAGE_SIZE, PROT_READ);
		mprotect(p, PAGE_SIZE, PROT_READ | PROT_WRITE);
		munmap(p, CHURN_PAGES * PAGE_SIZE);
	}
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t th[CHURN_THREADS];
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

	atomic_init(&stop_churn, 0);
	for (i = 0; i < CHURN_THREADS; i++) {
		if (pthread_create(&th[i], NULL, churn_thread, NULL)) {
			pr_perror("pthread_create");
			return 1;
		}
	}

	test_daemon();
	test_waitsig();

	atomic_store(&stop_churn, 1);
	for (i = 0; i < CHURN_THREADS; i++)
		pthread_join(th[i], NULL);

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
		fail("%d stable pages corrupted after --clone-dump restore", errors);
		return 1;
	}

	pass();
	return 0;
}
