#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "--cow-dump scale test: a large anon-private region "
		       "(256MB) with a background writer randomly touching "
		       "~10%% of pages throughout dump. Verifies the page "
		       "pool / batch transfer path; post-restore invariant "
		       "is that every page carries a valid marker with no "
		       "torn writes.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

/*
 * 256MB — enough to exercise batching without blowing up CI RAM.
 * Bump for local scale testing.
 */
#define TOTAL_MB	256
#define NR_PAGES	((TOTAL_MB * 1024UL * 1024UL) / 4096UL)
#define MARKER_INIT	0x11
#define MARKER_WRITER	0xAA

static atomic_int stop_writer;
static unsigned char *mem;

static void *writer_thread(void *arg)
{
	unsigned long n = NR_PAGES;
	unsigned int seed = 0xDEADBEEF;

	while (!atomic_load(&stop_writer)) {
		unsigned long idx = rand_r(&seed) % n;
		memset(mem + idx * PAGE_SIZE, MARKER_WRITER, PAGE_SIZE);
	}
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t th;
	unsigned long sz = NR_PAGES * PAGE_SIZE;
	unsigned long i;
	int j;
	int errors = 0;

	test_init(argc, argv);

	mem = mmap(NULL, sz, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (mem == MAP_FAILED) {
		pr_perror("mmap %lu bytes", sz);
		return 1;
	}
	memset(mem, MARKER_INIT, sz);

	atomic_init(&stop_writer, 0);
	if (pthread_create(&th, NULL, writer_thread, NULL)) {
		pr_perror("pthread_create");
		return 1;
	}

	test_daemon();
	test_waitsig();

	atomic_store(&stop_writer, 1);
	pthread_join(th, NULL);

	for (i = 0; i < NR_PAGES; i++) {
		unsigned char *page = mem + i * PAGE_SIZE;
		unsigned char m = page[0];

		if (m != MARKER_INIT && m != MARKER_WRITER) {
			test_msg("page %lu: bad marker 0x%02x\n", i, m);
			errors++;
			continue;
		}
		for (j = 1; j < PAGE_SIZE; j++) {
			if (page[j] != m) {
				test_msg("page %lu: torn at offset %d (0x%02x != 0x%02x)\n",
					 i, j, page[j], m);
				errors++;
				break;
			}
		}
		if (errors > 16)
			break;  /* cap diagnostic spam */
	}

	if (errors) {
		fail("%d bad pages after large --cow-dump restore", errors);
		return 1;
	}

	pass();
	return 0;
}
