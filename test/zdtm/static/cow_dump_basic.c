#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "zdtmtst.h"
#include "cow_dump_util.h"

const char *test_doc = "--cow-dump skeleton + pipe sanity: 4 MB static "
		       "anon private mapping, no Phase-2 writes. Gates "
		       "verification on the lazy-pages daemon having drained "
		       "every page into the restored address space (via "
		       "mincore) so data-content bugs are distinguishable "
		       "from drain/race bugs. Does NOT exercise the WP-fault "
		       "or dirty-page-resend paths — see cow_dump_write_storm "
		       "for that.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define NR_PAGES	1024
#define DRAIN_TIMEOUT	10000	/* ms */

int main(int argc, char **argv)
{
	unsigned char *mem;
	unsigned long sz = NR_PAGES * PAGE_SIZE;
	int i, j, errors = 0;

	test_init(argc, argv);

	mem = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (mem == MAP_FAILED) {
		pr_perror("mmap");
		return 1;
	}

	/* Fill each page with a unique marker byte derived from its index. */
	for (i = 0; i < NR_PAGES; i++)
		memset(mem + i * PAGE_SIZE, (unsigned char)(i & 0xff), PAGE_SIZE);

	test_daemon();
	test_waitsig();

	/*
	 * Gate verification on the lazy-pages drain actually completing.
	 * mincore() reports residency without faulting, so pages the
	 * daemon has not yet UFFDIO_COPY-ed show as non-resident until
	 * the drain puts them in. Without this gate we'd silently read
	 * pages through the on-demand fault path instead of verifying
	 * the drain did its job.
	 */
	if (cow_wait_for_drain(mem, sz, DRAIN_TIMEOUT) < 0) {
		fail("lazy-pages drain did not complete within %d ms", DRAIN_TIMEOUT);
		return 1;
	}

	for (i = 0; i < NR_PAGES; i++) {
		unsigned char expected = (unsigned char)(i & 0xff);
		unsigned char *page = mem + i * PAGE_SIZE;

		for (j = 0; j < PAGE_SIZE; j++) {
			if (page[j] != expected) {
				test_msg("page %d offset %d: got 0x%02x expected 0x%02x\n",
					 i, j, page[j], expected);
				errors++;
				break;
			}
		}
	}

	if (errors) {
		fail("%d pages corrupted after --cow-dump restore", errors);
		return 1;
	}

	pass();
	return 0;
}
