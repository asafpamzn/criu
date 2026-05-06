#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "Basic --cow-dump end-to-end: dump + lazy-pages restore "
		       "with a static anon private mapping; verify every page "
		       "is restored byte-exact.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define NR_PAGES 1024

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
