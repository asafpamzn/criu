#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "--cow-dump with MAP_PRIVATE file-backed region: writes "
		       "to the mapping create private COW pages that must be "
		       "captured. A background writer keeps dirtying pages "
		       "throughout dump. Post-restore verification: pages are "
		       "filled with the writer's marker (i.e. COW copies were "
		       "captured, not the original file contents).";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define NR_PAGES	64
#define FILE_FILL	0x33
#define WRITER_MARKER	0xCC

char *test_dir;
TEST_OPTION(test_dir, string, "directory for temporary file", 1);

static atomic_int stop_writer;
static unsigned char *mem;

static void *writer_thread(void *arg)
{
	unsigned long sz = NR_PAGES * PAGE_SIZE;
	while (!atomic_load(&stop_writer))
		memset(mem, WRITER_MARKER, sz);
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t th;
	char path[256];
	unsigned char *buf;
	unsigned long sz = NR_PAGES * PAGE_SIZE;
	int fd, i, j, errors = 0;

	test_init(argc, argv);

	snprintf(path, sizeof(path), "%s/cow_dump_file_backed.%d", test_dir, getpid());
	fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0600);
	if (fd < 0) {
		pr_perror("open %s", path);
		return 1;
	}
	buf = malloc(sz);
	if (!buf) {
		pr_perror("malloc");
		return 1;
	}
	memset(buf, FILE_FILL, sz);
	if (write(fd, buf, sz) != (ssize_t)sz) {
		pr_perror("write file");
		return 1;
	}
	free(buf);

	mem = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	if (mem == MAP_FAILED) {
		pr_perror("mmap MAP_PRIVATE");
		return 1;
	}
	/* Trigger initial COW copies so the region has private pages before dump. */
	memset(mem, WRITER_MARKER, sz);

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
		for (j = 0; j < PAGE_SIZE; j++) {
			if (page[j] != WRITER_MARKER) {
				test_msg("page %d offset %d: 0x%02x != 0x%02x\n",
					 i, j, page[j], WRITER_MARKER);
				errors++;
				break;
			}
		}
	}

	close(fd);
	unlink(path);

	if (errors) {
		fail("%d pages corrupted in MAP_PRIVATE file-backed region", errors);
		return 1;
	}

	pass();
	return 0;
}
