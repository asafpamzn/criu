#include <fcntl.h>
#include <linux/memfd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <pthread.h>

#include "zdtmtst.h"

const char *test_doc = "many memfd mappings";
const char *test_author = "Dan Feigin <dfeigin@nvidia.com>";

#define NR_THREADS 64
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
static int done;

static void *parked_thread(void *arg)
{
	pthread_mutex_lock(&mutex);
	while (!done)
		pthread_cond_wait(&cond, &mutex);
	pthread_mutex_unlock(&mutex);

	return NULL;
}

#define MEMFD_COUNT 16
#define LEN	    PAGE_SIZE

struct memfd_mapping {
	int fd;
	void *shared;
	void *private;
	int fl_flags;
	int fd_flags;
	off_t pos;
	dev_t shared_dev;
	dev_t private_dev;
};

static struct memfd_mapping mappings[MEMFD_COUNT];

static int _memfd_create(const char *name, unsigned int flags)
{
	return syscall(SYS_memfd_create, name, flags);
}

int main(int argc, char *argv[])
{
	pthread_t threads[NR_THREADS];
	uint8_t buf[LEN];
	uint32_t crc;
	int i;

	test_init(argc, argv);

	for (i = 0; i < MEMFD_COUNT; i++) {
		struct memfd_mapping *m = &mappings[i];
		char name[32];

		snprintf(name, sizeof(name), "many-memfd-%d", i);
		m->fd = _memfd_create(name, MFD_CLOEXEC);
		if (m->fd < 0) {
			pr_perror("Can't call memfd_create");
			exit(1);
		}

		crc = ~0;
		datagen(buf, LEN, &crc);
		if (write(m->fd, buf, LEN) != LEN) {
			pr_perror("write error");
			exit(1);
		}

		if (fcntl(m->fd, F_SETFL, O_APPEND) < 0) {
			pr_perror("Can't set fl flags");
			exit(1);
		}

		m->fl_flags = fcntl(m->fd, F_GETFL);
		if (m->fl_flags == -1) {
			pr_perror("Can't get fl flags");
			exit(1);
		}

		m->fd_flags = fcntl(m->fd, F_GETFD);
		if (m->fd_flags == -1) {
			pr_perror("Can't get fd flags");
			exit(1);
		}

		m->pos = i * 17;
		if (lseek(m->fd, m->pos, SEEK_SET) < 0) {
			pr_perror("seek error");
			exit(1);
		}

		m->shared = mmap(NULL, LEN, PROT_READ | PROT_WRITE, MAP_SHARED, m->fd, 0);
		if (m->shared == MAP_FAILED) {
			pr_perror("Can't mmap shared");
			exit(1);
		}

		m->private = mmap(NULL, LEN, PROT_READ | PROT_WRITE, MAP_PRIVATE, m->fd, 0);
		if (m->private == MAP_FAILED) {
			pr_perror("Can't mmap private");
			exit(1);
		}

		m->shared_dev = get_mapping_dev(m->shared);
		if (m->shared_dev == (dev_t)-1) {
			fail("Can't get shared mapping dev");
			return 1;
		}

		m->private_dev = get_mapping_dev(m->private);
		if (m->private_dev == (dev_t)-1) {
			fail("Can't get private mapping dev");
			return 1;
		}

		/* Diverge the private mapping so it differs from the shared one. */
		crc = ~0;
		datagen(m->private, LEN, &crc);
	}

	for (i = 0; i < NR_THREADS; i++) {
		if (pthread_create(&threads[i], NULL, parked_thread, NULL)) {
			pr_perror("Can't pthread_create");
			pthread_mutex_lock(&mutex);
			done = 1;
			pthread_cond_broadcast(&cond);
			pthread_mutex_unlock(&mutex);
			exit(1);
		}
	}

	test_daemon();
	test_waitsig();

	/* Release the parked threads and reap them. */
	pthread_mutex_lock(&mutex);
	done = 1;
	pthread_cond_broadcast(&cond);
	pthread_mutex_unlock(&mutex);
	for (i = 0; i < NR_THREADS; i++)
		pthread_join(threads[i], NULL);

	for (i = 0; i < MEMFD_COUNT; i++) {
		struct memfd_mapping *m = &mappings[i];
		int fl_flags, fd_flags;

		fl_flags = fcntl(m->fd, F_GETFL);
		if (fl_flags == -1) {
			pr_perror("Can't get fl flags");
			exit(1);
		}
		if (m->fl_flags != fl_flags) {
			fail("fl flags differ for memfd %d", i);
			return 1;
		}

		fd_flags = fcntl(m->fd, F_GETFD);
		if (fd_flags == -1) {
			pr_perror("Can't get fd flags");
			exit(1);
		}
		if (m->fd_flags != fd_flags) {
			fail("fd flags differ for memfd %d", i);
			return 1;
		}

		if (m->pos != lseek(m->fd, 0, SEEK_CUR)) {
			fail("position differs for memfd %d", i);
			return 1;
		}

		if (m->shared_dev != get_mapping_dev(m->shared)) {
			fail("shared mapping dev mismatch for memfd %d", i);
			return 1;
		}

		if (m->private_dev != get_mapping_dev(m->private)) {
			fail("private mapping dev mismatch for memfd %d", i);
			return 1;
		}

		/* The fd, the shared mapping and the private mapping all survived. */
		if (pread(m->fd, buf, LEN, 0) != LEN) {
			fail("read problem for memfd %d", i);
			return 1;
		}
		crc = ~0;
		if (datachk(buf, LEN, &crc)) {
			fail("fd content mismatch for memfd %d", i);
			return 1;
		}
		crc = ~0;
		if (datachk(m->shared, LEN, &crc)) {
			fail("shared content mismatch for memfd %d", i);
			return 1;
		}
		crc = ~0;
		if (datachk(m->private, LEN, &crc)) {
			fail("private content mismatch for memfd %d", i);
			return 1;
		}

		/* The fd and the shared mapping are the same object. */
		if (memcmp(buf, m->shared, LEN)) {
			fail("fd and shared mapping differ for memfd %d", i);
			return 1;
		}

		/* The private mapping stayed distinct from the shared one. */
		if (!memcmp(m->shared, m->private, LEN)) {
			fail("private mapping not isolated for memfd %d", i);
			return 1;
		}

		/* A write through the shared mapping is visible via the fd. */
		crc = ~0;
		datagen(m->shared, LEN, &crc);
		if (pread(m->fd, buf, LEN, 0) != LEN) {
			fail("read problem for memfd %d", i);
			return 1;
		}
		if (memcmp(buf, m->shared, LEN)) {
			fail("shared write not visible via fd for memfd %d", i);
			return 1;
		}

		/* A write through the private mapping leaves the fd and shared untouched. */
		crc = ~0;
		datagen(m->private, LEN, &crc);
		if (pread(m->fd, buf, LEN, 0) != LEN) {
			fail("read problem for memfd %d", i);
			return 1;
		}
		if (memcmp(buf, m->shared, LEN)) {
			fail("private write leaked into fd/shared for memfd %d", i);
			return 1;
		}
	}

	pass();

	return 0;
}
