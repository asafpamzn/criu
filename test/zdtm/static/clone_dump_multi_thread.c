#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "zdtmtst.h"

const char *test_doc = "--clone-dump with a multi-threaded process: each thread "
		       "owns a page-sized scratch region plus a __thread TLS "
		       "cookie it continuously re-derives from its id. After "
		       "restore every thread must still observe its own TLS "
		       "value and its scratch page must be filled with its "
		       "own id marker.";
const char *test_author = "Asaf Pamuk <asafp@anthropic.com>";

#define NR_THREADS	8

static __thread uint64_t tls_cookie;

struct ctx {
	int id;
	unsigned char *scratch;
	uint64_t observed_cookie;   /* filled by thread before join */
	unsigned char observed_marker;
};

static atomic_int stop_workers;

static void *worker(void *arg)
{
	struct ctx *c = arg;
	unsigned char marker = (unsigned char)(0x80 | c->id);

	tls_cookie = 0xA5A5000000000000ULL | (uint64_t)c->id;

	while (!atomic_load(&stop_workers)) {
		memset(c->scratch, marker, PAGE_SIZE);
		/* Refresh TLS each iteration so it's "live" during dump. */
		tls_cookie = 0xA5A5000000000000ULL | (uint64_t)c->id;
	}

	c->observed_cookie = tls_cookie;
	c->observed_marker = c->scratch[0];
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t threads[NR_THREADS];
	struct ctx ctxs[NR_THREADS];
	int i, errors = 0;

	test_init(argc, argv);

	for (i = 0; i < NR_THREADS; i++) {
		ctxs[i].id = i;
		ctxs[i].scratch = mmap(NULL, PAGE_SIZE,
				       PROT_READ | PROT_WRITE,
				       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (ctxs[i].scratch == MAP_FAILED) {
			pr_perror("mmap thread %d scratch", i);
			return 1;
		}
		memset(ctxs[i].scratch, (unsigned char)(0x80 | i), PAGE_SIZE);
	}

	atomic_init(&stop_workers, 0);
	for (i = 0; i < NR_THREADS; i++) {
		if (pthread_create(&threads[i], NULL, worker, &ctxs[i])) {
			pr_perror("pthread_create %d", i);
			return 1;
		}
	}

	test_daemon();
	test_waitsig();

	atomic_store(&stop_workers, 1);
	for (i = 0; i < NR_THREADS; i++)
		pthread_join(threads[i], NULL);

	for (i = 0; i < NR_THREADS; i++) {
		uint64_t expected_cookie = 0xA5A5000000000000ULL | (uint64_t)i;
		unsigned char expected_marker = (unsigned char)(0x80 | i);

		if (ctxs[i].observed_cookie != expected_cookie) {
			test_msg("thread %d: TLS cookie 0x%016lx != 0x%016lx\n",
				 i,
				 (unsigned long)ctxs[i].observed_cookie,
				 (unsigned long)expected_cookie);
			errors++;
		}
		if (ctxs[i].observed_marker != expected_marker) {
			test_msg("thread %d: scratch marker 0x%02x != 0x%02x\n",
				 i, ctxs[i].observed_marker, expected_marker);
			errors++;
		}
	}

	if (errors) {
		fail("%d thread-state errors after --clone-dump restore", errors);
		return 1;
	}

	pass();
	return 0;
}
