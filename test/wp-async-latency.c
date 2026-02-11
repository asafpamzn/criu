/*
 * wp-async-latency.c — Measure write latency during UFFDIO_WRITEPROTECT
 *
 * Proves whether WP_ASYNC eliminates write stalls during WP setup.
 *
 * Test:
 *   1. mmap a large region (configurable, default 10GB)
 *   2. Fault all pages in (memset)
 *   3. Create userfaultfd with WP_ASYNC
 *   4. Register region for WP
 *   5. Spawn writer thread: tight loop writing to random pages,
 *      measuring per-write latency with clock_gettime
 *   6. Main thread: apply UFFDIO_WRITEPROTECT on the region
 *   7. Report max/p99/avg write latency during WP application
 *
 * Build:
 *   gcc -O2 -pthread -o wp-async-latency wp-async-latency.c
 *
 * Run:
 *   sudo ./wp-async-latency [size_gb]
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/userfaultfd.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdatomic.h>

#define PAGE_SIZE 4096UL

/* Histogram buckets (microseconds) */
#define HIST_BUCKETS 7
static const char *hist_labels[HIST_BUCKETS] = {
	"<1us", "1-10us", "10-100us", "100us-1ms", "1-10ms", "10-100ms", ">100ms"
};

struct writer_ctx {
	volatile char *region;
	unsigned long region_size;
	atomic_int phase;		/* 0=warmup, 1=measuring, 2=stop */
	unsigned long hist[HIST_BUCKETS];
	unsigned long max_ns;
	unsigned long total_ns;
	unsigned long count;
};

static int bucket_for_ns(unsigned long ns)
{
	if (ns < 1000)        return 0;	/* <1us */
	if (ns < 10000)       return 1;	/* 1-10us */
	if (ns < 100000)      return 2;	/* 10-100us */
	if (ns < 1000000)     return 3;	/* 100us-1ms */
	if (ns < 10000000)    return 4;	/* 1-10ms */
	if (ns < 100000000)   return 5;	/* 10-100ms */
	return 6;				/* >100ms */
}

static void *writer_thread(void *arg)
{
	struct writer_ctx *ctx = arg;
	unsigned long pages = ctx->region_size / PAGE_SIZE;
	unsigned int seed = 42;
	struct timespec t1, t2;

	/* Warmup: write without measurement until phase changes */
	while (atomic_load(&ctx->phase) == 0) {
		unsigned long idx = (rand_r(&seed) % pages) * PAGE_SIZE;
		ctx->region[idx] = 0x42;
	}

	/* Measuring: tight loop with per-write latency */
	while (atomic_load(&ctx->phase) == 1) {
		unsigned long idx = (rand_r(&seed) % pages) * PAGE_SIZE;
		unsigned long ns;

		clock_gettime(CLOCK_MONOTONIC, &t1);
		ctx->region[idx] = 0x42;
		clock_gettime(CLOCK_MONOTONIC, &t2);

		ns = (t2.tv_sec - t1.tv_sec) * 1000000000UL +
		     (t2.tv_nsec - t1.tv_nsec);

		ctx->hist[bucket_for_ns(ns)]++;
		ctx->total_ns += ns;
		ctx->count++;
		if (ns > ctx->max_ns)
			ctx->max_ns = ns;
	}

	return NULL;
}

int main(int argc, char **argv)
{
	unsigned long size_gb = 10;
	unsigned long region_size;
	void *region;
	int uffd;
	struct uffdio_api api;
	struct uffdio_register reg;
	struct uffdio_writeprotect wp;
	struct writer_ctx ctx;
	pthread_t writer;
	struct timespec wp_start, wp_end;
	unsigned long wp_ns;
	int i;

	if (argc > 1)
		size_gb = atol(argv[1]);

	region_size = size_gb * (1UL << 30);
	printf("=== WP_ASYNC Write Latency Test ===\n");
	printf("Region: %lu GB (%lu pages)\n", size_gb,
	       region_size / PAGE_SIZE);

	/* Step 1: mmap + fault in */
	printf("Step 1: mmap + fault in %lu GB...\n", size_gb);
	region = mmap(NULL, region_size, PROT_READ | PROT_WRITE,
		      MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
	if (region == MAP_FAILED) {
		perror("mmap");
		return 1;
	}
	printf("  Region at %p\n", region);

	/* Step 2: Create userfaultfd with WP_ASYNC */
	printf("Step 2: Create userfaultfd with WP_ASYNC...\n");
	uffd = syscall(__NR_userfaultfd, O_CLOEXEC | O_NONBLOCK);
	if (uffd < 0) {
		perror("userfaultfd");
		return 1;
	}

	memset(&api, 0, sizeof(api));
	api.api = UFFD_API;
	api.features = UFFD_FEATURE_WP_ASYNC;
	if (ioctl(uffd, UFFDIO_API, &api)) {
		perror("UFFDIO_API");
		return 1;
	}
	if (!(api.features & UFFD_FEATURE_WP_ASYNC)) {
		fprintf(stderr, "Kernel does not support WP_ASYNC\n");
		return 1;
	}
	printf("  WP_ASYNC enabled (features=0x%llx)\n", api.features);

	/* Step 3: Register region for WP */
	printf("Step 3: Register region for WP...\n");
	memset(&reg, 0, sizeof(reg));
	reg.range.start = (unsigned long)region;
	reg.range.len = region_size;
	reg.mode = UFFDIO_REGISTER_MODE_WP;
	if (ioctl(uffd, UFFDIO_REGISTER, &reg)) {
		perror("UFFDIO_REGISTER");
		return 1;
	}
	printf("  Registered %lu GB for WP\n", size_gb);

	/* Step 4: Start writer thread (warmup phase) */
	printf("Step 4: Start writer thread...\n");
	memset(&ctx, 0, sizeof(ctx));
	ctx.region = region;
	ctx.region_size = region_size;
	atomic_store(&ctx.phase, 0);
	pthread_create(&writer, NULL, writer_thread, &ctx);

	/* Let writer warm up for 100ms */
	usleep(100000);

	/* Step 5: Switch to measuring + apply WRITEPROTECT */
	printf("Step 5: Apply WRITEPROTECT (%lu GB) while writer runs...\n",
	       size_gb);
	atomic_store(&ctx.phase, 1);

	/* Small delay so writer enters measurement loop */
	usleep(1000);

	clock_gettime(CLOCK_MONOTONIC, &wp_start);
	wp.range.start = (unsigned long)region;
	wp.range.len = region_size;
	wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;
	if (ioctl(uffd, UFFDIO_WRITEPROTECT, &wp)) {
		perror("UFFDIO_WRITEPROTECT");
		return 1;
	}
	clock_gettime(CLOCK_MONOTONIC, &wp_end);

	wp_ns = (wp_end.tv_sec - wp_start.tv_sec) * 1000000000UL +
		(wp_end.tv_nsec - wp_start.tv_nsec);

	/* Let writer run a bit more after WP completes */
	usleep(10000);

	/* Stop writer */
	atomic_store(&ctx.phase, 2);
	pthread_join(writer, NULL);

	/* Results */
	printf("\n=== Results ===\n");
	printf("WRITEPROTECT time: %lu.%03lu ms\n",
	       wp_ns / 1000000, (wp_ns / 1000) % 1000);
	printf("Writer samples:    %lu\n", ctx.count);
	printf("Max write latency: %lu.%03lu us\n",
	       ctx.max_ns / 1000, ctx.max_ns % 1000);
	if (ctx.count)
		printf("Avg write latency: %lu.%03lu us\n",
		       (ctx.total_ns / ctx.count) / 1000,
		       (ctx.total_ns / ctx.count) % 1000);

	printf("\nLatency histogram:\n");
	for (i = 0; i < HIST_BUCKETS; i++) {
		if (ctx.hist[i])
			printf("  %-12s %8lu  (%5.1f%%)\n", hist_labels[i],
			       ctx.hist[i],
			       100.0 * ctx.hist[i] / ctx.count);
	}

	printf("\nVerdict: ");
	if (ctx.max_ns > 10000000)  /* >10ms */
		printf("FAIL — writes stalled during WP (max %lu.%01lu ms)\n",
		       ctx.max_ns / 1000000, (ctx.max_ns / 100000) % 10);
	else if (ctx.max_ns > 1000000)  /* >1ms */
		printf("MARGINAL — some write stalls (max %lu us)\n",
		       ctx.max_ns / 1000);
	else
		printf("PASS — writes not stalled during WP (max %lu us)\n",
		       ctx.max_ns / 1000);

	munmap(region, region_size);
	close(uffd);
	return 0;
}
