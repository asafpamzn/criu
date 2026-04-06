/*
 * COW Bulk Page Sender - Optimized P3 (regular page) transfer
 *
 * Sends pages in batches of 64 (256KB) for better throughput:
 * - Single process_vm_readv for 64 pages
 * - Single LZ4 compression for 256KB
 * - Single socket send
 */

#include <sys/uio.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <lz4.h>

#include "int.h"
#include "page.h"
#include "types.h"
#include "criu-log.h"
#include "xmalloc.h"
#include "common/list.h"
#include "mem.h"
#include "cow/cow-bulk-send.h"
#include "page-xfer.h"
#include "cow/cow-page-xfer.h"
#include "atomic-bitmap.h"
#include "cr_options.h"
#include "tls.h"
#include "pagemap.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bulk: "

#define COW_BATCH_PAGES 64
#define COW_BATCH_SIZE  (COW_BATCH_PAGES * PAGE_SIZE)

/*
 * Protocol structs, constants, and helpers are now in page-xfer.h:
 * - struct page_server_iov
 * - PS_CMD_BITS, encode_ps_cmd()
 * - page_server_send() (replaces __send)
 *
 * COW-specific protocol defines (PS_IOV_ADD_F_COMPRESS, etc.) are in cow-page-xfer.h
 */

/* NUM_P3_THREADS is defined in cow-bulk-send.h */
#define NUM_P3_SPLITTER_THREADS (NUM_P3_THREADS - 1)  /* Threads 1-(N-1) split large VMAs */
#define MIN_VMA_SIZE_FOR_SPLIT (256 * 1024)  /* 256KB threshold */

/* Per-thread state */
struct p3_thread_ctx {
	pthread_t thread;
	int thread_id;
	int socket;           /* Per-thread socket for parallel transfer */
	u64 dst_id;
	pid_t source_pid;
	unsigned long pages_sent;
	volatile bool active;
	volatile bool error;  /* Set if thread encountered an error */
};

static struct p3_thread_ctx p3_threads[NUM_P3_THREADS];
static volatile int p3_threads_active = 0;
static unsigned long p3_total_pages_sent = 0;

/*
 * Send a batch of pages with LZ4 compression.
 * Protocol: header (PS_IOV_ADD_F_COMPRESS) + compressed_size + compressed_data
 * Header contains nr_pages and base_vaddr.
 */
int send_pages_batch_compressed(int sk, const void *data,
				int nr_pages, u64 dst_id,
				unsigned long base_vaddr)
{
	/* Allocate buffer for: header + compressed_size + compressed_data */
	int max_compressed = LZ4_compressBound(nr_pages * PAGE_SIZE);
	int total_uncompressed = nr_pages * PAGE_SIZE;
	char *send_buf;
	struct page_server_iov *pi;
	int *compressed_size;
	char *compressed_data;
	int total_len, ret;

	send_buf = xmalloc(sizeof(struct page_server_iov) + sizeof(int) + max_compressed);
	if (!send_buf)
		return -1;

	pi = (struct page_server_iov *)send_buf;
	compressed_size = (int *)(send_buf + sizeof(*pi));
	compressed_data = send_buf + sizeof(*pi) + sizeof(int);

	/* Compress entire batch */
	*compressed_size = LZ4_compress_default(data, compressed_data,
						total_uncompressed, max_compressed);
	if (*compressed_size <= 0) {
		pr_err("LZ4 compression failed for batch at %lx (%d pages)\n",
		       base_vaddr, nr_pages);
		xfree(send_buf);
		return -1;
	}

	/* Track compression statistics (atomic for multi-threaded access) */
	__sync_fetch_and_add(&g_compress_uncompressed_bytes, total_uncompressed);
	__sync_fetch_and_add(&g_compress_compressed_bytes, *compressed_size);

	pr_debug("Compressed batch at %lx: %d pages, %d -> %d bytes (%.1f%%)\n",
		 base_vaddr, nr_pages, total_uncompressed, *compressed_size,
		 (float)(*compressed_size) * 100 / total_uncompressed);

	/* Fill header - use nr_pages to indicate batch size */
	pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT);
	pi->nr_pages = nr_pages;
	pi->vaddr = base_vaddr;
	pi->dst_id = dst_id;

	/* Single send: header + size + compressed data */
	total_len = sizeof(*pi) + sizeof(int) + *compressed_size;
	ret = page_server_send(sk, send_buf, total_len, 0);

	xfree(send_buf);

	if (ret != total_len) {
		pr_perror("Failed to send compressed batch (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

/*
 * Read and send a batch of contiguous pages from source process.
 * Returns number of pages actually sent, or -1 on error.
 */
static int send_lazy_vma_pages_batch(int sk, struct lazy_vma_entry *lve,
				     unsigned long base_vaddr, int max_pages,
				     u64 dst_id, pid_t source_pid)
{
	void *buffer;
	struct iovec local_iov, remote_iov;
	int nr_pages = 0;
	unsigned long vaddr;
	unsigned long page_idx_base;
	int ret, i;

	/* Find contiguous run of unsent, non-COW pages from base_vaddr */
	page_idx_base = (base_vaddr - lve->start) / PAGE_SIZE;

	for (i = 0; i < max_pages; i++) {
		unsigned long page_idx = 0;
		vaddr = base_vaddr + i * PAGE_SIZE;
		if (vaddr >= lve->end)
			break;

		 page_idx = page_idx_base + i;

		if (atomic_bitmap_test(lve->sent_bitmap, page_idx))
			break;  /* Stop at first already-sent */

		if (lve->cow_bitmap && atomic_bitmap_test(lve->cow_bitmap, page_idx))
			break;  /* Stop at first COW page */

		nr_pages++;
	}

	if (nr_pages == 0)
		return 0;

	/* Allocate buffer for batch */
	buffer = xmalloc(nr_pages * PAGE_SIZE);
	if (!buffer)
		return -1;

	/* Single process_vm_readv for all pages */
	local_iov.iov_base = buffer;
	local_iov.iov_len = nr_pages * PAGE_SIZE;
	remote_iov.iov_base = (void *)base_vaddr;
	remote_iov.iov_len = nr_pages * PAGE_SIZE;

	ret = process_vm_readv(source_pid, &local_iov, 1, &remote_iov, 1, 0);
	if (ret != (ssize_t)(nr_pages * PAGE_SIZE)) {
		pr_perror("Failed to read %d pages at %lx from pid %d (got %d)",
			  nr_pages, base_vaddr, source_pid, ret);
		xfree(buffer);
		return -1;
	}

	/* Compress and send batch */
	ret = send_pages_batch_compressed(sk, buffer, nr_pages, dst_id, base_vaddr);
	xfree(buffer);

	if (ret < 0)
		return -1;

	/* Mark all pages as sent (atomic for multi-threaded access) */
	for (i = 0; i < nr_pages; i++) {
		unsigned long page_idx = page_idx_base + i;
		/* atomic_bitmap_test_and_set returns true if already set */
		if (!atomic_bitmap_test_and_set(lve->sent_bitmap, page_idx))
			__sync_fetch_and_add(&lve->sent_pages, 1);
	}

	return nr_pages;
}

/*
 * P3 bulk sender thread - sends regular pages in batches.
 * Each thread handles 1/NUM_P3_THREADS of each VMA's address range.
 */
static void *p3_bulk_sender_thread(void *arg)
{
	struct p3_thread_ctx *ctx = (struct p3_thread_ctx *)arg;
	struct lazy_vma_entry *lve;
	struct list_head *lazy_vmas;
	unsigned long total_sent = 0;
	struct timespec t_start, t_end;
	int thread_id = ctx->thread_id;

	pr_info("P3[%d] bulk sender thread started (batch=%d pages)\n",
		thread_id, COW_BATCH_PAGES);
	pr_debug("DEBUG_THREAD: P3 sender[%d] STARTED socket=%d dst_id=%lu\n",
	       thread_id, ctx->socket, (unsigned long)ctx->dst_id);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	lazy_vmas = get_global_lazy_vmas();

	list_for_each_entry(lve, lazy_vmas, list) {
		unsigned long vma_size, chunk_size;
		unsigned long my_start, my_end;
		unsigned long vaddr;

		if (lve->dst_id != ctx->dst_id)
			continue;

		/* Calculate this thread's chunk of the VMA */
		vma_size = lve->end - lve->start;

		if (vma_size < MIN_VMA_SIZE_FOR_SPLIT) {
			/*
			 * Small VMAs (< 256KB) - only thread 0 handles them.
			 * No splitting to avoid overhead and edge cases.
			 */
			if (thread_id != 0)
				continue;
			my_start = lve->start;
			my_end = lve->end;
		} else {
			/*
			 * Large VMAs (>= 256KB) - threads 1-9 split them.
			 * Thread 0 skips these entirely.
			 */
			if (thread_id == 0)
				continue;

			/* 9 splitter threads (thread_id 1-9) */
			chunk_size = vma_size / NUM_P3_SPLITTER_THREADS;
			chunk_size = (chunk_size / PAGE_SIZE) * PAGE_SIZE;

			/* Map thread_id 1-9 to index 0-8 */
			my_start = lve->start + (thread_id - 1) * chunk_size;

			/* Last splitter thread (id=9) takes remainder */
			if (thread_id == NUM_P3_THREADS - 1)
				my_end = lve->end;
			else
				my_end = my_start + chunk_size;
		}

		pr_info("P3[%d]: Processing VMA %lx-%lx chunk %lx-%lx\n",
			thread_id,
			(unsigned long)lve->start, (unsigned long)lve->end,
			my_start, my_end);

		for (vaddr = my_start; vaddr < my_end;
		     vaddr += COW_BATCH_PAGES * PAGE_SIZE) {
			int batch_pages;
			int sent;

			/* Limit batch to not exceed this thread's assigned range */
			batch_pages = (my_end - vaddr) / PAGE_SIZE;
			if (batch_pages > COW_BATCH_PAGES)
				batch_pages = COW_BATCH_PAGES;

			sent = send_lazy_vma_pages_batch(
				ctx->socket, lve, vaddr, batch_pages,
				ctx->dst_id, ctx->source_pid);

			if (sent < 0) {
				pr_err("P3[%d]: Failed to send batch at %lx\n",
				       thread_id, vaddr);
				ctx->error = true;
				goto out;
			}

			total_sent += sent;
			if (total_sent % 1000 == 0 && total_sent > 0)
				pr_debug("DEBUG_THREAD: P3 sender[%d] progress: %lu pages sent\n",
				       thread_id, total_sent);
		}
	}

out:
	pr_debug("DEBUG_THREAD: P3 sender[%d] CLOSING socket=%d pages_sent=%lu\n",
	       thread_id, ctx->socket, total_sent);
	clock_gettime(CLOCK_MONOTONIC, &t_end);
	{
		long elapsed_ms = (t_end.tv_sec - t_start.tv_sec) * 1000 +
				  (t_end.tv_nsec - t_start.tv_nsec) / 1000000;
		float rate = elapsed_ms > 0 ? (float)total_sent * 1000 / elapsed_ms : 0;

		pr_info("P3[%d] done: %lu pages in %ld ms (%.0f pages/sec)\n",
			thread_id, total_sent, elapsed_ms, rate);
	}

	ctx->pages_sent = total_sent;
	ctx->active = false;
	__sync_fetch_and_sub(&p3_threads_active, 1);
	pr_debug("DEBUG_THREAD: P3 sender[%d] TERMINATED pages=%lu\n", thread_id, total_sent);
	return NULL;
}

int cow_start_p3_threads(int *sockets, int num_sockets, u64 dst_id, pid_t source_pid)
{
	int i;
	int threads_to_start;

	if (p3_threads_active > 0) {
		pr_warn("P3 threads already running\n");
		return 0;
	}

	/* Start one thread per socket */
	threads_to_start = num_sockets < NUM_P3_THREADS ? num_sockets : NUM_P3_THREADS;
	p3_total_pages_sent = 0;

	for (i = 0; i < threads_to_start; i++) {
		p3_threads[i].thread_id = i;
		p3_threads[i].socket = sockets[i];
		p3_threads[i].dst_id = dst_id;
		p3_threads[i].source_pid = source_pid;
		p3_threads[i].pages_sent = 0;
		p3_threads[i].active = true;
		p3_threads[i].error = false;
		p3_threads[i].thread = 0;
		__sync_fetch_and_add(&p3_threads_active, 1);

		if (pthread_create(&p3_threads[i].thread, NULL,
				   p3_bulk_sender_thread, &p3_threads[i])) {
			pr_perror("Failed to create P3 thread %d", i);
			p3_threads[i].active = false;
			__sync_fetch_and_sub(&p3_threads_active, 1);
			/* Continue with remaining threads */
		}
	}

	pr_info("Started %d P3 bulk sender threads (%d sockets)\n",
		p3_threads_active, threads_to_start);
	return p3_threads_active > 0 ? 0 : -1;
}

/* Legacy single-thread interface for backward compatibility */
int cow_start_p3_thread(int sk, u64 dst_id, pid_t source_pid)
{
	int sockets[1] = { sk };
	return cow_start_p3_threads(sockets, 1, dst_id, source_pid);
}

void cow_wait_p3_threads(void)
{
	int i;
	unsigned long total = 0;
	int errors = 0;

	for (i = 0; i < NUM_P3_THREADS; i++) {
		if (p3_threads[i].thread) {
			pr_debug("DEBUG_THREAD: Waiting for P3 sender[%d] to join\n", i);
			pthread_join(p3_threads[i].thread, NULL);
			pr_debug("DEBUG_THREAD: P3 sender[%d] JOINED pages=%lu error=%d\n",
			       i, p3_threads[i].pages_sent, p3_threads[i].error);
			total += p3_threads[i].pages_sent;
			if (p3_threads[i].error)
				errors++;
			p3_threads[i].thread = 0;
		}
	}

	p3_total_pages_sent = total;
	p3_threads_active = 0;

	if (errors > 0)
		pr_warn("P3 threads completed with %d errors, %lu pages sent\n",
			errors, total);
	else
		pr_info("All P3 threads joined: %lu total pages\n", total);
}

/* Legacy single-thread interface */
void cow_wait_p3_thread(void)
{
	cow_wait_p3_threads();
}

bool cow_p3_thread_running(void)
{
	return p3_threads_active > 0;
}

unsigned long cow_p3_pages_sent(void)
{
	return p3_total_pages_sent;
}

int cow_get_num_p3_threads(void)
{
	return NUM_P3_THREADS;
}
