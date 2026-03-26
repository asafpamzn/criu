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
#include "cow-bulk-send.h"
#include "page-xfer.h"
#include "atomic-bitmap.h"
#include "cr_options.h"
#include "tls.h"
#include "pagemap.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-bulk: "

#define COW_BATCH_PAGES 64
#define COW_BATCH_SIZE  (COW_BATCH_PAGES * PAGE_SIZE)

/* Protocol constants and structures (from page-xfer.c) */
#define PS_IOV_ADD_F_COMPRESS 10
#define PS_CMD_BITS 16

struct page_server_iov {
	u32 cmd;
	u64 nr_pages;
	u64 vaddr;
	u64 dst_id;
};

static inline u32 encode_ps_cmd(u32 cmd, u32 flags)
{
	return flags << PS_CMD_BITS | cmd;
}

static inline int __send(int sk, const void *buf, size_t sz, int fl)
{
	return opts.tls ? tls_send(buf, sz, fl) : send(sk, buf, sz, fl);
}

/* Global compression stats - shared with page-xfer.c */
extern unsigned long g_compress_uncompressed_bytes;
extern unsigned long g_compress_compressed_bytes;

/* Thread state */
static pthread_t p3_thread;
static volatile bool p3_thread_stop = false;
static volatile bool p3_thread_active = false;
static unsigned long p3_total_pages_sent = 0;

/* Thread parameters */
static int p3_socket;
static u64 p3_dst_id;
static pid_t p3_source_pid;

/*
 * Send a batch of pages with LZ4 compression.
 * Protocol: header (PS_IOV_ADD_F_COMPRESS) + compressed_size + compressed_data
 * Header contains nr_pages and base_vaddr.
 */
static int send_pages_batch_compressed(int sk, const void *data,
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

	/* Track compression statistics */
	g_compress_uncompressed_bytes += total_uncompressed;
	g_compress_compressed_bytes += *compressed_size;

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
	ret = __send(sk, send_buf, total_len, 0);

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

		if (bitmap_test_nonatomic(lve->sent_bitmap, page_idx))
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

	/* Mark all pages as sent */
	for (i = 0; i < nr_pages; i++) {
		unsigned long page_idx = page_idx_base + i;
		bitmap_set_nonatomic(lve->sent_bitmap, page_idx, &lve->sent_pages);
	}

	return nr_pages;
}

/*
 * P3 bulk sender thread - sends regular pages in batches.
 */
static void *p3_bulk_sender_thread(void *arg)
{
	struct lazy_vma_entry *lve;
	struct list_head *lazy_vmas;
	unsigned long total_sent = 0;
	struct timespec t_start, t_end;

	pr_info("P3 bulk sender thread started (batch=%d pages)\n", COW_BATCH_PAGES);
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	lazy_vmas = get_global_lazy_vmas();

	list_for_each_entry(lve, lazy_vmas, list) {
		unsigned long vaddr;

		if (lve->dst_id != p3_dst_id)
			continue;

		pr_info("P3: Processing VMA %lx-%lx (%lu pages)\n",
			(unsigned long)lve->start, (unsigned long)lve->end,
			lve->total_pages);

		for (vaddr = lve->start; vaddr < lve->end && !p3_thread_stop;
		     vaddr += COW_BATCH_PAGES * PAGE_SIZE) {

			int sent = send_lazy_vma_pages_batch(
				p3_socket, lve, vaddr, COW_BATCH_PAGES,
				p3_dst_id, p3_source_pid);

			if (sent < 0) {
				pr_err("P3: Failed to send batch at %lx\n", vaddr);
				goto out;
			}

			total_sent += sent;
		}
	}

out:
	clock_gettime(CLOCK_MONOTONIC, &t_end);
	{
		long elapsed_ms = (t_end.tv_sec - t_start.tv_sec) * 1000 +
				  (t_end.tv_nsec - t_start.tv_nsec) / 1000000;
		float rate = elapsed_ms > 0 ? (float)total_sent * 1000 / elapsed_ms : 0;

		pr_info("P3 bulk sender done: %lu pages in %ld ms (%.0f pages/sec)\n",
			total_sent, elapsed_ms, rate);
	}

	p3_total_pages_sent = total_sent;
	p3_thread_active = false;
	return NULL;
}

int cow_start_p3_thread(int sk, u64 dst_id, pid_t source_pid)
{
	if (p3_thread_active) {
		pr_warn("P3 thread already running\n");
		return 0;
	}

	p3_socket = sk;
	p3_dst_id = dst_id;
	p3_source_pid = source_pid;
	p3_thread_stop = false;
	p3_thread_active = true;
	p3_total_pages_sent = 0;

	if (pthread_create(&p3_thread, NULL, p3_bulk_sender_thread, NULL)) {
		pr_perror("Failed to create P3 bulk sender thread");
		p3_thread_active = false;
		return -1;
	}

	pr_info("Started P3 bulk sender thread\n");
	return 0;
}

void cow_wait_p3_thread(void)
{
	if (!p3_thread_active)
		return;

	p3_thread_stop = true;
	pthread_join(p3_thread, NULL);
	p3_thread_active = false;
	pr_info("P3 bulk sender thread joined\n");
}

bool cow_p3_thread_running(void)
{
	return p3_thread_active;
}

unsigned long cow_p3_pages_sent(void)
{
	return p3_total_pages_sent;
}
