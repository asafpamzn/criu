/*
 * COW P3 Parallel Receiver - REPLICA side parallel page reception
 *
 * Handles parallel page reception for COW migration:
 * - Creates connections to PRIMARY
 * - Spawns receiver threads for each connection
 * - Receives compressed batches and adds to page buffer
 */

#include <sys/socket.h>
#include <sys/mman.h>
#include <pthread.h>
#include <poll.h>
#include <unistd.h>
#include <errno.h>
#include <lz4.h>

#include "types.h"
#include "page.h"
#include "criu-log.h"
#include "page-xfer.h"
#include "cow/cow-page-xfer.h"
#include "cow/cow-conf.h"
#include "cow/cow-bulk-send.h"
#include "cr_options.h"
#include "tls.h"
#include "cow/page-pool.h"
#include "cow/cow-uffd.h"
#include "util.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-p3-recv: "

/* Use same thread count as sender (COW_NUM_P3_THREADS in cow-conf.h) */
#define MAX_P3_RECEIVERS COW_NUM_P3_THREADS

/* Max batch size for P3 transfer (COW_BATCH_PAGES in cow-conf.h) */
#define P3_DECOMPRESS_BUF_SIZE (COW_BATCH_PAGES * PAGE_SIZE)
#define P3_COMPRESS_BUF_SIZE LZ4_compressBound(P3_DECOMPRESS_BUF_SIZE)

struct p3_receiver_ctx {
	pthread_t thread;
	int thread_id;
	int socket;
	unsigned long pages_received;
	volatile bool active;
	volatile bool error;
	/* Pre-allocated buffers to avoid malloc/mprotect contention */
	char *compressed_buf;
	char *decompressed_buf;
};

static struct p3_receiver_ctx p3_receivers[MAX_P3_RECEIVERS];
static volatile int p3_receivers_active = 0;
static pthread_t p3_acceptor_thread;
static volatile bool p3_acceptor_running = false;

/*
 * Receive one compressed batch from socket and add to page buffer.
 * Uses pre-allocated buffers from ctx to avoid malloc contention.
 * Returns number of pages received, 0 on EOF, -1 on error.
 */
static int p3_receive_and_buffer(struct p3_receiver_ctx *ctx)
{
	struct page_server_iov pi;
	int compressed_size;
	char *compressed_buf = ctx->compressed_buf;
	int sk = ctx->socket;
	int nr_pages, i, ret = -1;
	int decomp_ret;
	char *chunk_buf;

	/* Receive header */
	ret = page_server_recv(sk, &pi, sizeof(pi), MSG_WAITALL);
	if (ret == 0)
		return 0;  /* EOF */
	if (ret != sizeof(pi)) {
		pr_perror("P3 receive: failed to read header");
		return -1;
	}

	nr_pages = pi.nr_pages;
	if (nr_pages <= 0 || nr_pages > COW_BATCH_PAGES) {
		pr_err("P3 receive: invalid nr_pages %d\n", nr_pages);
		return -1;
	}

	/* Receive compressed size */
	if (page_server_recv(sk, &compressed_size, sizeof(compressed_size), MSG_WAITALL) != sizeof(compressed_size)) {
		pr_perror("P3 receive: failed to read compressed size");
		return -1;
	}

	if (compressed_size <= 0 || compressed_size > P3_COMPRESS_BUF_SIZE) {
		pr_err("P3 receive: invalid compressed size %d\n", compressed_size);
		return -1;
	}

	/* Receive compressed data (using pre-allocated buffer) */
	if (page_server_recv(sk, compressed_buf, compressed_size, MSG_WAITALL) != compressed_size) {
		pr_perror("P3 receive: failed to read compressed data");
		return -1;
	}

	/*
	 * Get exactly nr_pages from page pool for direct decompression.
	 * Using page_pool_get_pages() instead of page_pool_get_chunk() to
	 * allocate only what we need, avoiding wasted pages that would need
	 * to be freed immediately.
	 */
	chunk_buf = page_pool_get_pages(ctx->thread_id, nr_pages);
	if (!chunk_buf) {
		pr_err("P3 receive: failed to get %d pages from pool\n", nr_pages);
		return -1;
	}

	/* Decompress directly into page pool pages */
	decomp_ret = LZ4_decompress_safe(compressed_buf, chunk_buf,
					 compressed_size, nr_pages * PAGE_SIZE);
	if (decomp_ret <= 0 || decomp_ret % PAGE_SIZE != 0) {
		pr_err("BUG: P3 receive: decompression failed or not page-aligned (got %d)\n",
		       decomp_ret);
		BUG();
	}
	if (decomp_ret != nr_pages * PAGE_SIZE) {
		pr_err("BUG: P3 receive: decompression size mismatch (expected %d, got %d)\n",
		       nr_pages * (int)PAGE_SIZE, decomp_ret);
		BUG();
	}

	/* Add each page to buffer - no copy, just store the pointer */
	for (i = 0; i < nr_pages; i++) {
		unsigned long vaddr = pi.vaddr + i * PAGE_SIZE;
		if (cow_page_buffer_add(vaddr, chunk_buf + i * PAGE_SIZE, ctx->thread_id, true) < 0) {
			pr_err("P3 receive: failed to buffer page at 0x%lx\n", vaddr);
			/* Free remaining pages on error */
			for (; i < nr_pages; i++)
				page_pool_put(chunk_buf + i * PAGE_SIZE);
			return -1;
		}
	}

	return nr_pages;
}

static void *p3_receiver_thread_func(void *arg)
{
	struct p3_receiver_ctx *ctx = (struct p3_receiver_ctx *)arg;
	unsigned long pages = 0;
	int ret = 0;

	pr_err("P3 receiver[%d] started on socket %d\n", ctx->thread_id, ctx->socket);
	pr_debug("DEBUG_THREAD: P3 receiver[%d] STARTED socket=%d\n",
	       ctx->thread_id, ctx->socket);

	/* Initialize per-thread page pool for lock-free allocation */
	if (cow_page_buffer_thread_init(ctx->thread_id) < 0) {
		pr_err("P3 receiver[%d]: page pool init failed\n", ctx->thread_id);
		ctx->error = true;
		goto out;
	}

	/* Initialize TLS if enabled */
	if (tls_x509_init(ctx->socket, true)) {
		pr_err("P3 receiver[%d]: TLS init failed\n", ctx->thread_id);
		ctx->error = true;
		goto out;
	}

	/* Receive pages until socket closes */
	while ((ret = p3_receive_and_buffer(ctx)) > 0) {
		pages += ret;
		if (pages % COW_LOG_SAMPLE_1K == 0 && pages > 0)
			pr_debug("DEBUG_THREAD: P3 receiver[%d] progress: %lu pages received\n",
			       ctx->thread_id, pages);
	}

	if (ret < 0) {
		pr_err("P3 receiver[%d]: error receiving pages\n", ctx->thread_id);
		ctx->error = true;
	}

out:
	pr_debug("DEBUG_THREAD: P3 receiver[%d] CLOSING socket=%d pages_received=%lu ret=%d\n",
	       ctx->thread_id, ctx->socket, pages, ret);
	ctx->pages_received = pages;
	ctx->active = false;
	__sync_fetch_and_sub(&p3_receivers_active, 1);
	pr_err("P3 receiver[%d] done: %lu pages\n", ctx->thread_id, pages);
	pr_debug("DEBUG_THREAD: P3 receiver[%d] TERMINATED pages=%lu\n", ctx->thread_id, pages);
	return NULL;
}

/*
 * Accept P3 connections for parallel transfer (PRIMARY/server side).
 * Returns sockets to caller for use with cow_start_p3_threads().
 * Does NOT spawn threads - caller is responsible for using the sockets.
 *
 * Returns number of connections accepted, fills sockets array.
 */
int accept_p3_connections(int *sockets, int max_connections, int timeout_ms)
{
	int listen_sk = get_listen_socket();
	int num_accepted = 0;
	struct sockaddr_storage caddr;
	socklen_t clen;
	int elapsed_ms = 0;

	if (listen_sk < 0) {
		pr_err("accept_p3_connections: no listening socket\n");
		return 0;
	}

	pr_info("Waiting for up to %d P3 connections (timeout=%dms)\n",
		max_connections, timeout_ms);

	while (num_accepted < max_connections && elapsed_ms < timeout_ms) {
		int sk;
		struct pollfd pfd = { .fd = listen_sk, .events = POLLIN };
		int poll_timeout = 100;  /* 100ms poll intervals */

		if (poll(&pfd, 1, poll_timeout) <= 0) {
			elapsed_ms += poll_timeout;
			continue;
		}

		clen = sizeof(caddr);
		sk = accept(listen_sk, (struct sockaddr *)&caddr, &clen);
		if (sk < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				continue;
			pr_perror("accept_p3_connections: accept failed");
			break;
		}

		/* Initialize TLS if enabled */
		if (tls_x509_init(sk, true)) {
			pr_err("accept_p3_connections: TLS init failed for socket %d\n", num_accepted);
			close(sk);
			continue;
		}

		sockets[num_accepted] = sk;
		num_accepted++;
		pr_info("Accepted P3 connection %d (fd=%d)\n", num_accepted, sk);

		/* Reset timeout after each successful accept */
		elapsed_ms = 0;
	}

	pr_info("Accepted %d/%d P3 connections\n", num_accepted, max_connections);
	return num_accepted;
}

void stop_p3_acceptor_thread(void)
{
	int i;
	unsigned long total_pages = 0;

	if (!p3_acceptor_running)
		return;

	p3_acceptor_running = false;
	pthread_join(p3_acceptor_thread, NULL);

	/* Wait for all receiver threads */
	for (i = 0; i < MAX_P3_RECEIVERS; i++) {
		if (p3_receivers[i].thread) {
			pthread_join(p3_receivers[i].thread, NULL);
			total_pages += p3_receivers[i].pages_received;
			if (p3_receivers[i].socket >= 0) {
				close(p3_receivers[i].socket);
				p3_receivers[i].socket = -1;
			}
			/* Free pre-allocated buffers */
			if (p3_receivers[i].compressed_buf) {
				munmap(p3_receivers[i].compressed_buf, P3_COMPRESS_BUF_SIZE);
				p3_receivers[i].compressed_buf = NULL;
			}
			if (p3_receivers[i].decompressed_buf) {
				munmap(p3_receivers[i].decompressed_buf, P3_DECOMPRESS_BUF_SIZE);
				p3_receivers[i].decompressed_buf = NULL;
			}
			p3_receivers[i].thread = 0;
		}
	}

	close_listen_socket();
	pr_info("P3 receivers stopped: %lu total pages\n", total_pages);
}

/*
 * Create multiple connections to page server for parallel P3 transfer.
 * Returns number of connections created, fills sockets array.
 */
static int connect_p3_sockets(int *sockets, int num_requested)
{
	int i, num_created = 0;

	if (!opts.use_page_server || !opts.addr)
		return 0;

	for (i = 0; i < num_requested; i++) {
		int sk = setup_tcp_client(opts.addr);
		if (sk < 0) {
			pr_err("Failed to create P3 socket %d/%d\n", i, num_requested);
			break;
		}

		/* Initialize TLS if enabled */
		if (tls_x509_init(sk, false)) {
			close(sk);
			pr_err("TLS init failed for P3 socket %d\n", i);
			break;
		}

		sockets[i] = sk;
		num_created++;
		pr_debug("Created P3 socket %d: fd=%d\n", i, sk);
	}

	pr_info("Created %d/%d P3 sockets for parallel transfer\n",
		num_created, num_requested);
	return num_created;
}

/*
 * Close P3 sockets after transfer completes.
 */
void close_p3_sockets(int *sockets, int num_sockets)
{
	int i;
	for (i = 0; i < num_sockets; i++) {
		if (sockets[i] >= 0) {
			close(sockets[i]);
			sockets[i] = -1;
		}
	}
}

/*
 * REPLICA side: Create P3 connections to PRIMARY and start receiver threads.
 * Called during lazy-pages startup to enable parallel page reception.
 * Returns number of receiver threads started, 0 on error.
 */
int start_p3_receiver_connections(int num_connections)
{
	int p3_sockets[MAX_P3_RECEIVERS];
	int num_sockets, i;

	if (num_connections > MAX_P3_RECEIVERS)
		num_connections = MAX_P3_RECEIVERS;

	/* Create connections to PRIMARY */
	num_sockets = connect_p3_sockets(p3_sockets, num_connections);
	if (num_sockets == 0) {
		pr_warn("No P3 connections created, parallel receive disabled\n");
		return 0;
	}

	/* Start receiver thread for each connection */
	for (i = 0; i < num_sockets; i++) {
		p3_receivers[i].thread_id = i;
		p3_receivers[i].socket = p3_sockets[i];
		p3_receivers[i].pages_received = 0;
		p3_receivers[i].active = true;
		p3_receivers[i].error = false;

		/* Pre-allocate buffers to avoid malloc/mprotect contention */
		p3_receivers[i].compressed_buf = mmap(NULL, P3_COMPRESS_BUF_SIZE,
						      PROT_READ | PROT_WRITE,
						      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		p3_receivers[i].decompressed_buf = mmap(NULL, P3_DECOMPRESS_BUF_SIZE,
							PROT_READ | PROT_WRITE,
							MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (p3_receivers[i].compressed_buf == MAP_FAILED ||
		    p3_receivers[i].decompressed_buf == MAP_FAILED) {
			pr_perror("Failed to allocate P3 receiver buffers");
			close(p3_sockets[i]);
			p3_receivers[i].socket = -1;
			continue;
		}

		__sync_fetch_and_add(&p3_receivers_active, 1);

		if (pthread_create(&p3_receivers[i].thread, NULL,
				   p3_receiver_thread_func, &p3_receivers[i])) {
			pr_perror("Failed to create P3 receiver thread %d", i);
			close(p3_sockets[i]);
			munmap(p3_receivers[i].compressed_buf, P3_COMPRESS_BUF_SIZE);
			munmap(p3_receivers[i].decompressed_buf, P3_DECOMPRESS_BUF_SIZE);
			p3_receivers[i].active = false;
			p3_receivers[i].socket = -1;
			p3_receivers[i].compressed_buf = NULL;
			p3_receivers[i].decompressed_buf = NULL;
			__sync_fetch_and_sub(&p3_receivers_active, 1);
		}
	}

	pr_info("Started %d P3 receiver threads for parallel transfer\n",
		p3_receivers_active);
	return p3_receivers_active;
}

/*
 * REPLICA side: Stop P3 receiver threads and close connections.
 * Called when page transfer is complete.
 */
void stop_p3_receiver_connections(void)
{
	int i;
	unsigned long total_pages = 0;

	for (i = 0; i < MAX_P3_RECEIVERS; i++) {
		if (p3_receivers[i].thread) {
			pr_warn("DEBUG_THREAD: Waiting for P3 receiver[%d] to join\n", i);
			pthread_join(p3_receivers[i].thread, NULL);
			pr_debug("DEBUG_THREAD: P3 receiver[%d] JOINED pages=%lu\n",
			       i, p3_receivers[i].pages_received);
			total_pages += p3_receivers[i].pages_received;
			if (p3_receivers[i].socket >= 0) {
				pr_debug("DEBUG_THREAD: P3 receiver[%d] closing socket=%d\n",
				       i, p3_receivers[i].socket);
				close(p3_receivers[i].socket);
				p3_receivers[i].socket = -1;
			}
			/* Free pre-allocated buffers */
			if (p3_receivers[i].compressed_buf) {
				munmap(p3_receivers[i].compressed_buf, P3_COMPRESS_BUF_SIZE);
				p3_receivers[i].compressed_buf = NULL;
			}
			if (p3_receivers[i].decompressed_buf) {
				munmap(p3_receivers[i].decompressed_buf, P3_DECOMPRESS_BUF_SIZE);
				p3_receivers[i].decompressed_buf = NULL;
			}
			p3_receivers[i].thread = 0;
		}
	}

	p3_receivers_active = 0;
	pr_debug("DEBUG_THREAD: All P3 receivers stopped: %lu total pages\n", total_pages);
	pr_info("P3 receiver connections stopped: %lu total pages received\n", total_pages);
}
