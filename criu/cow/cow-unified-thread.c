/*
 * COW unified page server thread.
 *
 * This module handles the PRIMARY side COW page transfer by starting
 * P3 bulk sender threads to transfer pages to the replica.
 */

#include <sys/socket.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <string.h>

#include "types.h"
#include "page.h"
#include "criu-log.h"
#include "page-xfer.h"
#include "cow/cow-page-xfer.h"
#include "cow/cow-unified-thread.h"
#include "cow/cow-bulk-send.h"
#include "cow/cow-mem.h"
#include "cow/cow-conf.h"
#include "cow/spsc-queue.h"
#include "xmalloc.h"
#include "cr_options.h"
#include "common/bug.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-thread: "


/* ========== Thread State ========== */

static pthread_t g_unified_thread;
static _Atomic bool g_unified_thread_running = false;

/* Thread arguments */
struct unified_thread_args {
	u64 dst_id;
	int sk;
};

void cow_wait_for_page_server_thread(void)
{
	if (!g_unified_thread_running) {
		pr_info("Page server thread not running, nothing to wait for\n");
		return;
	}

	pr_err("Waiting for page server thread to finish...\n");
	pthread_join(g_unified_thread, NULL);
	g_unified_thread_running = false;
	pr_info("Page server thread finished\n");
}

static void print_compress_stats(void)
{
	float compress_ratio = 0.0;

	if (g_compress_uncompressed_bytes > 0)
		compress_ratio = (float)g_compress_compressed_bytes * 100.0 /
				 g_compress_uncompressed_bytes;

	pr_info("Compress stats: %lu->%lu (%.1f%%)\n",
		g_compress_uncompressed_bytes, g_compress_compressed_bytes,
		compress_ratio);

	g_compress_uncompressed_bytes = 0;
	g_compress_compressed_bytes = 0;
}

/* ========== Unified Thread ========== */

static void *unified_page_server_thread(void *arg)
{
	struct unified_thread_args *args = arg;
	struct lazy_vma_entry *lve;
	pid_t source_pid = 0;
	int num_threads, num_sockets;
	int p3_sockets[COW_NUM_P3_THREADS];

	pthread_setname_np(pthread_self(), "criu-page-srv");
	pr_info("Page server thread started for dst_id=%lu\n", args->dst_id);

	/* Find source_pid from lazy VMAs */
	list_for_each_entry(lve, get_global_lazy_vmas(), list) {
		if (lve->dst_id == args->dst_id) {
			source_pid = lve->source_pid;
			break;
		}
	}

	if (source_pid == 0) {
		pr_err("No lazy VMA found for dst_id=%lu\n", args->dst_id);
		goto out;
	}

	/* Accept P3 connections and start bulk sender threads */
	num_threads = cow_get_num_p3_threads();
	num_sockets = accept_p3_connections(p3_sockets, num_threads, 5000);

	if (num_sockets == 0) {
		pr_warn("No P3 connections accepted\n");
		goto out;
	}

	pr_err("Starting %d P3 bulk sender threads for dst_id=%lu\n",
		num_sockets, args->dst_id);

	if (cow_start_p3_threads(p3_sockets, num_sockets,
				 args->dst_id, source_pid) < 0) {
		pr_err("Failed to start P3 threads\n");
		close_p3_sockets(p3_sockets, num_sockets);
		goto out;
	}

	pr_info("P3 threads started successfully\n");

out:
	print_compress_stats();
	/*
	 * Don't send PS_IOV_CLOSE here - P3 threads are still running.
	 * Main dump loop will send end-of-transfer after cow_wait_p3_threads().
	 */

	xfree(args);
	g_unified_thread_running = false;
	pr_info("Page server thread stopped\n");
	return NULL;
}

int cow_page_server_get_all_pages(int sk, u64 dst_id)
{
	struct unified_thread_args *args;
	unsigned long total_pages;
	int ret;

	pr_info("Starting page server for dst_id=%lu\n", dst_id);

	total_pages = count_lazy_vma_pages(dst_id);
	if (total_pages == 0) {
		pr_err("dst_id=%lu matched ZERO lazy VMA pages\n", dst_id);
		return 0;
	}

	pr_info("Found %lu total pages for dst_id=%lu\n", total_pages, dst_id);

	args = xmalloc(sizeof(*args));
	BUG_ON(!args);
	args->dst_id = dst_id;
	args->sk = sk;

	ret = pthread_create(&g_unified_thread, NULL,
			     unified_page_server_thread, args);
	if (ret) {
		pr_perror("Failed to create page server thread");
		xfree(args);
		return -1;
	}

	g_unified_thread_running = true;
	return 0;
}
