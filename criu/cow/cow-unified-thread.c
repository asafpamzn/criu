/*
 * COW unified page server thread.
 *
 * This module handles the PRIMARY side COW page transfer:
 * - Page request queue (SPSC) for PS_IOV_GET requests
 * - Active images queue for tracking open transfers
 * - Unified background thread for sending pages (P1/P2/P3 priorities)
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

/* ========== Page Request Queue (SPSC) ========== */

struct page_request_entry {
	unsigned long vaddr;
	unsigned long nr_pages;
	int sk;
	u64 dst_id;

	/* Location info (filled on first access) */
	struct page_pipe_buf *ppb;
	unsigned int seg_idx;
	unsigned long page_idx_in_seg;
	bool location_found;
};

DECLARE_SPSC_NODE(page_request, struct page_request_entry);

static struct page_request_spsc_node *page_request_head;
static char _page_req_pad[COW_SPSC_PADDING - sizeof(struct page_request_spsc_node *)] __attribute__((unused));
static struct page_request_spsc_node *page_request_tail;
static unsigned long page_request_queue_size;
static bool page_request_queue_initialized = false;

void cow_init_page_request_queue(void)
{
	if (page_request_queue_initialized)
		return;

	if (spsc_init(page_request_head, page_request_tail,
		      page_request_queue_size,
		      struct page_request_spsc_node)) {
		pr_err("Failed to allocate dummy node for page request queue\n");
		return;
	}

	page_request_queue_initialized = true;
}

void cow_add_page_request(unsigned long vaddr, unsigned long nr_pages, int sk, u64 dst_id)
{
	struct page_request_entry *entry;

	entry = xmalloc(sizeof(*entry));
	BUG_ON(!entry);

	entry->vaddr = vaddr;
	entry->nr_pages = nr_pages;
	entry->sk = sk;
	entry->dst_id = dst_id;
	entry->ppb = NULL;
	entry->seg_idx = 0;
	entry->page_idx_in_seg = 0;
	entry->location_found = false;

	pr_debug("Requesting page at %lx (nr_pages=%lu, dst_id=%lu)\n",
		 vaddr, nr_pages, dst_id);

	BUG_ON(spsc_enqueue(page_request_tail, page_request_queue_size,
			    entry, struct page_request_spsc_node));
}

static struct page_request_entry *get_next_page_request(void)
{
	return spsc_dequeue(page_request_head, page_request_queue_size);
}

bool cow_has_page_requests(void)
{
	return spsc_peek(page_request_head);
}

unsigned long cow_get_page_request_queue_size(void)
{
	return spsc_size(page_request_queue_size);
}

void cow_enqueue_page_requests(unsigned long vaddr, unsigned long nr_pages, int sk, u64 dst_id)
{
	unsigned long i;
	for (i = 0; i < nr_pages; i++) {
		cow_add_page_request(vaddr + (i * PAGE_SIZE), 1, sk, dst_id);
	}
	pr_debug("Enqueued %lu page requests starting at vaddr=%lx\n",
		 nr_pages, vaddr);
}

/* ========== Active Images Queue ========== */

struct active_image {
	u64 dst_id;
	int main_sk;
	struct list_head list;
};

static LIST_HEAD(active_images_queue);
static pthread_spinlock_t active_images_lock;
static pthread_once_t active_images_lock_once = PTHREAD_ONCE_INIT;

static pthread_t g_unified_thread;
static _Atomic bool g_unified_thread_running = false;
static _Atomic bool g_unified_thread_stop = false;

static void cleanup_active_images_queue(void);

static void init_active_images_lock_once(void)
{
	pthread_spin_init(&active_images_lock, PTHREAD_PROCESS_PRIVATE);
}

void cow_init_active_images_queue(void)
{
	pthread_once(&active_images_lock_once, init_active_images_lock_once);
}

static void cleanup_active_images_queue(void)
{
	struct active_image *img, *tmp;

	if (list_empty(&active_images_queue))
		return;

	cow_init_active_images_queue();

	pthread_spin_lock(&active_images_lock);
	list_for_each_entry_safe(img, tmp, &active_images_queue, list) {
		list_del(&img->list);
		if (img->main_sk >= 0)
			close(img->main_sk);
		xfree(img);
	}
	pthread_spin_unlock(&active_images_lock);

	pthread_spin_destroy(&active_images_lock);
}

void cow_wait_for_page_server_thread(void)
{
	if (!g_unified_thread_running) {
		pr_err("ERROR wait_for_page_server_thread: thread is not running.\n");
		return;
	}

	pr_info("Waiting for page server thread to finish...\n");
	pthread_join(g_unified_thread, NULL);
	g_unified_thread_running = false;
	pr_info("Page server thread finished\n");

	cleanup_active_images_queue();
}

static struct active_image *find_active_image(u64 dst_id)
{
	struct active_image *img;

	list_for_each_entry(img, &active_images_queue, list) {
		if (img->dst_id == dst_id)
			return img;
	}
	return NULL;
}

int cow_add_active_image(u64 dst_id, int sk)
{
	struct active_image *img;
	unsigned long total_pages;

	pthread_spin_lock(&active_images_lock);

	if (find_active_image(dst_id)) {
		pthread_spin_unlock(&active_images_lock);
		pr_info("Image dst_id=%lu already active\n", dst_id);
		return 0;
	}

	pthread_spin_unlock(&active_images_lock);

	total_pages = count_lazy_vma_pages(dst_id);

	if (total_pages == 0) {
		pr_err("Image dst_id=%lu matched ZERO lazy VMA pages\n", dst_id);
		return 0;
	}

	img = xzalloc(sizeof(*img));
	BUG_ON(!img);

	img->dst_id = dst_id;
	img->main_sk = sk;
	INIT_LIST_HEAD(&img->list);

	pthread_spin_lock(&active_images_lock);
	list_add_tail(&img->list, &active_images_queue);
	pthread_spin_unlock(&active_images_lock);

	pr_info("Added active image dst_id=%lu with %lu total pages\n",
		dst_id, total_pages);
	return 0;
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

static int send_image_complete(struct active_image *img)
{
	struct page_server_iov close_cmd = {
		.cmd = PS_IOV_CLOSE,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = img->dst_id,
	};

	pr_info("Image dst_id=%lu complete\n", img->dst_id);

	if (send_psi(img->main_sk, &close_cmd)) {
		if (errno == EPIPE || errno == ECONNRESET) {
			pr_info("Receiver closed after close marker, treating as completion\n");
			return 0;
		}
		pr_err("Failed to send close command\n");
		return -1;
	}
	return 0;
}

/* ========== Unified Thread ========== */

static void *unified_page_server_thread(void *arg)
{
	pthread_setname_np(pthread_self(), "criu-page-srv");
	pr_info("Unified page server thread started\n");

	while (!g_unified_thread_stop) {
		struct active_image *img, *tmp;

		pthread_spin_lock(&active_images_lock);

		list_for_each_entry_safe(img, tmp, &active_images_queue, list) {
			struct lazy_vma_entry *lve;
			pid_t source_pid = 0;

			pthread_spin_unlock(&active_images_lock);

			pr_info("Processing image dst_id=%lu\n", img->dst_id);

			/* Find source_pid from lazy VMAs */
			list_for_each_entry(lve, get_global_lazy_vmas(), list) {
				if (lve->dst_id == img->dst_id) {
					source_pid = lve->source_pid;
					break;
				}
			}

			if (source_pid != 0) {
				int num_threads = cow_get_num_p3_threads();
				int p3_sockets[COW_NUM_P3_THREADS];
				int num_sockets = 0;

				num_sockets = accept_p3_connections(p3_sockets, num_threads, 5000);
				if (num_sockets == 0) {
					pr_warn("No P3 connections accepted, falling back to single-threaded\n");
				} else {
					pr_info("Starting %d P3 bulk sender threads for dst_id=%lu\n",
						num_sockets, img->dst_id);
					if (cow_start_p3_threads(p3_sockets, num_sockets,
								 img->dst_id, source_pid) < 0) {
						pr_err("Failed to start P3 threads\n");
						close_p3_sockets(p3_sockets, num_sockets);
					} else {
						/*
						 * P3 threads now run in iterative dirty scan loop.
						 * Main thread monitors convergence and signals last_scan.
						 * Do NOT wait here - let unified thread exit.
						 * Do NOT close sockets - threads still using them.
						 */
						pr_info("P3 threads started, unified thread exiting\n");
					}
				}
			}

			print_compress_stats();
			BUG_ON(send_image_complete(img) < 0);

			pthread_spin_lock(&active_images_lock);
			list_del(&img->list);
			xfree(img);
		}

		g_unified_thread_stop = list_empty(&active_images_queue);
		pthread_spin_unlock(&active_images_lock);
	}

	print_compress_stats();
	pr_info("Unified page server thread stopped\n");
	g_unified_thread_running = false;
	return NULL;
}

int cow_page_server_get_all_pages(int sk, u64 dst_id)
{
	int ret;

	pr_warn("Adding image dst_id=%lu to batch transfer queue\n", dst_id);

	cow_init_active_images_queue();
	cow_init_page_request_queue();

	ret = cow_add_active_image(dst_id, sk);
	if (ret < 0)
		return -1;

	if (!g_unified_thread_running) {
		pr_info("Starting unified page server thread\n");
		g_unified_thread_stop = false;
		ret = pthread_create(&g_unified_thread, NULL,
				     unified_page_server_thread, NULL);
		if (ret) {
			pr_perror("Failed to create unified thread");
			return -1;
		}
		g_unified_thread_running = true;
	}

	return 0;
}
