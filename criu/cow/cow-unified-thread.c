/*
 * COW unified page server thread.
 *
 * This module handles the PRIMARY side COW page transfer:
 * - Page request queue (SPSC) for PS_IOV_GET requests
 * - Active images queue for tracking open transfers
 * - Unified background thread for sending pages (P1/P2/P3 priorities)
 */

#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <linux/userfaultfd.h>
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
#include "cow/cow-uffd.h"
#include "cow/cow-dump.h"
#include "cow/cow-bulk-send.h"
#include "cow/cow-mem.h"
#include "cow/spsc-queue.h"
#include "xmalloc.h"
#include "atomic-bitmap.h"
#include "cr_options.h"

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
static char _page_req_pad[128 - sizeof(struct page_request_spsc_node *)] __attribute__((unused));
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
	if (!entry) {
		pr_err("Failed to allocate page request entry\n");
		return;
	}

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

	if (spsc_enqueue(page_request_tail, page_request_queue_size,
			 entry, struct page_request_spsc_node)) {
		pr_err("Failed to allocate SPSC node for page request\n");
		xfree(entry);
	}
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
	unsigned long total_cow_pages;
	unsigned long total_req_pages;
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

	if (is_convergence_mode()) {
		unsigned long dirty_pages = get_convergence_dirty_pages();
		pr_info("Convergence mode: %lu dirty pages to send (total VMAs: %lu)\n",
			dirty_pages, total_pages);
	}

	img = xzalloc(sizeof(*img));
	if (!img) {
		pr_err("Failed to allocate active image\n");
		return -1;
	}

	img->dst_id = dst_id;
	img->main_sk = sk;
	img->total_cow_pages = 0;
	img->total_req_pages = 0;
	INIT_LIST_HEAD(&img->list);

	pthread_spin_lock(&active_images_lock);
	list_add_tail(&img->list, &active_images_queue);
	pthread_spin_unlock(&active_images_lock);

	pr_info("Added active image dst_id=%lu with %lu total pages\n",
		dst_id, total_pages);
	return 0;
}

/* ========== Timing Statistics ========== */

static struct {
	unsigned long vma_lookup_total_ns;
	unsigned long vma_lookup_count;
	unsigned long send_page_total_ns;
	unsigned long send_page_count;
	unsigned long queue_dequeue_total_ns;
	unsigned long queue_dequeue_count;
	unsigned long send_vm_readv_ns;
	unsigned long send_compress_ns;
	unsigned long send_unprotect_ns;
	unsigned long send_sub_count;
} cow_timing;

struct unified_thread_stats {
	time_t last_print_time;
	unsigned long priority1_pages;
	unsigned long priority2_pages;
	unsigned long priority3_pages;
	unsigned long skip_already_sent;
	unsigned long skip_cow_bitmap;
};

static void print_thread_stats(struct unified_thread_stats *stats)
{
	unsigned long cow_queue = cow_get_pages_queue_size();
	unsigned long req_queue = cow_get_page_request_queue_size();
	float compress_ratio = 0.0;
	struct timespec ts;
	struct tm *tm;

	if (g_compress_uncompressed_bytes > 0)
		compress_ratio = (float)g_compress_compressed_bytes * 100.0 /
				 g_compress_uncompressed_bytes;

	clock_gettime(CLOCK_REALTIME, &ts);
	tm = localtime(&ts.tv_sec);

	pr_err("[UNIFIED_THREAD_STATS] [%02d:%02d:%02d.%03ld] P1(COW)=%lu P2(Req)=%lu P3(Reg)=%lu | Skip: sent=%lu cow=%lu | COW_Q=%lu Req_Q=%lu | Compress: %lu->%lu (%.1f%%)\n",
		tm->tm_hour, tm->tm_min, tm->tm_sec, ts.tv_nsec / 1000000,
		stats->priority1_pages, stats->priority2_pages,
		stats->priority3_pages,
		stats->skip_already_sent, stats->skip_cow_bitmap,
		cow_queue, req_queue,
		g_compress_uncompressed_bytes, g_compress_compressed_bytes,
		compress_ratio);

	pr_err("[COW_TIMING] Queue: %lu ns (%lu ops) | VMA_lookup: %lu ns (%lu ops) | Send: %lu ns (%lu ops)\n",
		cow_timing.queue_dequeue_total_ns, cow_timing.queue_dequeue_count,
		cow_timing.vma_lookup_total_ns, cow_timing.vma_lookup_count,
		cow_timing.send_page_total_ns, cow_timing.send_page_count);

	if (cow_timing.send_sub_count > 0) {
		pr_debug("[SEND_BREAKDOWN] readv=%lu compress+send=%lu unprot=%lu ns (avg per %lu ops)\n",
			cow_timing.send_vm_readv_ns / cow_timing.send_sub_count,
			cow_timing.send_compress_ns / cow_timing.send_sub_count,
			cow_timing.send_unprotect_ns / cow_timing.send_sub_count,
			cow_timing.send_sub_count);
	}

	g_compress_uncompressed_bytes = 0;
	g_compress_compressed_bytes = 0;
	memset(&cow_timing, 0, sizeof(cow_timing));
	stats->priority1_pages = 0;
	stats->priority2_pages = 0;
	stats->priority3_pages = 0;
	stats->skip_already_sent = 0;
	stats->skip_cow_bitmap = 0;
}

static void maybe_print_stats(struct unified_thread_stats *stats)
{
	time_t now = time(NULL);

	if (now - stats->last_print_time >= 30) {
		print_thread_stats(stats);
		stats->last_print_time = now;
	}
}

/* ========== Page Send Functions ========== */

static int send_lazy_vma_page(int sk, unsigned long vaddr, u64 dst_id, pid_t source_pid)
{
	void *buffer;
	int ret;
	int uffd;
	struct iovec local_iov, remote_iov;
	struct timespec t_start, t_readv, t_socket, t_unprot;

	pr_debug("[SEND_PAGE] Sending non-COW page at vaddr=0x%lx pid=%d\n",
		 vaddr, source_pid);

	clock_gettime(CLOCK_MONOTONIC, &t_start);

	buffer = xmalloc(PAGE_SIZE);
	if (!buffer)
		return -1;

	local_iov.iov_base = buffer;
	local_iov.iov_len = PAGE_SIZE;
	remote_iov.iov_base = (void *)vaddr;
	remote_iov.iov_len = PAGE_SIZE;

	ret = process_vm_readv(source_pid, &local_iov, 1, &remote_iov, 1, 0);
	clock_gettime(CLOCK_MONOTONIC, &t_readv);

	if (ret != PAGE_SIZE) {
		pr_perror("Failed to read page at %lx from pid %d", vaddr, source_pid);
		xfree(buffer);
		return -1;
	}

	ret = send_page_compressed(sk, buffer, dst_id, vaddr);
	clock_gettime(CLOCK_MONOTONIC, &t_socket);
	xfree(buffer);

	if (ret != 0) {
		pr_perror("Failed to send page at 0x%lx", vaddr);
		return -1;
	}

	if (cow_get_phase() == COW_PHASE_SYNC_CONVERGE) {
		pr_debug("[SEND_PAGE unprotect] Sending non-COW page at vaddr=0x%lx pid=%d\n",
			 vaddr, source_pid);
		uffd = cow_get_uffd_for_pid(source_pid);
		if (uffd >= 0) {
			struct uffdio_writeprotect wp;
			wp.range.start = vaddr;
			wp.range.len = PAGE_SIZE;
			wp.mode = 0;
			if (ioctl(uffd, UFFDIO_WRITEPROTECT, &wp))
				pr_perror("Failed to unprotect page at 0x%lx", vaddr);
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &t_unprot);

	cow_timing.send_vm_readv_ns += (t_readv.tv_sec - t_start.tv_sec) * 1000000000 +
				       (t_readv.tv_nsec - t_start.tv_nsec);
	cow_timing.send_compress_ns += (t_socket.tv_sec - t_readv.tv_sec) * 1000000000 +
				       (t_socket.tv_nsec - t_readv.tv_nsec);
	cow_timing.send_unprotect_ns += (t_unprot.tv_sec - t_socket.tv_sec) * 1000000000 +
					(t_unprot.tv_nsec - t_socket.tv_nsec);
	cow_timing.send_sub_count++;

	return 1;
}

static int send_cow_page_lazy(struct cow_page_queue_entry *entry,
			      struct active_image *img, pid_t source_pid)
{
	struct lazy_vma_entry *lve;
	unsigned long page_idx;
	int ret;
	struct timespec t1, t2;
	bool was_already_sent;

	clock_gettime(CLOCK_MONOTONIC, &t1);

	lve = find_lazy_vma_for_addr(entry->vaddr, img->dst_id);

	clock_gettime(CLOCK_MONOTONIC, &t2);
	cow_timing.vma_lookup_total_ns += (t2.tv_sec - t1.tv_sec) * 1000000000 +
					  (t2.tv_nsec - t1.tv_nsec);
	cow_timing.vma_lookup_count++;

	if (!lve) {
		pr_err("COW page 0x%lx not in any lazy VMA (dst_id=%lu)\n",
		       entry->vaddr, img->dst_id);
		return -1;
	}

	page_idx = (entry->vaddr - lve->start) / PAGE_SIZE;
	was_already_sent = bitmap_test_nonatomic(lve->sent_bitmap, page_idx);

	if (was_already_sent)
		return 2;

	if (!entry->data) {
		pr_err("COW queue entry 0x%lx has no data!\n", entry->vaddr);
		return -1;
	}

	clock_gettime(CLOCK_MONOTONIC, &t1);

	ret = send_page_compressed(img->main_sk, entry->data, img->dst_id,
				   entry->vaddr);
	pr_debug("COW page 0x%lx sent VMA (dst_id=%lu)\n",
		 entry->vaddr, img->dst_id);

	clock_gettime(CLOCK_MONOTONIC, &t2);
	cow_timing.send_page_total_ns += (t2.tv_sec - t1.tv_sec) * 1000000000 +
					 (t2.tv_nsec - t1.tv_nsec);
	cow_timing.send_page_count++;

	if (ret < 0) {
		pr_warn("Failed to send COW page 0x%lx, re-queueing for retry\n",
			entry->vaddr);
		cow_put_back_page(entry);
		return -2;
	}

	bitmap_set_nonatomic(lve->sent_bitmap, page_idx, &lve->sent_pages);
	return 1;
}

static int send_request_page_lazy(struct page_request_entry *req,
				  struct active_image *img, pid_t source_pid)
{
	unsigned long i;
	int ret;
	int sent_count = 0;

	for (i = 0; i < req->nr_pages; i++) {
		unsigned long page_vaddr = req->vaddr + (i * PAGE_SIZE);
		struct lazy_vma_entry *lve;
		unsigned long page_idx;

		lve = find_lazy_vma_for_addr(page_vaddr, req->dst_id);
		if (!lve) {
			pr_err("Request page 0x%lx not in any lazy VMA\n", page_vaddr);
			return -1;
		}

		page_idx = (page_vaddr - lve->start) / PAGE_SIZE;

		if (bitmap_test_nonatomic(lve->sent_bitmap, page_idx)) {
			pr_debug("Request page 0x%lx already sent, skipping\n", page_vaddr);
			continue;
		}

		if (lve->cow_bitmap &&
		    atomic_bitmap_test(lve->cow_bitmap, page_idx)) {
			pr_err("P2: page 0x%lx is COW, skipping for P1\n", page_vaddr);
			continue;
		}

		pr_debug("[SEND_PAGE] Sending #PF req page at vaddr=0x%lx pid=%d\n",
			 page_vaddr, source_pid);

		ret = send_lazy_vma_page(img->main_sk, page_vaddr, req->dst_id, source_pid);
		if (ret < 0)
			return -1;

		bitmap_set_nonatomic(lve->sent_bitmap, page_idx, &lve->sent_pages);
		sent_count++;
	}

	return sent_count;
}

/* ========== Drain Functions ========== */

static int drain_cow_pages(struct active_image *img, pid_t source_pid,
			   int max_pages, struct unified_thread_stats *stats)
{
	int sent = 0;

	while (max_pages > 0 && cow_has_pending_pages()) {
		struct cow_page_queue_entry *entry;
		struct timespec tq1, tq2;
		int ret;

		clock_gettime(CLOCK_MONOTONIC, &tq1);
		entry = cow_get_next_page();
		clock_gettime(CLOCK_MONOTONIC, &tq2);
		cow_timing.queue_dequeue_total_ns +=
			(tq2.tv_sec - tq1.tv_sec) * 1000000000 +
			(tq2.tv_nsec - tq1.tv_nsec);
		cow_timing.queue_dequeue_count++;

		if (!entry)
			break;

		ret = send_cow_page_lazy(entry, img, source_pid);

		if (ret == -2) {
			max_pages--;
			continue;
		}

		if (entry->data)
			xfree(entry->data);
		xfree(entry);

		if (ret < 0) {
			pr_err("Failed to send COW page (fatal error)\n");
			return -1;
		}

		if (ret == 1) {
			img->total_cow_pages++;
			stats->priority1_pages++;
			sent++;
		}
		max_pages--;
	}

	return sent;
}

static int drain_page_requests(struct active_image *img, pid_t source_pid,
			       struct unified_thread_stats *stats)
{
	int sent = 0;

	while (cow_has_page_requests()) {
		struct page_request_entry *req = get_next_page_request();
		int ret;

		if (!req)
			break;

		ret = send_request_page_lazy(req, img, source_pid);

		if (ret > 0) {
			img->total_req_pages += ret;
			stats->priority2_pages += ret;
			sent += ret;
		}

		xfree(req);

		if (ret < 0) {
			pr_err("Failed to send request page\n");
			return -1;
		}
	}

	return sent;
}

static int send_single_lazy_page(struct active_image *img,
				 struct lazy_vma_entry *lve,
				 unsigned long vaddr, unsigned long page_idx,
				 pid_t source_pid,
				 struct unified_thread_stats *stats)
{
	int ret;

	if (bitmap_test_nonatomic(lve->sent_bitmap, page_idx)) {
		stats->skip_already_sent++;
		return 0;
	}

	if (lve->cow_bitmap &&
	    atomic_bitmap_test(lve->cow_bitmap, page_idx)) {
		stats->skip_cow_bitmap++;
		return 0;
	}

	ret = send_lazy_vma_page(img->main_sk, vaddr, img->dst_id, source_pid);
	if (ret < 0) {
		pr_err("Failed to send lazy VMA page at %lx\n", vaddr);
		return -1;
	}

	bitmap_set_nonatomic(lve->sent_bitmap, page_idx, &lve->sent_pages);
	stats->priority3_pages++;

	return 1;
}

static int send_image_complete(struct active_image *img)
{
	struct page_server_iov close_cmd = {
		.cmd = PS_IOV_CLOSE,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = img->dst_id,
	};

	pr_warn("Image dst_id=%lu complete (%lu COW, %lu req pages)\n",
		img->dst_id, img->total_cow_pages, img->total_req_pages);

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

static int process_vma_pages(struct active_image *img,
			     struct lazy_vma_entry *lve,
			     pid_t source_pid,
			     struct unified_thread_stats *stats)
{
	unsigned long vaddr;
	unsigned long page_idx = 0;

	pr_info("Processing VMA: %lx-%lx len=%lu\n",
		lve->start, lve->end, lve->end - lve->start);

	for (vaddr = lve->start; vaddr < lve->end; vaddr += PAGE_SIZE, page_idx++) {
		maybe_print_stats(stats);

		if (drain_cow_pages(img, source_pid, 100, stats) < 0)
			return -1;

		if (drain_page_requests(img, source_pid, stats) < 0)
			return -1;

		if (send_single_lazy_page(img, lve, vaddr, page_idx,
					  source_pid, stats) < 0)
			return -1;
	}

	return 0;
}

static int final_queue_drain(struct active_image *img, pid_t source_pid,
			     struct unified_thread_stats *stats)
{
	pr_debug("final_queue_drain: cow_has_pending=%d has_requests=%d\n",
		 cow_has_pending_pages(), cow_has_page_requests());

	while (cow_has_pending_pages() || cow_has_page_requests()) {
		int cow_sent, req_sent;

		cow_sent = drain_cow_pages(img, source_pid, 100, stats);
		if (cow_sent < 0) {
			pr_err("cow_sent < 0\n");
			return -1;
		}

		req_sent = drain_page_requests(img, source_pid, stats);
		if (req_sent < 0) {
			pr_err("req_sent < 0\n");
			return -1;
		}

		if (cow_sent == 0 && req_sent == 0) {
			pr_err("break from final_queue_drain\n");
			break;
		}
	}

	return 0;
}

/* ========== Unified Thread ========== */

static void *unified_page_server_thread(void *arg)
{
	struct unified_thread_stats stats = { 0 };

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

			/* Debug: dump all lazy VMAs to understand dst_id matching */
			{
				int lve_count = 0;
				pr_err("DEBUG: Searching lazy_vmas for dst_id=%lu:\n", img->dst_id);
				list_for_each_entry(lve, get_global_lazy_vmas(), list) {
					pr_err("  LVE[%d]: dst_id=%lu start=0x%lx end=0x%lx source_pid=%d\n",
					       lve_count++, lve->dst_id, lve->start, lve->end,
					       lve->source_pid);
					if (lve->dst_id == img->dst_id) {
						source_pid = lve->source_pid;
						pr_err("  -> MATCH FOUND! source_pid=%d\n", source_pid);
						break;
					}
				}
				if (lve_count == 0)
					pr_err("  -> lazy_vmas list is EMPTY!\n");
				else if (source_pid == 0)
					pr_err("  -> NO MATCH found for dst_id=%lu\n", img->dst_id);
			}

			pr_err("DEBUG: source_pid=%d is_convergence_mode=%d\n",
			       source_pid, is_convergence_mode());

			if (!is_convergence_mode() && source_pid != 0) {
				int num_threads = cow_get_num_p3_threads();
				int p3_sockets[NUM_P3_THREADS];
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
			} else {
				list_for_each_entry(lve, get_global_lazy_vmas(), list) {
					if (lve->dst_id != img->dst_id)
						continue;

					source_pid = lve->source_pid;

					if (process_vma_pages(img, lve, source_pid, &stats) < 0) {
						pr_err("Error processing VMA %lx-%lx\n",
						       lve->start, lve->end);
						break;
					}
				}
			}

			print_thread_stats(&stats);

			pthread_spin_lock(&active_images_lock);
			if (final_queue_drain(img, source_pid, &stats) < 0) {
				pr_err("Error in final queue drain\n");
			}
			pthread_spin_unlock(&active_images_lock);

			if (is_convergence_mode()) {
				long unsent;

				unsent = verify_all_lazy_vmas_sent();
				BUG_ON(unsent > 0);

				if (send_all_pages_sent_signal(img->main_sk) < 0)
					pr_err("Failed to send all_pages_sent signal\n");

				if (wait_for_all_pages_sent_ack(img->main_sk) < 0)
					pr_err("Failed to receive all_pages_sent ACK\n");
			}

			if (send_image_complete(img) < 0)
				pr_err("Failed to complete image dst_id=%lu\n",
				       img->dst_id);

			pthread_spin_lock(&active_images_lock);
			list_del(&img->list);
			xfree(img);
		}

		g_unified_thread_stop = list_empty(&active_images_queue);
		pthread_spin_unlock(&active_images_lock);
	}

	print_thread_stats(&stats);
	pr_err("Unified page server thread stopped\n");
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
