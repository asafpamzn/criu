/*
 * COW Phase 2 Lazy Pages - Page buffering for phased migration
 *
 * In COW phased migration, Phase 2 runs before the skeleton dump.
 * At this point we only have pagemap/pages images - no inventory.img
 * or pstree.img yet. This file implements a minimal lazy-pages mode
 * that just buffers incoming pages without requiring the full pstree.
 */

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <signal.h>

#include "types.h"
#include "cr_options.h"
#include "criu-log.h"
#include "page-xfer.h"
#include "rst-malloc.h"
#include "util.h"
#include "xmalloc.h"
#include "common/list.h"
#include "servicefd.h"
#include "cow-lazy-pages.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-lazy: "

/* List of discovered task PIDs from pagemap files */
struct cow_task {
	int pid;
	struct list_head l;
};

static LIST_HEAD(cow_tasks);
static int nr_cow_tasks;

/* Page buffer for storing received pages */
struct cow_page_entry {
	unsigned long vaddr;
	int pid;
	void *data;
	struct hlist_node hash;
};

#define COW_PAGE_HASH_BITS	16
#define COW_PAGE_HASH_SIZE	(1 << COW_PAGE_HASH_BITS)

static struct hlist_head *cow_page_hash;
static unsigned long cow_pages_buffered;
static unsigned long cow_pages_total_bytes;

static inline unsigned int cow_page_hash_fn(int pid, unsigned long vaddr)
{
	return (pid ^ (vaddr >> 12)) & (COW_PAGE_HASH_SIZE - 1);
}

static int cow_page_buffer_init(void)
{
	cow_page_hash = xzalloc(COW_PAGE_HASH_SIZE * sizeof(struct hlist_head));
	if (!cow_page_hash)
		return -1;

	for (int i = 0; i < COW_PAGE_HASH_SIZE; i++)
		INIT_HLIST_HEAD(&cow_page_hash[i]);

	cow_pages_buffered = 0;
	cow_pages_total_bytes = 0;
	pr_info("Page buffer initialized (%d buckets)\n", COW_PAGE_HASH_SIZE);
	return 0;
}

int cow_page_buffer_add(int pid, unsigned long vaddr, void *data, size_t len)
{
	struct cow_page_entry *entry;
	unsigned int hash;

	if (!cow_page_hash)
		return -1;

	entry = xmalloc(sizeof(*entry));
	if (!entry)
		return -1;

	entry->data = xmalloc(len);
	if (!entry->data) {
		xfree(entry);
		return -1;
	}

	memcpy(entry->data, data, len);
	entry->vaddr = vaddr;
	entry->pid = pid;

	hash = cow_page_hash_fn(pid, vaddr);
	hlist_add_head(&entry->hash, &cow_page_hash[hash]);

	cow_pages_buffered++;
	cow_pages_total_bytes += len;

	return 0;
}

void *cow_page_buffer_lookup(int pid, unsigned long vaddr)
{
	struct cow_page_entry *entry;
	unsigned int hash;

	if (!cow_page_hash)
		return NULL;

	hash = cow_page_hash_fn(pid, vaddr);
	hlist_for_each_entry(entry, &cow_page_hash[hash], hash) {
		if (entry->pid == pid && entry->vaddr == vaddr)
			return entry->data;
	}

	return NULL;
}

static void cow_page_buffer_destroy(void)
{
	struct cow_page_entry *entry;
	struct hlist_node *tmp;
	int i;

	if (!cow_page_hash)
		return;

	for (i = 0; i < COW_PAGE_HASH_SIZE; i++) {
		hlist_for_each_entry_safe(entry, tmp, &cow_page_hash[i], hash) {
			hlist_del(&entry->hash);
			xfree(entry->data);
			xfree(entry);
		}
	}

	xfree(cow_page_hash);
	cow_page_hash = NULL;

	pr_info("Page buffer destroyed (had %lu pages, %lu bytes)\n",
		cow_pages_buffered, cow_pages_total_bytes);
}

/*
 * Discover tasks by scanning for pagemap-*.img files in images directory.
 * Returns 0 on success, -1 on error.
 */
static int discover_tasks_from_pagemaps(void)
{
	DIR *dir;
	struct dirent *de;
	int img_dir_fd;

	img_dir_fd = get_service_fd(IMG_FD_OFF);
	if (img_dir_fd < 0) {
		pr_err("No image directory fd\n");
		return -1;
	}

	dir = fdopendir(dup(img_dir_fd));
	if (!dir) {
		pr_perror("Cannot open images directory");
		return -1;
	}

	while ((de = readdir(dir)) != NULL) {
		int pid;
		struct cow_task *ct;

		/* Look for pagemap-NNNN.img files */
		if (strncmp(de->d_name, "pagemap-", 8) != 0)
			continue;
		if (sscanf(de->d_name, "pagemap-%d.img", &pid) != 1)
			continue;

		ct = xmalloc(sizeof(*ct));
		if (!ct) {
			closedir(dir);
			return -1;
		}

		ct->pid = pid;
		list_add_tail(&ct->l, &cow_tasks);
		nr_cow_tasks++;
		pr_info("Discovered task pid=%d from %s\n", pid, de->d_name);
	}

	closedir(dir);

	if (nr_cow_tasks == 0) {
		pr_err("No pagemap files found in images directory\n");
		return -1;
	}

	pr_info("Discovered %d tasks from pagemap files\n", nr_cow_tasks);
	return 0;
}

static void free_cow_tasks(void)
{
	struct cow_task *ct, *tmp;

	list_for_each_entry_safe(ct, tmp, &cow_tasks, l) {
		list_del(&ct->l);
		xfree(ct);
	}
	nr_cow_tasks = 0;
}

/*
 * COW Phase 2 lazy-pages entry point.
 *
 * This is called instead of the normal cr_lazy_pages() when
 * --cow-dump --page-server are both specified. It:
 *   1. Discovers tasks from pagemap files (no pstree needed)
 *   2. Connects to the page server
 *   3. Requests all pages for each task
 *   4. Buffers received pages for later use in Phase 3
 */
int cr_lazy_pages_cow_phase2(bool daemon)
{
	struct epoll_event *events = NULL;
	struct cow_task *ct;
	int epollfd;
	int ret = -1;
	int nr_fds;

	pr_info("=== COW Phase 2: Page buffering mode ===\n");

	/* 1. Discover tasks from pagemap files */
	if (discover_tasks_from_pagemaps())
		return -1;

	/* 2. Initialize page buffer */
	if (cow_page_buffer_init())
		goto err_tasks;

	/* 3. Daemonize if requested */
	if (daemon) {
		ret = cr_daemon(1, 0, -1);
		if (ret == -1) {
			pr_err("Can't run in the background\n");
			goto err_buffer;
		}
		if (ret > 0) {
			/* Parent - daemon started successfully */
			if (opts.pidfile) {
				if (write_pidfile(ret) == -1) {
					pr_perror("Can't write pidfile");
					kill(ret, SIGKILL);
					waitpid(ret, NULL, 0);
					goto err_buffer;
				}
			}
			return 0;
		}
		/* Child continues */
	}

	/* 4. Set up epoll - just need page server socket */
	nr_fds = 4;  /* page server + some margin */
	epollfd = epoll_prepare(nr_fds, &events);
	if (epollfd < 0)
		goto err_buffer;

	/* 5. Connect to page server */
	if (connect_to_page_server_to_recv(epollfd)) {
		pr_err("Failed to connect to page server\n");
		goto err_epoll;
	}

	/* 6. Request all pages for each discovered task */
	list_for_each_entry(ct, &cow_tasks, l) {
		pr_info("Requesting all pages for pid=%d\n", ct->pid);
		if (request_all_remote_pages(ct->pid) < 0) {
			pr_err("Failed to request pages for pid=%d\n", ct->pid);
			goto err_disconnect;
		}
	}

	pr_info("Waiting to receive pages from primary...\n");

	/* 7. Event loop - receive and buffer pages */
	ret = cow_phase2_handle_pages(epollfd, events, nr_fds);

	pr_info("Phase 2 complete: buffered %lu pages (%lu bytes)\n",
		cow_pages_buffered, cow_pages_total_bytes);

err_disconnect:
	disconnect_from_page_server();
err_epoll:
	xfree(events);
err_buffer:
	cow_page_buffer_destroy();
err_tasks:
	free_cow_tasks();
	return ret;
}

/*
 * Simple event loop for Phase 2 - just receive pages until done.
 */
int cow_phase2_handle_pages(int epollfd, struct epoll_event *events, int nr_fds)
{
	int ret;

	while (1) {
		ret = epoll_run_rfds(epollfd, events, nr_fds, -1);
		if (ret < 0) {
			pr_err("epoll_run_rfds failed\n");
			return -1;
		}

		/* Check if bulk transfer is complete */
		if (page_server_bulk_stream_done()) {
			pr_info("Bulk stream complete\n");
			return 0;
		}
	}

	return 0;
}

unsigned long cow_get_buffered_pages_count(void)
{
	return cow_pages_buffered;
}

unsigned long cow_get_buffered_pages_bytes(void)
{
	return cow_pages_total_bytes;
}
