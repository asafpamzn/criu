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
#include "util.h"
#include "xmalloc.h"
#include "common/list.h"
#include "servicefd.h"
#include "uffd.h"
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
 *   2. Initializes page buffer (using existing g_page_buffer in uffd.c)
 *   3. Connects to the page server
 *   4. Sets up async bulk reader for receiving pages
 *   5. Requests all pages for each task
 *   6. Runs event loop to receive and buffer pages
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

	/* 2. Initialize page buffer (uses g_page_buffer in uffd.c) */
	if (page_buffer_init()) {
		pr_err("Failed to initialize page buffer\n");
		goto err_tasks;
	}

	/* 3. Daemonize if requested */
	if (daemon) {
		ret = cr_daemon(1, 0, -1);
		if (ret == -1) {
			pr_err("Can't run in the background\n");
			goto err_tasks;
		}
		if (ret > 0) {
			/* Parent - daemon started successfully */
			if (opts.pidfile) {
				if (write_pidfile(ret) == -1) {
					pr_perror("Can't write pidfile");
					kill(ret, SIGKILL);
					waitpid(ret, NULL, 0);
					goto err_tasks;
				}
			}
			return 0;
		}
		/* Child continues */
	}

	/* 4. Set up epoll - page server + margin */
	nr_fds = 4;
	epollfd = epoll_prepare(nr_fds, &events);
	if (epollfd < 0)
		goto err_tasks;

	/* 5. Connect to page server */
	if (connect_to_page_server_to_recv(epollfd)) {
		pr_err("Failed to connect to page server\n");
		goto err_epoll;
	}

	/* 6. Set up async bulk reader (uses prebuffer_io_complete in uffd.c) */
	if (setup_prebuffer_reader()) {
		pr_err("Failed to setup prebuffer reader\n");
		goto err_disconnect;
	}

	/* 7. Request all pages for each discovered task */
	list_for_each_entry(ct, &cow_tasks, l) {
		pr_info("Requesting all pages for pid=%d\n", ct->pid);
		if (request_all_remote_pages(ct->pid) < 0) {
			pr_err("Failed to request pages for pid=%d\n", ct->pid);
			goto err_disconnect;
		}
	}

	pr_info("Waiting to receive pages from primary...\n");

	/* 8. Event loop - receive and buffer pages */
	ret = cow_phase2_handle_pages(epollfd, events, nr_fds);

	pr_info("Phase 2 complete\n");

err_disconnect:
	disconnect_from_page_server();
err_epoll:
	xfree(events);
err_tasks:
	free_cow_tasks();
	return ret;
}

/*
 * Event loop for Phase 2 - receive pages until bulk transfer complete.
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
