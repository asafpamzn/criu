/*
 * COW Phase 2/3 Lazy Pages - Phased migration page handling
 *
 * Phase 2: Buffer pages from primary before skeleton dump exists
 * Phase 3: After dirty bitmap arrives, start restore with buffered pages
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
#include "cow/cow-uffd.h"
#include "cow/cow-bulk-send.h"
#include "pstree.h"
#include "cow/pf-tracker.h"
#include "cow/unmapped-tracker.h"
#include "rst_info.h"
#include "cow/cow-lazy-pages.h"

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
 * COW Phase 2/3 lazy-pages entry point.
 *
 * Phase 2:
 *   1. Discovers tasks from pagemap files (no pstree needed)
 *   2. Connects to page server, buffers all pages
 *   3. Waits for dirty bitmap (signals Phase 3 skeleton dump ready)
 *
 * Phase 3:
 *   4. Now inventory.img exists - call prepare_dummy_pstree()
 *   5. Start restore with buffered pages
 *   6. Handle page faults (WP_SYNC convergence)
 */
int cr_lazy_pages_cow_phase2(bool daemon)
{
	struct epoll_event *events = NULL;
	struct cow_task *ct;
	int epollfd;
	int ret = -1;
	int nr_fds;

	pr_err("=== COW Phase 2: Page buffering mode ===\n");

	/* 1. Discover tasks from pagemap files */
	if (discover_tasks_from_pagemaps())
		return -1;

	/* 2. Initialize COW page buffer (thread-safe version in cow-uffd.c) */
	if (cow_page_buffer_init()) {
		pr_err("Failed to initialize page buffer\n");
		goto err_tasks;
	}

	/* Initialize page state tracker for debugging */
	if (page_state_init()) {
		pr_warn("Failed to initialize page state tracker (non-fatal)\n");
	}

	/* Initialize hung page tracker for debugging */
	if (pf_tracker_init()) {
		pr_warn("Failed to init hung page tracker (non-fatal)\n");
	}

	/* Initialize unmapped pages tracker */
	if (unmapped_tracker_init()) {
		pr_warn("Failed to init unmapped tracker (non-fatal)\n");
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

	/* 4. Set up epoll - page server + restore socket + margin */
	nr_fds = 8;
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

	/*
	 * 7b. Create P3 parallel connections AFTER sending page requests.
	 * PRIMARY is now in unified_page_server_thread and ready to accept.
	 */
	if (start_p3_receiver_connections(NUM_P3_THREADS) > 0) {
		pr_info("P3 parallel receiver enabled\n");
	}

	pr_err("Waiting to receive pages from primary...\n");

	/* 8. Phase 2 event loop - buffer pages until dirty bitmap arrives */
	ret = cow_phase2_handle_pages(epollfd, events, nr_fds);
	if (ret < 0) {
		pr_err("Phase 2 failed\n");
		goto err_disconnect;
	}

	pr_err("=== COW Phase 3: Starting restore ===\n");

	/*
	 * Phase 3: Dirty bitmap received, skeleton dump is ready.
	 * Primary closed the socket after sending dirty bitmap.
	 * Stop P3 receivers and reconnect for convergence phase.
	 */
	stop_p3_receiver_connections();
	close_page_server_socket();

	/* Small delay for primary to start new page server */
	usleep(100000);  /* 100ms */

	/* Reconnect to convergence page server */
	if (connect_to_page_server_to_recv(epollfd)) {
		pr_warn("Cannot reconnect to page server - will serve from buffer only\n");
		/* Continue anyway - most pages should be in buffer */
	} else {
		pr_info("Reconnected to convergence page server\n");
	}

	/*
	 * Now inventory.img and pstree.img exist on disk.
	 * The PS_IOV_INVENTORY_READY signal ensures we don't race
	 * with the primary writing inventory.img.
	 */
	if (!cow_is_inventory_ready_received()) {
		pr_err("Inventory ready signal not received!\n");
		goto err_disconnect;
	}

	if (prepare_dummy_pstree()) {
		pr_err("Failed to prepare pstree\n");
		goto err_disconnect;
	}

	pr_info("Pstree loaded, ready to accept restore connection\n");

	/*
	 * Recalculate nr_fds now that pstree is loaded.
	 * We need: task uffd fds + page server + lazy socket + margin
	 */
	nr_fds = task_entries->nr_tasks + 4;

	/*
	 * Phase 3 continues in the normal lazy-pages flow.
	 * The buffered pages in g_page_buffer will be:
	 *   - Served to page faults (handle_page_fault checks buffer first)
	 */
	ret = cow_phase3_restore_loop(epollfd, &events, nr_fds);
	if (ret < 0)
		pr_err("Phase 3 restore loop failed\n");

err_disconnect:
	/* Print page state statistics and cleanup */
	stop_p3_receiver_connections();
	pf_tracker_destroy();
	/* Verify all pages reached terminal states before cleanup */
	page_state_verify_all_terminal();
	page_state_destroy();
	disconnect_from_page_server();
err_epoll:
	xfree(events);
err_tasks:
	free_cow_tasks();
	return ret;
}

/*
 * Phase 2 event loop - receive pages until dirty bitmap arrives.
 * Dirty bitmap signals that Phase 3 skeleton dump is complete.
 */
int cow_phase2_handle_pages(int epollfd, struct epoll_event *events, int nr_fds)
{
	int ret;
	bool bulk_done = false;
	bool inventory_ready = false;

	while (1) {
		ret = epoll_run_rfds(epollfd, events, nr_fds, -1);
		if (ret < 0) {
			pr_err("epoll_run_rfds failed\n");
			return -1;
		}

		/* Track bulk transfer completion */
		if (!bulk_done && page_server_bulk_stream_done()) {
			pr_err("Bulk page transfer complete, waiting for dirty bitmap...\n");
			bulk_done = true;
		}

		/* Track inventory ready signal */
		if (!inventory_ready && cow_is_inventory_ready_received()) {
			pr_err("Inventory ready signal received\n");
			inventory_ready = true;
		}

		/* Dirty bitmap signals Phase 3 is ready */
		if (cow_is_dirty_bitmap_received()) {
			pr_err("Dirty bitmap received - Phase 3 ready\n");
			return 0;
		}
	}

	return 0;
}
