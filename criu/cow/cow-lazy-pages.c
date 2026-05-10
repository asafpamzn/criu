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
#include <limits.h>
#include <spawn.h>

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
#include "cow/cow-bulk-recv.h"
#include "pstree.h"
#include "cow/pf-tracker.h"
#include "cow/unmapped-tracker.h"
#include "rst_info.h"
#include "cow/cow-lazy-pages.h"
#include "cow/cow-conf.h"
#include "common/bug.h"

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

	if (opts.tree_id) {
		struct cow_task *ct = xmalloc(sizeof(*ct));
		BUG_ON(!ct);
		ct->pid = opts.tree_id;
		list_add_tail(&ct->l, &cow_tasks);
		nr_cow_tasks = 1;
		pr_info("Using task pid=%d from --tree option\n", opts.tree_id);
		return 0;
	}

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
		BUG_ON(!ct);

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

extern char **environ;

static pid_t cow_start_restore(void)
{
	pid_t pid;
	char log_path[PATH_MAX];
	int ret;
	const char *img_dir;
	char *argv[16];

	img_dir = cow_get_skeleton_dir();
	if (!img_dir)
		img_dir = opts.imgs_dir;

	snprintf(log_path, sizeof(log_path), "%s/lazy-restore.log", img_dir);

	argv[0] = opts.argv_0;
	argv[1] = "restore";
	argv[2] = "--images-dir";
	argv[3] = (char *)img_dir;
	argv[4] = "--lazy-pages";
	argv[5] = "--tcp-close";
	argv[6] = "--cow-dump";
	argv[7] = "--restore-detached";
	argv[8] = "--skip-file-rwx-check";
	argv[9] = "--skip-file-size-check";
	argv[10] = "--file-validation";
	argv[11] = "filesize";
	argv[12] = "-v1";
	argv[13] = "-o";
	argv[14] = log_path;
	argv[15] = NULL;

	ret = posix_spawn(&pid, opts.argv_0, NULL, NULL, argv, environ);
	if (ret != 0) {
		pr_err("posix_spawn of criu restore failed: %s\n", strerror(ret));
		return -1;
	}

	pr_info("Started criu restore (PID: %d)\n", pid);
	return pid;
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

	pr_info("=== REPLICA PHASE 2: Page buffering mode ===\n");

	/* 1. Discover tasks from pagemap files */
	if (discover_tasks_from_pagemaps())
		return -1;

	/* 2. Initialize COW page buffer (thread-safe version in cow-uffd.c) */
	if (cow_page_buffer_init()) {
		pr_err("Failed to initialize page buffer\n");
		goto err_tasks;
	}

	/* Initialize page state tracker */
	BUG_ON(page_state_init());

	/* Initialize hung page tracker */
	BUG_ON(pf_tracker_init());

	/* Initialize unmapped pages tracker */
	BUG_ON(unmapped_tracker_init());

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

	/* 4. Set up epoll with fixed large buffer */
	nr_fds = COW_MAX_EPOLL_FDS;
	epollfd = epoll_prepare(nr_fds, &events);
	if (epollfd < 0)
		goto err_tasks;

	/* 5. Connect to page server */
	if (connect_to_page_server_to_recv(epollfd)) {
		pr_err("Failed to connect to page server\n");
		goto err_epoll;
	}

	/* 6. Set up async bulk reader (uses prebuffer_io_complete in uffd.c) */
	if (cow_setup_prebuffer_reader()) {
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
	if (start_p3_receiver_connections(COW_NUM_P3_THREADS) > 0) {
		pr_info("P3 parallel receiver enabled\n");
	}

	pr_info("Waiting to receive pages from primary...\n");

	/* 8. Phase 2 event loop - buffer pages until dirty bitmap arrives */
	ret = cow_phase2_handle_pages(epollfd, events, nr_fds);
	if (ret < 0) {
		pr_err("Phase 2 failed\n");
		goto err_disconnect;
	}

	pr_info("=== REPLICA PHASE 5: Starting restore ===\n");

	/*
	 * All pages received, primary closed connection.
	 * No reconnect needed - serve everything from buffer.
	 * Remove page server fd from epoll to avoid hangup events.
	 */
	stop_p3_receiver_connections();
	remove_page_server_from_epoll(epollfd);

	/*
	 * Now inventory.img and pstree.img exist on disk.
	 * The all_pages_sent signal indicates all pages are sent
	 * and skeleton dump is complete.
	 */
	if (!cow_is_all_pages_sent_received()) {
		pr_err("Completion signal (all_pages_sent) not received!\n");
		goto err_disconnect;
	}

	if (prepare_dummy_pstree()) {
		pr_err("Failed to prepare pstree\n");
		goto err_disconnect;
	}

	pr_info("Pstree loaded, ready to accept restore connection\n");

	/*
	 * Verify nr_fds fits in our fixed buffer.
	 * Fds: task uffd (nr_tasks) + lazy_listen + lazy_client = nr_tasks + 2
	 */
	BUG_ON(task_entries->nr_tasks + 2 > COW_MAX_EPOLL_FDS);

	if (cow_start_restore() < 0)
		goto err_disconnect;

	ret = cow_phase3_restore_loop(epollfd, &events, nr_fds);
	if (ret < 0)
		pr_err("Phase 3 restore loop failed\n");

err_disconnect:
	stop_p3_receiver_connections();
	pf_tracker_destroy();
	page_state_verify_all_terminal();
	page_state_destroy();
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

	while (1) {
		ret = epoll_run_rfds(epollfd, events, nr_fds, -1);
		if (ret < 0) {
			pr_err("epoll_run_rfds failed\n");
			return -1;
		}


		/* All pages sent = completion signal, ready for restore */
		if (cow_is_all_pages_sent_received()) {
			pr_err("=== REPLICA: Completion signal received, ready for restore ===\n");
			BUG_ON(send_all_pages_sent_ack() < 0);
			/* Clean up async bulk reader before socket is closed */
			page_server_cleanup_async_bulk();
			return 0;
		}
	}

	return 0;
}
