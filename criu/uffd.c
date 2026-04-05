#include <stddef.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "linux/userfaultfd.h"

#include "int.h"
#include "page.h"
#include "criu-log.h"
#include "criu-plugin.h"
#include "pagemap.h"
#include "files-reg.h"
#include "kerndat.h"
#include "mem.h"
#include "uffd.h"
#include "util-pie.h"
#include "protobuf.h"
#include "pstree.h"
#include "crtools.h"
#include "cr_options.h"
#include "xmalloc.h"
#include "common/list.h"
#include <compel/plugins/std/syscall-codes.h>
#include "restorer.h"
#include "page-xfer.h"
#include "common/lock.h"
#include "rst-malloc.h"
#include "tls.h"
#include "fdstore.h"
#include "util.h"
#include "namespaces.h"
#include "pagemap.h"
#include "cow/pf-tracker.h"
#include "cow/cow-lazy-pages.h"
#include "cow/cow-uffd.h"
#include "uffd-internal.h"
#include "cow/unmapped-tracker.h"
#include "cow/page-pool.h"

#undef LOG_PREFIX
#define LOG_PREFIX "uffd: "

#define NEED_UFFD_API_FEATURES \
	(UFFD_FEATURE_EVENT_FORK | UFFD_FEATURE_EVENT_REMAP | UFFD_FEATURE_EVENT_UNMAP | UFFD_FEATURE_EVENT_REMOVE)

#define LAZY_PAGES_SOCK_NAME "lazy-pages.socket"

#define LAZY_PAGES_RESTORE_FINISHED 0x52535446 /* ReSTore Finished */

/*
 * Background transfer parameters.
 * The default xfer length is arbitrary set to 64Kbytes
 * The limit of 4Mbytes matches the maximal chunk size we can have in
 * a pipe in the page-server
 */
#define DEFAULT_XFER_LEN (64 << 10)
#define MAX_XFER_LEN	 (4 << 20)

static mutex_t *lazy_sock_mutex;

/* global lazy-pages daemon state */
static LIST_HEAD(lpis);
static LIST_HEAD(exiting_lpis);
static LIST_HEAD(pending_lpis);
static int epollfd;
static bool restore_finished;
/* phase3_active is now in cow-uffd.c: cow_is_phase3_active() / cow_set_phase3_active() */
static struct epoll_rfd lazy_sk_rfd;
/* socket for communication with lazy-pages daemon */
static int lazy_pages_sk_id = -1;

/*
 * COW stats, EAGAIN handling, and histogram functions are in cow-uffd.c:
 * - check_and_print_uffd_stats()
 * - cow_queue_eagain_request(), cow_process_eagain_requests()
 * - cow_uffd_stats_inc_pf(), cow_uffd_stats_inc_bg(), etc.
 */

static int handle_uffd_event(struct epoll_rfd *lpfd);

static struct lazy_pages_info *lpi_init(void)
{
	struct lazy_pages_info *lpi = NULL;

	lpi = xmalloc(sizeof(*lpi));
	if (!lpi)
		return NULL;

	memset(lpi, 0, sizeof(*lpi));
	INIT_LIST_HEAD(&lpi->iovs);
	INIT_LIST_HEAD(&lpi->reqs);
	INIT_LIST_HEAD(&lpi->l);
	lpi->lpfd.read_event = handle_uffd_event;
	lpi->xfer_len = DEFAULT_XFER_LEN;
	lpi->ref_cnt = 1;

	return lpi;
}

static void free_iovs(struct lazy_pages_info *lpi)
{
	struct lazy_iov *p, *n;

	list_for_each_entry_safe(p, n, &lpi->iovs, l) {
		list_del(&p->l);
		xfree(p);
	}

	list_for_each_entry_safe(p, n, &lpi->reqs, l) {
		list_del(&p->l);
		xfree(p);
	}
}

static void lpi_fini(struct lazy_pages_info *lpi);

/* Non-static for use by uffd_cow.c */
void lpi_put(struct lazy_pages_info *lpi)
{
	lpi->ref_cnt--;
	if (!lpi->ref_cnt)
		lpi_fini(lpi);
}

static inline void lpi_get(struct lazy_pages_info *lpi)
{
	lpi->ref_cnt++;
}

static void lpi_fini(struct lazy_pages_info *lpi)
{
	if (!lpi)
		return;
	xfree(lpi->buf);
	free_iovs(lpi);
	if (lpi->lpfd.fd > 0)
		close(lpi->lpfd.fd);
	if (lpi->parent)
		lpi_put(lpi->parent);
	if (!lpi->parent && lpi->pr.close)
		lpi->pr.close(&lpi->pr);
	xfree(lpi);
}

static int prepare_sock_addr(struct sockaddr_un *saddr)
{
	int len;

	memset(saddr, 0, sizeof(struct sockaddr_un));

	saddr->sun_family = AF_UNIX;
	len = snprintf(saddr->sun_path, sizeof(saddr->sun_path), "%s", LAZY_PAGES_SOCK_NAME);
	if (len >= sizeof(saddr->sun_path)) {
		pr_err("Wrong UNIX socket name: %s\n", LAZY_PAGES_SOCK_NAME);
		return -1;
	}

	return 0;
}

static int send_uffd(int sendfd, int pid)
{
	int fd;
	int ret = -1;

	if (sendfd < 0)
		return -1;

	fd = fdstore_get(lazy_pages_sk_id);
	if (fd < 0) {
		pr_err("%s: get_service_fd\n", __func__);
		return -1;
	}

	mutex_lock(lazy_sock_mutex);

	/* The "transfer protocol" is first the pid as int and then
	 * the FD for UFFD */
	pr_debug("Sending PID %d\n", pid);
	if (send(fd, &pid, sizeof(pid), 0) < 0) {
		pr_perror("PID sending error");
		goto out;
	}

	/* for a zombie process pid will be negative */
	if (pid < 0) {
		ret = 0;
		goto out;
	}

	if (send_fd(fd, NULL, 0, sendfd) < 0) {
		pr_err("send_fd error\n");
		goto out;
	}

	ret = 0;
out:
	mutex_unlock(lazy_sock_mutex);
	close(fd);
	return ret;
}

int lazy_pages_setup_zombie(int pid)
{
	if (!opts.lazy_pages)
		return 0;

	if (send_uffd(0, -pid))
		return -1;

	return 0;
}

bool uffd_noncooperative(void)
{
	unsigned long features = NEED_UFFD_API_FEATURES;

	return (kdat.uffd_features & features) == features;
}

static int uffd_api_ioctl(void *arg, int fd, pid_t pid)
{
	struct uffdio_api *uffdio_api = arg;

	return ioctl(fd, UFFDIO_API, uffdio_api);
}

int uffd_open(int flags, unsigned long *features, int *err)
{
	struct uffdio_api uffdio_api = { 0 };
	int uffd;

	uffd = syscall(SYS_userfaultfd, flags);
	if (uffd == -1) {
		pr_info("Lazy pages are not available: %s\n", strerror(errno));
		if (err)
			*err = errno;
		return -1;
	}

	uffdio_api.api = UFFD_API;
	if (features)
		uffdio_api.features = *features;

	if (userns_call(uffd_api_ioctl, 0, &uffdio_api, sizeof(uffdio_api), uffd)) {
		pr_perror("Failed to get uffd API");
		goto close;
	}

	if (uffdio_api.api != UFFD_API) {
		pr_err("Incompatible uffd API: expected %llu, got %llu\n", UFFD_API, uffdio_api.api);
		goto close;
	}

	if (features)
		*features = uffdio_api.features;

	return uffd;

close:
	close(uffd);
	return -1;
}

/* This function is used by 'criu restore --lazy-pages' */
int setup_uffd(int pid, struct task_restore_args *task_args)
{
	unsigned long features = kdat.uffd_features & NEED_UFFD_API_FEATURES;

	if (!opts.lazy_pages) {
		task_args->uffd = -1;
		return 0;
	}

	/*
	 * Open userfaulfd FD which is passed to the restorer blob and
	 * to a second process handling the userfaultfd page faults.
	 */
	task_args->uffd = uffd_open(O_CLOEXEC | O_NONBLOCK, &features, NULL);
	if (task_args->uffd < 0) {
		pr_perror("Unable to open an userfaultfd descriptor");
		return -1;
	}

	if (send_uffd(task_args->uffd, pid) < 0)
		goto err;

	return 0;
err:
	close(task_args->uffd);
	return -1;
}

int prepare_lazy_pages_socket(void)
{
	int fd, len, ret = -1;
	struct sockaddr_un sun;

	if (!opts.lazy_pages)
		return 0;

	if (prepare_sock_addr(&sun))
		return -1;

	lazy_sock_mutex = shmalloc(sizeof(*lazy_sock_mutex));
	if (!lazy_sock_mutex)
		return -1;

	mutex_init(lazy_sock_mutex);

	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	len = offsetof(struct sockaddr_un, sun_path) + strlen(sun.sun_path);
	if (connect(fd, (struct sockaddr *)&sun, len) < 0) {
		pr_perror("connect to %s failed", sun.sun_path);
		goto out;
	}

	lazy_pages_sk_id = fdstore_add(fd);
	if (lazy_pages_sk_id < 0) {
		pr_perror("Can't add fd to fdstore");
		goto out;
	}

	ret = 0;
out:
	close(fd);
	return ret;
}

static int server_listen(struct sockaddr_un *saddr)
{
	int fd;
	int len;

	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	unlink(saddr->sun_path);

	len = offsetof(struct sockaddr_un, sun_path) + strlen(saddr->sun_path);

	if (bind(fd, (struct sockaddr *)saddr, len) < 0) {
		goto out;
	}

	if (listen(fd, 10) < 0) {
		goto out;
	}

	return fd;

out:
	close(fd);
	return -1;
}

static MmEntry *init_mm_entry(struct lazy_pages_info *lpi)
{
	struct cr_img *img;
	MmEntry *mm;
	int ret;

	img = open_image(CR_FD_MM, O_RSTR, lpi->pid);
	if (!img)
		return NULL;

	ret = pb_read_one_eof(img, &mm, PB_MM);
	close_image(img);
	if (ret == -1)
		return NULL;
	lp_debug(lpi, "Found %zd VMAs in image\n", mm->n_vmas);

	return mm;
}

static struct lazy_iov *find_iov(struct lazy_pages_info *lpi, unsigned long addr)
{
	struct lazy_iov *iov;

	list_for_each_entry(iov, &lpi->iovs, l)
		if (addr >= iov->start && addr < iov->end)
			return iov;

	return NULL;
}

/* Non-static wrapper for use by cow-uffd.c */
struct lazy_iov *cow_find_iov(struct lazy_pages_info *lpi, unsigned long addr)
{
	return find_iov(lpi, addr);
}

static int split_iov(struct lazy_iov *iov, unsigned long addr)
{
	struct lazy_iov *new;

	new = xzalloc(sizeof(*new));
	if (!new)
		return -1;

	new->start = addr;
	new->img_start = iov->img_start + addr - iov->start;
	new->end = iov->end;
	iov->end = addr;
	list_add(&new->l, &iov->l);

	return 0;
}

static void iov_list_insert(struct lazy_iov *new, struct list_head *dst)
{
	struct lazy_iov *iov;

	if (list_empty(dst)) {
		list_move(&new->l, dst);
		return;
	}

	list_for_each_entry(iov, dst, l) {
		if (new->start < iov->start) {
			list_move_tail(&new->l, &iov->l);
			break;
		}
		if (list_is_last(&iov->l, dst) && new->start > iov->start) {
			list_move(&new->l, &iov->l);
			break;
		}
	}
}

static void merge_iov_lists(struct list_head *src, struct list_head *dst)
{
	struct lazy_iov *iov, *n;

	if (list_empty(src))
		return;

	list_for_each_entry_safe(iov, n, src, l)
		iov_list_insert(iov, dst);
}

static int __copy_iov_list(struct list_head *src, struct list_head *dst)
{
	struct lazy_iov *iov, *new;

	list_for_each_entry(iov, src, l) {
		new = xzalloc(sizeof(*new));
		if (!new)
			return -1;

		new->start = iov->start;
		new->img_start = iov->img_start;
		new->end = iov->end;

		list_add_tail(&new->l, dst);
	}

	return 0;
}

static int copy_iovs(struct lazy_pages_info *src, struct lazy_pages_info *dst)
{
	if (__copy_iov_list(&src->iovs, &dst->iovs))
		goto free_iovs;

	if (__copy_iov_list(&src->reqs, &dst->reqs))
		goto free_iovs;

	/*
	 * The IOVs already in flight for the parent process need to be
	 * transferred again for the child process
	 */
	merge_iov_lists(&dst->reqs, &dst->iovs);

	dst->buf_size = src->buf_size;
	if (posix_memalign(&dst->buf, PAGE_SIZE, dst->buf_size))
		goto free_iovs;

	return 0;

free_iovs:
	free_iovs(dst);
	return -1;
}

/*
 * Purge range (addr, addr + len) from lazy_iovs. The range may
 * cover several continuous IOVs.
 */
static int __drop_iovs(struct list_head *iovs, unsigned long addr, unsigned long len)
{
	struct lazy_iov *iov, *n;
	unsigned long drop_end;

	if (!len)
		return 0;

	drop_end = addr + len;
	if (drop_end < addr)
		drop_end = ULONG_MAX;

	list_for_each_entry_safe(iov, n, iovs, l) {
		unsigned long start = iov->start;
		unsigned long end = iov->end;
		unsigned long overlap_start;
		unsigned long overlap_end;

		if (end <= addr)
			continue;

		if (start >= drop_end)
			break;

		overlap_start = max(start, addr);
		overlap_end = min(end, drop_end);
		if (overlap_start >= overlap_end)
			continue;

		if (overlap_start == start && overlap_end == end) {
			list_del(&iov->l);
			xfree(iov);
			continue;
		}

		if (overlap_start == start) {
			iov->start = overlap_end;
			iov->img_start += overlap_end - start;
			continue;
		}

		if (overlap_end == end) {
			iov->end = overlap_start;
			continue;
		}

		if (split_iov(iov, overlap_end))
			return -1;
		iov->end = overlap_start;
		break;
	}

	return 0;
}

static int drop_iovs(struct lazy_pages_info *lpi, unsigned long addr, unsigned long len)
{
	if (__drop_iovs(&lpi->iovs, addr, len))
		return -1;

	if (__drop_iovs(&lpi->reqs, addr, len))
		return -1;

	return 0;
}

/* cow_dump_lazy_iov_list() is in cow-uffd.c */

static struct lazy_iov *extract_range(struct lazy_iov *iov, unsigned long start, unsigned long end)
{
	/* move the IOV tail into a new IOV */
	if (end < iov->end)
		if (split_iov(iov, end))
			return NULL;

	if (start == iov->start)
		return iov;

	/* after splitting the IOV head we'll need the ->next IOV */
	if (split_iov(iov, start))
		return NULL;

	return list_entry(iov->l.next, struct lazy_iov, l);
}

static int __remap_iovs(struct list_head *iovs, unsigned long from, unsigned long to, unsigned long len)
{
	LIST_HEAD(remaps);

	unsigned long off = to - from;
	struct lazy_iov *iov, *n;

	list_for_each_entry_safe(iov, n, iovs, l) {
		if (from >= iov->end)
			continue;

		if (len <= 0 || from + len <= iov->start)
			break;

		if (from < iov->start) {
			len -= (iov->start - from);
			from = iov->start;
		}

		if (from > iov->start) {
			if (split_iov(iov, from))
				return -1;
			list_safe_reset_next(iov, n, l);
			continue;
		}

		if (from + len < iov->end) {
			if (split_iov(iov, from + len))
				return -1;
			list_safe_reset_next(iov, n, l);
		}

		/* here we have iov->start = from, iov->end <= from + len */
		from = iov->end;
		len -= iov->end - iov->start;
		iov->start += off;
		iov->end += off;
		list_move_tail(&iov->l, &remaps);
	}

	merge_iov_lists(&remaps, iovs);

	return 0;
}

static int remap_iovs(struct lazy_pages_info *lpi, unsigned long from, unsigned long to, unsigned long len)
{
	if (__remap_iovs(&lpi->iovs, from, to, len))
		return -1;

	if (__remap_iovs(&lpi->reqs, from, to, len))
		return -1;

	return 0;
}

/*
 * Create a list of IOVs that can be handled using userfaultfd. The
 * IOVs generally correspond to lazy pagemap entries, except the cases
 * when a single pagemap entry covers several VMAs. In those cases
 * IOVs are split at VMA boundaries because UFFDIO_COPY may be done
 * only inside a single VMA.
 * We assume here that pagemaps and VMAs are sorted.
 */
static int collect_iovs(struct lazy_pages_info *lpi)
{
	unsigned long start, end, len, nr_pages = 0;
	int n_vma = 0, max_iov_len = 0, ret = -1;
	struct page_read *pr = &lpi->pr;
	struct lazy_iov *iov;
	MmEntry *mm;

	mm = init_mm_entry(lpi);
	if (!mm)
		return -1;

	while (pr->advance(pr)) {
		if (!pagemap_lazy(pr->pe))
			continue;

		start = pr->pe->vaddr;
		end = start + pr->pe->nr_pages * page_size();
		nr_pages += pr->pe->nr_pages;

		for (; n_vma < mm->n_vmas; n_vma++) {
			VmaEntry *vma = mm->vmas[n_vma];

			if (start >= vma->end)
				continue;

			iov = xzalloc(sizeof(*iov));
			if (!iov)
				goto free_iovs;

			len = min_t(uint64_t, end, vma->end) - start;
			iov->start = start;
			iov->img_start = start;
			iov->end = iov->start + len;
			list_add_tail(&iov->l, &lpi->iovs);

			if (len > max_iov_len)
				max_iov_len = len;

			if (end <= vma->end)
				break;

			start = vma->end;
		}
	}

	lpi->buf_size = max_iov_len;
	if (posix_memalign(&lpi->buf, PAGE_SIZE, lpi->buf_size))
		goto free_iovs;

	ret = nr_pages;
	goto free_mm;

free_iovs:
	free_iovs(lpi);
free_mm:
	mm_entry__free_unpacked(mm, NULL);

	return ret;
}

static int uffd_io_complete(struct page_read *pr, unsigned long vaddr, unsigned long nr);
static int uffd_io_complete_bulk(struct page_read *pr, unsigned long vaddr, unsigned long nr);

static int ud_open(int client, struct lazy_pages_info **_lpi)
{
	struct lazy_pages_info *lpi;
	int ret = -1;
	int pr_flags = PR_TASK;

	lpi = lpi_init();
	if (!lpi)
		goto out;

	/* The "transfer protocol" is first the pid as int and then
	 * the FD for UFFD */
	ret = recv(client, &lpi->pid, sizeof(lpi->pid), 0);
	if (ret != sizeof(lpi->pid)) {
		if (ret < 0)
			pr_perror("PID recv error");
		else
			pr_err("PID recv: short read\n");
		goto out;
	}

	if (lpi->pid < 0) {
		pr_debug("Zombie PID: %d\n", lpi->pid);
		lpi_fini(lpi);
		return 0;
	}

	lpi->lpfd.fd = recv_fd(client);
	if (lpi->lpfd.fd < 0) {
		pr_err("recv_fd error\n");
		goto out;
	}
	pr_debug("Received PID: %d, uffd: %d\n", lpi->pid, lpi->lpfd.fd);

	if (opts.use_page_server)
		pr_flags |= PR_REMOTE;
	ret = open_page_read(lpi->pid, &lpi->pr, pr_flags);
	if (ret <= 0) {
		lp_err(lpi, "Failed to open pagemap\n");
		goto out;
	}

	if (opts.cow_dump) {
		/* Bulk mode: pages arrive automatically from background thread */
		lpi->pr.io_complete = uffd_io_complete_bulk;
	} else {
		/* On-demand mode: manage individual page requests */
		lpi->pr.io_complete = uffd_io_complete;
	}

	/*
	 * Find the memory pages belonging to the restored process
	 * so that it is trackable when all pages have been transferred.
	 */
	ret = collect_iovs(lpi);
	if (ret < 0)
		goto out;
	lpi->total_pages = ret;

	lp_debug(lpi, "Found %ld pages to be handled by UFFD\n", lpi->total_pages);

	list_add_tail(&lpi->l, &lpis);
	*_lpi = lpi;

	return 0;

out:
	lpi_fini(lpi);
	return -1;
}

static int handle_exit(struct lazy_pages_info *lpi)
{
	lp_debug(lpi, "EXIT\n");
	if (epoll_del_rfd(epollfd, &lpi->lpfd))
		return -1;
	free_iovs(lpi);
	close(lpi->lpfd.fd);
	lpi->lpfd.fd = -lpi->lpfd.fd;
	lpi->exited = true;

	/* keep it for tracking in-flight requests and for the summary */
	list_move_tail(&lpi->l, &lpis);

	return 0;
}

static bool uffd_recoverable_error(int mcopy_rc)
{
	if (errno == EAGAIN || errno == ENOENT || errno == EEXIST)
		return true;

	if (mcopy_rc == -ENOENT || mcopy_rc == -EEXIST)
		return true;

	return false;
}

static int uffd_check_op_error(struct lazy_pages_info *lpi, const char *op, unsigned long *nr_pages, long mcopy_rc)
{
	if (errno == ENOSPC || errno == ESRCH) {
		lp_err(lpi, "uffd_copy1:ERROR errno=%d\n", errno);
		handle_exit(lpi);
		return -1;
	}

	if (!uffd_recoverable_error(mcopy_rc)) {
		lp_perror(lpi, "%s: mcopy_rc:%ld\n", op, mcopy_rc);
		return -1;
	}

	lp_debug(lpi, "%s: mcopy_rc:%ld, errno:%d\n", op, mcopy_rc, errno);

	if (mcopy_rc <= 0)
		*nr_pages = 0;
	else
		*nr_pages = mcopy_rc / PAGE_SIZE;

	return 0;
}

static int xfer_pages(struct lazy_pages_info *lpi);

/*
 * EAGAIN request handling is in cow-uffd.c:
 * - cow_queue_eagain_request()
 * - cow_queue_drain_eagain_request()
 */

static int uffd_copy(struct lazy_pages_info *lpi, __u64 address, unsigned long *nr_pages)
{
	struct uffdio_copy uffdio_copy;
	unsigned long len = *nr_pages * page_size();

	uffdio_copy.dst = address;
	uffdio_copy.src = (unsigned long)lpi->buf;
	uffdio_copy.len = len;
	uffdio_copy.mode = 0;
	uffdio_copy.copy = 0;

	lp_debug(lpi, "uffd_copy: 0x%llx/%ld\n", uffdio_copy.dst, len);

	if (ioctl(lpi->lpfd.fd, UFFDIO_COPY, &uffdio_copy) == -1) {
		/* In COW dump mode, queue EAGAIN requests instead of blocking */
		if (errno == EAGAIN && opts.cow_dump) {
			pf_tracker_set_state(address, PF_STATE_PENDING_EAGAIN);
			/* page_state set to EAGAIN_QUEUED inside cow_queue_eagain_request on success */
			return cow_queue_eagain_request(lpi, address, *nr_pages, lpi->buf, "copy");
		}

		/* Non-COW mode or non-EAGAIN: check for other errors */
		if (uffd_check_op_error(lpi, "copy", nr_pages, uffdio_copy.copy)) {
			lp_err(lpi, "UFFDIO_COPY got error\n");
			page_state_print_history(address);
			/*
			 * Don't set DISCARDED if page is DIRTY/UNMAPPED.
			 * These are terminal states or will be re-sent.
			 */
			if (!unmapped_tracker_is_unmapped(address) &&
			    page_state_get(address) != PAGE_STATE_DIRTY)
				page_state_set(address, PAGE_STATE_DISCARDED);
			return -1;
		}

		/*
		 * EEXIST means duplicate copy attempt - this is a bug!
		 * Report and fail to catch coordination issues.
		 */
		if (errno == EEXIST) {
			lp_err(lpi, "BUG: UFFDIO_COPY EEXIST at 0x%llx - duplicate copy!\n", address);
			page_state_print_history(address);
			return -1;
		}
		/* Don't set DISCARDED if page is DIRTY/UNMAPPED */
		if (!unmapped_tracker_is_unmapped(address) &&
		    page_state_get(address) != PAGE_STATE_DIRTY)
			page_state_set(address, PAGE_STATE_DISCARDED);
		return 0;
	}

	if (uffdio_copy.copy < 0) {
		/* Soft userfaultfd error: encoded as -errno in copy */
		errno = -uffdio_copy.copy;

		/* In COW dump mode, queue EAGAIN requests */
		if (errno == EAGAIN && opts.cow_dump) {
			pf_tracker_set_state(address, PF_STATE_PENDING_EAGAIN);
			/* page_state set to EAGAIN_QUEUED inside cow_queue_eagain_request on success */
			return cow_queue_eagain_request(lpi, address, *nr_pages, lpi->buf, "copy");
		}

		if (uffd_check_op_error(lpi, "copy", nr_pages, uffdio_copy.copy)) {
			lp_err(lpi, "UFFDIO_COPY err \n");
			page_state_print_history(address);
			/* Don't set DISCARDED if page is DIRTY/UNMAPPED */
			if (!unmapped_tracker_is_unmapped(address) &&
			    page_state_get(address) != PAGE_STATE_DIRTY)
				page_state_set(address, PAGE_STATE_DISCARDED);
			return -1;
		}
		/*
		 * EEXIST means duplicate copy attempt - this is a bug!
		 */
		if (errno == EEXIST) {
			lp_err(lpi, "BUG: UFFDIO_COPY soft EEXIST at 0x%llx - duplicate copy!\n", address);
			page_state_print_history(address);
			return -1;
		}
		/* Don't set DISCARDED if page is DIRTY/UNMAPPED */
		if (!unmapped_tracker_is_unmapped(address) &&
		    page_state_get(address) != PAGE_STATE_DIRTY)
			page_state_set(address, PAGE_STATE_DISCARDED);
		return 0;
	}

	/* Success */
	if (uffdio_copy.copy == 0) {
		lp_err(lpi, "UFFDIO_COPY copied 0 bytes at 0x%llx\n", uffdio_copy.dst);
		*nr_pages = 0;
	}

	lpi->copied_pages += *nr_pages;

	/* Mark as completed in the tracker */
	pf_tracker_set_state(address, PF_STATE_COMPLETED);
	page_state_set(address, PAGE_STATE_COPIED);

	return 0;
}

static int uffd_io_complete(struct page_read *pr, unsigned long img_addr, unsigned long nr)
{
	struct lazy_pages_info *lpi;
	unsigned long addr = 0, req_pages;
	struct lazy_iov *req;
	int ret;

	lpi = container_of(pr, struct lazy_pages_info, pr);
	/*
	 * The process may exit while we still have requests in
	 * flight. We just drop the request and the received data in
	 * this case to avoid making uffd unhappy
	 */
	if (lpi->exited)
		return 0;

	list_for_each_entry(req, &lpi->reqs, l) {
		if (req->img_start == img_addr) {
			addr = req->start;
			break;
		}
	}

	/* the request may be already gone because if unmap/remove */
	if (!addr)
		return 0;

	/*
	 * By the time we get the pages from the remote source, parts
	 * of the request may already be gone because of unmap/remove
	 * OTOH, the remote side may send less pages than we requested.
	 * Make sure we are not trying to uffd_copy more memory than
	 * we should.
	 */
	req_pages = (req->end - req->start) / PAGE_SIZE;
	nr = min(nr, req_pages);

	/*
	 * In COW mode, the same page may be in the buffer from bulk transfer.
	 * Remove it to avoid EEXIST when drain thread tries to copy it later.
	 */
	if (opts.cow_dump) {
		unsigned long i;
		for (i = 0; i < nr; i++) {
			unsigned long page_addr = addr + i * PAGE_SIZE;
			void *buffered = cow_page_buffer_lookup_and_remove(page_addr);
			if (buffered) {
				page_state_set(page_addr, PAGE_STATE_URGENT_PENDING);
				page_pool_put(buffered);
			} else {
				page_state_set(page_addr, PAGE_STATE_URGENT_PENDING);
			}
		}
	}

	ret = uffd_copy(lpi, addr, &nr);
	if (ret < 0)
		return ret;

	/* recheck if the process exited, it may be detected in uffd_copy */
	if (lpi->exited)
		return 0;

	/*
	 * Since the completed request length may differ from the
	 * actual data we've received we re-insert the request to IOVs
	 * list and let drop_iovs do the range math, free memory etc.
	 */
	iov_list_insert(req, &lpi->iovs);
	ret = drop_iovs(lpi, addr, nr * PAGE_SIZE);

	return ret;
}

static int uffd_io_complete_bulk(struct page_read *pr, unsigned long vaddr, unsigned long nr)
{
	struct lazy_pages_info *lpi;
	unsigned long pages = nr;
	unsigned long tracked_pages;
	struct lazy_iov *iov;
	int ret;
	struct timespec t_start, t_copy, t_drop, t_end;

	if (opts.cow_dump)
		cow_uffd_stats_inc_io_bulk_start();

	clock_gettime(CLOCK_MONOTONIC, &t_start);

	lpi = container_of(pr, struct lazy_pages_info, pr);

	/* Process may exit while pages are in flight */
	if (lpi->exited) {
		lp_debug(lpi, "Page at 0x%lx no longer needed existed\n", vaddr);
		return 0;
	}

	/* Check if this address is still tracked (not removed/unmapped) */
	/* First check main IOVs list */
	iov = find_iov(lpi, vaddr);

	/* If not found in main list, check requests list (may have been queued by page fault) */
	if (!iov) {
		list_for_each_entry(iov, &lpi->reqs, l) {
			if (vaddr >= iov->start && vaddr < iov->end) {
				lp_debug(lpi, "Page at 0x%lx found in requests list\n", vaddr);
				goto found_iov;
			}
		}
		iov = NULL; /* Reset if not found in reqs either */
	}

	if (!iov) {
		lp_debug(lpi, "Page at 0x%lx no longer needed (unmapped), dropping\n", vaddr);
		return 0;
	}

found_iov:
	tracked_pages = (iov->end - vaddr) / PAGE_SIZE;
	pages = min(pages, tracked_pages);
	if (!pages)
		return 0;

	/*
	 * NOTE: In COW mode, this callback is NOT called - pages flow through
	 * convergence_io_complete() or prebuffer_io_complete() instead.
	 * This code path is only for non-COW lazy pages.
	 */

	/* Copy pages to userspace */
	ret = uffd_copy(lpi, vaddr, &pages);
	clock_gettime(CLOCK_MONOTONIC, &t_copy);
	if (opts.cow_dump)
		cow_uffd_stats_add_copy((t_copy.tv_sec - t_start.tv_sec) * 1000000000 +
					(t_copy.tv_nsec - t_start.tv_nsec));

	if (ret < 0)
		return ret;

	/* Recheck if process exited (may be detected in uffd_copy) */
	if (lpi->exited)
		return 0;

	/* CRITICAL: Remove copied pages from IOV tracking to prevent duplicate faults */
#if 0 //TODO
	ret = drop_iovs(lpi, vaddr, pages * PAGE_SIZE);
		if (ret < 0)
		return ret;

#endif
	clock_gettime(CLOCK_MONOTONIC, &t_drop);
	if (opts.cow_dump)
		cow_uffd_stats_add_drop((t_drop.tv_sec - t_copy.tv_sec) * 1000000000 +
					(t_drop.tv_nsec - t_copy.tv_nsec));

	clock_gettime(CLOCK_MONOTONIC, &t_end);
	if (opts.cow_dump)
		cow_uffd_stats_add_io_bulk((t_end.tv_sec - t_start.tv_sec) * 1000000000 +
					   (t_end.tv_nsec - t_start.tv_nsec));

	return ret;
}

static int uffd_zero(struct lazy_pages_info *lpi, __u64 address, unsigned long nr_pages)
{
	struct uffdio_zeropage uffdio_zeropage;
	unsigned long len = page_size() * nr_pages;

	uffdio_zeropage.range.start = address;
	uffdio_zeropage.range.len = len;
	uffdio_zeropage.mode = 0;
	uffdio_zeropage.zeropage = 0;

	lp_debug(lpi, "zero page at 0x%llx\n", address);

	if (ioctl(lpi->lpfd.fd, UFFDIO_ZEROPAGE, &uffdio_zeropage) == -1) {
		/* In COW dump mode, queue EAGAIN requests instead of blocking */
		if (errno == EAGAIN && opts.cow_dump) {
			/* page_state set to EAGAIN_QUEUED inside cow_queue_eagain_request on success */
			return cow_queue_eagain_request(lpi, address, nr_pages, NULL, "zero");
		}

		/* Non-COW mode or non-EAGAIN: check for errors */
		if (uffd_check_op_error(lpi, "zero", &nr_pages, uffdio_zeropage.zeropage))
			return -1;
		return 0;
	}

	/* Check for soft error */
	if (uffdio_zeropage.zeropage < 0) {
		errno = -uffdio_zeropage.zeropage;

		/* In COW dump mode, queue EAGAIN requests */
		if (errno == EAGAIN && opts.cow_dump) {
			/* page_state set to EAGAIN_QUEUED inside cow_queue_eagain_request on success */
			return cow_queue_eagain_request(lpi, address, nr_pages, NULL, "zero");
		}

		if (uffd_check_op_error(lpi, "zero", &nr_pages, uffdio_zeropage.zeropage))
			return -1;
		return 0;
	}

	page_state_set(address, PAGE_STATE_COPIED);
	return 0;
}

/*
 * Seek for the requested address in the pagemap. If it is found, the
 * subsequent call to pr->page_read will bring us the data. If the
 * address is not found in the pagemap, but no error occurred, the
 * address should be mapped to zero pfn.
 *
 * Returns 0 for zero pages, 1 for "real" pages and negative value on
 * error
 */
static int uffd_seek_pages(struct lazy_pages_info *lpi, __u64 address, unsigned long nr)
{
	int ret;

	lpi->pr.reset(&lpi->pr);

	ret = lpi->pr.seek_pagemap(&lpi->pr, address);
	if (!ret) {
		lp_err(lpi, "no pagemap covers %llx\n", address);
		return -1;
	}

	return 0;
}

static int uffd_handle_pages(struct lazy_pages_info *lpi, __u64 address, unsigned long nr, unsigned flags)
{
	int ret;	

	ret = uffd_seek_pages(lpi, address, nr);
	if (ret) {
		lp_warn(lpi, "#PF at 0x%llx uffd_seek_pages failed\n", address);
		return ret;
	}

	ret = lpi->pr.read_pages(&lpi->pr, address, nr, lpi->buf, flags);

	if (ret <= 0) {
		lp_err(lpi, "failed reading pages at %llx\n", address);
		return ret;
	}

	return 0;
}

static struct lazy_iov *pick_next_range(struct lazy_pages_info *lpi)
{
	return list_first_entry(&lpi->iovs, struct lazy_iov, l);
}

/*
 * This is very simple heurstics for background transfer control.
 * The idea is to transfer larger chunks when there is no page faults
 * and drop the background transfer size each time #PF occurs to some
 * default value. The default is empirically set to 64Kbytes
 */
static void update_xfer_len(struct lazy_pages_info *lpi, bool pf)
{
	if (pf)
		lpi->xfer_len = DEFAULT_XFER_LEN;
	else
		lpi->xfer_len += DEFAULT_XFER_LEN;

	if (lpi->xfer_len > MAX_XFER_LEN)
		lpi->xfer_len = MAX_XFER_LEN;
}

static int xfer_pages(struct lazy_pages_info *lpi)
{
	struct lazy_iov *iov;
	unsigned long nr_pages;
	unsigned long len;
	int err;

	iov = pick_next_range(lpi);
	if (!iov)
		return 0;

	len = min(iov->end - iov->start, lpi->xfer_len);

	iov = extract_range(iov, iov->start, iov->start + len);
	if (!iov)
		return -1;
	iov_list_insert(iov, &lpi->reqs);

	nr_pages = (iov->end - iov->start) / PAGE_SIZE;

	/* Update COW statistics */
	if (opts.cow_dump)
		cow_uffd_stats_inc_bg(nr_pages);

	update_xfer_len(lpi, false);

	err = uffd_handle_pages(lpi, iov->img_start, nr_pages, PR_ASYNC | PR_ASAP);
	if (err < 0) {
		lp_err(lpi, "Error during UFFD copy\n");
		return -1;
	}

	/* Track this background transfer as waiting for server response */
	pf_tracker_add(iov->start, nr_pages, lpi->pid, false);

	return 0;
}

static int handle_remove(struct lazy_pages_info *lpi, struct uffd_msg *msg)
{
	struct uffdio_range unreg;

	unreg.start = msg->arg.remove.start;
	unreg.len = msg->arg.remove.end - msg->arg.remove.start;

	lp_debug(lpi, "UNMAP: %llx-%llx\n", unreg.start, unreg.start + unreg.len);

	/* Mark all pages in range as unmapped for state tracking */
	page_state_mark_range_unmapped(unreg.start, unreg.len);

	/* Track unmapped pages for production validation */
	unmapped_tracker_mark_range(unreg.start, unreg.len);

	/* Remove these pages from buffer - no point draining them */
	cow_page_buffer_remove_range(unreg.start, unreg.len);

	/*
	 * The REMOVE event does not change the VMA, so we need to
	 * make sure that we won't handle #PFs in the removed
	 * range. With UNMAP, there's no VMA to worry about
	 */

	if (msg->event == UFFD_EVENT_REMOVE && ioctl(lpi->lpfd.fd, UFFDIO_UNREGISTER, &unreg)) {
		/*
		 * The kernel returns -ENOMEM when unregister is
		 * called after the process has gone
		 */
		if (errno == ENOMEM) {
			handle_exit(lpi);
			return 0;
		}

		pr_perror("Failed to unregister (%llx - %llx)", unreg.start, unreg.start + unreg.len);
		return -1;
	}

	return 0;//drop_iovs(lpi, unreg.start, unreg.len);

}

static int handle_remap(struct lazy_pages_info *lpi, struct uffd_msg *msg)
{
	unsigned long from = msg->arg.remap.from;
	unsigned long to = msg->arg.remap.to;
	unsigned long len = msg->arg.remap.len;

	lp_debug(lpi, "REMAP: %lx -> %lx (%ld)\n", from, to, len);

	return remap_iovs(lpi, from, to, len);
}

static int handle_fork(struct lazy_pages_info *parent_lpi, struct uffd_msg *msg)
{
	struct lazy_pages_info *lpi;
	int uffd = msg->arg.fork.ufd;

	lp_debug(parent_lpi, "FORK: child with ufd=%d\n", uffd);

	lpi = lpi_init();
	if (!lpi)
		return -1;

	if (copy_iovs(parent_lpi, lpi))
		goto out;

	lpi->pid = parent_lpi->pid;
	lpi->lpfd.fd = uffd;
	lpi->parent = parent_lpi->parent ? parent_lpi->parent : parent_lpi;
	lpi->copied_pages = lpi->parent->copied_pages;
	lpi->total_pages = lpi->parent->total_pages;
	list_add_tail(&lpi->l, &pending_lpis);

	dup_page_read(&lpi->parent->pr, &lpi->pr);

	lpi_get(lpi->parent);

	page_read_disable_dedup(&parent_lpi->pr);
	page_read_disable_dedup(&lpi->pr);
	return 1;

out:
	lpi_fini(lpi);
	return -1;
}

/*
 * We may exit epoll_run_rfds() loop because of non-fork() event. In
 * such case we return 1 rather than 0 to let the caller know that no
 * fork() events were pending
 */
static int complete_forks(int epollfd, struct epoll_event **events, int *nr_fds)
{
	struct lazy_pages_info *lpi, *n;
	struct epoll_event *tmp;

	if (list_empty(&pending_lpis))
		return 1;

	list_for_each_entry(lpi, &pending_lpis, l)
		(*nr_fds)++;

	tmp = xrealloc(*events, sizeof(struct epoll_event) * (*nr_fds));
	if (!tmp)
		return -1;
	*events = tmp;

	list_for_each_entry_safe(lpi, n, &pending_lpis, l) {
		if (epoll_add_rfd(epollfd, &lpi->lpfd))
			return -1;

		list_del_init(&lpi->l);
		list_add_tail(&lpi->l, &lpis);
	}

	return 0;
}

static bool is_page_queued(struct lazy_pages_info *lpi, unsigned long addr)
{
	struct lazy_iov *req;

	list_for_each_entry(req, &lpi->reqs, l)
		if (addr >= req->start && addr < req->end)
			return true;

	return false;
}

static int handle_page_fault(struct lazy_pages_info *lpi, struct uffd_msg *msg)
{
	struct lazy_iov *iov;
	unsigned long long address;
	int ret;
	unsigned long nr_pages;
	static unsigned long pf_count = 0;

	/* Align requested address to the next page boundary */
	address = msg->arg.pagefault.address & ~(page_size() - 1);

	pf_count++;

	lp_debug(lpi, "#PF at 0x%llx\n", address);

	/* COW mode: try to serve from buffer first */
	if (opts.cow_dump) {
		ret = cow_handle_page_fault_buffer(lpi, address);
		if (ret < 0)
			return ret;
		if (ret > 0)
			return 0;  /* Page served from buffer */

		/* Page not in buffer - check if all pages sent */
		if (cow_is_all_pages_sent_received()) {
			lp_debug(lpi, "Page 0x%llx not in buffer, all pages sent - zero-filling\n", address);
			page_state_set(address, PAGE_STATE_PF_PENDING);
			return uffd_zero(lpi, address, 1);
		}
	}

	if (opts.cow_dump) {
		/*
		 * In COW/bulk mode, pages arrive via the bulk stream,
		 * but we still need to send an urgent request so the
		 * server prioritizes this page. We do NOT split the IOV
		 * or move it to the reqs list to avoid fragmenting the
		 * IOV list.
		 */
		unsigned long long img_addr;

		/* Check if server is available for convergence requests */
		if (get_page_server_sk() < 0) {
			/*
			 * In COW mode, don't zero-fill - the correct data should
			 * arrive via the drain thread. Return 0 to let the process
			 * retry the fault when drain thread fills the page.
			 */
			lp_debug(lpi, "Page 0x%llx server unavailable - waiting for drain\n", address);
			return 0;
		}

		iov = find_iov(lpi, address);

		if (!iov) {
			/*
			 * IOV not found. If dirty bitmap received, all pages should
			 * have been transferred - zero-fill this page. Otherwise
			 * wait for drain thread to fill it.
			 */
			if (cow_is_dirty_bitmap_received()) {
				lp_debug(lpi, "Page 0x%llx IOV not found - zero fill\n", address);
				return uffd_zero(lpi, address, 1);
			}
			lp_debug(lpi, "Page 0x%llx IOV not found - waiting for drain\n", address);
			return 0;
		}

		/*
		 * New VMAs don't have pagemap entries - they didn't exist in Phase 1.
		 * Request page directly from page server.
		 */
		if (iov->is_new_vma) {
			lp_debug(lpi, "Page 0x%llx in new VMA - requesting from server\n", address);
			cow_uffd_stats_inc_pf(1);
			pf_tracker_add(address, 1, lpi->pid, true);
			if (request_remote_pages(lpi->pr.img_id, address, 1) < 0) {
				lp_err(lpi, "Error requesting new VMA page 0x%llx\n", address);
				return -1;
			}
			return 0;
		}

		img_addr = iov->img_start + (address - iov->start);

		cow_uffd_stats_inc_pf(1);
		pf_tracker_add(address, 1, lpi->pid, true);

		if (cow_is_phase3_active()) {
			/* In Phase 3, pages arrive via convergence stream */
			if (request_remote_pages(lpi->pr.img_id, address, 1) < 0) {
				lp_err(lpi, "Error requesting page 0x%llx in Phase 3\n", address);
				return -1;
			}

		} else {		
			ret = uffd_handle_pages(lpi, img_addr, 1, PR_ASYNC | PR_ASAP);			
			if (ret < 0) {
				lp_err(lpi, "Error during COW page fault request\n");
				return -1;
			}
		}

		return 0;
	}

	if (is_page_queued(lpi, address)) {
		lp_debug(lpi, "#PF at 0x%llx queued\n", address);
		return 0;
	}

	iov = find_iov(lpi, address);
	if (!iov) {
		lp_debug(lpi, "#PF at 0x%llx !iov\n", address);
		return uffd_zero(lpi, address, 1);
	}

	iov = extract_range(iov, address, address + PAGE_SIZE);
	if (!iov) {
		lp_debug(lpi, "#PF at 0x%llx !iov2\n", address);
		return -1;
	}

	iov_list_insert(iov, &lpi->reqs);

	nr_pages = (iov->end - iov->start) / PAGE_SIZE;

	/* Update COW statistics */
	if (opts.cow_dump)
		cow_uffd_stats_inc_pf(nr_pages);

	update_xfer_len(lpi, true);

	ret = uffd_handle_pages(lpi, iov->img_start, nr_pages, PR_ASYNC | PR_ASAP);
	if (ret < 0) {
		lp_err(lpi, "Error during regular page copy\n");
		return -1;
	}

	/* Track this page fault as waiting for server response */
	pf_tracker_add(address, nr_pages, lpi->pid, true);

	return 0;
}

static int handle_uffd_event(struct epoll_rfd *lpfd)
{
	struct lazy_pages_info *lpi;
	struct uffd_msg msg;
	int ret;

	lpi = container_of(lpfd, struct lazy_pages_info, lpfd);
	ret = read(lpfd->fd, &msg, sizeof(msg));
	if (ret < 0) {
		/* we've already handled the page fault for another thread */
		if (errno == EAGAIN)
			return 0;
		if (errno == EBADF && lpi->exited) {
			lp_debug(lpi, "excess message in queue: %d", msg.event);
			return 0;
		}
		lp_perror(lpi, "Can't read uffd message");
		return -1;
	} else if (ret == 0) {
		return 1;
	} else if (ret != sizeof(msg)) {
		lp_err(lpi, "Can't read uffd message: short read");
		return -1;
	}

	switch (msg.event) {
	case UFFD_EVENT_PAGEFAULT:
		return handle_page_fault(lpi, &msg);
	case UFFD_EVENT_REMOVE:
	case UFFD_EVENT_UNMAP:
		return handle_remove(lpi, &msg);
	case UFFD_EVENT_REMAP:
		return handle_remap(lpi, &msg);
	case UFFD_EVENT_FORK:
		return handle_fork(lpi, &msg);
	default:
		lp_err(lpi, "unexpected uffd event %u\n", msg.event);
		return -1;
	}

	return 0;
}

/* Non-static for use by uffd_cow.c */
void lazy_pages_summary(struct lazy_pages_info *lpi)
{
	lp_debug(lpi, "UFFD transferred pages: (%ld/%ld)\n", lpi->copied_pages, lpi->total_pages);

#if 0
	if ((lpi->copied_pages != lpi->total_pages) && (lpi->total_pages > 0)) {
		lp_warn(lpi, "Only %ld of %ld pages transferred via UFFD\n"
			"Something probably went wrong.\n",
			lpi->copied_pages, lpi->total_pages);
		return 1;
	}
#endif
}

/*
 * EAGAIN retry functions (retry_uffd_copy, retry_uffd_zero) and
 * cow_process_eagain_requests, cow_is_eagain_queue_empty are in cow-uffd.c
 */

static int handle_requests(int epollfd, struct epoll_event **events, int nr_fds)
{
	struct lazy_pages_info *lpi, *n;
	int poll_timeout = -1;
	int ret;

	for (;;) {
		ret = epoll_run_rfds(epollfd, *events, nr_fds, poll_timeout);
		if (ret < 0)
			goto out;

		if (ret > 0) {
			ret = complete_forks(epollfd, events, &nr_fds);
			if (ret < 0)
				goto out;
			if (restore_finished)
				poll_timeout = opts.cow_dump ? 100 : 0;
			if (!restore_finished)
				continue;
			if (!opts.cow_dump && !ret)
				continue;
		}

		/* make sure we return success if there is nothing to xfer */
		ret = 0;

		if (opts.cow_dump && !cow_is_eagain_queue_empty()) {
			if (cow_process_eagain_requests()) {
				ret = -1;
				goto out;
			}
		}

		if (!opts.cow_dump) {
			/* Non-COW mode: background transfer loop */
			list_for_each_entry_safe(lpi, n, &lpis, l) {
				if (!list_empty(&lpi->iovs) &&
				    list_empty(&lpi->reqs)) {
					ret = xfer_pages(lpi);
					if (ret < 0)
						goto out;
					break;
				}

				if (!list_empty(&lpi->reqs))
					continue;

				lazy_pages_summary(lpi);
				list_del(&lpi->l);
				lpi_put(lpi);
			}

			if (list_empty(&lpis))
				break;
			continue;
		}

		/*
		 * COW/bulk mode: Wait for exit conditions:
		 * 1. all_pages_sent signal received
		 * 2. drain thread finished (buffer empty)
		 * Then send ACK to primary and exit.
		 */
		ret = cow_handle_exit(&lpis);
		if (ret)
			break;
	}

out:
	return ret;
}

int lazy_pages_finish_restore(void)
{
	uint32_t fin = LAZY_PAGES_RESTORE_FINISHED;
	int fd, ret;

	if (!opts.lazy_pages)
		return 0;

	fd = fdstore_get(lazy_pages_sk_id);
	if (fd < 0) {
		pr_err("No lazy-pages socket\n");
		return -1;
	}

	ret = send(fd, &fin, sizeof(fin), 0);
	if (ret != sizeof(fin)) {
		if (ret < 0 && errno == EPIPE) {
			pr_warn("Lazy-pages socket closed before finish; assuming transfer complete\n");
			close(fd);
			return 0;
		}
		pr_perror("Failed sending restore finished indication");
		close(fd);
		return -1;
	}

	close(fd);

	return ret < 0 ? ret : 0;
}

static int prepare_lazy_socket(void)
{
	int listen;
	struct sockaddr_un saddr;

	if (prepare_sock_addr(&saddr))
		return -1;

	pr_debug("Waiting for incoming connections on %s\n", saddr.sun_path);
	if ((listen = server_listen(&saddr)) < 0) {
		pr_perror("server_listen error");
		return -1;
	}

	return listen;
}

static int lazy_sk_read_event(struct epoll_rfd *rfd)
{
	uint32_t fin;
	int ret;

	ret = recv(rfd->fd, &fin, sizeof(fin), 0);
	/*
	 * epoll sets POLLIN | POLLHUP for the EOF case, so we get short
	 * read just before hangup_event
	 */
	if (!ret)
		return 0;

	if (ret != sizeof(fin)) {
		pr_perror("Failed getting restore finished indication");
		return -1;
	}

	if (fin != LAZY_PAGES_RESTORE_FINISHED) {
		pr_err("Unexpected response: %x\n", fin);
		return -1;
	}
	restore_finished = true;

	return 1;
}

static int lazy_sk_hangup_event(struct epoll_rfd *rfd)
{
	if (!restore_finished) {
		pr_err("Restorer unexpectedly closed the connection\n");
		return -1;
	}

	return 0;
}

static int prepare_uffds(int listen, int epollfd)
{
	int i;
	int client;
	socklen_t len;
	struct sockaddr_un saddr;

	/* accept new client request */
	len = sizeof(struct sockaddr_un);
	if ((client = accept(listen, (struct sockaddr *)&saddr, &len)) < 0) {
		pr_perror("server_accept error");
		close(listen);
		return -1;
	}

	for (i = 0; i < task_entries->nr_tasks; i++) {
		struct lazy_pages_info *lpi = NULL;
		if (ud_open(client, &lpi))
			goto close_uffd;
		if (lpi == NULL)
			continue;
		if (epoll_add_rfd(epollfd, &lpi->lpfd))
			goto close_uffd;
	}

	lazy_sk_rfd.fd = client;
	lazy_sk_rfd.read_event = lazy_sk_read_event;
	lazy_sk_rfd.hangup_event = lazy_sk_hangup_event;
	if (epoll_add_rfd(epollfd, &lazy_sk_rfd))
		goto close_uffd;

	close(listen);
	return 0;

close_uffd:
	close_safe(&client);
	close(listen);
	return -1;
}

/* Pre-buffer and convergence infrastructure is in cow-uffd.c */
static struct epoll_rfd lazy_listen_rfd;

/* Convergence callback wrapper - calls into cow-uffd.c logic */
static int convergence_io_complete(unsigned long dst_id, unsigned long vaddr,
				   unsigned long nr_pages, void *priv)
{
	void *buf = priv;
	struct lazy_pages_info *lpi;
	int ret;

	/* Find lpi for this vaddr and do UFFDIO_COPY */
	list_for_each_entry(lpi, &lpis, l) {
		unsigned long pages;

		if (lpi->exited || lpi->lpfd.fd < 0)
			continue;
		if (!find_iov(lpi, vaddr))
			continue;

		/* Copy buffer to lpi->buf and call uffd_copy */
		memcpy(lpi->buf, buf, nr_pages * PAGE_SIZE);
		pages = nr_pages;
		ret = uffd_copy(lpi, vaddr, &pages);
		if (ret < 0) {
			lp_err(lpi, "Direct convergence copy failed at 0x%lx\n", vaddr);
			return ret;
		}
		lp_debug(lpi, "Direct copy %lu pages at 0x%lx (convergence)\n", pages, vaddr);
		return 0;
	}

	pr_err("BUG: Convergence callback with no lpi for vaddr 0x%lx\n", vaddr);
	BUG();
	return -1;
}

/* Switch to convergence mode - wrapper that sets up callback */
static void switch_to_convergence_callback(void)
{
	void *buf = cow_get_prebuffer_buf();
	if (!buf) {
		pr_warn("Cannot switch to convergence: no prebuffer_buf\n");
		return;
	}

	if (page_server_update_async_callback(convergence_io_complete, buf) < 0)
		pr_warn("Failed to switch to convergence callback\n");
}

/*
 * Non-blocking accept handler for when criu restore connects.
 * Called from epoll loop when restore connects on the Unix socket.
 */
static int handle_lazy_accept(struct epoll_rfd *rfd)
{
	int client;
	int i;
	struct sockaddr_un saddr;
	socklen_t len = sizeof(saddr);

	client = accept(rfd->fd, (struct sockaddr *)&saddr, &len);
	if (client < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		pr_perror("accept failed");
		return -1;
	}

	/* Set up lpi for each task (reads uffd from restore) */
	for (i = 0; i < task_entries->nr_tasks; i++) {
		struct lazy_pages_info *lpi = NULL;

		if (ud_open(client, &lpi))
			goto err;
		if (lpi == NULL)
			continue;
		if (epoll_add_rfd(epollfd, &lpi->lpfd))
			goto err;
	}

	/* Set up restore-finished notification socket */
	lazy_sk_rfd.fd = client;
	lazy_sk_rfd.read_event = lazy_sk_read_event;
	lazy_sk_rfd.hangup_event = lazy_sk_hangup_event;
	if (epoll_add_rfd(epollfd, &lazy_sk_rfd))
		goto err;

	/* Remove listen socket from epoll — only one connection needed */
	epoll_del_rfd(epollfd, rfd);
	close(rfd->fd);

	/*
	 * Keep buffering ON — handle_page_fault() will serve from buffer.
	 * Start background drain thread to proactively apply buffered pages.
	 */
	cow_set_restore_connected(true);

	pr_info("criu restore setup complete, %lu pages buffered\n",
		cow_page_buffer_count());

	/*
	 * Start drain thread and switch to convergence callback only if
	 * dirty bitmap already received. Both require uffd available.
	 * If bitmap hasn't arrived yet, we'll do this when it does.
	 */
	if (opts.cow_dump && cow_is_dirty_bitmap_received()) {
		unsigned int nr_ranges;
		unsigned long *dirty_ranges = cow_get_pending_dirty_ranges(&nr_ranges);

		if (dirty_ranges) {
			pr_info("Creating IOVs for %u pending dirty ranges\n", nr_ranges);
			if (cow_create_iovs_for_new_ranges(&lpis, dirty_ranges, nr_ranges) < 0)
				pr_warn("Failed to create IOVs for some new ranges\n");
			xfree(dirty_ranges);
		}

		pr_info("Dirty bitmap already received, entering convergence\n");
		switch_to_convergence_callback();
		cow_start_drain_thread(&lpis);
	}

	/* Phase 3: request all pages now that lpis are created */
	if (cow_is_phase3_active()) {
		struct pstree_item *pi;

		for_each_pstree_item(pi) {
			if (task_alive(pi)) {
				pr_info("Requesting all remote pages for pid=%d\n",
					vpid(pi));
				if (request_all_remote_pages(vpid(pi)) < 0) {
					pr_err("Failed to request pages for pid=%d\n",
						vpid(pi));
					goto err;
				}
			}
		}
	}

	return 0;

err:
	close(client);
	return -1;
}

/*
 * Simple COW state accessors are in cow-uffd.c:
 * - cow_is_dirty_bitmap_received(), cow_set_dirty_bitmap_received()
 * - cow_is_restore_connected(), cow_set_restore_connected()
 * - cow_is_inventory_ready_received(), cow_set_inventory_ready_received()
 * - cow_is_all_pages_sent_received(), cow_set_all_pages_sent_received()
 */

/* Wrapper: implementation is in cow-uffd.c */
static int create_iovs_for_new_ranges(unsigned long *dirty_ranges,
				      unsigned int nr_dirty_ranges)
{
	return cow_create_iovs_for_new_ranges(&lpis, dirty_ranges, nr_dirty_ranges);
}

/* Set dirty bitmap received flag (called when dirty bitmap fully received) */
void set_dirty_bitmap_received(unsigned long *dirty_ranges,
			       unsigned int nr_dirty_ranges)
{
	pr_info("Dirty bitmap received from primary (%u ranges)\n", nr_dirty_ranges);

	cow_set_dirty_bitmap_received(true);

	/*
	 * If restore is already connected, create IOVs for new VMAs now.
	 * Otherwise store dirty_ranges for later processing in handle_lazy_accept().
	 */
	if (cow_is_restore_connected()) {
		if (create_iovs_for_new_ranges(dirty_ranges, nr_dirty_ranges) < 0)
			pr_warn("Failed to create IOVs for some new ranges\n");

		xfree(dirty_ranges);

		pr_info("Restore already connected, entering convergence\n");
		switch_to_convergence_callback();
		cow_start_drain_thread(&lpis);
	} else {
		pr_info("Storing dirty ranges for later (%u ranges)\n", nr_dirty_ranges);
		cow_store_pending_dirty_ranges(dirty_ranges, nr_dirty_ranges);
	}
}

/*
 * COW Phase 3: Enter restore loop after pages are buffered and pstree loaded.
 * Called from cow-lazy-pages.c after Phase 2 completes.
 *
 * This sets up the lazy socket for restore to connect and enters the
 * main event loop to handle page faults (WP_SYNC convergence).
 */
int cow_phase3_restore_loop(int ep_fd, struct epoll_event **events, int nr_fds)
{
	int lazy_sk;
	int flags;
	int ret;

	/* Set global epollfd for use by handle_lazy_accept() */
	epollfd = ep_fd;
	cow_set_phase3_active(true);

	/* Create lazy socket for restore to connect */
	lazy_sk = prepare_lazy_socket();
	if (lazy_sk < 0) {
		pr_err("Failed to create lazy socket for Phase 3\n");
		return -1;
	}

	/* Make listen socket non-blocking and add to epoll */
	flags = fcntl(lazy_sk, F_GETFL, 0);
	fcntl(lazy_sk, F_SETFL, flags | O_NONBLOCK);

	lazy_listen_rfd.fd = lazy_sk;
	lazy_listen_rfd.read_event = handle_lazy_accept;
	if (epoll_add_rfd(epollfd, &lazy_listen_rfd)) {
		close(lazy_sk);
		return -1;
	}

	pr_info("COW Phase 3: Waiting for restore to connect\n");

	/* Initialize buffer and switch directly to convergence callback for Phase 3 */
	if (cow_setup_prebuffer_reader()) {
		pr_err("Failed to setup prebuffer reader for Phase 3\n");
		close(lazy_sk);
		return -1;
	}
	switch_to_convergence_callback(); //TODO register to the callback at setup_prebuffer_reader

	/* Pages will be requested in handle_lazy_accept() after restore connects */

	/* Enter main event loop - handle page faults until restore finishes */
	ret = handle_requests(epollfd, events, nr_fds);

	return ret;
}

int cr_lazy_pages(bool daemon)
{
	struct epoll_event *events = NULL;
	int nr_fds;
	int lazy_sk;
	int ret;

	if (!kdat.has_uffd)
		return -1;

	/*
	 * COW Phase 2: No inventory/pstree yet, just buffer pages.
	 * Use separate code path with minimal dependencies.
	 */
	if (opts.cow_dump && opts.use_page_server)
		return cr_lazy_pages_cow_phase2(daemon);

	if (prepare_dummy_pstree())
		return -1;

	lazy_sk = prepare_lazy_socket();
	if (lazy_sk < 0)
		return -1;

	if (daemon) {
		ret = cr_daemon(1, 0, -1);
		if (ret == -1) {
			pr_err("Can't run in the background\n");
			return -1;
		}
		if (ret > 0) { /* parent task, daemon started */
			if (opts.pidfile) {
				if (write_pidfile(ret) == -1) {
					pr_perror("Can't write pidfile");
					kill(ret, SIGKILL);
					waitpid(ret, NULL, 0);
					return -1;
				}
			}

			return 0;
		}
	}

	if (status_ready())
		return -1;

	/*
	 * we poll nr_tasks userfault fds, UNIX socket between lazy-pages
	 * daemon and the cr-restore, and, optionally TCP socket for
	 * remote pages. Add +1 for the listen socket in pre-buffer mode.
	 */
	nr_fds = task_entries->nr_tasks + (opts.use_page_server ? 2 : 1) + 1;
	epollfd = epoll_prepare(nr_fds, &events);
	if (epollfd < 0)
		return -1;

	if (prepare_uffds(lazy_sk, epollfd)) {
		xfree(events);
		return -1;
	}

	if (opts.use_page_server) {
		struct lazy_pages_info *lpi;

		if (connect_to_page_server_to_recv(epollfd)) {
			xfree(events);
			return -1;
		}

		/* Request all pages for bulk mode */
		if (opts.cow_dump) {
			/* Initialize COW infrastructure */
			if (cow_lazy_pages_init() < 0) {
				pr_err("Failed to initialize COW infrastructure\n");
				xfree(events);
				return -1;
			}

			list_for_each_entry(lpi, &lpis, l) {
				pr_info("Requesting all remote pages for pid=%d\n",
					lpi->pid);
				if (request_all_remote_pages(lpi->pr.img_id) < 0) {
					pr_err("Failed to request pages for pid=%d\n",
					       lpi->pid);
					xfree(events);
					return -1;
				}
			}
		}
	}

	ret = handle_requests(epollfd, &events, nr_fds);

	disconnect_from_page_server();

	/* Clean up COW infrastructure if it was initialized */
	if (opts.cow_dump)
		cow_lazy_pages_cleanup();

	xfree(events);
	return ret;
}
