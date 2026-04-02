#include <dirent.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <linux/falloc.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <linux/userfaultfd.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <poll.h>
#include <lz4.h>

#undef LOG_PREFIX
#define LOG_PREFIX "page-xfer: "

#include "types.h"
#include "cr_options.h"
#include "servicefd.h"
#include "image.h"
#include "page-xfer.h"
#include "page-pipe.h"
#include "util.h"
#include "protobuf.h"
#include "images/pagemap.pb-c.h"
#include "fcntl.h"
#include "pstree.h"
#include "parasite-syscall.h"
#include "parasite.h"
#include "rst_info.h"
#include "stats.h"
#include "tls.h"
#include "uffd.h"
#include "cow-dump.h"
#include "cow-bpf.h"
#include "image-xfer.h"
#include "criu-plugin.h"
#include "plugin.h"
#include "dump.h"
#include "mem.h"

static int page_server_sk = -1;
static bool bulk_stream_done = false;

/* COW_TRANSFER_STREAMS defined in include/page-xfer.h */
static int g_listen_sk = -1;	/* kept open for additional accepts */
static int g_bulk_streams_closed;	/* count of streams that sent close */
bool page_server_bulk_stream_done(void)
{
	return bulk_stream_done;
}

#define BULK_STREAM_WOULD_BLOCK 0
#define BULK_STREAM_PROGRESS 1
#define BULK_STREAM_COMPLETE 2
/* No ACK on bulk close: end-of-stream marker is enough. */

/* Global compression statistics for stats printing */
static unsigned long g_compress_uncompressed_bytes = 0;
static unsigned long g_compress_compressed_bytes = 0;

struct page_server_iov {
	u32 cmd;
	u64 nr_pages;
	u64 vaddr;
	u64 dst_id;
};

static void psi2iovec(struct page_server_iov *ps, struct iovec *iov)
{
	iov->iov_base = decode_pointer(ps->vaddr);
	iov->iov_len = ps->nr_pages * PAGE_SIZE;
}

#define PS_IOV_ADD    1
#define PS_IOV_HOLE   2
#define PS_IOV_OPEN   3
#define PS_IOV_OPEN2  4
#define PS_IOV_PARENT 5
#define PS_IOV_ADD_F  6
#define PS_IOV_GET    7
#define PS_IOV_GET_ALL 8
#define PS_IOV_ADD_F_PF 9
#define PS_IOV_ADD_F_COMPRESS 10
#define PS_IOV_VMA_DIFF       11
#define PS_IOV_CONVERGE_REGS        12
#define PS_IOV_CONVERGE_FDS         13
#define PS_IOV_CONVERGE_SIGACTS     14
#define PS_IOV_CONVERGE_ITIMERS     15
#define PS_IOV_CONVERGE_MISC        16
#define PS_IOV_CONVERGE_THREAD_STATE 17

extern struct pstree_item *root_item;

struct t3_fd_entry {
	u32 fd;
	u32 flags;		/* O_RDONLY etc from /proc/pid/fdinfo */
	u64 pos;		/* file position */
	char path[256];		/* readlink of /proc/pid/fd/N */
};

struct vma_diff_entry {
	u64 start;
	u64 end;
	u32 prot;
	u32 pad;
};

struct t3_thread_regs {
	u64 regs[31];
	u64 sp;
	u64 pc;
	u64 pstate;
	u64 tls;
};

#define PS_IOV_CLOSE	   0x1023

/* Compression state machine states for bulk stream reader */
enum compress_read_state {
	COMPRESS_STATE_READING_HEADER = 0,    /* Reading page_server_iov header */
	COMPRESS_STATE_READING_SIZE,          /* Reading compressed_size (4 bytes) */
	COMPRESS_STATE_READING_COMPRESSED,    /* Reading compressed data */
	COMPRESS_STATE_READING_UNCOMPRESSED,  /* Reading uncompressed page data */
};
#define PS_IOV_FORCE_CLOSE 0x1024

#define PS_CMD_BITS 16
#define PS_CMD_MASK ((1 << PS_CMD_BITS) - 1)

#define PS_TYPE_BITS 8
#define PS_TYPE_MASK ((1 << PS_TYPE_BITS) - 1)

#define PS_TYPE_PID   (1)
#define PS_TYPE_SHMEM (2)
/*
 * XXX: When adding new types here check decode_pm for legacy
 * numbers that can be met from older CRIUs
 */

static inline u64 encode_pm(int type, unsigned long id)
{
	if (type == CR_FD_PAGEMAP)
		type = PS_TYPE_PID;
	else if (type == CR_FD_SHMEM_PAGEMAP)
		type = PS_TYPE_SHMEM;
	else {
		BUG();
		return 0;
	}

	return ((u64)id) << PS_TYPE_BITS | type;
}

static int decode_pm(u64 dst_id, unsigned long *id)
{
	int type;

	/*
	 * Magic numbers below came from the older CRIU versions that
	 * erroneously used the changing CR_FD_* constants. The
	 * changes were made when we merged images together and moved
	 * the CR_FD_-s at the tail of the enum
	 */
	type = dst_id & PS_TYPE_MASK;
	switch (type) {
	case 10: /* 3.1 3.2 */
	case 11: /* 1.3 1.4 1.5 1.6 1.7 1.8 2.* 3.0 */
	case 16: /* 1.2 */
	case 17: /* 1.0 1.1 */
	case PS_TYPE_PID:
		*id = dst_id >> PS_TYPE_BITS;
		type = CR_FD_PAGEMAP;
		break;
	case 27: /* 1.3 */
	case 28: /* 1.4 1.5 */
	case 29: /* 1.6 1.7 */
	case 32: /* 1.2 1.8 */
	case 33: /* 1.0 1.1 3.1 3.2 */
	case 34: /* 2.* 3.0 */
	case PS_TYPE_SHMEM:
		*id = dst_id >> PS_TYPE_BITS;
		type = CR_FD_SHMEM_PAGEMAP;
		break;
	default:
		type = -1;
		break;
	}

	return type;
}

static inline u32 encode_ps_cmd(u32 cmd, u32 flags)
{
	return flags << PS_CMD_BITS | cmd;
}

static inline u32 decode_ps_cmd(u32 cmd)
{
	return cmd & PS_CMD_MASK;
}

static inline u32 decode_ps_flags(u32 cmd)
{
	return cmd >> PS_CMD_BITS;
}

static inline int __send(int sk, const void *buf, size_t sz, int fl)
{
	return opts.tls ? tls_send(buf, sz, fl) : send(sk, buf, sz, fl);
}

static inline int __recv(int sk, void *buf, size_t sz, int fl)
{
	return opts.tls ? tls_recv(buf, sz, fl) : recv(sk, buf, sz, fl);
}

static inline int send_psi_flags(int sk, struct page_server_iov *pi, int flags)
{
	if (__send(sk, pi, sizeof(*pi), flags) != sizeof(*pi)) {
		pr_perror("Can't send PSI %d to server", pi->cmd);
		return -1;
	}
	return 0;
}

static inline int send_psi(int sk, struct page_server_iov *pi)
{
	return send_psi_flags(sk, pi, 0);
}

/*
 * Send a page with LZ4 compression.
 * Protocol: header (PS_IOV_ADD_F_COMPRESS) + compressed_size (4 bytes) + compressed_data
 * Optimized: single buffer, single send() syscall
 */
static __maybe_unused int send_page_compressed(int sk, const void *data, u64 dst_id,
					      unsigned long vaddr)
{
	/* Buffer layout: [header][compressed_size][compressed_data] */
	char send_buf[sizeof(struct page_server_iov) + sizeof(int) + LZ4_compressBound(PAGE_SIZE)];
	struct page_server_iov *pi = (struct page_server_iov *)send_buf;
	int *compressed_size = (int *)(send_buf + sizeof(*pi));
	char *compressed_data = send_buf + sizeof(*pi) + sizeof(int);
	int total_len;
	int ret;

	/* 1. Compress directly into send buffer (no memcpy!) */
	*compressed_size = LZ4_compress_default(data, compressed_data, PAGE_SIZE, 
						LZ4_compressBound(PAGE_SIZE));
	if (*compressed_size <= 0) {
		pr_err("LZ4 compression failed for page at %lx\n", vaddr);
		return -1;
	}

	/* Track compression statistics (atomic — multiple threads) */
	__sync_fetch_and_add(&g_compress_uncompressed_bytes, PAGE_SIZE);
	__sync_fetch_and_add(&g_compress_compressed_bytes, *compressed_size);

	pr_debug("Compressed page at %lx: %lu -> %d bytes (%.1f%%)\n", 
		 vaddr, PAGE_SIZE, *compressed_size, 
		 (float)(*compressed_size) * 100 / PAGE_SIZE);

	/* 2. Fill in header (after compression so we know it succeeded) */
	pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT);
	pi->nr_pages = 1;
	pi->vaddr = vaddr;
	pi->dst_id = dst_id;

	/* 3. Single send: header + size + compressed data */
	total_len = sizeof(*pi) + sizeof(int) + *compressed_size;
	ret = __send(sk, send_buf, total_len, 0);
	if (ret != total_len) {
		pr_perror("Failed to send compressed page (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

static __maybe_unused int send_page_uncompressed(int sk, const void *data,
						 u64 dst_id,
						 unsigned long vaddr)
{
	char send_buf[sizeof(struct page_server_iov) + PAGE_SIZE];
	struct page_server_iov *pi = (struct page_server_iov *)send_buf;
	void *payload = send_buf + sizeof(*pi);
	int total_len;
	int ret;

	memcpy(payload, data, PAGE_SIZE);

	pi->cmd = encode_ps_cmd(PS_IOV_ADD_F, PE_PRESENT);
	pi->nr_pages = 1;
	pi->vaddr = vaddr;
	pi->dst_id = dst_id;

	total_len = sizeof(*pi) + PAGE_SIZE;
	ret = __send(sk, send_buf, total_len, 0);
	if (ret != total_len) {
		pr_perror("Failed to send page (sent %d/%d)", ret, total_len);
		return -1;
	}

	return 0;
}

static void tcp_cork(int sk, bool on)
{
	int val = on ? 1 : 0;
	if (setsockopt(sk, SOL_TCP, TCP_CORK, &val, sizeof(val)))
		pr_pwarn("Unable to set TCP_CORK=%d", val);
}

static void tcp_nodelay(int sk, bool on)
{
	int val = on ? 1 : 0;
	if (setsockopt(sk, SOL_TCP, TCP_NODELAY, &val, sizeof(val)))
		pr_pwarn("Unable to set TCP_NODELAY=%d", val);
}

/* page-server xfer */
static int write_pages_to_server(struct page_xfer *xfer, int p, unsigned long len)
{
	ssize_t ret, left = len;

	if (opts.tls) {
		pr_debug("Sending %lx bytes\n", len);

		if (tls_send_data_from_fd(p, len))
			return -1;
	} else {
		pr_debug("Splicing %lx bytes into socket\n", len);

		while (left > 0) {
			ret = splice(p, NULL, xfer->sk, NULL, left, SPLICE_F_MOVE);
			if (ret < 0) {
				pr_perror("Can't write pages to socket");
				return -1;
			}

			pr_debug("\tSpliced: %lx bytes sent\n", (unsigned long)ret);
			left -= ret;
		}
	}

	return 0;
}

static int write_pagemap_to_server(struct page_xfer *xfer, struct iovec *iov, u32 flags)
{
	struct page_server_iov pi = {
		.cmd = encode_ps_cmd(PS_IOV_ADD_F, flags),
		.nr_pages = iov->iov_len / PAGE_SIZE,
		.vaddr = encode_pointer(iov->iov_base),
		.dst_id = xfer->dst_id,
	};

	return send_psi(xfer->sk, &pi);
}

static void close_server_xfer(struct page_xfer *xfer)
{
	xfer->sk = -1;
}

static int open_page_server_xfer(struct page_xfer *xfer, int fd_type, unsigned long img_id)
{
	char has_parent;
	struct page_server_iov pi = {
		.cmd = PS_IOV_OPEN2,
	};

	xfer->sk = page_server_sk;
	xfer->write_pagemap = write_pagemap_to_server;
	xfer->write_pages = write_pages_to_server;
	xfer->close = close_server_xfer;
	xfer->dst_id = encode_pm(fd_type, img_id);
	xfer->parent = NULL;

	pi.dst_id = xfer->dst_id;
	if (send_psi(xfer->sk, &pi)) {
		pr_perror("Can't write to page server");
		return -1;
	}

	/* Push the command NOW */
	tcp_nodelay(xfer->sk, true);

	if (__recv(xfer->sk, &has_parent, 1, 0) != 1) {
		pr_perror("The page server doesn't answer");
		return -1;
	}

	if (has_parent)
		xfer->parent = (void *)1; /* This is required for generate_iovs() */

	return 0;
}

/* local xfer */
static int write_pages_loc(struct page_xfer *xfer, int p, unsigned long len)
{
	ssize_t ret;
	ssize_t curr = 0;

	while (1) {
		ret = splice(p, NULL, img_raw_fd(xfer->pi), NULL, len - curr, SPLICE_F_MOVE);
		if (ret == -1) {
			pr_perror("Unable to spice data");
			return -1;
		}
		if (ret == 0) {
			pr_err("A pipe was closed unexpectedly\n");
			return -1;
		}
		curr += ret;
		if (curr == len)
			break;
	}

	return 0;
}

static int check_pagehole_in_parent(struct page_read *p, struct iovec *iov)
{
	int ret;
	unsigned long off, end;

	/*
	 * Try to find pagemap entry in parent, from which
	 * the data will be read on restore.
	 *
	 * This is the optimized version of the page-by-page
	 * read_pagemap_page routine.
	 */

	pr_debug("Checking %p - %p hole\n", iov->iov_base, iov->iov_base + iov->iov_len);
	off = (unsigned long)iov->iov_base;
	end = off + iov->iov_len;
	while (1) {
		unsigned long pend;

		ret = p->seek_pagemap(p, off);
		if (ret <= 0 || !p->pe) {
			pr_err("Missing %lx in parent pagemap\n", off);
			return -1;
		}

		pr_debug("\tFound %" PRIx64 " - %" PRIx64 "\n",
			 p->pe->vaddr, p->pe->vaddr + pagemap_len(p->pe));

		/*
		 * The pagemap entry in parent may happen to be
		 * shorter, than the hole we write. In this case
		 * we should go ahead and check the remainder.
		 */

		pend = p->pe->vaddr + pagemap_len(p->pe);
		if (end <= pend)
			return 0;

		pr_debug("\t\tcontinue on %lx\n", pend);
		off = pend;
	}
}

static int write_pagemap_loc(struct page_xfer *xfer, struct iovec *iov, u32 flags)
{
	int ret;
	PagemapEntry pe = PAGEMAP_ENTRY__INIT;

	pe.vaddr = encode_pointer(iov->iov_base);
	pe.nr_pages = iov->iov_len / PAGE_SIZE;
	pe.has_flags = true;
	pe.flags = flags;
	pe.has_nr_pages = true;

	if (flags & PE_PRESENT) {
		if (opts.auto_dedup && xfer->parent != NULL) {
			ret = dedup_one_iovec(xfer->parent, pe.vaddr, pagemap_len(&pe));
			if (ret == -1) {
				pr_perror("Auto-deduplication failed");
				return ret;
			}
		}
	} else if (flags & PE_PARENT) {
		if (xfer->parent != NULL) {
			ret = check_pagehole_in_parent(xfer->parent, iov);
			if (ret) {
				pr_err("Hole %p - %p not found in parent\n",
				       iov->iov_base, iov->iov_base + iov->iov_len);
				return -1;
			}
		}
	}

	if (pb_write_one(xfer->pmi, &pe, PB_PAGEMAP) < 0)
		return -1;

	return 0;
}

static void close_page_xfer(struct page_xfer *xfer)
{
	if (xfer->parent != NULL) {
		xfer->parent->close(xfer->parent);
		xfree(xfer->parent);
		xfer->parent = NULL;
	}
	close_image(xfer->pi);
	close_image(xfer->pmi);
}

static int open_page_local_xfer(struct page_xfer *xfer, int fd_type, unsigned long img_id)
{
	u32 pages_id;

	xfer->pmi = open_image(fd_type, O_DUMP, img_id);
	if (!xfer->pmi)
		return -1;

	xfer->pi = open_pages_image(O_DUMP, xfer->pmi, &pages_id);
	if (!xfer->pi)
		goto err_pmi;

	/*
	 * Open page-read for parent images (if it exists). It will
	 * be used for two things:
	 * 1) when writing a page, those from parent will be dedup-ed
	 * 2) when writing a hole, the respective place would be checked
	 *    to exist in parent (either pagemap or hole)
	 */
	xfer->parent = NULL;
	if (fd_type == CR_FD_PAGEMAP || fd_type == CR_FD_SHMEM_PAGEMAP) {
		int ret;
		int pfd;
		int pr_flags = (fd_type == CR_FD_PAGEMAP) ? PR_TASK : PR_SHMEM;

		/* Image streaming lacks support for incremental images */
		if (opts.stream)
			goto out;

		if (open_parent(get_service_fd(IMG_FD_OFF), &pfd))
			goto err_pi;
		if (pfd < 0)
			goto out;

		xfer->parent = xmalloc(sizeof(*xfer->parent));
		if (!xfer->parent) {
			close(pfd);
			goto err_pi;
		}

		ret = open_page_read_at(pfd, img_id, xfer->parent, pr_flags);
		if (ret <= 0) {
			pr_perror("No parent image found, though parent directory is set");
			xfree(xfer->parent);
			xfer->parent = NULL;
			close(pfd);
			goto out;
		}
		close(pfd);
	}

out:
	xfer->write_pagemap = write_pagemap_loc;
	xfer->write_pages = write_pages_loc;
	xfer->close = close_page_xfer;
	return 0;

err_pi:
	close_image(xfer->pi);
err_pmi:
	close_image(xfer->pmi);
	return -1;
}

int open_page_xfer(struct page_xfer *xfer, int fd_type, unsigned long img_id)
{
	xfer->offset = 0;
	xfer->transfer_lazy = true;

	if (opts.use_page_server)
		return open_page_server_xfer(xfer, fd_type, img_id);
	else
		return open_page_local_xfer(xfer, fd_type, img_id);
}

static int page_xfer_dump_hole(struct page_xfer *xfer, struct iovec *hole, u32 flags)
{
	BUG_ON(hole->iov_base < (void *)xfer->offset);
	hole->iov_base -= xfer->offset;
	pr_debug("\th %p [%u]\n", hole->iov_base, (unsigned int)(hole->iov_len / PAGE_SIZE));

		pr_info("  Writing hole pagemap asaf: 0x%lx-0x%lx (%lu pages)\n",
						(unsigned long)hole->iov_base, (unsigned long)(hole->iov_base+hole->iov_len), (unsigned long)(hole->iov_len/PAGE_SIZE));
	if (xfer->write_pagemap(xfer, hole, flags))
		return -1;

	return 0;
}

static int get_hole_flags(struct page_pipe *pp, int n)
{
	unsigned int hole_flags = pp->hole_flags[n];

	if (hole_flags == PP_HOLE_PARENT)
		return PE_PARENT;
	else
		BUG();

	return -1;
}

static int dump_holes(struct page_xfer *xfer, struct page_pipe *pp, unsigned int *cur_hole, void *limit)
{
	int ret;

	for (; *cur_hole < pp->free_hole; (*cur_hole)++) {
		struct iovec hole = pp->holes[*cur_hole];
		u32 hole_flags;

		if (limit && hole.iov_base >= limit)
			break;

		hole_flags = get_hole_flags(pp, *cur_hole);
		ret = page_xfer_dump_hole(xfer, &hole, hole_flags);
		if (ret)
			return ret;
	}

	return 0;
}

static inline u32 ppb_xfer_flags(struct page_xfer *xfer, struct page_pipe_buf *ppb)
{
	if (ppb->flags & PPB_LAZY)
		/*
		 * Pages that can be lazily restored are always marked as such.
		 * In the case we actually transfer them into image mark them
		 * as present as well.
		 */
		return (xfer->transfer_lazy ? PE_PRESENT : 0) | PE_LAZY;
	else
		return PE_PRESENT;
}

/*
 * Optimized pre-dump algorithm
 * ==============================
 *
 * Note: Please refer man(2) page of process_vm_readv syscall.
 *
 * The following discussion covers the possibly faulty-iov
 * locations in an iovec, which hinders process_vm_readv from
 * dumping the entire iovec in a single invocation.
 *
 * Memory layout of target process:
 *
 * Pages: A        B        C
 *	  +--------+--------+--------+--------+--------+--------+
 *	  |||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *	  +--------+--------+--------+--------+--------+--------+
 *
 * Single "iov" representation: {starting_address, length_in_bytes}
 * An iovec is array of iov-s.
 *
 * NOTE: For easy representation and discussion purpose, we carry
 *	 out further discussion at "page granularity".
 *	 length_in_bytes will represent page count in iov instead
 *	 of byte count. Same assumption applies for the syscall's
 *	 return value. Instead of returning the number of bytes
 *	 read, it returns a page count.
 *
 * For above memory mapping, generated iovec: {A,1}{B,1}{C,4}
 *
 * This iovec remains unmodified once generated. At the same
 * time some of memory regions listed in iovec may get modified
 * (unmap/change protection) by the target process while syscall
 * is trying to dump iovec regions.
 *
 * Case 1:
 *	A is unmapped, {A,1} become faulty iov
 *
 *      A        B        C
 *      +--------+--------+--------+--------+--------+--------+
 *      |        ||||||||||||||||||||||||||||||||||||||||||||||
 *      +--------+--------+--------+--------+--------+--------+
 *      ^        ^
 *      |        |
 *      start    |
 *      (1)      |
 *               start
 *               (2)
 *
 *	process_vm_readv will return -1. Increment start pointer(2),
 *	syscall will process {B,1}{C,4} in one go and copy 5 pages
 *	to userbuf from iov-B and iov-C.
 *
 * Case 2:
 *	B is unmapped, {B,1} become faulty iov
 *
 *      A        B        C
 *      +--------+--------+--------+--------+--------+--------+
 *      |||||||||         |||||||||||||||||||||||||||||||||||||
 *      +--------+--------+--------+--------+--------+--------+
 *      ^                 ^
 *      |                 |
 *      start             |
 *      (1)               |
 *                        start
 *                        (2)
 *
 *	process_vm_readv will return 1, i.e. page A copied to
 *	userbuf successfully and syscall stopped, since B got
 *	unmapped.
 *
 *	Increment the start pointer to C(2) and invoke syscall.
 *	Userbuf contains 5 pages overall from iov-A and iov-C.
 *
 * Case 3:
 *	This case deals with partial unmapping of iov representing
 *	more than one pagesize region.
 *
 *	Syscall can't process such faulty iov as whole. So we
 *	process such regions part-by-part and form new sub-iovs
 *	in aux_iov from successfully processed pages.
 *
 *
 *	Part 3.1:
 *		First page of C is unmapped
 *
 *      A        B        C
 *      +--------+--------+--------+--------+--------+--------+
 *      ||||||||||||||||||         ||||||||||||||||||||||||||||
 *      +--------+--------+--------+--------+--------+--------+
 *      ^                          ^
 *      |                          |
 *      start                      |
 *      (1)                        |
 *                                 dummy
 *                                 (2)
 *
 *	process_vm_readv will return 2, i.e. pages A and B copied.
 *	We identify length of iov-C is more than 1 page, that is
 *	where this case differs from Case 2.
 *
 *	dummy-iov is introduced(2) as: {C+1,3}. dummy-iov can be
 *	directly placed at next page to failing page. This will copy
 *	remaining 3 pages from iov-C to userbuf. Finally create
 *	modified iov entry in aux_iov. Complete aux_iov look like:
 *
 *	aux_iov: {A,1}{B,1}{C+1,3}*
 *
 *
 *	Part 3.2:
 *		In between page of C is unmapped, let's say third
 *
 *      A        B        C
 *      +--------+--------+--------+--------+--------+--------+
 *      ||||||||||||||||||||||||||||||||||||         ||||||||||
 *      +--------+--------+--------+--------+--------+--------+
 *      ^                                            ^
 *      |                 |-----------------|        |
 *      start              partial_read_bytes        |
 *      (1)                                          |
 *                                                   dummy
 *                                                   (2)
 *
 *	process_vm_readv will return 4, i.e. pages A and B copied
 *	completely and first two pages of C are also copied.
 *
 *	Since, iov-C is not processed completely, we need to find
 *	"partial_read_byte" count to place out dummy-iov for
 *	remaining processing of iov-C. This function is performed by
 *	analyze_iov function.
 *
 *	dummy-iov will be(2): {C+3,1}. dummy-iov will be placed
 *	next to first failing address to process remaining iov-C.
 *	New entries in aux_iov will look like:
 *
 *	aux_iov: {A,1}{B,1}{C,2}*{C+3,1}*
 */

unsigned long handle_faulty_iov(int pid, struct iovec *riov, unsigned long faulty_index, struct iovec *bufvec,
				struct iovec *aux_iov, unsigned long *aux_len)
{
	struct iovec dummy;
	ssize_t bytes_read;
	unsigned long final_read_cnt = 0;

	/* Handling Case 3-Part 3.2*/
	dummy.iov_base = riov[faulty_index].iov_base;
	dummy.iov_len = riov[faulty_index].iov_len;

	while (dummy.iov_len) {
		bytes_read = process_vm_readv(pid, bufvec, 1, &dummy, 1, 0);
		if (bytes_read == -1) {
			/* Handling faulty page read in faulty iov */
			cnt_sub(CNT_PAGES_WRITTEN, 1);
			dummy.iov_base += PAGE_SIZE;
			dummy.iov_len -= PAGE_SIZE;
			continue;
		}

		/* If aux-iov can merge and expand or new entry required */
		if (aux_iov[(*aux_len) - 1].iov_base + aux_iov[(*aux_len) - 1].iov_len == dummy.iov_base)
			aux_iov[(*aux_len) - 1].iov_len += bytes_read;
		else {
			aux_iov[*aux_len].iov_base = dummy.iov_base;
			aux_iov[*aux_len].iov_len = bytes_read;
			(*aux_len) += 1;
		}

		dummy.iov_base += bytes_read;
		dummy.iov_len -= bytes_read;
		bufvec->iov_base += bytes_read;
		bufvec->iov_len -= bytes_read;
		final_read_cnt += bytes_read;
	}

	return final_read_cnt;
}

/*
 * This function will position start pointer to the latest
 * successfully read iov in iovec.
 */
static unsigned long analyze_iov(ssize_t bytes_read, struct iovec *riov, unsigned long *index, struct iovec *aux_iov,
				 unsigned long *aux_len)
{
	ssize_t processed_bytes = 0;

	/* correlating iovs with read bytes */
	while (processed_bytes < bytes_read) {
		processed_bytes += riov[*index].iov_len;
		aux_iov[*aux_len].iov_base = riov[*index].iov_base;
		aux_iov[*aux_len].iov_len = riov[*index].iov_len;

		(*aux_len) += 1;
		(*index) += 1;
	}

	/* handling partially processed faulty iov*/
	if (processed_bytes - bytes_read) {
		unsigned long partial_read_bytes = 0;

		(*index) -= 1;

		partial_read_bytes = riov[*index].iov_len - (processed_bytes - bytes_read);
		aux_iov[*aux_len - 1].iov_len = partial_read_bytes;
		riov[*index].iov_base += partial_read_bytes;
		riov[*index].iov_len -= partial_read_bytes;
	}

	return 0;
}

/*
 * This function iterates over complete ppb->iov entries and pass
 * them to process_vm_readv syscall.
 *
 * Since process_vm_readv returns count of successfully read bytes.
 * It does not point to iovec entry associated to last successful
 * byte read. The correlation between bytes read and corresponding
 * iovec is setup through analyze_iov function.
 *
 * If all iovecs are not processed in one go, it means there exists
 * some faulty iov entry(memory mapping modified after it was grabbed)
 * in iovec. process_vm_readv syscall stops at such faulty iov and
 * skip processing further any entry in iovec. This is handled by
 * handle_faulty_iov function.
 */
static long fill_userbuf(int pid, struct page_pipe_buf *ppb, struct iovec *bufvec, struct iovec *aux_iov,
			 unsigned long *aux_len)
{
	struct iovec *riov = ppb->iov;
	ssize_t bytes_read;
	unsigned long total_read = 0;
	unsigned long start = 0;

	while (start < ppb->nr_segs) {
		bytes_read = process_vm_readv(pid, bufvec, 1, &riov[start], ppb->nr_segs - start, 0);
		if (bytes_read == -1) {
			if (errno == ESRCH) {
				pr_debug("Target process PID:%d not found\n", pid);
				return -ESRCH;
			}
			if (errno != EFAULT) {
				pr_perror("process_vm_readv failed");
				return -1;
			}
			/* Handling Case 1*/
			if (riov[start].iov_len == PAGE_SIZE) {
				cnt_sub(CNT_PAGES_WRITTEN, 1);
				start += 1;
				continue;
			}
			total_read += handle_faulty_iov(pid, riov, start, bufvec, aux_iov, aux_len);
			start += 1;
			continue;
		}

		if (bytes_read > 0) {
			if (analyze_iov(bytes_read, riov, &start, aux_iov, aux_len) < 0)
				return -1;
			bufvec->iov_base += bytes_read;
			bufvec->iov_len -= bytes_read;
			total_read += bytes_read;
		}
	}

	return total_read;
}

/*
 * This function is similar to page_xfer_dump_pages, instead it uses
 * auxiliary_iov array for pagemap generation.
 *
 * The entries of ppb->iov may mismatch with actual process mappings
 * present at time of pre-dump. Such entries need to be adjusted as per
 * the pages read by process_vm_readv syscall. These adjusted entries
 * along with unmodified entries are present in aux_iov array.
 */

int page_xfer_predump_pages(int pid, struct page_xfer *xfer, struct page_pipe *pp)
{
	struct page_pipe_buf *ppb;
	unsigned int cur_hole = 0, i;
	unsigned long ret, bytes_read;
	unsigned long userbuf_len;
	struct iovec bufvec;

	struct iovec *aux_iov;
	unsigned long aux_len;
	void *userbuf;

	userbuf_len = PIPE_MAX_BUFFER_SIZE;
	userbuf = mmap(NULL, userbuf_len, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
	if (userbuf == MAP_FAILED) {
		pr_perror("Unable to mmap a buffer");
		return -1;
	}
	aux_iov = xmalloc(userbuf_len / PAGE_SIZE * sizeof(aux_iov[0]));
	if (!aux_iov)
		goto err;

	list_for_each_entry(ppb, &pp->bufs, l) {
		if (ppb->pipe_size * PAGE_SIZE > userbuf_len) {
			void *addr;

			addr = mremap(userbuf, userbuf_len, ppb->pipe_size * PAGE_SIZE, MREMAP_MAYMOVE);
			if (addr == MAP_FAILED) {
				pr_perror("Unable to mmap a buffer");
				goto err;
			}
			userbuf_len = ppb->pipe_size * PAGE_SIZE;
			userbuf = addr;
			addr = xrealloc(aux_iov, ppb->pipe_size * sizeof(aux_iov[0]));
			if (!addr)
				goto err;
			aux_iov = addr;
		}
		timing_start(TIME_MEMDUMP);

		aux_len = 0;
		bufvec.iov_len = userbuf_len;
		bufvec.iov_base = userbuf;

		bytes_read = fill_userbuf(pid, ppb, &bufvec, aux_iov, &aux_len);
		if (bytes_read == -ESRCH) {
			timing_stop(TIME_MEMDUMP);
			munmap(userbuf, userbuf_len);
			xfree(aux_iov);
			return 0;
		}
		if (bytes_read < 0)
			goto err;

		bufvec.iov_base = userbuf;
		bufvec.iov_len = bytes_read;
		ret = vmsplice(ppb->p[1], &bufvec, 1, SPLICE_F_NONBLOCK | SPLICE_F_GIFT);

		if (ret == -1 || ret != bytes_read) {
			pr_err("vmsplice: Failed to splice user buffer to pipe %ld\n", ret);
			goto err;
		}

		timing_stop(TIME_MEMDUMP);
		timing_start(TIME_MEMWRITE);

		/* generating pagemap */
		for (i = 0; i < aux_len; i++) {
			struct iovec iov = aux_iov[i];
			u32 flags;

			ret = dump_holes(xfer, pp, &cur_hole, iov.iov_base);
			if (ret)
				goto err;

			BUG_ON(iov.iov_base < (void *)xfer->offset);
			iov.iov_base -= xfer->offset;
			pr_debug("\t p %p - %p\n", iov.iov_base, iov.iov_base + iov.iov_len);

			flags = ppb_xfer_flags(xfer, ppb);

			if (xfer->write_pagemap(xfer, &iov, flags))
				goto err;

			if (xfer->write_pages(xfer, ppb->p[0], iov.iov_len))
				goto err;
		}

		timing_stop(TIME_MEMWRITE);
	}

	munmap(userbuf, userbuf_len);
	xfree(aux_iov);
	timing_start(TIME_MEMWRITE);

	return dump_holes(xfer, pp, &cur_hole, NULL);
err:
	munmap(userbuf, userbuf_len);
	xfree(aux_iov);
	return -1;
}

/* Helper to write lazy VMA pagemap entries that come before a given vaddr */
static int write_lazy_vmas_before(struct page_xfer *xfer, unsigned long before_vaddr,
				   struct lazy_vma_entry **cur_lve)
{
	struct list_head *global_list = get_global_lazy_vmas();
	struct lazy_vma_entry *lve = *cur_lve;

	/* Start from beginning if not set */
	if (!lve && !list_empty(global_list))
		lve = list_first_entry(global_list, struct lazy_vma_entry, list);

	/* Write all lazy VMAs that start before before_vaddr.
	 * Use lve->start/end (not lve->vma->e) since vma structs
	 * may be freed after the dump completes.
	 */
	while (lve && &lve->list != global_list) {
		struct iovec iov;
		u32 flags = PE_LAZY;

		if (lve->start >= before_vaddr)
			break;

		iov.iov_base = (void *)(unsigned long)lve->start;
		iov.iov_len = lve->end - lve->start;

		BUG_ON(iov.iov_base < (void *)xfer->offset);
		iov.iov_base -= xfer->offset;

		pr_debug("Writing lazy VMA pagemap: 0x%lx-0x%lx (%lu pages)\n",
			(unsigned long)lve->start, (unsigned long)lve->end,
			(unsigned long)(iov.iov_len / PAGE_SIZE));

		if (xfer->write_pagemap(xfer, &iov, flags)) {
			pr_err("Failed to write pagemap for lazy VMA\n");
			return -1;
		}

		lve = list_entry(lve->list.next, struct lazy_vma_entry, list);
	}

	*cur_lve = lve;
	return 0;
}

int page_xfer_dump_pages(struct page_xfer *xfer, struct page_pipe *pp)
{
	struct page_pipe_buf *ppb;
	unsigned int cur_hole = 0;
	struct lazy_vma_entry *cur_lve = NULL;
	int ret;

	pr_debug("Transferring pages:\n");
	

	if (opts.cow_dump)
		pr_info("pagemap: interleaved mode for dst_id=%lu\n",
			(unsigned long)xfer->dst_id);

	list_for_each_entry(ppb, &pp->bufs, l) {
		unsigned int i;

		pr_debug("\tbuf %lx/%d\n", ppb->pages_in, ppb->nr_segs);

		for (i = 0; i < ppb->nr_segs; i++) {
			struct iovec iov = ppb->iov[i];
			u32 flags;
			unsigned long seg_vaddr = (unsigned long)iov.iov_base + xfer->offset;
			

			ret = dump_holes(xfer, pp, &cur_hole, iov.iov_base);
			if (ret)
				return ret;

			/* Write any lazy VMAs that should come before this segment */
			if (opts.cow_dump) {
				ret = write_lazy_vmas_before(xfer, seg_vaddr, &cur_lve);
				if (ret)
					return ret;
			}

			BUG_ON(iov.iov_base < (void *)xfer->offset);
			iov.iov_base -= xfer->offset;
			pr_debug("\tp %p - %p\n", iov.iov_base, iov.iov_base + iov.iov_len);

			flags = ppb_xfer_flags(xfer, ppb);
			

			pr_debug("Writing pagemap segment: 0x%lx-0x%lx (%lu pages)\n",
				 (unsigned long)iov.iov_base, (unsigned long)(iov.iov_base + iov.iov_len),
				 (unsigned long)(iov.iov_len / PAGE_SIZE));

			if (xfer->write_pagemap(xfer, &iov, flags))
				return -1;
			if ((flags & PE_PRESENT) && xfer->write_pages(xfer, ppb->p[0], iov.iov_len))
				return -1;
			

		}
	}
	

	ret = dump_holes(xfer, pp, &cur_hole, NULL);
	if (ret)
		return ret;

	/* Write any remaining lazy VMAs after all pipe entries */
	if (opts.cow_dump) {
		ret = write_lazy_vmas_before(xfer, ULONG_MAX, &cur_lve);
		if (ret)
			return ret;
	}

	return 0;
}

/*
 * Return:
 *	 1 - if a parent image exists
 *	 0 - if a parent image doesn't exist
 *	-1 - in error cases
 */
int check_parent_local_xfer(int fd_type, unsigned long img_id)
{
	char path[PATH_MAX];
	struct stat st;
	int ret, pfd;

	/* Image streaming lacks support for incremental images */
	if (opts.stream)
		return 0;

	if (open_parent(get_service_fd(IMG_FD_OFF), &pfd))
		return -1;
	if (pfd < 0)
		return 0;

	snprintf(path, sizeof(path), imgset_template[fd_type].fmt, img_id);
	ret = fstatat(pfd, path, &st, 0);
	if (ret == -1 && errno != ENOENT) {
		pr_perror("Unable to stat %s", path);
		close(pfd);
		return -1;
	}

	close(pfd);
	return (ret == 0);
}

/* page server */
static int page_server_check_parent(int sk, struct page_server_iov *pi)
{
	int type, ret;
	unsigned long id;

	type = decode_pm(pi->dst_id, &id);
	if (type == -1) {
		pr_err("Unknown pagemap type received\n");
		return -1;
	}

	ret = check_parent_local_xfer(type, id);
	if (ret < 0)
		return -1;

	if (__send(sk, &ret, sizeof(ret), 0) != sizeof(ret)) {
		pr_perror("Unable to send response");
		return -1;
	}

	return 0;
}

static int check_parent_server_xfer(int fd_type, unsigned long img_id)
{
	struct page_server_iov pi = {};
	int has_parent;

	pi.cmd = PS_IOV_PARENT;
	pi.dst_id = encode_pm(fd_type, img_id);

	if (send_psi(page_server_sk, &pi))
		return -1;

	tcp_nodelay(page_server_sk, true);

	if (__recv(page_server_sk, &has_parent, sizeof(int), 0) != sizeof(int)) {
		pr_perror("The page server doesn't answer");
		return -1;
	}

	return has_parent;
}

int check_parent_page_xfer(int fd_type, unsigned long img_id)
{
	if (opts.use_page_server)
		return check_parent_server_xfer(fd_type, img_id);
	else
		return check_parent_local_xfer(fd_type, img_id);
}

struct page_xfer_job {
	u64 dst_id;
	int p[2];
	unsigned pipe_size;
	struct page_xfer loc_xfer;
};

static struct page_xfer_job cxfer = {
	.dst_id = ~0,
};

static struct pipe_read_dest pipe_read_dest = {
	.sink_fd = -1,
};

static void page_server_close(void)
{
	if (cxfer.dst_id != ~0)
		cxfer.loc_xfer.close(&cxfer.loc_xfer);
	if (pipe_read_dest.sink_fd != -1) {
		close(pipe_read_dest.sink_fd);
		close(pipe_read_dest.p[0]);
		close(pipe_read_dest.p[1]);
	}
}

static int page_server_open(int sk, struct page_server_iov *pi)
{
	int type;
	unsigned long id;

	type = decode_pm(pi->dst_id, &id);
	if (type == -1) {
		pr_err("Unknown pagemap type received\n");
		return -1;
	}

	pr_info("Opening %d/%lu\n", type, id);

	page_server_close();

	if (open_page_local_xfer(&cxfer.loc_xfer, type, id))
		return -1;

	cxfer.dst_id = pi->dst_id;

	if (sk >= 0) {
		char has_parent = !!cxfer.loc_xfer.parent;
		if (__send(sk, &has_parent, 1, 0) != 1) {
			pr_perror("Unable to send response");
			close_page_xfer(&cxfer.loc_xfer);
			return -1;
		}
	}

	return 0;
}

static int prep_loc_xfer(struct page_server_iov *pi)
{
	if (cxfer.dst_id != pi->dst_id) {
		pr_warn("Deprecated IO w/o open\n");
		return page_server_open(-1, pi);
	} else
		return 0;
}

/* Statistics tracking structure */
static struct {
	/* page_server_get_pages counters */
	unsigned long get_total_requests;
	unsigned long get_with_cow;
	unsigned long get_no_cow;
	unsigned long get_total_pages;
	unsigned long get_cow_pages;
	unsigned long get_errors;
	
	/* page_server_serve counters */
	unsigned long serve_open;
	unsigned long serve_open2;
	unsigned long serve_parent;
	unsigned long serve_add_f;
	unsigned long serve_add;
	unsigned long serve_hole;
	unsigned long serve_close;
	unsigned long serve_force_close;
	unsigned long serve_get;
	unsigned long serve_unknown;
	
	time_t last_print_time;
} ps_stats;

static void check_and_print_stats(void)
{
	time_t now = time(NULL);
	
	if (now - ps_stats.last_print_time >= 1) {
		pr_debug("[PAGE_SERVER_STATS] get_pages: reqs=%lu with_cow=%lu no_cow=%lu pages=%lu cow=%lu errs=%lu | serve: open2=%lu parent=%lu add_f=%lu get=%lu close=%lu\n",
			ps_stats.get_total_requests,
			ps_stats.get_with_cow,
			ps_stats.get_no_cow,
			ps_stats.get_total_pages,
			ps_stats.get_cow_pages,
			ps_stats.get_errors,
			ps_stats.serve_open2,
			ps_stats.serve_parent,
			ps_stats.serve_add_f,
			ps_stats.serve_get,
			ps_stats.serve_close + ps_stats.serve_force_close);
		
		/* Reset all counters */
		memset(&ps_stats, 0, sizeof(ps_stats));
		ps_stats.last_print_time = now;
	}
}

static int page_server_add(int sk, struct page_server_iov *pi, u32 flags, bool compressed)
{
	size_t len;
	struct page_xfer *lxfer = &cxfer.loc_xfer;
	struct iovec iov;

	pr_debug("Adding %" PRIx64 " - %" PRIx64 " (compressed=%d)\n",
		 pi->vaddr, pi->vaddr + pi->nr_pages * PAGE_SIZE, compressed);

	if (prep_loc_xfer(pi))
		return -1;

	psi2iovec(pi, &iov);
	if (lxfer->write_pagemap(lxfer, &iov, flags))
		return -1;

	if (!(flags & PE_PRESENT))
		return 0;

	/* Handle compressed data - receive, decompress, write page by page */
	if (compressed) {
		unsigned long pages_left = pi->nr_pages;
		
		while (pages_left > 0) {
			int compressed_size;
			char compressed_buf[LZ4_compressBound(PAGE_SIZE)];
			char decompressed[PAGE_SIZE];
			int decomp_ret;

			/* Receive compressed size */
			if (__recv(sk, &compressed_size, sizeof(compressed_size), MSG_WAITALL) != sizeof(compressed_size)) {
				pr_perror("Failed to receive compressed size");
				return -1;
			}

			if (compressed_size <= 0 || compressed_size > LZ4_compressBound(PAGE_SIZE)) {
				pr_err("Invalid compressed size: %d\n", compressed_size);
				return -1;
			}

			/* Receive compressed data */
			if (__recv(sk, compressed_buf, compressed_size, MSG_WAITALL) != compressed_size) {
				pr_perror("Failed to receive compressed data");
				return -1;
			}

			/* Decompress */
			decomp_ret = LZ4_decompress_safe(compressed_buf, decompressed, compressed_size, PAGE_SIZE);
			if (decomp_ret != PAGE_SIZE) {
				pr_err("LZ4 decompression failed: expected %lu, got %d\n", PAGE_SIZE, decomp_ret);
				return -1;
			}

			pr_debug("Decompressed page: %d -> %lu bytes\n", compressed_size, PAGE_SIZE);

			/* Write decompressed page data to pipe and then to image */
			if (write(cxfer.p[1], decompressed, PAGE_SIZE) != PAGE_SIZE) {
				pr_perror("Failed to write decompressed page to pipe");
				return -1;
			}

			if (lxfer->write_pages(lxfer, cxfer.p[0], PAGE_SIZE))
				return -1;

			pages_left--;
		}
		return 0;
	}

	/* Handle uncompressed data - original splice-based path */
	len = iov.iov_len;
	while (len > 0) {
		ssize_t chunk;

		chunk = len;
		if (chunk > cxfer.pipe_size)
			chunk = cxfer.pipe_size;

		/*
		 * Splicing into a pipe may end up blocking if pipe is "full",
		 * and we need the SPLICE_F_NONBLOCK flag here. At the same time
		 * splicing from UNIX socket with this flag aborts splice with
		 * the EAGAIN if there's no data in it (TCP looks at the socket
		 * O_NONBLOCK flag _only_ and waits for data), so before doing
		 * the non-blocking splice we need to explicitly wait.
		 */

		if (sk_wait_data(sk) < 0) {
			pr_perror("Can't poll socket");
			return -1;
		}

		if (opts.tls) {
			if (tls_recv_data_to_fd(cxfer.p[1], chunk)) {
				pr_err("Can't read from socket\n");
				return -1;
			}
		} else {
			chunk = splice(sk, NULL, cxfer.p[1], NULL, chunk, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);

			if (chunk < 0) {
				pr_perror("Can't read from socket");
				return -1;
			}
			if (chunk == 0) {
				pr_err("A socket was closed unexpectedly\n");
				return -1;
			}
		}

		if (lxfer->write_pages(lxfer, cxfer.p[0], chunk))
			return -1;

		len -= chunk;
	}

	return 0;
}

/* Chunk size for batch transfer: 4KB = 1 page (to avoid COW race conditions) */
#define BATCH_CHUNK_SIZE (1)

/* Page request queue for PS_IOV_GET requests */
struct page_request_entry {
	unsigned long vaddr;
	unsigned long nr_pages;
	int sk;
	u64 dst_id;
	
	/* Location info (filled on first access) */
	struct page_pipe_buf *ppb;
	unsigned int seg_idx;
	unsigned long page_idx_in_seg;
	bool location_found;  /* Flag: have we looked up location yet? */
	
	struct list_head list;
};

static LIST_HEAD(page_request_queue);
static pthread_spinlock_t page_request_lock;
static bool page_request_lock_initialized = false;

static void init_page_request_queue(void)
{
	if (!page_request_lock_initialized) {
		pthread_spin_init(&page_request_lock, PTHREAD_PROCESS_PRIVATE);
		page_request_lock_initialized = true;
	}
}

static void add_page_request(unsigned long vaddr, unsigned long nr_pages, int sk, u64 dst_id)
{
	struct page_request_entry *entry = xmalloc(sizeof(*entry));

	if (!entry) {
		pr_err("Failed to allocate page request entry\n");
		return;
	}

	entry->vaddr = vaddr;
	entry->nr_pages = nr_pages;
	entry->sk = sk;
	entry->dst_id = dst_id;
	
	pr_debug("Requesting page at %lx (nr_pages=%lu, dst_id=%lu)\n", vaddr, nr_pages, dst_id);
	
	/* Location will be looked up on first access */
	entry->ppb = NULL;
	entry->seg_idx = 0;
	entry->page_idx_in_seg = 0;
	entry->location_found = false;
	
	INIT_LIST_HEAD(&entry->list);

	pthread_spin_lock(&page_request_lock);
	list_add_tail(&entry->list, &page_request_queue);
	pthread_spin_unlock(&page_request_lock);

}

static struct page_request_entry *get_next_page_request(void)
{
	struct page_request_entry *entry = NULL;

	pthread_spin_lock(&page_request_lock);
	if (!list_empty(&page_request_queue)) {
		entry = list_first_entry(&page_request_queue, struct page_request_entry, list);
		list_del(&entry->list);
	}
	pthread_spin_unlock(&page_request_lock);

	return entry;
}

static bool has_page_requests(void)
{
	bool has_requests;

	pthread_spin_lock(&page_request_lock);
	has_requests = !list_empty(&page_request_queue);
	pthread_spin_unlock(&page_request_lock);

	return has_requests;
}

static unsigned long get_page_request_queue_size(void)
{
	unsigned long count = 0;
	struct page_request_entry *entry;

	pthread_spin_lock(&page_request_lock);
	list_for_each_entry(entry, &page_request_queue, list) {
		count++;
	}
	pthread_spin_unlock(&page_request_lock);

	return count;
}

struct active_image {
	u64 dst_id;
	int main_sk;
	unsigned long total_pages;
	unsigned long remaining_pages;
	unsigned long total_cow_pages;
	unsigned long total_req_pages;
	
	struct list_head list;
};

static LIST_HEAD(active_images_queue);
static pthread_spinlock_t active_images_lock;
static bool active_images_lock_initialized = false;

/* Single global background thread */
static pthread_t g_unified_thread;
static volatile bool g_unified_thread_running = false;
static volatile bool g_unified_thread_stop = false;

void wait_for_page_server_thread(void)
{
	if (!g_unified_thread_running)
		return;
	pr_info("Waiting for page server thread to finish...\n");
	pthread_join(g_unified_thread, NULL);
	g_unified_thread_running = false;
	pr_info("Page server thread finished\n");
}

/* Active image tracking for unified background thread */


static void init_active_images_queue(void)
{
	if (!active_images_lock_initialized) {
		pthread_spin_init(&active_images_lock, PTHREAD_PROCESS_PRIVATE);
		active_images_lock_initialized = true;
	}
}

static struct active_image *find_active_image(u64 dst_id)
{
	struct active_image *img;
	
	/* Caller must hold lock */
	list_for_each_entry(img, &active_images_queue, list) {
		if (img->dst_id == dst_id)
			return img;
	}
	return NULL;
}

static int add_active_image(u64 dst_id, int sk)
{
	struct active_image *img;
	unsigned long total_pages;
	
	pthread_spin_lock(&active_images_lock);
	
	/* Check if already active */
	if (find_active_image(dst_id)) {
		pthread_spin_unlock(&active_images_lock);
		pr_info("Image dst_id=%lu already active\n", dst_id);
		return 0;
	}
	
	pthread_spin_unlock(&active_images_lock);
	
	/* Count total pages in lazy VMAs for this dst_id (uses global list) */
	pr_info("=== Scanning lazy VMAs for dst_id=%lu ===\n", dst_id);
	total_pages = count_lazy_vma_pages(dst_id);
	pr_info("=== Total lazy VMA pages: %lu ===\n", total_pages);
	
	if (total_pages == 0) {
		pr_warn("Image dst_id=%lu has no lazy VMA pages\n", dst_id);
		return 0;  /* Nothing to send */
	}
	
	/* Create active image entry */
	img = xzalloc(sizeof(*img));
	if (!img) {
		pr_err("Failed to allocate active image\n");
		return -1;
	}
	
	img->dst_id = dst_id;
	img->main_sk = sk;
	img->total_pages = total_pages;
	img->remaining_pages = total_pages;
	img->total_cow_pages = 0;
	img->total_req_pages = 0;
	
	INIT_LIST_HEAD(&img->list);
	
	pthread_spin_lock(&active_images_lock);
	list_add_tail(&img->list, &active_images_queue);
	pthread_spin_unlock(&active_images_lock);
	
	pr_err("Added active image dst_id=%lu with %lu lazy VMA pages\n", 
		dst_id, total_pages);
	return 0;
}
/* Timing statistics for COW page flow (accumulated in nanoseconds, printed once/sec) */
static struct {
	unsigned long vma_lookup_total_ns;
	unsigned long vma_lookup_count;
	unsigned long send_page_total_ns;
	unsigned long send_page_count;
	unsigned long queue_dequeue_total_ns;
	unsigned long queue_dequeue_count;
	/* Sub-timing within send_lazy_vma_page (nanoseconds) */
	unsigned long send_lock_ns;
	unsigned long send_cow_lookup_ns;
	unsigned long send_vm_readv_ns;
	unsigned long send_compress_ns;
	unsigned long send_socket_ns;
	unsigned long send_unprotect_ns;
	unsigned long send_unlock_ns;
	unsigned long send_sub_count;
} cow_timing;

/* Helper to send a lazy VMA page using process_vm_readv */
static int send_lazy_vma_page(int sk, unsigned long vaddr, u64 dst_id,
			      pid_t source_pid)
{
	struct cow_page *cow_pg = NULL;
	const void *data;
	pthread_spinlock_t *lock = NULL;
	char buffer[PAGE_SIZE];
	int ret;
	int uffd;
	struct iovec local_iov, remote_iov;
	struct timespec t_start, t_lock, t_cow, t_readv, t_socket, t_unprot,
			t_end;

	pr_debug("[SEND_PAGE] Entering send_lazy_vma_page: vaddr=0x%lx dst_id=%lu pid=%d\n",
		 vaddr, (unsigned long)dst_id, source_pid);

	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* Sync mode: check COW hash for dump-time snapshot */
	if (!cow_is_wp_async()) {
		lock = cow_get_hash_lock(vaddr);
		if (lock)
			pthread_spin_lock(lock);
		cow_pg = lock ? cow_lookup_page(vaddr) : NULL;
		if (lock)
			pthread_spin_unlock(lock);
	}
	clock_gettime(CLOCK_MONOTONIC, &t_lock);
	clock_gettime(CLOCK_MONOTONIC, &t_cow);

	if (cow_pg) {
		pr_debug("[SEND_PAGE] Sending COW page at vaddr=0x%lx\n", vaddr);

		data = cow_pg->data;
		t_readv = t_cow;
	} else {
		int saved_errno;

		pr_debug("[SEND_PAGE] Reading regular page at vaddr=0x%lx pid=%d\n",
			 vaddr, source_pid);

		local_iov.iov_base = buffer;
		local_iov.iov_len = PAGE_SIZE;
		remote_iov.iov_base = (void *)vaddr;
		remote_iov.iov_len = PAGE_SIZE;

		ret = process_vm_readv(source_pid, &local_iov, 1, &remote_iov, 1,
				       0);
		saved_errno = errno;
		clock_gettime(CLOCK_MONOTONIC, &t_readv);

		if (ret != PAGE_SIZE) {
			if (ret < 0 && saved_errno == EFAULT) {
				/*
				 * The source can unmap/shrink VMAs while we're
				 * copying in --leave-running mode. Treat an
				 * unmapped page as a zero page and keep going,
				 * otherwise a tiny allocator trim can abort
				 * the whole bulk transfer.
				 */
				memset(buffer, 0, PAGE_SIZE);
			} else {
				errno = saved_errno;
				pr_perror("Failed to read page at %lx from pid %d",
					  vaddr, source_pid);
				return -1;
			}
		}

		/*
		 * Sync mode: re-check COW hash after the read.
		 * A concurrent write-fault handler may have captured
		 * the pre-write snapshot while we were racing.
		 */
		if (!cow_is_wp_async()) {
			lock = cow_get_hash_lock(vaddr);
			if (lock)
				pthread_spin_lock(lock);
			cow_pg = lock ? cow_lookup_page(vaddr) : NULL;
			if (lock)
				pthread_spin_unlock(lock);
		}

		data = cow_pg ? cow_pg->data : buffer;
	}

	ret = send_page_compressed(sk, data, dst_id, vaddr);
	clock_gettime(CLOCK_MONOTONIC, &t_socket);

	if (ret != 0) {
		pr_perror("Failed to send page");
		return -1;
	}

	if (cow_pg) {
		t_unprot = t_socket;

		/* Remove only after the send completes. */
		lock = cow_get_hash_lock(vaddr);
		if (lock) {
			pthread_spin_lock(lock);
			cow_remove_page(vaddr);
			pthread_spin_unlock(lock);
		}
	} else if (!cow_is_wp_async()) {
		/* Sync mode: clear WP and drop any racing snapshot */
		uffd = cow_get_uffd_for_pid(source_pid);
		if (uffd >= 0) {
			struct uffdio_writeprotect wp;

			wp.range.start = vaddr;
			wp.range.len = PAGE_SIZE;
			wp.mode = 0;
			if (ioctl(uffd, UFFDIO_WRITEPROTECT, &wp)) {
				pr_pwarn("Failed to unprotect page at 0x%lx",
					 vaddr);
			}
		}

		clock_gettime(CLOCK_MONOTONIC, &t_unprot);

		lock = cow_get_hash_lock(vaddr);
		if (lock) {
			pthread_spin_lock(lock);
			cow_remove_page(vaddr);
			pthread_spin_unlock(lock);
		}
	} else {
		clock_gettime(CLOCK_MONOTONIC, &t_unprot);
	}

	clock_gettime(CLOCK_MONOTONIC, &t_end);

	/* Accumulate sub-timings (nanoseconds) */
	cow_timing.send_lock_ns += (t_lock.tv_sec - t_start.tv_sec) *
				  1000000000UL +
				  (t_lock.tv_nsec - t_start.tv_nsec);
	cow_timing.send_cow_lookup_ns += (t_cow.tv_sec - t_lock.tv_sec) *
					1000000000UL +
					(t_cow.tv_nsec - t_lock.tv_nsec);
	cow_timing.send_vm_readv_ns += (t_readv.tv_sec - t_cow.tv_sec) *
				      1000000000UL +
				      (t_readv.tv_nsec - t_cow.tv_nsec);
	cow_timing.send_compress_ns += (t_socket.tv_sec - t_readv.tv_sec) *
				      1000000000UL +
				      (t_socket.tv_nsec - t_readv.tv_nsec);
	cow_timing.send_unprotect_ns += (t_unprot.tv_sec - t_socket.tv_sec) *
				       1000000000UL +
				       (t_unprot.tv_nsec - t_socket.tv_nsec);
	cow_timing.send_unlock_ns += (t_end.tv_sec - t_unprot.tv_sec) *
				    1000000000UL +
				    (t_end.tv_nsec - t_unprot.tv_nsec);
	cow_timing.send_sub_count++;

	return 0;
}




/* Helper to send a COW page from lazy VMA */
static int send_cow_page_lazy(struct cow_page_queue_entry *entry, struct active_image *img, pid_t source_pid)
{
	struct lazy_vma_entry *lve;
	unsigned long page_idx;
	int ret;
	struct timespec t1, t2;
	
	/* Time VMA lookup */
	clock_gettime(CLOCK_MONOTONIC, &t1);
	
	/* Find which lazy VMA contains this page (uses global list) */
	lve = find_lazy_vma_for_addr(entry->vaddr, img->dst_id);
	
	clock_gettime(CLOCK_MONOTONIC, &t2);
	cow_timing.vma_lookup_total_ns += (t2.tv_sec - t1.tv_sec) * 1000000000 + (t2.tv_nsec - t1.tv_nsec);
	cow_timing.vma_lookup_count++;
	
	if (!lve){
		pr_err("COW page 0x%lx not in any lazy VMA\n", entry->vaddr);
		return -1;
	}
	/* Calculate page index within VMA */
	page_idx = (entry->vaddr - lve->start) / PAGE_SIZE;
	
	/* Check if already sent */
	if (lve->sent_bitmap[page_idx / 8] & (1 << (page_idx % 8))) {
		pr_debug("COW page 0x%lx already sent\n", entry->vaddr);
		return 0;
	}
	
	/* Time page send */
	clock_gettime(CLOCK_MONOTONIC, &t1);
	
	/* Send the page */
	ret = send_lazy_vma_page(img->main_sk, entry->vaddr, img->dst_id, source_pid);
	
	clock_gettime(CLOCK_MONOTONIC, &t2);
	cow_timing.send_page_total_ns += (t2.tv_sec - t1.tv_sec) * 1000000000 + (t2.tv_nsec - t1.tv_nsec);
	cow_timing.send_page_count++;
	
	if (ret < 0)
		return -1;
	
	/* Mark as sent */
	lve->sent_bitmap[page_idx / 8] |= (1 << (page_idx % 8));
	
	return 1;  /* Successfully sent */
}

/* Helper to send a page request from lazy VMA */
static int send_request_page_lazy(struct page_request_entry *req, struct active_image *img, pid_t source_pid)
{
	unsigned long i;
	int ret;
	int sent_count = 0;
	
	/* Send multiple pages if requested */
	for (i = 0; i < req->nr_pages; i++) {
		unsigned long page_vaddr = req->vaddr + (i * PAGE_SIZE);
		struct lazy_vma_entry *lve;
		unsigned long page_idx;
		/* Find which lazy VMA contains this page (uses global list) */
		lve = find_lazy_vma_for_addr(page_vaddr, req->dst_id);
		if (!lve) {
			pr_err("Request page 0x%lx not in any lazy VMA\n", page_vaddr);
			return -1;
		}
		
		/* Calculate page index within VMA */
		page_idx = (page_vaddr - lve->start) / PAGE_SIZE;
		
		/* Check if already sent */
		if (lve->sent_bitmap[page_idx / 8] & (1 << (page_idx % 8))) {
			pr_debug("Request page 0x%lx already sent, skipping\n", page_vaddr);
			continue;
		}
		
		/* Send the page */
		ret = send_lazy_vma_page(req->sk, page_vaddr, req->dst_id, source_pid);
		if (ret < 0)
			return -1;
		
		/* Mark as sent */
		lve->sent_bitmap[page_idx / 8] |= (1 << (page_idx % 8));
		sent_count++;
	}
	
	return sent_count;  /* Return number of pages actually sent */
}


/* Thread statistics context */
struct unified_thread_stats {
	time_t last_print_time;
	unsigned long priority1_pages;  /* COW pages */
	unsigned long priority2_pages;  /* Request pages */
	unsigned long priority3_pages;  /* Regular pages */
	unsigned long priority3_skips;  /* Skipped pages in P3 */
};

static void print_thread_stats(struct unified_thread_stats *stats)
{
	unsigned long cow_queue = cow_get_queue_size();
	unsigned long req_queue = get_page_request_queue_size();
	float compress_ratio = 0.0;
	struct timespec ts;
	struct tm *tm;

	if (g_compress_uncompressed_bytes > 0)
		compress_ratio = (float)g_compress_compressed_bytes * 100.0 /
				 g_compress_uncompressed_bytes;

	clock_gettime(CLOCK_REALTIME, &ts);
	tm = localtime(&ts.tv_sec);

	pr_err("[UNIFIED_THREAD_STATS] [%02d:%02d:%02d.%03ld] P1(COW)=%lu P2(Req)=%lu P3(Reg)=%lu P3_Skips=%lu pages/sec | COW_Q=%lu Req_Q=%lu | Compress: %lu->%lu (%.1f%%)\n",
		tm->tm_hour, tm->tm_min, tm->tm_sec, ts.tv_nsec / 1000000,
		stats->priority1_pages, stats->priority2_pages,
		stats->priority3_pages, stats->priority3_skips,
		cow_queue, req_queue,
		g_compress_uncompressed_bytes, g_compress_compressed_bytes,
		compress_ratio);

	pr_debug("[COW_TIMING] Queue: %lu ns (%lu ops) | VMA_lookup: %lu ns (%lu ops) | Send: %lu ns (%lu ops)\n",
		cow_timing.queue_dequeue_total_ns, cow_timing.queue_dequeue_count,
		cow_timing.vma_lookup_total_ns, cow_timing.vma_lookup_count,
		cow_timing.send_page_total_ns, cow_timing.send_page_count);

	if (cow_timing.send_sub_count > 0) {
		pr_debug("[SEND_BREAKDOWN] lock=%lu readv=%lu compress+send=%lu unprot=%lu unlock=%lu ns (avg per %lu ops)\n",
			cow_timing.send_lock_ns / cow_timing.send_sub_count,
			cow_timing.send_vm_readv_ns / cow_timing.send_sub_count,
			cow_timing.send_compress_ns / cow_timing.send_sub_count,
			cow_timing.send_unprotect_ns / cow_timing.send_sub_count,
			cow_timing.send_unlock_ns / cow_timing.send_sub_count,
			cow_timing.send_sub_count);
	}

	/* Reset counters */
	g_compress_uncompressed_bytes = 0;
	g_compress_compressed_bytes = 0;
	memset(&cow_timing, 0, sizeof(cow_timing));
	stats->priority1_pages = 0;
	stats->priority2_pages = 0;
	stats->priority3_pages = 0;
	stats->priority3_skips = 0;
}

static void maybe_print_stats(struct unified_thread_stats *stats)
{
	time_t now = time(NULL);

	if (now - stats->last_print_time >= 1) {
		print_thread_stats(stats);
		stats->last_print_time = now;
	}
}

/*
 * Drain pending COW pages (Priority 1)
 * Returns: number of pages sent, or -1 on error
 */
static int drain_cow_pages(struct active_image *img, pid_t source_pid,
			   int max_pages, struct unified_thread_stats *stats)
{
	int sent = 0;

	while (max_pages > 0 && cow_has_pending_pages() && img->remaining_pages > 0) {
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
		xfree(entry);

		if (ret < 0) {
			pr_err("Failed to send COW page\n");
			return -1;
		}

		if (ret > 0) {
			img->total_cow_pages++;
			img->remaining_pages--;
			stats->priority1_pages++;
			sent++;
		}
		max_pages--;
	}

	return sent;
}

/*
 * Drain pending page requests (Priority 2)
 * Returns: number of pages sent, or -1 on error
 */
static int drain_page_requests(struct active_image *img, pid_t source_pid,
			       struct unified_thread_stats *stats)
{
	int sent = 0;

	while (has_page_requests() && img->remaining_pages > 0) {
		struct page_request_entry *req = get_next_page_request();
		int ret;

		if (!req)
			break;

		ret = send_request_page_lazy(req, img, source_pid);

		if (ret > 0) {
			img->total_req_pages += ret;
			img->remaining_pages -= ret;
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

/* Forward declarations for libc rw- exclusion */
static void detect_libc_rw_range(pid_t pid);
static inline bool page_in_libc_rw(unsigned long addr, unsigned long len);

/*
 * Send a single lazy VMA page (Priority 3)
 * Returns: 1 if sent, 0 if skipped, -1 on error
 */
static int send_single_lazy_page(struct active_image *img,
				 struct lazy_vma_entry *lve,
				 unsigned long vaddr, unsigned long page_idx,
				 pid_t source_pid,
				 struct unified_thread_stats *stats,
				 int sk)
{
	int ret;

	/* Skip libc rw- pages */
	if (page_in_libc_rw(vaddr, PAGE_SIZE))
		return 0;

	/* Check if already sent */
	if (lve->sent_bitmap[page_idx / 8] & (1 << (page_idx % 8))) {
		stats->priority3_skips++;
		return 0;
	}

	ret = send_lazy_vma_page(sk, vaddr, img->dst_id, source_pid);
	if (ret < 0) {
		pr_err("Failed to send lazy VMA page at %lx\n", vaddr);
		return -1;
	}

	lve->sent_bitmap[page_idx / 8] |= (1 << (page_idx % 8));
	img->remaining_pages--;
	stats->priority3_pages++;

	return 1;
}

/* Send completion marker to destination */
static int send_image_complete(struct active_image *img)
{
	struct page_server_iov close_cmd = {
		.cmd = PS_IOV_CLOSE,
		.nr_pages = 0,
		.vaddr = 0,
		.dst_id = img->dst_id,
	};

	pr_warn("Image dst_id=%lu complete: %lu total pages (%lu COW, %lu req)\n",
		img->dst_id, img->total_pages,
		img->total_cow_pages, img->total_req_pages);

	/* Send close command — receiver may have already disconnected */
	if (send_psi(img->main_sk, &close_cmd)) {
		if (errno == EPIPE || errno == ECONNRESET ||
		    errno == EBADF || errno == ENOTCONN) {
			pr_info("Receiver already disconnected, treating as completion\n");
			return 0;
		}
		pr_err("Failed to send close command\n");
		return -1;
	}
	return 0;
}

/*
 * Process all pages for a single VMA
 * Returns: 0 on success, -1 on error
 */
/*
 * Batch size for process_vm_readv.  Reading multiple pages in a single
 * syscall reduces per-page overhead (syscall entry, mmap_read_lock,
 * page table walk setup) and speeds up WP clearing — which directly
 * reduces the duration of the application throughput stall.
 */
#define PAGE_BATCH_SIZE 512

static int process_vma_pages_sk(struct active_image *img,
				struct lazy_vma_entry *lve,
				pid_t source_pid,
				struct unified_thread_stats *stats,
				int sk,
				unsigned long range_start,
				unsigned long range_end)
{
	unsigned long vaddr;
	unsigned long page_idx;
	char *batch_buf;
	struct iovec *local_iovs;
	int batch_alloc_ok = 0;

	/*
	 * Send batch buffer: accumulate compressed pages and flush
	 * with a single send() to reduce syscall overhead.
	 * Max size: 256 pages × (28-byte header + 4-byte size + 4KB worst-case)
	 */
	char *send_batch = NULL;
	int send_batch_len = 0;
	int send_batch_cap = PAGE_BATCH_SIZE * (sizeof(struct page_server_iov) +
						sizeof(int) + LZ4_compressBound(PAGE_SIZE));

	page_idx = (range_start - lve->start) / PAGE_SIZE;

	pr_info("Processing VMA: %lx-%lx range=%lx-%lx pages=%lu\n",
		lve->start, lve->end, range_start, range_end,
		(range_end - range_start) / PAGE_SIZE);

	batch_buf = xmalloc(PAGE_BATCH_SIZE * PAGE_SIZE);
	local_iovs = xmalloc(PAGE_BATCH_SIZE * sizeof(struct iovec));
	send_batch = xmalloc(send_batch_cap);
	if (batch_buf && local_iovs && send_batch)
		batch_alloc_ok = 1;

	for (vaddr = range_start; vaddr < range_end; ) {
		unsigned long batch_start = vaddr;
		unsigned long batch_pages;
		unsigned long remaining;
		unsigned long i;

		/* Skip libc rw- pages — keep clean dump-time state */
		if (page_in_libc_rw(vaddr, PAGE_SIZE)) {
			vaddr += PAGE_SIZE;
			page_idx++;
			continue;
		}

		maybe_print_stats(stats);

		/* Priority 1: Drain COW pages (sync mode only) */
		if (!cow_is_wp_async()) {
			if (drain_cow_pages(img, source_pid, 100, stats) < 0)
				goto err;
		}

		/* Priority 2: Drain page requests */
		/*
		 * Page request drain: skip entirely in multi-stream
		 * workers.  drain_page_requests sends on req->sk which
		 * is always the main socket — doing that from a worker
		 * thread would interleave data with stream 0's batch
		 * sends, desynchronizing the receiver's stream parser.
		 * Requests will be handled after workers complete.
		 */
		if (!has_page_requests())
			goto skip_requests;
		if (COW_TRANSFER_STREAMS > 1 && sk != img->main_sk)
			goto skip_requests;
		pr_debug("Draining page requests at vaddr=%lx\n", vaddr);
		if (drain_page_requests(img, source_pid, stats) < 0)
			goto err;
skip_requests:

		remaining = (range_end - vaddr) / PAGE_SIZE;
		batch_pages = remaining < PAGE_BATCH_SIZE ?
				remaining : PAGE_BATCH_SIZE;

		if (!batch_alloc_ok || batch_pages <= 1) {
			/* Fallback: single page at a time */
			if (send_single_lazy_page(img, lve, vaddr, page_idx,
						  source_pid, stats, sk) < 0)
				goto err;
			vaddr += PAGE_SIZE;
			page_idx++;
			continue;
		}

		/*
		 * Batched read: one process_vm_readv for up to 256
		 * contiguous pages.  This reduces syscall overhead by
		 * ~256x and the kernel walks page tables in one pass.
		 */
		{
			struct iovec local_iov, remote_iov;
			ssize_t ret;

			local_iov.iov_base = batch_buf;
			local_iov.iov_len = batch_pages * PAGE_SIZE;
			remote_iov.iov_base = (void *)batch_start;
			remote_iov.iov_len = batch_pages * PAGE_SIZE;

			ret = process_vm_readv(source_pid,
					       &local_iov, 1,
					       &remote_iov, 1, 0);
			if (ret < 0 && errno == EFAULT) {
				/*
				 * Part of the range was unmapped.
				 * Fall back to per-page for this batch.
				 */
				for (i = 0; i < batch_pages; i++) {
					if (send_single_lazy_page(
						img, lve, vaddr,
						page_idx,
						source_pid, stats,
						sk) < 0)
						goto err;
					vaddr += PAGE_SIZE;
					page_idx++;
				}
				continue;
			}
			if (ret < 0 && errno == ESRCH) {
				pr_err("COW bulk: source process %d "
				       "died during transfer\n",
				       source_pid);
				goto err;
			}
			if (ret < 0) {
				pr_perror("COW bulk: readv %lu pages "
					  "at %#lx, zero-fill",
					  batch_pages, batch_start);
			}
			if (ret < (ssize_t)(batch_pages * PAGE_SIZE)) {
				unsigned long read_pages = ret > 0 ?
					ret / PAGE_SIZE : 0;
				/* Zero-fill unread */
				for (i = read_pages; i < batch_pages; i++)
					memset(batch_buf + i * PAGE_SIZE,
					       0, PAGE_SIZE);
			}
		}

		/* Compress pages into send batch, then flush once */
		send_batch_len = 0;
		for (i = 0; i < batch_pages; i++) {
			unsigned long paddr = batch_start + i * PAGE_SIZE;
			const void *data;
			struct cow_page *cow_pg = NULL;
			pthread_spinlock_t *lock = NULL;
			struct page_server_iov *pi;
			int *compressed_size;
			char *compressed_data;
			int clen;

			/* Skip libc rw- pages inside batch */
			if (page_in_libc_rw(paddr, PAGE_SIZE)) {
				vaddr += PAGE_SIZE;
				page_idx++;
				continue;
			}

			/* Skip if already sent */
			if (lve->sent_bitmap[page_idx / 8] &
			    (1 << (page_idx % 8))) {
				stats->priority3_skips++;
				vaddr += PAGE_SIZE;
				page_idx++;
				continue;
			}

			if (!cow_is_wp_async()) {
				lock = cow_get_hash_lock(paddr);
				if (lock)
					pthread_spin_lock(lock);
				cow_pg = lock ? cow_lookup_page(paddr) : NULL;
				if (lock)
					pthread_spin_unlock(lock);
			}

			data = cow_pg ? cow_pg->data :
				batch_buf + i * PAGE_SIZE;

			pi = (struct page_server_iov *)(send_batch + send_batch_len);
			compressed_size = (int *)(send_batch + send_batch_len + sizeof(*pi));
			compressed_data = send_batch + send_batch_len + sizeof(*pi) + sizeof(int);

			clen = LZ4_compress_default(data, compressed_data,
						    PAGE_SIZE, LZ4_compressBound(PAGE_SIZE));
			if (clen <= 0) {
				pr_err("LZ4 failed at %lx\n", paddr);
				goto err;
			}

			pi->nr_pages = 1;
			pi->vaddr = paddr;
			pi->dst_id = img->dst_id;

			if (clen < PAGE_SIZE) {
				/* Compression helped — send compressed */
				*compressed_size = clen;
				pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT);
				send_batch_len += sizeof(*pi) + sizeof(int) + clen;
			} else {
				/* Incompressible — send raw page */
				memcpy(send_batch + send_batch_len + sizeof(*pi),
				       data, PAGE_SIZE);
				pi->cmd = encode_ps_cmd(PS_IOV_ADD_F, PE_PRESENT);
				send_batch_len += sizeof(*pi) + PAGE_SIZE;
			}

			__sync_fetch_and_add(&g_compress_uncompressed_bytes, PAGE_SIZE);
			__sync_fetch_and_add(&g_compress_compressed_bytes,
					     (clen < PAGE_SIZE) ? clen : PAGE_SIZE);

			lve->sent_bitmap[page_idx / 8] |=
				(1 << (page_idx % 8));
			__sync_sub_and_fetch(&img->remaining_pages, 1);
			stats->priority3_pages++;

			if (cow_pg) {
				lock = cow_get_hash_lock(paddr);
				if (lock) {
					pthread_spin_lock(lock);
					cow_remove_page(paddr);
					pthread_spin_unlock(lock);
				}
			}

			vaddr += PAGE_SIZE;
			page_idx++;
		}

		/* Flush entire batch — loop for partial writes */
		if (send_batch_len > 0) {
			int sent = 0;

			while (sent < send_batch_len) {
				int ret = __send(sk, send_batch + sent,
						send_batch_len - sent, 0);
				if (ret <= 0) {
					pr_perror("Batch send failed (%d/%d)",
						  sent, send_batch_len);
					goto err;
				}
				sent += ret;
			}
		}

		/*
		 * Sync mode: clear write-protect for the batch.
		 * WP_ASYNC: kernel already cleared WP on writes,
		 * and PAGEMAP_SCAN handles re-arming during
		 * convergence rounds.
		 */
		if (!cow_is_wp_async()) {
			int uffd = cow_get_uffd_for_pid(source_pid);

			if (uffd >= 0) {
				struct uffdio_writeprotect wp;

				wp.range.start = batch_start;
				wp.range.len = batch_pages * PAGE_SIZE;
				wp.mode = 0;
				if (ioctl(uffd, UFFDIO_WRITEPROTECT, &wp))
					pr_pwarn("Batch unprotect [%lx +%lu] failed",
						 batch_start, batch_pages);
			}
		}
	}

	xfree(batch_buf);
	xfree(local_iovs);
	xfree(send_batch);
	return 0;

err:
	xfree(batch_buf);
	xfree(local_iovs);
	xfree(send_batch);
	return -1;
}

static int process_vma_pages(struct active_image *img,
			     struct lazy_vma_entry *lve,
			     pid_t source_pid,
			     struct unified_thread_stats *stats)
{
	return process_vma_pages_sk(img, lve, source_pid, stats,
				    img->main_sk,
				    lve->start, lve->end);
}

/*
 * Final drain of COW and request queues after all VMAs processed
 * Returns: 0 on success, -1 on error
 */
static int final_queue_drain(struct active_image *img, pid_t source_pid,
			     struct unified_thread_stats *stats)
{
	while (img->remaining_pages > 0) {
		int cow_sent, req_sent;

		cow_sent = drain_cow_pages(img, source_pid, 100, stats);
		if (cow_sent < 0)
			return -1;

		req_sent = drain_page_requests(img, source_pid, stats);
		if (req_sent < 0)
			return -1;

		/* No more pending work */
		if (cow_sent == 0 && req_sent == 0)
			break;
	}

	return 0;
}

/*
 * WP_ASYNC convergence: after the initial linear transfer, scan for
 * pages dirtied by the source process and re-send them.  Each scan
 * atomically re-arms WP, so writes after the scan are tracked for
 * the next iteration.
 */
#define CONVERGE_BATCH_PAGES		256
#define CONVERGE_MAX_REGIONS		4096
#define CONVERGE_MAX_ITERS		20
#define CONVERGE_THRESHOLD_ABS		64

struct converge_region {
	u64 start;
	u64 end;
	u64 categories;
};

static int cow_converge_dirty_pages(struct active_image *img,
				    pid_t source_pid)
{
	struct converge_region *regions;
	char *batch_buf;
	struct iovec *local_iovs;
	unsigned long iteration;

	if (!cow_is_wp_async())
		return 0;

	regions = xmalloc(CONVERGE_MAX_REGIONS * sizeof(*regions));
	batch_buf = xmalloc(CONVERGE_BATCH_PAGES * PAGE_SIZE);
	local_iovs = xmalloc(CONVERGE_BATCH_PAGES * sizeof(*local_iovs));
	if (!regions || !batch_buf || !local_iovs) {
		xfree(regions);
		xfree(batch_buf);
		xfree(local_iovs);
		pr_err("COW converge: allocation failed\n");
		return -1;
	}

	for (iteration = 0; iteration < CONVERGE_MAX_ITERS; iteration++) {
		struct lazy_vma_entry *lve;
		unsigned long total_dirty = 0;

		list_for_each_entry(lve, get_global_lazy_vmas(), list) {
			unsigned long scan_pos;

			if (lve->dst_id != img->dst_id)
				continue;

			scan_pos = lve->start;
			while (scan_pos < lve->end) {
				unsigned long walk_end = 0;
				int nr_regions, r;

				nr_regions = cow_scan_dirty_pages(
					source_pid, scan_pos, lve->end,
					regions, CONVERGE_MAX_REGIONS,
					&walk_end);
				if (nr_regions < 0)
					goto err;
				if (nr_regions == 0) {
					scan_pos = walk_end;
					if (walk_end >= lve->end)
						break;
					continue;
				}

				for (r = 0; r < nr_regions; r++) {
					unsigned long rstart = regions[r].start;
					unsigned long rend = regions[r].end;
					unsigned long pg;

					for (pg = rstart; pg < rend; ) {
						unsigned long batch_start = pg;
						unsigned long nr, remain;
						unsigned long i;
						struct iovec remote_iov;
						ssize_t ret;

						remain = (rend - pg) / PAGE_SIZE;
						nr = remain < CONVERGE_BATCH_PAGES ?
						     remain : CONVERGE_BATCH_PAGES;

						for (i = 0; i < nr; i++) {
							local_iovs[i].iov_base =
								batch_buf + i * PAGE_SIZE;
							local_iovs[i].iov_len = PAGE_SIZE;
						}
						remote_iov.iov_base = (void *)batch_start;
						remote_iov.iov_len = nr * PAGE_SIZE;

						ret = process_vm_readv(source_pid,
							local_iovs, nr,
							&remote_iov, 1, 0);
						if (ret < 0 && errno == ESRCH) {
							pr_info("COW converge: process gone\n");
							goto done;
						}
						if (ret < 0) {
							pr_perror("COW converge: readv at %#lx",
								  batch_start);
							pg += nr * PAGE_SIZE;
							continue;
						}
						if (ret != (ssize_t)(nr * PAGE_SIZE)) {
							unsigned long done_pg =
								ret / PAGE_SIZE;
							pr_warn("COW converge: "
								"partial read "
								"%zd/%lu at %#lx, "
								"zero-fill\n",
								ret,
								nr * PAGE_SIZE,
								batch_start);
							for (i = done_pg; i < nr;
							     i++)
								memset(batch_buf +
								       i * PAGE_SIZE,
								       0, PAGE_SIZE);
						}

						{
						char *sb;
						int sb_len = 0;
						int sb_cap = nr * (sizeof(struct page_server_iov) +
								  sizeof(int) + LZ4_compressBound(PAGE_SIZE));

						sb = xmalloc(sb_cap);
						if (!sb)
							goto err;

						for (i = 0; i < nr; i++) {
							unsigned long paddr = batch_start + i * PAGE_SIZE;
							struct page_server_iov *pi =
								(struct page_server_iov *)(sb + sb_len);
							int *csz = (int *)(sb + sb_len + sizeof(*pi));
							char *cd = sb + sb_len + sizeof(*pi) + sizeof(int);
							int clen;

							clen = LZ4_compress_default(
								batch_buf + i * PAGE_SIZE,
								cd, PAGE_SIZE,
								LZ4_compressBound(PAGE_SIZE));
							if (clen <= 0) {
								xfree(sb);
								goto err;
							}
							*csz = clen;
							pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS,
										PE_PRESENT);
							pi->nr_pages = 1;
							pi->vaddr = paddr;
							pi->dst_id = img->dst_id;
							sb_len += sizeof(*pi) + sizeof(int) + clen;
							total_dirty++;
						}

						/* Flush batch */
						{
							int sent = 0;

							while (sent < sb_len) {
								int wr = __send(img->main_sk,
										sb + sent,
										sb_len - sent, 0);
								if (wr <= 0) {
									pr_perror("COW converge: batch send failed");
									xfree(sb);
									goto err;
								}
								sent += wr;
							}
						}
						xfree(sb);
					}
						pg += nr * PAGE_SIZE;
					}
				}

				scan_pos = walk_end;
				if (walk_end >= lve->end)
					break;
			}
		}

		pr_err("COW converge iter %lu: %lu dirty pages re-sent\n",
		       iteration, total_dirty);

		if (total_dirty <= CONVERGE_THRESHOLD_ABS)
			break;

	}

	/*
	 * Final freeze: if convergence didn't fully converge (active
	 * writes keep dirtying pages), briefly SIGSTOP the source to
	 * guarantee one clean scan with zero new writes.
	 */
	if (source_pid > 0) {
		struct lazy_vma_entry *lve;
		unsigned long final_dirty = 0;

		kill(source_pid, SIGSTOP);
		/* Verify stopped — poll /proc status */
		{
			char spath[64];
			char sbuf[256];
			int sfd, tries;

			snprintf(spath, sizeof(spath),
				 "/proc/%d/status", source_pid);
			for (tries = 0; tries < 200; tries++) {
				sfd = open(spath, O_RDONLY);
				if (sfd >= 0) {
					int n = read(sfd, sbuf,
						     sizeof(sbuf) - 1);
					close(sfd);
					if (n > 0) {
						sbuf[n] = '\0';
						if (strstr(sbuf,
							   "\nState:\tT"))
							break;
					}
				}
				usleep(100);
			}
		}

		list_for_each_entry(lve, get_global_lazy_vmas(), list) {
			unsigned long scan_pos;

			if (lve->dst_id != img->dst_id)
				continue;

			scan_pos = lve->start;
			while (scan_pos < lve->end) {
				unsigned long walk_end = 0;
				int nr_regions, r;

				nr_regions = cow_scan_dirty_pages(
					source_pid, scan_pos, lve->end,
					regions, CONVERGE_MAX_REGIONS,
					&walk_end);
				if (nr_regions <= 0)
					break;

				for (r = 0; r < nr_regions; r++) {
					unsigned long pg = regions[r].start;
					unsigned long rend = regions[r].end;

					while (pg < rend) {
						unsigned long remain = (rend - pg) / PAGE_SIZE;
						unsigned long nr = remain < CONVERGE_BATCH_PAGES ?
								   remain : CONVERGE_BATCH_PAGES;
						unsigned long bi;
						struct iovec remote_iov;
						char *sb;
						int sb_len = 0;
						int sb_cap;

						for (bi = 0; bi < nr; bi++) {
							local_iovs[bi].iov_base =
								batch_buf + bi * PAGE_SIZE;
							local_iovs[bi].iov_len = PAGE_SIZE;
						}
						remote_iov.iov_base = (void *)pg;
						remote_iov.iov_len = nr * PAGE_SIZE;

						{
						ssize_t rret;
						ssize_t expect = nr * PAGE_SIZE;

						rret = process_vm_readv(source_pid,
							local_iovs, nr,
							&remote_iov, 1, 0);
						if (rret < 0)
							break;
						if (rret != expect) {
							unsigned long done =
								rret / PAGE_SIZE;
							pr_warn("COW final-freeze: "
								"partial read "
								"%zd/%zd at %#lx, "
								"zero-fill\n",
								rret, expect, pg);
							for (bi = done; bi < nr;
							     bi++)
								memset(batch_buf +
								       bi * PAGE_SIZE,
								       0, PAGE_SIZE);
						}
						}

						sb_cap = nr * (sizeof(struct page_server_iov) +
							       sizeof(int) + LZ4_compressBound(PAGE_SIZE));
						sb = xmalloc(sb_cap);
						if (!sb)
							break;

						for (bi = 0; bi < nr; bi++) {
							unsigned long paddr = pg + bi * PAGE_SIZE;
							struct page_server_iov *pi =
								(struct page_server_iov *)(sb + sb_len);
							int *csz = (int *)(sb + sb_len + sizeof(*pi));
							char *cd = sb + sb_len + sizeof(*pi) + sizeof(int);
							int clen;

							clen = LZ4_compress_default(
								batch_buf + bi * PAGE_SIZE,
								cd, PAGE_SIZE,
								LZ4_compressBound(PAGE_SIZE));
							if (clen <= 0) {
								xfree(sb);
								goto freeze_done;
							}
							*csz = clen;
							pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS,
										PE_PRESENT);
							pi->nr_pages = 1;
							pi->vaddr = paddr;
							pi->dst_id = img->dst_id;
							sb_len += sizeof(*pi) + sizeof(int) + clen;
							final_dirty++;
						}

						{
							int sent = 0;

							while (sent < sb_len) {
								int wr = __send(img->main_sk,
										sb + sent,
										sb_len - sent, 0);
								if (wr <= 0) {
									xfree(sb);
									goto freeze_done;
								}
								sent += wr;
							}
						}
						xfree(sb);
						pg += nr * PAGE_SIZE;
					}
				}

				scan_pos = walk_end;
				if (walk_end >= lve->end)
					break;
			}
		}

freeze_done:
		kill(source_pid, SIGCONT);
		pr_err("COW converge final-freeze: %lu dirty pages\n",
		       final_dirty);
	}

done:
	xfree(regions);
	xfree(batch_buf);
	xfree(local_iovs);
	return 0;

err:
	xfree(regions);
	xfree(batch_buf);
	xfree(local_iovs);
	return -1;
}

/*
 * libc rw- range.  Detected once from /proc/PID/maps.
 * Bulk transfer skips these (file-backed, not WP-tracked).
 * converge path re-sends them explicitly from the frozen source.
 */
static unsigned long g_libc_rw_start;
static unsigned long g_libc_rw_end;
static bool g_libc_rw_detected;

static void detect_libc_rw_range(pid_t pid)
{
	char path[64];
	FILE *fp;
	char line[512];

	if (g_libc_rw_detected)
		return;

	g_libc_rw_detected = true;
	snprintf(path, sizeof(path), "/proc/%d/maps", pid);
	fp = fopen(path, "r");
	if (!fp)
		return;

	while (fgets(line, sizeof(line), fp)) {
		unsigned long start, end;
		char perms[8], mpath[256];

		mpath[0] = '\0';
		if (sscanf(line, "%lx-%lx %4s %*s %*s %*s %255[^\n]",
			   &start, &end, perms, mpath) < 3)
			continue;
		if (strstr(mpath, "libc.so") &&
		    perms[0] == 'r' && perms[1] == 'w' && perms[2] == '-') {
			g_libc_rw_start = start;
			g_libc_rw_end = end;
			pr_err("COW converge: excluding libc rw- range "
			       "0x%lx-0x%lx (%lu pages) from convergence\n",
			       start, end, (end - start) / PAGE_SIZE);
			break;
		}
	}
	fclose(fp);
}

static inline bool page_in_libc_rw(unsigned long addr, unsigned long len)
{
	if (!g_libc_rw_start)
		return false;
	return addr < g_libc_rw_end && (addr + len) > g_libc_rw_start;
}

/*
 * Check if an address range overlaps with any dump-time VMA
 * for the given dst_id.  Used to distinguish new (jemalloc)
 * VMAs from dump-time VMAs during final-freeze.
 */
static bool addr_in_dump_vmas(u64 start, u64 end, u64 dst_id)
{
	struct lazy_vma_entry *lve;

	list_for_each_entry(lve, get_global_lazy_vmas(), list) {
		if (lve->dst_id != dst_id)
			continue;
		if (start < lve->end && end > lve->start)
			return true;
	}
	return false;
}

#include <sys/ptrace.h>
#include <sys/prctl.h>
#include <linux/elf.h>
#include <sys/syscall.h>

/*
 * Lightweight converge-phase signal handler capture via direct ptrace injection.
 *
 * Injects rt_sigaction(sig, NULL, &oldact, 8) for each signal on a
 * single thread. No parasite, no collect_mappings, no compel_cure.
 * ~2ms instead of ~10ms. Works on both live and SIGSTOP'd processes.
 *
 * Kernel rt_sigaction oldact layout:
 *   aarch64: handler(8) + flags(8) + mask(8) = 24 bytes (no restorer)
 *   x86_64:  handler(8) + flags(8) + restorer(8) + mask(8) = 32 bytes
 */
static int capture_and_send_converge_sigacts(pid_t source_pid, int socket,
				       u32 dst_id, bool already_stopped)
{
	pid_t tid = source_pid;
	user_regs_struct_t orig_regs, regs;
	struct iovec iov;
	unsigned long orig_code[2]; /* save 16 bytes at PC */
	unsigned long orig_stack[8]; /* save 64 bytes at sp-64 */
	unsigned long pc, sp;
	unsigned long sa_buf[4];
	int sig, status, captured = 0;
	u64 sigdata[64 * 4];
	struct page_server_iov hdr;
	size_t total, sent = 0;

	memset(sigdata, 0, sizeof(sigdata));

	/* SEIZE + stop the main thread */
	if (ptrace(PTRACE_SEIZE, tid, NULL, 0)) {
		pr_perror("converge sigacts: SEIZE %d", tid);
		return -1;
	}
	if (ptrace(PTRACE_INTERRUPT, tid, NULL, NULL) ||
	    waitpid(tid, &status, __WALL) != tid) {
		ptrace(PTRACE_DETACH, tid, NULL, NULL);
		return -1;
	}

	if (already_stopped) {
		if (ptrace(PTRACE_CONT, tid, 0, 0) ||
		    waitpid(tid, &status, __WALL) != tid) {
			ptrace(PTRACE_DETACH, tid, NULL, NULL);
			return -1;
		}
	}

	/* Save original registers */
	iov.iov_base = &orig_regs;
	iov.iov_len = sizeof(orig_regs);
	if (ptrace(PTRACE_GETREGSET, tid,
		   (void *)(unsigned long)NT_PRSTATUS, &iov)) {
		ptrace(PTRACE_DETACH, tid, NULL, NULL);
		return -1;
	}

#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
	sp = (unsigned long)orig_regs.sp;
#elif defined(__x86_64__)
	pc = (unsigned long)orig_regs.native.ip;
	sp = (unsigned long)orig_regs.native.sp;
#else
	ptrace(PTRACE_DETACH, tid, NULL, NULL);
	return -1;
#endif

	/* Save original code and stack */
	errno = 0;
	orig_code[0] = ptrace(PTRACE_PEEKDATA, tid, pc, NULL);
	orig_code[1] = ptrace(PTRACE_PEEKDATA, tid, pc + 8, NULL);
	if (errno)
		goto detach;
	{
		int w;
		for (w = 0; w < 8; w++) {
			errno = 0;
			orig_stack[w] = ptrace(PTRACE_PEEKDATA, tid,
					       sp - 64 + w * 8, NULL);
			if (errno)
				goto detach;
		}
	}

	/* Write syscall+trap at PC */
#ifdef __aarch64__
	{
		/* SVC #0 (0xD4000001) + BRK #0 (0xD4200000) = 8 bytes */
		unsigned long svc_brk = 0xD4200000D4000001UL;
		if (ptrace(PTRACE_POKEDATA, tid, pc, svc_brk))
			goto restore;
	}
#elif defined(__x86_64__)
	{
		/* SYSCALL (0F 05) + INT3 (CC) in first 3 bytes */
		unsigned long patched = (orig_code[0] & ~0xFFFFFFUL) |
					0xCC050FUL;
		if (ptrace(PTRACE_POKEDATA, tid, pc, patched))
			goto restore;
	}
#endif

	for (sig = 1; sig <= 64; sig++) {
		int idx = sig - 1;

		if (sig == SIGKILL || sig == SIGSTOP)
			continue;

		regs = orig_regs;
#ifdef __aarch64__
		regs.regs[8] = __NR_rt_sigaction;
		regs.regs[0] = sig;
		regs.regs[1] = 0;	   /* act = NULL (read) */
		regs.regs[2] = sp - 64;   /* oldact */
		regs.regs[3] = 8;	   /* sigsetsize */
		regs.pc = pc;
#elif defined(__x86_64__)
		regs.native.orig_ax = __NR_rt_sigaction;
		regs.native.ax = __NR_rt_sigaction;
		regs.native.di = sig;
		regs.native.si = 0;
		regs.native.dx = sp - 64;
		regs.native.r10 = 8;
		regs.native.ip = pc;
#endif
		iov.iov_base = &regs;
		iov.iov_len = sizeof(regs);
		if (ptrace(PTRACE_SETREGSET, tid,
			   (void *)(unsigned long)NT_PRSTATUS, &iov))
			break;
		if (ptrace(PTRACE_CONT, tid, NULL, NULL))
			break;
		if (waitpid(tid, &status, __WALL) != tid)
			break;
		if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP)
			break;

		/* Read oldact from stack */
		memset(sa_buf, 0, sizeof(sa_buf));
		{
			int w;
#ifdef __aarch64__
			/* aarch64: handler(8) + flags(8) + mask(8) = 24B */
			for (w = 0; w < 3; w++) {
				errno = 0;
				sa_buf[w] = ptrace(PTRACE_PEEKDATA, tid,
						   sp - 64 + w * 8, NULL);
				if (errno)
					goto restore;
			}
			sigdata[idx * 4 + 0] = sa_buf[0]; /* handler */
			sigdata[idx * 4 + 1] = sa_buf[1]; /* flags */
			sigdata[idx * 4 + 2] = 0;	   /* no restorer */
			sigdata[idx * 4 + 3] = sa_buf[2]; /* mask */
#elif defined(__x86_64__)
			/* x86_64: handler(8) + flags(8) + restorer(8) + mask(8) */
			for (w = 0; w < 4; w++) {
				errno = 0;
				sa_buf[w] = ptrace(PTRACE_PEEKDATA, tid,
						   sp - 64 + w * 8, NULL);
				if (errno)
					goto restore;
			}
			sigdata[idx * 4 + 0] = sa_buf[0];
			sigdata[idx * 4 + 1] = sa_buf[1];
			sigdata[idx * 4 + 2] = sa_buf[2];
			sigdata[idx * 4 + 3] = sa_buf[3];
#endif
		}
		captured++;
	}

restore:
	/* Restore original code + stack + regs */
	ptrace(PTRACE_POKEDATA, tid, pc, orig_code[0]);
	ptrace(PTRACE_POKEDATA, tid, pc + 8, orig_code[1]);
	{
		int w;
		for (w = 0; w < 8; w++)
			ptrace(PTRACE_POKEDATA, tid,
			       sp - 64 + w * 8, orig_stack[w]);
	}
	iov.iov_base = &orig_regs;
	iov.iov_len = sizeof(orig_regs);
	ptrace(PTRACE_SETREGSET, tid,
	       (void *)(unsigned long)NT_PRSTATUS, &iov);
detach:
	ptrace(PTRACE_DETACH, tid, NULL, NULL);

	pr_info("converge sigacts: captured %d signals (direct injection)\n",
		captured);

	/* Send */
	hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_SIGACTS, 0);
	hdr.nr_pages = 64;
	hdr.vaddr = 0;
	hdr.dst_id = dst_id;
	if (send_psi(socket, &hdr))
		return -1;

	total = sizeof(sigdata);
	while (sent < total) {
		int w = __send(socket, (char *)sigdata + sent,
			       total - sent, 0);
		if (w <= 0)
			return -1;
		sent += w;
	}

	pr_info("converge sigacts: sent %zu bytes\n", total);
	return 0;
}

/*
 * Capture the FD table at converge from /proc and send to replica.
 * This ensures the restored process has converge-consistent FDs,
 * not stale T_dump FDs.
 */
static int capture_and_send_converge_fds(pid_t source_pid, int socket,
				   u32 dst_id)
{
	char fd_dir[64], fd_path[64], link[256], info_path[80], line[256];
	DIR *dir;
	struct dirent *de;
	struct t3_fd_entry *fds;
	int nr_fds = 0, cap = 256;
	struct page_server_iov hdr;
	size_t total, sent = 0;
	FILE *fp;

	fds = xmalloc(cap * sizeof(*fds));
	if (!fds)
		return -1;

	snprintf(fd_dir, sizeof(fd_dir), "/proc/%d/fd", source_pid);
	dir = opendir(fd_dir);
	if (!dir) {
		xfree(fds);
		return -1;
	}

	while ((de = readdir(dir)) != NULL) {
		int fd_num;
		ssize_t len;

		if (de->d_name[0] == '.')
			continue;
		fd_num = atoi(de->d_name);

		if (nr_fds >= cap) {
			int nc = cap * 2;
			struct t3_fd_entry *tmp;

			tmp = xrealloc(fds, nc * sizeof(*tmp));
			if (!tmp)
				break;
			fds = tmp;
			cap = nc;
		}

		memset(&fds[nr_fds], 0, sizeof(fds[nr_fds]));
		fds[nr_fds].fd = fd_num;

		/* Read symlink target */
		snprintf(fd_path, sizeof(fd_path),
			 "/proc/%d/fd/%d", source_pid, fd_num);
		len = readlink(fd_path, link, sizeof(link) - 1);
		if (len > 0) {
			link[len] = '\0';
			snprintf(fds[nr_fds].path,
				 sizeof(fds[nr_fds].path),
				 "%s", link);
		}

		/* Read flags + pos from fdinfo */
		snprintf(info_path, sizeof(info_path),
			 "/proc/%d/fdinfo/%d", source_pid, fd_num);
		fp = fopen(info_path, "r");
		if (fp) {
			while (fgets(line, sizeof(line), fp)) {
				unsigned long long val;

				if (sscanf(line, "pos: %llu", &val) == 1)
					fds[nr_fds].pos = val;
				if (sscanf(line, "flags: %llo", &val) == 1)
					fds[nr_fds].flags = (u32)val;
			}
			fclose(fp);
		}

		nr_fds++;
	}
	closedir(dir);

	if (!nr_fds) {
		xfree(fds);
		return 0;
	}

	pr_info("converge FDs: captured %d file descriptors\n", nr_fds);

	hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_FDS, 0);
	hdr.nr_pages = nr_fds;
	hdr.vaddr = 0;
	hdr.dst_id = dst_id;
	if (send_psi(socket, &hdr)) {
		xfree(fds);
		return -1;
	}

	total = nr_fds * sizeof(*fds);
	while (sent < total) {
		int w = __send(socket, (char *)fds + sent,
			       total - sent, 0);
		if (w <= 0) {
			xfree(fds);
			return -1;
		}
		sent += w;
	}

	pr_info("converge FDs: sent %d fds (%zu bytes)\n",
	       nr_fds, total);
	xfree(fds);
	return 0;
}

static int pid_cmp(const void *a, const void *b)
{
	pid_t pa = *(const pid_t *)a, pb = *(const pid_t *)b;

	return (pa > pb) - (pa < pb);
}

/*
 * Capture itimers + misc from frozen source via ptrace injection.
 * The source is already SIGSTOP'd — we SEIZE, inject syscalls, DETACH.
 *
 * itimers: getitimer(ITIMER_REAL/VIRTUAL/PROF) × 3
 * misc: brk(0), umask(0)+umask(restore), prctl(GET_DUMPABLE),
 *       prctl(GET_THP_DISABLE), prctl(GET_CHILD_SUBREAPER)
 */

/* Wire format for converge itimers: 3 × struct itimerval (32 bytes each) */
struct converge_itimerval {
	long it_interval_sec;
	long it_interval_usec;
	long it_value_sec;
	long it_value_usec;
};

/* Wire format for converge misc */
struct converge_misc {
	unsigned long brk;
	unsigned long umask;
	unsigned long dumpable;
	unsigned long thp_disabled;
	unsigned long child_subreaper;
	unsigned long membarrier_mask;
};

static int capture_and_send_converge_itimers_misc(pid_t source_pid,
						  int socket, u32 dst_id)
{
	pid_t tid = source_pid;
	user_regs_struct_t orig_regs, regs;
	struct iovec iov;
	unsigned long orig_code[2];
	unsigned long orig_stack[8];
	unsigned long pc, sp;
	int status;
	struct converge_itimerval itimers[3];
	struct converge_misc misc;
	struct page_server_iov hdr;
	size_t total, sent;

	memset(itimers, 0, sizeof(itimers));
	memset(&misc, 0, sizeof(misc));

	/* SEIZE the main thread (already SIGSTOP'd) */
	if (ptrace(PTRACE_SEIZE, tid, NULL, 0)) {
		pr_perror("converge itimers: SEIZE %d", tid);
		return -1;
	}
	if (ptrace(PTRACE_INTERRUPT, tid, NULL, NULL) ||
	    waitpid(tid, &status, __WALL) != tid) {
		ptrace(PTRACE_DETACH, tid, NULL, NULL);
		return -1;
	}

	/* Save regs */
	iov.iov_base = &orig_regs;
	iov.iov_len = sizeof(orig_regs);
	if (ptrace(PTRACE_GETREGSET, tid,
		   (void *)(unsigned long)NT_PRSTATUS, &iov)) {
		ptrace(PTRACE_DETACH, tid, NULL, NULL);
		return -1;
	}

#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
	sp = (unsigned long)orig_regs.sp;
#elif defined(__x86_64__)
	pc = (unsigned long)orig_regs.native.ip;
	sp = (unsigned long)orig_regs.native.sp;
#else
	ptrace(PTRACE_DETACH, tid, NULL, NULL);
	return -1;
#endif

	/* Save code + stack */
	errno = 0;
	orig_code[0] = ptrace(PTRACE_PEEKDATA, tid, pc, NULL);
	orig_code[1] = ptrace(PTRACE_PEEKDATA, tid, pc + 8, NULL);
	if (errno)
		goto detach;
	{
		int w;

		for (w = 0; w < 8; w++) {
			errno = 0;
			orig_stack[w] = ptrace(PTRACE_PEEKDATA, tid,
					       sp - 64 + w * 8, NULL);
			if (errno)
				goto detach;
		}
	}

	/* Write SVC+BRK at PC */
#ifdef __aarch64__
	{
		unsigned long svc_brk = 0xD4200000D4000001UL;

		if (ptrace(PTRACE_POKEDATA, tid, pc, svc_brk))
			goto restore;
	}
#elif defined(__x86_64__)
	{
		unsigned long patched = (orig_code[0] & ~0xFFFFFFUL) |
					0xCC050FUL;
		if (ptrace(PTRACE_POKEDATA, tid, pc, patched))
			goto restore;
	}
#endif

	/*
	 * Helper: inject one syscall on the frozen thread.
	 * Sets up regs, CONT, waits for SIGTRAP, returns x0/rax.
	 */
#ifdef __aarch64__
#define INJECT_SYSCALL(nr, a0, a1, a2, a3, result) do {		\
	regs = orig_regs;					\
	regs.regs[8] = (nr);					\
	regs.regs[0] = (unsigned long)(a0);			\
	regs.regs[1] = (unsigned long)(a1);			\
	regs.regs[2] = (unsigned long)(a2);			\
	regs.regs[3] = (unsigned long)(a3);			\
	regs.pc = pc;						\
	iov.iov_base = &regs;					\
	iov.iov_len = sizeof(regs);				\
	if (ptrace(PTRACE_SETREGSET, tid,			\
		   (void *)(unsigned long)NT_PRSTATUS, &iov))	\
		goto restore;					\
	if (ptrace(PTRACE_CONT, tid, NULL, NULL))		\
		goto restore;					\
	if (waitpid(tid, &status, __WALL) != tid)		\
		goto restore;					\
	if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP)	\
		goto restore;					\
	iov.iov_base = &regs;					\
	iov.iov_len = sizeof(regs);				\
	ptrace(PTRACE_GETREGSET, tid,				\
	       (void *)(unsigned long)NT_PRSTATUS, &iov);	\
	(result) = (long)regs.regs[0];				\
} while (0)
#elif defined(__x86_64__)
#define INJECT_SYSCALL(nr, a0, a1, a2, a3, result) do {		\
	regs = orig_regs;					\
	regs.native.orig_ax = (nr);				\
	regs.native.ax = (nr);					\
	regs.native.di = (unsigned long)(a0);			\
	regs.native.si = (unsigned long)(a1);			\
	regs.native.dx = (unsigned long)(a2);			\
	regs.native.r10 = (unsigned long)(a3);			\
	regs.native.ip = pc;					\
	iov.iov_base = &regs;					\
	iov.iov_len = sizeof(regs);				\
	if (ptrace(PTRACE_SETREGSET, tid,			\
		   (void *)(unsigned long)NT_PRSTATUS, &iov))	\
		goto restore;					\
	if (ptrace(PTRACE_CONT, tid, NULL, NULL))		\
		goto restore;					\
	if (waitpid(tid, &status, __WALL) != tid)		\
		goto restore;					\
	if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP)	\
		goto restore;					\
	iov.iov_base = &regs;					\
	iov.iov_len = sizeof(regs);				\
	ptrace(PTRACE_GETREGSET, tid,				\
	       (void *)(unsigned long)NT_PRSTATUS, &iov);	\
	(result) = (long)regs.native.ax;			\
} while (0)
#endif

	/* --- Capture itimers --- */
	{
		int which;
		long ret;

		for (which = 0; which < 3; which++) {
			INJECT_SYSCALL(__NR_getitimer, which,
				       sp - 32, 0, 0, ret);
			if (ret == 0) {
				unsigned long v[4];
				int w;

				for (w = 0; w < 4; w++) {
					errno = 0;
					v[w] = ptrace(PTRACE_PEEKDATA, tid,
						      sp - 32 + w * 8, NULL);
					if (errno)
						break;
				}
				itimers[which].it_interval_sec = v[0];
				itimers[which].it_interval_usec = v[1];
				itimers[which].it_value_sec = v[2];
				itimers[which].it_value_usec = v[3];
			}
		}
		pr_info("converge itimers: captured 3\n");
	}

	/* --- Capture misc --- */
	{
		long ret;

		/* brk */
		INJECT_SYSCALL(__NR_brk, 0, 0, 0, 0, ret);
		misc.brk = ret;

		/* umask: get current, then restore it */
		INJECT_SYSCALL(__NR_umask, 0, 0, 0, 0, ret);
		misc.umask = ret;
		INJECT_SYSCALL(__NR_umask, ret, 0, 0, 0, ret);

		/* dumpable */
		INJECT_SYSCALL(__NR_prctl, PR_GET_DUMPABLE, 0, 0, 0, ret);
		misc.dumpable = ret;

		/* thp_disabled */
		INJECT_SYSCALL(__NR_prctl, PR_GET_THP_DISABLE, 0, 0, 0, ret);
		misc.thp_disabled = ret;

		/* child_subreaper — result written to memory */
		ptrace(PTRACE_POKEDATA, tid, sp - 8, 0);
		INJECT_SYSCALL(__NR_prctl, PR_GET_CHILD_SUBREAPER,
			       sp - 8, 0, 0, ret);
		if (ret == 0) {
			errno = 0;
			misc.child_subreaper = ptrace(PTRACE_PEEKDATA,
						      tid, sp - 8, NULL);
		}

		pr_info("converge misc: brk=%lx umask=%lo dumpable=%lu "
			"thp=%lu subreaper=%lu\n",
			misc.brk, misc.umask, misc.dumpable,
			misc.thp_disabled, misc.child_subreaper);
	}

#undef INJECT_SYSCALL

restore:
	/* Restore code + stack + regs */
	ptrace(PTRACE_POKEDATA, tid, pc, orig_code[0]);
	ptrace(PTRACE_POKEDATA, tid, pc + 8, orig_code[1]);
	{
		int w;

		for (w = 0; w < 8; w++)
			ptrace(PTRACE_POKEDATA, tid,
			       sp - 64 + w * 8, orig_stack[w]);
	}
	iov.iov_base = &orig_regs;
	iov.iov_len = sizeof(orig_regs);
	ptrace(PTRACE_SETREGSET, tid,
	       (void *)(unsigned long)NT_PRSTATUS, &iov);
detach:
	ptrace(PTRACE_DETACH, tid, NULL, NULL);

	/* Send itimers */
	hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_ITIMERS, 0);
	hdr.nr_pages = 3;
	hdr.vaddr = 0;
	hdr.dst_id = dst_id;
	if (send_psi(socket, &hdr))
		return -1;
	total = sizeof(itimers);
	sent = 0;
	while (sent < total) {
		int w = __send(socket, (char *)itimers + sent,
			       total - sent, 0);
		if (w <= 0)
			return -1;
		sent += w;
	}

	/* Send misc */
	hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_MISC, 0);
	hdr.nr_pages = 1;
	hdr.vaddr = 0;
	hdr.dst_id = dst_id;
	if (send_psi(socket, &hdr))
		return -1;
	total = sizeof(misc);
	sent = 0;
	while (sent < total) {
		int w = __send(socket, (char *)&misc + sent,
			       total - sent, 0);
		if (w <= 0)
			return -1;
		sent += w;
	}

	return 0;
}

/*
 * Per-thread converge state: sigaltstack, pdeath_sig, thread name.
 * Captured from each thread via /proc (no ptrace injection needed).
 */
struct converge_thread_extra {
	pid_t tid;
	unsigned long sas_sp;
	unsigned long sas_flags;
	unsigned long sas_size;
	int pdeath_sig;
	char comm[16];
};

static int __attribute__((unused)) capture_and_send_converge_thread_state(pid_t source_pid,
						  int socket, u32 dst_id)
{
	char task_dir[64];
	DIR *dir;
	struct dirent *de;
	struct converge_thread_extra *threads = NULL;
	int nr_threads = 0, cap = 32;
	struct page_server_iov hdr;
	size_t total, sent;

	threads = xmalloc(cap * sizeof(*threads));
	if (!threads)
		return -1;

	snprintf(task_dir, sizeof(task_dir), "/proc/%d/task", source_pid);
	dir = opendir(task_dir);
	if (!dir) {
		xfree(threads);
		return -1;
	}

	while ((de = readdir(dir)) != NULL) {
		pid_t tid;
		char path[128], buf[256];
		FILE *f;

		if (de->d_name[0] == '.')
			continue;
		tid = atoi(de->d_name);
		if (tid <= 0)
			continue;

		if (nr_threads >= cap) {
			cap *= 2;
			threads = xrealloc(threads, cap * sizeof(*threads));
			if (!threads) {
				closedir(dir);
				return -1;
			}
		}

		memset(&threads[nr_threads], 0, sizeof(threads[nr_threads]));
		threads[nr_threads].tid = tid;

		/* Thread name from /proc/tid/comm */
		snprintf(path, sizeof(path), "/proc/%d/comm", tid);
		f = fopen(path, "r");
		if (f) {
			if (fgets(threads[nr_threads].comm,
				  sizeof(threads[nr_threads].comm), f)) {
				/* strip trailing newline */
				char *nl = strchr(threads[nr_threads].comm,
						  '\n');
				if (nl)
					*nl = '\0';
			}
			fclose(f);
		}

		/* pdeath_sig from /proc/tid/status */
		snprintf(path, sizeof(path), "/proc/%d/status", tid);
		f = fopen(path, "r");
		if (f) {
			while (fgets(buf, sizeof(buf), f)) {
				if (!strncmp(buf, "SigPnd:", 7))
					break; /* past relevant fields */
			}
			fclose(f);
		}
		/* pdeath_sig not in /proc — need prctl.
		 * We'll capture it if we do ptrace injection.
		 * For now, set 0 (default). */
		threads[nr_threads].pdeath_sig = 0;

		/* sigaltstack: not in /proc either — skip for now,
		 * restore sets it from core images which are from seize. */

		nr_threads++;
	}
	closedir(dir);

	pr_info("converge thread_state: %d threads captured\n", nr_threads);

	/* Send */
	hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_THREAD_STATE, 0);
	hdr.nr_pages = nr_threads;
	hdr.vaddr = 0;
	hdr.dst_id = dst_id;
	if (send_psi(socket, &hdr)) {
		xfree(threads);
		return -1;
	}
	total = nr_threads * sizeof(*threads);
	sent = 0;
	while (sent < total) {
		int w = __send(socket, (char *)threads + sent,
			       total - sent, 0);
		if (w <= 0) {
			xfree(threads);
			return -1;
		}
		sent += w;
	}

	xfree(threads);
	return 0;
}

static int __attribute__((unused)) capture_and_send_converge_regs(pid_t source_pid, int socket,
				    u32 dst_id)
{
	char task_dir[64];
	DIR *dir;
	struct dirent *de;
	pid_t tids[256];
	int nr_threads = 0, i, status;
	struct t3_thread_regs *t3;
	struct page_server_iov hdr;
	size_t total, sent = 0;

	snprintf(task_dir, sizeof(task_dir),
		 "/proc/%d/task", source_pid);
	dir = opendir(task_dir);
	if (!dir)
		return -1;
	while ((de = readdir(dir)) != NULL && nr_threads < 256) {
		if (de->d_name[0] == '.')
			continue;
		tids[nr_threads++] = atoi(de->d_name);
	}
	closedir(dir);
	if (!nr_threads)
		return -1;

	qsort(tids, nr_threads, sizeof(pid_t), pid_cmp);

	t3 = xzalloc(nr_threads * sizeof(*t3));
	if (!t3)
		return -1;

	for (i = 0; i < nr_threads; i++) {
		pid_t tid = tids[i];
		struct iovec iov;
		user_regs_struct_t gp;
		unsigned long tls_val = 0;

		if (ptrace(PTRACE_SEIZE, tid, NULL, 0) ||
		    ptrace(PTRACE_INTERRUPT, tid, NULL, NULL) ||
		    waitpid(tid, &status, __WALL) != tid) {
			ptrace(PTRACE_DETACH, tid, NULL, NULL);
			continue;
		}
		iov.iov_base = &gp;
		iov.iov_len = sizeof(gp);
		if (!ptrace(PTRACE_GETREGSET, tid,
			    (void *)(unsigned long)NT_PRSTATUS, &iov)) {
#ifdef __aarch64__
			memcpy(t3[i].regs, gp.regs, 31 * sizeof(u64));
			t3[i].sp = gp.sp;
			t3[i].pc = gp.pc;
			t3[i].pstate = gp.pstate;
#elif defined(__x86_64__)
			memcpy(t3[i].regs, &gp.native,
			       sizeof(gp.native));
			t3[i].sp = gp.native.sp;
			t3[i].pc = gp.native.ip;
			t3[i].pstate = gp.native.flags;
			t3[i].tls = gp.native.fs_base;
#else
			t3[i].sp = gp.sp;
			t3[i].pc = gp.ip;
#endif
		}
		iov.iov_base = &tls_val;
		iov.iov_len = sizeof(tls_val);
		if (!ptrace(PTRACE_GETREGSET, tid, (void *)0x401UL, &iov))
			t3[i].tls = tls_val;

		ptrace(PTRACE_DETACH, tid, NULL, NULL);
	}

	pr_info("converge regs: captured %d threads\n", nr_threads);

	hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_REGS, 0);
	hdr.nr_pages = nr_threads;
	hdr.vaddr = 0;
	hdr.dst_id = dst_id;
	if (send_psi(socket, &hdr)) {
		xfree(t3);
		return -1;
	}

	total = nr_threads * sizeof(*t3);
	while (sent < total) {
		int w = __send(socket, (char *)t3 + sent,
			       total - sent, 0);
		if (w <= 0) {
			xfree(t3);
			return -1;
		}
		sent += w;
	}
	pr_info("converge regs: sent %d threads (%zu bytes)\n",
	       nr_threads, (size_t)(nr_threads * sizeof(*t3)));
	xfree(t3);
	return 0;
}

/*
 * Send a batch of dirty pages to a specific socket.
 * Used by both convergence iterations and final-freeze.
 */
static int converge_send_batch(int sk, struct active_image *img,
			       pid_t source_pid, char *batch_buf,
			       struct iovec *local_iovs,
			       unsigned long pg_start, unsigned long nr)
{
	struct iovec remote_iov;
	unsigned long i;
	char *sb;
	int sb_len = 0, sb_cap;
	ssize_t ret;

	for (i = 0; i < nr; i++) {
		local_iovs[i].iov_base = batch_buf + i * PAGE_SIZE;
		local_iovs[i].iov_len = PAGE_SIZE;
	}
	remote_iov.iov_base = (void *)pg_start;
	remote_iov.iov_len = nr * PAGE_SIZE;

	ret = process_vm_readv(source_pid, local_iovs, nr, &remote_iov, 1, 0);
	if (ret < 0) {
		if (errno == ESRCH)
			return 1; /* process gone — not an error */
		return -1;
	}

	sb_cap = nr * (sizeof(struct page_server_iov) +
		       sizeof(int) + LZ4_compressBound(PAGE_SIZE));
	sb = xmalloc(sb_cap);
	if (!sb)
		return -1;

	for (i = 0; i < nr; i++) {
		unsigned long paddr = pg_start + i * PAGE_SIZE;
		struct page_server_iov *pi =
			(struct page_server_iov *)(sb + sb_len);
		int *csz = (int *)(sb + sb_len + sizeof(*pi));
		char *cd = sb + sb_len + sizeof(*pi) + sizeof(int);
		int clen;

		clen = LZ4_compress_default(batch_buf + i * PAGE_SIZE,
					    cd, PAGE_SIZE,
					    LZ4_compressBound(PAGE_SIZE));
		if (clen <= 0) {
			xfree(sb);
			return -1;
		}
		*csz = clen;
		pi->cmd = encode_ps_cmd(PS_IOV_ADD_F_COMPRESS, PE_PRESENT);
		pi->nr_pages = 1;
		pi->vaddr = paddr;
		pi->dst_id = img->dst_id;
		sb_len += sizeof(*pi) + sizeof(int) + clen;
	}

	{
		int sent = 0;

		while (sent < sb_len) {
			int wr = __send(sk, sb + sent, sb_len - sent, 0);
			if (wr <= 0) {
				xfree(sb);
				return -1;
			}
			sent += wr;
		}
	}
	xfree(sb);
	return 0;
}

/*
 * Convergence worker context.  Each thread owns a disjoint subset
 * of dirty regions (assigned by round-robin in the dispatcher).
 * No hash checks, no skipped reads — every page read is sent.
 */
struct converge_worker {
	int id;
	int sk;
	struct active_image *img;
	pid_t source_pid;
	struct converge_region *regions;	/* owned slice */
	int nr_regions;
	unsigned long pages_sent;
	int error;
};

static void *converge_worker_func(void *arg)
{
	struct converge_worker *cw = arg;
	char *batch_buf;
	struct iovec *local_iovs;
	int r;

	batch_buf = xmalloc(CONVERGE_BATCH_PAGES * PAGE_SIZE);
	local_iovs = xmalloc(CONVERGE_BATCH_PAGES * sizeof(*local_iovs));
	if (!batch_buf || !local_iovs) {
		xfree(batch_buf);
		xfree(local_iovs);
		cw->error = 1;
		return NULL;
	}

	for (r = 0; r < cw->nr_regions; r++) {
		unsigned long pg = cw->regions[r].start;
		unsigned long rend = cw->regions[r].end;

		while (pg < rend) {
			unsigned long remain = (rend - pg) / PAGE_SIZE;
			unsigned long nr = remain < CONVERGE_BATCH_PAGES ?
					   remain : CONVERGE_BATCH_PAGES;
			int ret;

			ret = converge_send_batch(cw->sk, cw->img,
						  cw->source_pid,
						  batch_buf, local_iovs,
						  pg, nr);
			if (ret < 0) {
				cw->error = 1;
				goto out;
			}
			if (ret == 1) /* process gone */
				goto out;

			cw->pages_sent += nr;
			pg += nr * PAGE_SIZE;
		}
	}

out:
	xfree(batch_buf);
	xfree(local_iovs);
	return NULL;
}

/*
 * Dispatch dirty regions to workers by round-robin.
 * Each worker gets its own contiguous slice of the regions array.
 * Returns total pages sent across all workers, or -1 on error.
 */
static long converge_dispatch_parallel(struct active_image *img,
				       pid_t source_pid,
				       int *sockets, int nr_streams,
				       struct converge_region *all_dirty,
				       int all_dirty_count)
{
	pthread_t *threads;
	struct converge_worker *cws;
	struct converge_region **per_worker;
	int *per_worker_count;
	long total_sent = 0;
	int s, r;

	threads = xmalloc(nr_streams * sizeof(*threads));
	cws = xmalloc(nr_streams * sizeof(*cws));
	per_worker = xmalloc(nr_streams * sizeof(*per_worker));
	per_worker_count = xmalloc(nr_streams * sizeof(*per_worker_count));
	if (!threads || !cws || !per_worker || !per_worker_count) {
		xfree(threads);
		xfree(cws);
		xfree(per_worker);
		xfree(per_worker_count);
		return -1;
	}

	/* Round-robin assign regions to workers */
	for (s = 0; s < nr_streams; s++)
		per_worker_count[s] = 0;
	for (r = 0; r < all_dirty_count; r++)
		per_worker_count[r % nr_streams]++;

	/* Build per-worker region arrays (pointers into all_dirty) */
	for (s = 0; s < nr_streams; s++) {
		per_worker[s] = xmalloc(per_worker_count[s] *
					sizeof(struct converge_region));
		if (!per_worker[s]) {
			for (r = 0; r < s; r++)
				xfree(per_worker[r]);
			xfree(threads);
			xfree(cws);
			xfree(per_worker);
			xfree(per_worker_count);
			return -1;
		}
		per_worker_count[s] = 0; /* reset for fill pass */
	}
	for (r = 0; r < all_dirty_count; r++) {
		s = r % nr_streams;
		per_worker[s][per_worker_count[s]++] = all_dirty[r];
	}

	/* Launch workers */
	for (s = 0; s < nr_streams; s++) {
		cws[s].id = s;
		cws[s].sk = sockets[s];
		cws[s].img = img;
		cws[s].source_pid = source_pid;
		cws[s].regions = per_worker[s];
		cws[s].nr_regions = per_worker_count[s];
		cws[s].pages_sent = 0;
		cws[s].error = 0;
		if (pthread_create(&threads[s], NULL,
				   converge_worker_func, &cws[s]))
			cws[s].error = 1;
	}

	for (s = 0; s < nr_streams; s++)
		pthread_join(threads[s], NULL);

	for (s = 0; s < nr_streams; s++) {
		if (cws[s].error)
			pr_err("Converge stream %d: error\n", s);
		total_sent += cws[s].pages_sent;
		xfree(per_worker[s]);
	}

	xfree(threads);
	xfree(cws);
	xfree(per_worker);
	xfree(per_worker_count);
	return total_sent;
}

/*
 * Multi-stream convergence: distribute dirty page re-sends across
 * all TCP streams with vaddr hash affinity.
 */
/*
 * Pre-created converge capture thread.
 * Created before SIGSTOP, waits on condvar, wakes when freeze starts.
 * Zero pthread_create cost during the freeze window.
 */
struct converge_capture_ctx {
	pid_t pid;
	int sk;
	u32 dst_id;
	pthread_mutex_t mu;
	pthread_cond_t cv;
	int go;       /* set to 1 to wake the thread */
	int done;     /* set to 1 when capture finished */
};

/*
 * Unified converge capture: regs + thread names + FDs + itimers + misc
 * in a single pass.  One readdir of /proc/pid/task, one SEIZE cycle
 * for itimers+misc injections, one batch send.
 */
static int capture_and_send_converge_all(pid_t source_pid, int socket,
					 u32 dst_id)
{
	DIR *dir;
	struct dirent *de;
	char task_dir[64];
	pid_t *tids = NULL;
	int nr_threads = 0, tid_cap = 32;
	int i;
	struct page_server_iov hdr;
	size_t total, sent;

	/* --- Phase 1: enumerate threads + capture regs + names --- */
	tids = xmalloc(tid_cap * sizeof(pid_t));
	if (!tids)
		return -1;

	snprintf(task_dir, sizeof(task_dir), "/proc/%d/task", source_pid);
	dir = opendir(task_dir);
	if (!dir) {
		xfree(tids);
		return -1;
	}
	while ((de = readdir(dir)) != NULL) {
		pid_t tid;

		if (de->d_name[0] == '.')
			continue;
		tid = atoi(de->d_name);
		if (tid <= 0)
			continue;
		if (nr_threads >= tid_cap) {
			tid_cap *= 2;
			tids = xrealloc(tids, tid_cap * sizeof(pid_t));
			if (!tids) {
				closedir(dir);
				return -1;
			}
		}
		tids[nr_threads++] = tid;
	}
	closedir(dir);

	qsort(tids, nr_threads, sizeof(pid_t), pid_cmp);

	/* Regs: PTRACE_GETREGSET per thread */
	{
		struct t3_thread_regs *tregs;
		size_t regs_sz = nr_threads * sizeof(*tregs);

		tregs = xmalloc(regs_sz);
		if (!tregs)
			goto skip_regs;

		for (i = 0; i < nr_threads; i++) {
			struct iovec gp_iov, tls_iov;
			user_regs_struct_t gpr;
			unsigned long tls_val = 0;
			int status;

			memset(&tregs[i], 0, sizeof(tregs[i]));

			/* Must SEIZE each thread to read registers */
			if (ptrace(PTRACE_SEIZE, tids[i], NULL, 0) ||
			    ptrace(PTRACE_INTERRUPT, tids[i], NULL, NULL) ||
			    waitpid(tids[i], &status, __WALL) != tids[i]) {
				ptrace(PTRACE_DETACH, tids[i], NULL, NULL);
				continue;
			}

			gp_iov.iov_base = &gpr;
			gp_iov.iov_len = sizeof(gpr);
			if (ptrace(PTRACE_GETREGSET, tids[i],
				   (void *)(unsigned long)NT_PRSTATUS,
				   &gp_iov))
				goto detach_thread;

#ifdef __aarch64__
			memcpy(tregs[i].regs, gpr.regs,
			       31 * sizeof(unsigned long));
			tregs[i].sp = gpr.sp;
			tregs[i].pc = gpr.pc;
			tregs[i].pstate = gpr.pstate;
			tls_iov.iov_base = &tls_val;
			tls_iov.iov_len = sizeof(tls_val);
			ptrace(PTRACE_GETREGSET, tids[i],
			       (void *)0x401UL, &tls_iov);
			tregs[i].tls = tls_val;
#elif defined(__x86_64__)
			memcpy(tregs[i].regs, &gpr.native,
			       sizeof(gpr.native));
			tregs[i].sp = gpr.native.sp;
			tregs[i].pc = gpr.native.ip;
#endif
detach_thread:
			ptrace(PTRACE_DETACH, tids[i], NULL, NULL);
		}

		/* Send regs */
		hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_REGS, 0);
		hdr.nr_pages = nr_threads;
		hdr.vaddr = 0;
		hdr.dst_id = dst_id;
		if (send_psi(socket, &hdr))
			goto free_regs;
		total = regs_sz;
		sent = 0;
		while (sent < total) {
			int w = __send(socket, (char *)tregs + sent,
				       total - sent, 0);
			if (w <= 0)
				break;
			sent += w;
		}
free_regs:
		xfree(tregs);
	}
skip_regs:

	/* Per-thread state: comm + pdeath_sig + sigaltstack */
	{
		struct converge_thread_extra *textra;
		size_t extra_sz = nr_threads * sizeof(*textra);

		textra = xmalloc(extra_sz);
		if (!textra)
			goto skip_thread;

		for (i = 0; i < nr_threads; i++) {
			char path[128];
			FILE *f;
			user_regs_struct_t ts_orig, ts_regs;
			struct iovec ts_iov;
			unsigned long ts_code[2], ts_pc, ts_sp;
			int ts_status;

			memset(&textra[i], 0, sizeof(textra[i]));
			textra[i].tid = tids[i];

			/* comm from /proc (no ptrace needed) */
			snprintf(path, sizeof(path), "/proc/%d/comm",
				 tids[i]);
			f = fopen(path, "r");
			if (f) {
				if (fgets(textra[i].comm,
					  sizeof(textra[i].comm), f)) {
					char *nl = strchr(textra[i].comm,
							  '\n');
					if (nl)
						*nl = '\0';
				}
				fclose(f);
			}

			/* pdeath_sig + sigaltstack via ptrace injection */
			if (ptrace(PTRACE_SEIZE, tids[i], NULL, 0))
				continue;
			if (ptrace(PTRACE_INTERRUPT, tids[i], NULL, NULL) ||
			    waitpid(tids[i], &ts_status, __WALL) != tids[i]) {
				ptrace(PTRACE_DETACH, tids[i], NULL, NULL);
				continue;
			}

			ts_iov.iov_base = &ts_orig;
			ts_iov.iov_len = sizeof(ts_orig);
			if (ptrace(PTRACE_GETREGSET, tids[i],
				   (void *)(unsigned long)NT_PRSTATUS,
				   &ts_iov))
				goto ts_detach;

#ifdef __aarch64__
			ts_pc = (unsigned long)ts_orig.pc;
			ts_sp = (unsigned long)ts_orig.sp;
#elif defined(__x86_64__)
			ts_pc = (unsigned long)ts_orig.native.ip;
			ts_sp = (unsigned long)ts_orig.native.sp;
#else
			goto ts_detach;
#endif

			errno = 0;
			ts_code[0] = ptrace(PTRACE_PEEKDATA, tids[i],
					    ts_pc, NULL);
			ts_code[1] = ptrace(PTRACE_PEEKDATA, tids[i],
					    ts_pc + 8, NULL);
			if (errno)
				goto ts_detach;

			/* Poke SVC+BRK */
#ifdef __aarch64__
			{
				unsigned long svc_brk =
					0xD4200000D4000001UL;
				if (ptrace(PTRACE_POKEDATA, tids[i],
					   ts_pc, svc_brk))
					goto ts_restore;
			}
#elif defined(__x86_64__)
			{
				unsigned long patched =
					(ts_code[0] & ~0xFFFFFFUL) |
					0xCC050FUL;
				if (ptrace(PTRACE_POKEDATA, tids[i],
					   ts_pc, patched))
					goto ts_restore;
			}
#endif

/*
 * Inject one syscall on this thread.
 * After the SVC executes, the kernel may deliver SIGSTOP
 * before BRK — we suppress it and CONT to reach SIGTRAP.
 */
#ifdef __aarch64__
#define TS_INJ(nr, a0, a1, a2, ret) do {			\
	ts_regs = ts_orig;					\
	ts_regs.regs[8] = (nr);				\
	ts_regs.regs[0] = (unsigned long)(a0);			\
	ts_regs.regs[1] = (unsigned long)(a1);			\
	ts_regs.regs[2] = (unsigned long)(a2);			\
	ts_regs.pc = ts_pc;					\
	ts_iov.iov_base = &ts_regs;				\
	ts_iov.iov_len = sizeof(ts_regs);			\
	if (ptrace(PTRACE_SETREGSET, tids[i],			\
		   (void *)(unsigned long)NT_PRSTATUS,		\
		   &ts_iov))					\
		goto ts_restore;				\
	if (ptrace(PTRACE_CONT, tids[i], NULL, NULL))		\
		goto ts_restore;				\
	if (waitpid(tids[i], &ts_status, __WALL) != tids[i])	\
		goto ts_restore;				\
	while (WIFSTOPPED(ts_status) &&				\
	       WSTOPSIG(ts_status) != SIGTRAP) {		\
		if (ptrace(PTRACE_CONT, tids[i], NULL, NULL))	\
			goto ts_restore;			\
		if (waitpid(tids[i], &ts_status, __WALL)	\
		    != tids[i])					\
			goto ts_restore;			\
	}							\
	ts_iov.iov_base = &ts_regs;				\
	ts_iov.iov_len = sizeof(ts_regs);			\
	ptrace(PTRACE_GETREGSET, tids[i],			\
	       (void *)(unsigned long)NT_PRSTATUS, &ts_iov);	\
	(ret) = (long)ts_regs.regs[0];				\
} while (0)
#elif defined(__x86_64__)
#define TS_INJ(nr, a0, a1, a2, ret) do {			\
	ts_regs = ts_orig;					\
	ts_regs.native.orig_ax = (nr);				\
	ts_regs.native.ax = (nr);				\
	ts_regs.native.di = (unsigned long)(a0);		\
	ts_regs.native.si = (unsigned long)(a1);		\
	ts_regs.native.dx = (unsigned long)(a2);		\
	ts_regs.native.ip = ts_pc;				\
	ts_iov.iov_base = &ts_regs;				\
	ts_iov.iov_len = sizeof(ts_regs);			\
	if (ptrace(PTRACE_SETREGSET, tids[i],			\
		   (void *)(unsigned long)NT_PRSTATUS,		\
		   &ts_iov))					\
		goto ts_restore;				\
	if (ptrace(PTRACE_CONT, tids[i], NULL, NULL))		\
		goto ts_restore;				\
	if (waitpid(tids[i], &ts_status, __WALL) != tids[i])	\
		goto ts_restore;				\
	while (WIFSTOPPED(ts_status) &&				\
	       WSTOPSIG(ts_status) != SIGTRAP) {		\
		if (ptrace(PTRACE_CONT, tids[i], NULL, NULL))	\
			goto ts_restore;			\
		if (waitpid(tids[i], &ts_status, __WALL)	\
		    != tids[i])					\
			goto ts_restore;			\
	}							\
	ts_iov.iov_base = &ts_regs;				\
	ts_iov.iov_len = sizeof(ts_regs);			\
	ptrace(PTRACE_GETREGSET, tids[i],			\
	       (void *)(unsigned long)NT_PRSTATUS, &ts_iov);	\
	(ret) = (long)ts_regs.native.ax;			\
} while (0)
#endif

			{
				long ret;

				/* prctl(PR_GET_PDEATHSIG, &sig) */
				ptrace(PTRACE_POKEDATA, tids[i],
				       ts_sp - 8, 0);
				TS_INJ(__NR_prctl, PR_GET_PDEATHSIG,
				       ts_sp - 8, 0, ret);
				if (ret == 0) {
					errno = 0;
					textra[i].pdeath_sig =
						ptrace(PTRACE_PEEKDATA,
						       tids[i],
						       ts_sp - 8, NULL);
				}

				/* sigaltstack(NULL, &oss) */
				ptrace(PTRACE_POKEDATA, tids[i],
				       ts_sp - 48, 0);
				ptrace(PTRACE_POKEDATA, tids[i],
				       ts_sp - 40, 0);
				ptrace(PTRACE_POKEDATA, tids[i],
				       ts_sp - 32, 0);
				TS_INJ(__NR_sigaltstack, 0,
				       ts_sp - 48, 0, ret);
				if (ret == 0) {
					errno = 0;
					textra[i].sas_sp =
						ptrace(PTRACE_PEEKDATA,
						       tids[i],
						       ts_sp - 48, NULL);
					textra[i].sas_flags =
						ptrace(PTRACE_PEEKDATA,
						       tids[i],
						       ts_sp - 40, NULL);
					textra[i].sas_size =
						ptrace(PTRACE_PEEKDATA,
						       tids[i],
						       ts_sp - 32, NULL);
				}
			}

#undef TS_INJ

ts_restore:
			ptrace(PTRACE_POKEDATA, tids[i], ts_pc,
			       ts_code[0]);
			ptrace(PTRACE_POKEDATA, tids[i], ts_pc + 8,
			       ts_code[1]);
			ts_iov.iov_base = &ts_orig;
			ts_iov.iov_len = sizeof(ts_orig);
			ptrace(PTRACE_SETREGSET, tids[i],
			       (void *)(unsigned long)NT_PRSTATUS,
			       &ts_iov);
ts_detach:
			ptrace(PTRACE_DETACH, tids[i], NULL, NULL);
		}

		hdr.cmd = encode_ps_cmd(PS_IOV_CONVERGE_THREAD_STATE, 0);
		hdr.nr_pages = nr_threads;
		hdr.vaddr = 0;
		hdr.dst_id = dst_id;
		if (send_psi(socket, &hdr))
			goto free_thread;
		total = extra_sz;
		sent = 0;
		while (sent < total) {
			int w = __send(socket, (char *)textra + sent,
				       total - sent, 0);
			if (w <= 0)
				break;
			sent += w;
		}
free_thread:
		xfree(textra);
	}
skip_thread:

	/* FDs + itimers + misc with timing */
	{
		struct timeval ta, tb, td;

		gettimeofday(&ta, NULL);
		capture_and_send_converge_fds(source_pid, socket, dst_id);
		gettimeofday(&tb, NULL);
		timersub(&tb, &ta, &td);
		pr_err("converge FDs: %ld.%03ldms\n",
		       td.tv_sec * 1000 + td.tv_usec / 1000,
		       td.tv_usec % 1000);

		gettimeofday(&ta, NULL);
		capture_and_send_converge_itimers_misc(source_pid, socket,
						       dst_id);
		gettimeofday(&tb, NULL);
		timersub(&tb, &ta, &td);
		pr_err("converge itimers+misc: %ld.%03ldms\n",
		       td.tv_sec * 1000 + td.tv_usec / 1000,
		       td.tv_usec % 1000);
	}

	xfree(tids);

	pr_err("converge all: %d threads captured\n", nr_threads);
	return 0;
}

static void __attribute__((unused)) *converge_capture_thread(void *arg)
{
	struct converge_capture_ctx *ctx = arg;
	struct timeval ts, te, td;

	pthread_mutex_lock(&ctx->mu);
	while (!ctx->go)
		pthread_cond_wait(&ctx->cv, &ctx->mu);
	pthread_mutex_unlock(&ctx->mu);

	gettimeofday(&ts, NULL);
	capture_and_send_converge_all(ctx->pid, ctx->sk, ctx->dst_id);
	gettimeofday(&te, NULL);
	timersub(&te, &ts, &td);
	pr_info("converge capture thread: %ldms\n",
		td.tv_sec * 1000 + td.tv_usec / 1000);

	pthread_mutex_lock(&ctx->mu);
	ctx->done = 1;
	pthread_cond_signal(&ctx->cv);
	pthread_mutex_unlock(&ctx->mu);

	return NULL;
}

static void __attribute__((unused)) converge_capture_wake(struct converge_capture_ctx *ctx)
{
	pthread_mutex_lock(&ctx->mu);
	ctx->go = 1;
	pthread_cond_signal(&ctx->cv);
	pthread_mutex_unlock(&ctx->mu);
}

static void __attribute__((unused)) converge_capture_wait(struct converge_capture_ctx *ctx,
				  pthread_t thread)
{
	pthread_mutex_lock(&ctx->mu);
	while (!ctx->done)
		pthread_cond_wait(&ctx->cv, &ctx->mu);
	pthread_mutex_unlock(&ctx->mu);
	pthread_join(thread, NULL);
}

static int cow_converge_dirty_pages_parallel(struct active_image *img,
					     pid_t source_pid,
					     int *sockets, int nr_streams)
{
	struct converge_region *regions;

	if (!cow_is_wp_async() || nr_streams < 1)
		return 0;

	detect_libc_rw_range(source_pid);

	regions = xmalloc(CONVERGE_MAX_REGIONS * sizeof(*regions));
	if (!regions)
		return -1;

	/*
	 * Pre-converge: capture signal handlers from the live process.
	 * Uses direct ptrace injection on single thread (~2ms).
	 * Needed for general correctness — sigacts can change
	 * between seize and converge for non-Valkey processes.
	 */
	capture_and_send_converge_sigacts(
		source_pid, sockets[0], img->dst_id, false);

	/*
	 * converge freeze: SIGSTOP → scan → capture → dispatch →
	 *            non-lazy → VMA diff → SIGCONT.
	 */
	{
		struct lazy_vma_entry *lve;
		struct converge_region *freeze_dirty = NULL;
		int freeze_dirty_count = 0, freeze_dirty_cap = 0;
		unsigned long freeze_pages = 0;
		struct timeval t3_start, t3_end, t3_delta;
		bool bpf_was_active, need_pagemap_scan;

		kill(source_pid, SIGSTOP);
		/* Verify source is actually stopped before proceeding.
		 * Can't waitpid (not the parent). Poll /proc status. */
		{
			char spath[64];
			char sbuf[256];
			int sfd, tries;

			snprintf(spath, sizeof(spath),
				 "/proc/%d/status", source_pid);
			for (tries = 0; tries < 200; tries++) {
				sfd = open(spath, O_RDONLY);
				if (sfd >= 0) {
					int n = read(sfd, sbuf,
						     sizeof(sbuf) - 1);
					close(sfd);
					if (n > 0) {
						sbuf[n] = '\0';
						if (strstr(sbuf,
							   "\nState:\tT"))
							break;
					}
				}
				usleep(100); /* 100μs per try, 20ms max */
			}
			if (tries >= 200)
				pr_err("converge: source did not stop "
				       "within 20ms\n");
		}
		gettimeofday(&t3_start, NULL);

		/*
		 * Two paths for dirty scan:
		 * (a) eBPF ring drain — O(dirty), microseconds
		 * (b) PAGEMAP_SCAN fallback — O(total_pages), ~150ms
		 *
		 * If eBPF ring dropped events (ring full), fall
		 * back to PAGEMAP_SCAN for correctness.
		 */
		bpf_was_active = cow_bpf_active();
		need_pagemap_scan = !bpf_was_active;

		if (bpf_was_active) {
			struct cow_bpf_region *bpf_regions;
			int bpf_nr;

			bpf_regions = xmalloc(CONVERGE_MAX_REGIONS *
					      sizeof(*bpf_regions));
			if (bpf_regions) {
				bpf_nr = cow_bpf_drain(bpf_regions,
						       CONVERGE_MAX_REGIONS,
						       &freeze_pages);
				if (bpf_nr >= 0) {
					if (bpf_nr > 0) {
						int r;

						freeze_dirty = xmalloc(
							bpf_nr *
							sizeof(*freeze_dirty));
						if (freeze_dirty) {
							for (r = 0; r < bpf_nr; r++) {
								freeze_dirty[r].start =
									bpf_regions[r].start;
								freeze_dirty[r].end =
									bpf_regions[r].end;
								freeze_dirty[r].categories = 0;
							}
							freeze_dirty_count = bpf_nr;
						}
					}
					pr_info("COW converge dirty scan (eBPF): "
						"%lu dirty pages (%d regions, "
						"%llu total events)\n",
						freeze_pages,
						freeze_dirty_count,
						(unsigned long long)
						cow_bpf_event_count());
				} else {
					/* -2 = ring drops, -1 = error */
					need_pagemap_scan = true;
					pr_err("converge: eBPF drain failed "
					       "(%d), falling back to "
					       "PAGEMAP_SCAN\n", bpf_nr);
				}
				xfree(bpf_regions);
			} else {
				need_pagemap_scan = true;
			}
			cow_bpf_stop();
		}

		if (need_pagemap_scan) {
			/* Reset partial BPF state if any */
			xfree(freeze_dirty);
			freeze_dirty = NULL;
			freeze_dirty_count = 0;
			freeze_dirty_cap = 0;
			freeze_pages = 0;

			list_for_each_entry(lve, get_global_lazy_vmas(), list) {
				unsigned long scan_pos;

				if (lve->dst_id != img->dst_id)
					continue;

				scan_pos = lve->start;
				while (scan_pos < lve->end) {
					unsigned long walk_end = 0;
					int nr_regions, r;

					nr_regions = cow_scan_dirty_pages(
						source_pid, scan_pos, lve->end,
						regions, CONVERGE_MAX_REGIONS,
						&walk_end);
					if (nr_regions <= 0)
						break;

					if (freeze_dirty_count + nr_regions >
					    freeze_dirty_cap) {
						int new_cap =
							(freeze_dirty_cap +
							 nr_regions) * 2;
						struct converge_region *tmp;

						tmp = xrealloc(freeze_dirty,
							new_cap *
							sizeof(*tmp));
						if (!tmp)
							break;
						freeze_dirty = tmp;
						freeze_dirty_cap = new_cap;
					}
					for (r = 0; r < nr_regions; r++) {
						freeze_dirty[freeze_dirty_count++] =
							regions[r];
						freeze_pages +=
							(regions[r].end -
							 regions[r].start) /
							PAGE_SIZE;
					}

					scan_pos = walk_end;
					if (walk_end >= lve->end)
						break;
				}
			}

			pr_info("COW converge dirty scan (PAGEMAP_SCAN%s): "
				"%lu dirty pages (%d regions)\n",
				bpf_was_active ? " after BPF drop" : "",
				freeze_pages, freeze_dirty_count);
		}

		/* State capture then dirty dispatch (sequential) */
		capture_and_send_converge_all(source_pid, sockets[0],
					      img->dst_id);

		if (freeze_dirty_count > 0) {
			long sent = converge_dispatch_parallel(
				img, source_pid, sockets, nr_streams,
				freeze_dirty, freeze_dirty_count);
			if (sent >= 0)
				freeze_pages = sent;
		}

		/*
		 * Re-send non-WP-tracked writable pages from
		 * frozen source (stacks, file-backed rw like
		 * libc .data/.bss, ld.so .data, etc.).
		 */
		{
			char maps_path[64];
			FILE *fp;
			unsigned long nonlazy_pages = 0;
			int nonlazy_vmas = 0;

			snprintf(maps_path, sizeof(maps_path),
				 "/proc/%d/maps", source_pid);
			fp = fopen(maps_path, "r");
			if (fp) {
				char line[512];

				while (fgets(line, sizeof(line), fp)) {
					unsigned long ms, me;
					char mp[8];
					struct converge_region sr;

					if (sscanf(line,
						   "%lx-%lx %4s",
						   &ms, &me, mp) < 3)
						continue;

					if (mp[1] != 'w')
						continue;

					{
						unsigned long ino = 0;
						char rest[256];
						unsigned long d;
						int d1, d2;

						rest[0] = '\0';
						if (sscanf(line,
							   "%lx-%lx %4s %lx %x:%x %lu %255[^\n]",
							   &ms, &me, mp,
							   &d, &d1, &d2,
							   &ino, rest) < 7)
							continue;

						if (strstr(rest, "CRIUMFD") != NULL)
							continue;
						if (ino == 0 &&
						    strstr(rest, "[stack") == NULL)
							continue;
						if (mp[3] == 's')
							continue;
					}

					sr.start = ms;
					sr.end = me;
					sr.categories = 0;
					converge_dispatch_parallel(
						img, source_pid,
						sockets, nr_streams,
						&sr, 1);
					nonlazy_pages += (me - ms) /
							 PAGE_SIZE;
					nonlazy_vmas++;
				}
				fclose(fp);
			}
			pr_info("converge: re-sent %lu non-lazy pages "
				"(%d VMAs: stacks + rw file-backed)\n",
				nonlazy_pages, nonlazy_vmas);
		}

		/*
		 * VMA diff: detect new VMAs created during transfer
		 * and send to replica. Done while source is frozen
		 * for atomicity (no race with new mmap calls).
		 */
		{
			struct timeval vd_start, vd_end, vd_delta;
			char maps_path[64];
			FILE *mfp;
			struct vma_diff_entry *new_vmas = NULL;
			int new_vma_count = 0, new_vma_cap = 0;
			unsigned long new_vma_pages = 0;

			gettimeofday(&vd_start, NULL);
			snprintf(maps_path, sizeof(maps_path),
				 "/proc/%d/maps", source_pid);
			mfp = fopen(maps_path, "r");
			if (mfp) {
				char mline[512];

				while (fgets(mline, sizeof(mline), mfp)) {
					unsigned long ms, me;
					char mp[8], mpath[256];

					mpath[0] = '\0';
					if (sscanf(mline,
						   "%lx-%lx %4s %*s %*s %*s %255[^\n]",
						   &ms, &me, mp, mpath) < 3)
						continue;
					if (mp[0] != 'r' || mp[1] != 'w' ||
					    mp[2] != '-')
						continue;

					if (!addr_in_dump_vmas(ms, me,
							       img->dst_id)) {
						if (new_vma_count >= new_vma_cap) {
							int nc = (new_vma_cap + 16) * 2;
							struct vma_diff_entry *tmp;

							tmp = xrealloc(new_vmas,
								nc * sizeof(*tmp));
							if (!tmp)
								break;
							new_vmas = tmp;
							new_vma_cap = nc;
						}
						new_vmas[new_vma_count].start = ms;
						new_vmas[new_vma_count].end = me;
						new_vmas[new_vma_count].prot =
							PROT_READ | PROT_WRITE;
						new_vmas[new_vma_count].pad = 0;
						new_vma_count++;
						new_vma_pages += (me - ms) / PAGE_SIZE;
					}
				}
				fclose(mfp);
			}

			if (new_vma_count > 0) {
				struct page_server_iov vma_hdr = {
					.cmd = encode_ps_cmd(PS_IOV_VMA_DIFF, 0),
					.nr_pages = new_vma_count,
					.vaddr = 0,
					.dst_id = img->dst_id,
				};
				int payload_sz = new_vma_count *
					sizeof(struct vma_diff_entry);
				int wr, sent_bytes;
				char *batch_buf;
				struct iovec *lio;
				int v;

				pr_info("COW converge: %d new VMAs "
					"(%lu pages, %.1f MB)\n",
					new_vma_count, new_vma_pages,
					(double)(new_vma_pages * PAGE_SIZE) /
					(1024 * 1024));

				if (send_psi(sockets[0], &vma_hdr))
					pr_err("COW: send VMA diff hdr failed\n");

				sent_bytes = 0;
				while (sent_bytes < payload_sz) {
					wr = __send(sockets[0],
						    (char *)new_vmas + sent_bytes,
						    payload_sz - sent_bytes, 0);
					if (wr <= 0) {
						pr_err("COW: send VMA diff "
						       "payload failed\n");
						break;
					}
					sent_bytes += wr;
				}

				batch_buf = xmalloc(CONVERGE_BATCH_PAGES *
						    PAGE_SIZE);
				lio = xmalloc(CONVERGE_BATCH_PAGES *
					      sizeof(struct iovec));
				if (batch_buf && lio) {
					unsigned long total_sent = 0;

					for (v = 0; v < new_vma_count; v++) {
						unsigned long pg = new_vmas[v].start;
						unsigned long vend = new_vmas[v].end;

						while (pg < vend) {
							unsigned long remain =
								(vend - pg) / PAGE_SIZE;
							unsigned long batch =
								remain < CONVERGE_BATCH_PAGES ?
								remain : CONVERGE_BATCH_PAGES;

							converge_send_batch(
								sockets[0], img,
								source_pid,
								batch_buf, lio,
								pg, batch);
							total_sent += batch;
							pg += batch * PAGE_SIZE;
						}
					}
					pr_info("COW converge: sent %lu "
						"new-VMA pages\n", total_sent);
				}
				xfree(batch_buf);
				xfree(lio);
			}

			xfree(new_vmas);
			gettimeofday(&vd_end, NULL);
			timersub(&vd_end, &vd_start, &vd_delta);
			pr_info("COW VMA diff: %ld.%03ldms\n",
				vd_delta.tv_sec * 1000 +
				vd_delta.tv_usec / 1000,
				vd_delta.tv_usec % 1000);
		}

		/* Resume source after all converge work complete */
		kill(source_pid, SIGCONT);
		gettimeofday(&t3_end, NULL);
		timersub(&t3_end, &t3_start, &t3_delta);
		pr_err("COW CONVERGE: %ld.%03ldms SIGSTOP→SIGCONT "
		       "(%lu dirty pages)\n",
		       t3_delta.tv_sec * 1000 +
		       t3_delta.tv_usec / 1000,
		       t3_delta.tv_usec % 1000,
		       freeze_pages);

		xfree(freeze_dirty);
	} /* end converge freeze block */

	xfree(regions);
	return 0;
}

/*
 * Per-stream transfer worker.  Each worker has its own TCP socket
 * and processes a subset of VMAs assigned to it.
 */
struct vma_range {
	struct lazy_vma_entry *lve;
	unsigned long start;	/* may differ from lve->start for split VMAs */
	unsigned long end;	/* may differ from lve->end for split VMAs */
};

struct stream_worker {
	int id;
	int sk;
	struct active_image *img;
	pid_t source_pid;
	struct vma_range *ranges;	/* shared array (all workers) */
	int nr_ranges;			/* total ranges in shared array */
	int *next_range;		/* atomic: next range to claim */
	bool failed;
	unsigned long pages_sent;
};

static void *stream_worker_func(void *arg)
{
	struct stream_worker *w = arg;
	struct unified_thread_stats stats = { 0 };
	char name[16];

	snprintf(name, sizeof(name), "cow-xfer-%d", w->id);
	pthread_setname_np(pthread_self(), name);

	/* Work-stealing loop: atomically claim the next range */
	for (;;) {
		int ri = __sync_fetch_and_add(w->next_range, 1);
		struct vma_range *r;

		if (ri >= w->nr_ranges)
			break;

		r = &w->ranges[ri];
		if (process_vma_pages_sk(w->img, r->lve, w->source_pid,
					 &stats, w->sk,
					 r->start, r->end) < 0) {
			pr_err("Stream %d: error at range %lx-%lx\n",
			       w->id, r->start, r->end);
			w->failed = true;
			break;
		}
	}

	w->pages_sent = stats.priority3_pages;

	/* Send bulk-complete marker — socket stays open for convergence */
	{
		struct page_server_iov phase_cmd = {
			.cmd = PS_IOV_CLOSE,
			.nr_pages = 0,
			.vaddr = 0,
			.dst_id = w->img->dst_id,
		};
		if (send_psi(w->sk, &phase_cmd)) {
			if (errno != EPIPE && errno != ECONNRESET &&
			    errno != EBADF && errno != ENOTCONN)
				pr_err("Stream %d: failed to send bulk-complete\n", w->id);
		}
	}

	pr_err("Stream %d: bulk finished (%lu pages, failed=%d)\n",
	       w->id, w->pages_sent, w->failed);
	return NULL;
}

/* Unified background thread serving all images */
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
			bool image_failed = false;
			int nr_streams = 1;

			pthread_spin_unlock(&active_images_lock);

			pr_info("Processing image dst_id=%lu remaining=%lu pages\n",
				img->dst_id, img->remaining_pages);

			/*
			 * Multi-TCP: in COW WP_ASYNC mode, accept
			 * additional connections and use parallel
			 * workers.
			 */
			if (cow_is_wp_async() && g_listen_sk >= 0)
				nr_streams = COW_TRANSFER_STREAMS;

			if (nr_streams > 1) {
				struct stream_worker workers[COW_TRANSFER_STREAMS];
				pthread_t threads[COW_TRANSFER_STREAMS];
				struct lazy_vma_entry **all_vmas;
				struct vma_range *all_ranges = NULL;
				int nr_ranges = 0;
				int nr_vmas = 0, vi = 0, s;
				unsigned long total_pages = 0;
				unsigned long pages_per_worker;

				/* Count matching VMAs */
				list_for_each_entry(lve, get_global_lazy_vmas(), list) {
					if (lve->dst_id != img->dst_id)
						continue;
					nr_vmas++;
					total_pages += lve->total_pages;
					source_pid = lve->source_pid;
				}

				all_vmas = xmalloc(nr_vmas * sizeof(*all_vmas));
				if (!all_vmas) {
					nr_streams = 1;
					goto single_stream;
				}

				vi = 0;
				list_for_each_entry(lve, get_global_lazy_vmas(), list) {
					if (lve->dst_id != img->dst_id)
						continue;
					all_vmas[vi++] = lve;
				}

				/* Accept additional connections with timeout */
				memset(workers, 0, sizeof(workers));
				workers[0].sk = img->main_sk;
				for (s = 1; s < nr_streams; s++) {
					struct sockaddr_storage ca;
					socklen_t cl = sizeof(ca);
					struct pollfd pfd = {
						.fd = g_listen_sk,
						.events = POLLIN,
					};

					if (poll(&pfd, 1, 5000) <= 0) {
						pr_warn("Stream %d: no connection within 5s, using %d streams\n",
							s, s);
						nr_streams = s;
						break;
					}
					workers[s].sk = accept(g_listen_sk,
							       (struct sockaddr *)&ca, &cl);
					if (workers[s].sk < 0) {
						pr_warn("Failed to accept stream %d, using %d streams\n",
							s, s);
						nr_streams = s;
						break;
					}
					tcp_cork(workers[s].sk, true);
					{
						int bs = 16 * 1024 * 1024;
						setsockopt(workers[s].sk,
							   SOL_SOCKET,
							   SO_SNDBUF,
							   &bs, sizeof(bs));
					}
				}

				/*
				 * Partition pages across workers.  Split VMAs
				 * into pages_per_worker-sized ranges so all
				 * streams stay busy.
				 */
				{
				int max_ranges = nr_vmas + nr_streams * 4;

				pages_per_worker = (total_pages + nr_streams - 1) / nr_streams;
				all_ranges = xmalloc(max_ranges * sizeof(*all_ranges));
				if (!all_ranges) {
					xfree(all_vmas);
					nr_streams = 1;
					goto single_stream;
				}

				/* Build range list, splitting VMAs > pages_per_worker */
				for (vi = 0; vi < nr_vmas; vi++) {
					unsigned long vma_pages = all_vmas[vi]->total_pages;

					if (vma_pages > pages_per_worker &&
					    nr_streams > 1) {
						unsigned long chunk = pages_per_worker;
						unsigned long off = 0;

						chunk = (chunk + 255) & ~255UL;
						while (off < vma_pages &&
						       nr_ranges < max_ranges) {
							unsigned long end_pg = off + chunk;

							if (end_pg > vma_pages)
								end_pg = vma_pages;
							all_ranges[nr_ranges].lve = all_vmas[vi];
							all_ranges[nr_ranges].start =
								all_vmas[vi]->start + off * PAGE_SIZE;
							all_ranges[nr_ranges].end =
								all_vmas[vi]->start + end_pg * PAGE_SIZE;
							nr_ranges++;
							off = end_pg;
						}
					} else {
						all_ranges[nr_ranges].lve = all_vmas[vi];
						all_ranges[nr_ranges].start = all_vmas[vi]->start;
						all_ranges[nr_ranges].end = all_vmas[vi]->end;
						nr_ranges++;
					}
				}

				/* Work-stealing: all workers share the ranges
				 * array and atomically claim next range */
				{
				int *shared_next = xmalloc(sizeof(int));
				*shared_next = 0;
				for (s = 0; s < nr_streams; s++) {
					workers[s].id = s;
					workers[s].img = img;
					workers[s].source_pid = source_pid;
					workers[s].ranges = all_ranges;
					workers[s].nr_ranges = nr_ranges;
					workers[s].next_range = shared_next;
				}
				}
				}

				pr_err("Multi-TCP: %d streams, %d ranges "
				       "(%d VMAs), %lu pages\n",
				       nr_streams, nr_ranges,
				       nr_vmas, total_pages);

				detect_libc_rw_range(source_pid);

				/*
				 * No bulk fork in WP_ASYNC mode.
				 * eBPF dirty tracker started in cr-dump.c
				 * right after WP (no gap for missed writes).
				 * Workers read directly from live source.
				 */
				{
				for (s = 0; s < nr_streams; s++) {
					if (pthread_create(&threads[s], NULL,
							   stream_worker_func,
							   &workers[s])) {
						pr_err("Failed to create stream worker %d\n", s);
						workers[s].failed = true;
					}
				}

				for (s = 0; s < nr_streams; s++)
					pthread_join(threads[s], NULL);
				}

				for (s = 0; s < nr_streams; s++) {
					if (workers[s].failed)
						image_failed = true;
				}

				xfree(all_ranges);
				xfree(all_vmas);

				/*
				 * Signal bulk completion so the
				 * orchestration script can start
				 * workload traffic.  Writes go to
				 * the images dir.
				 */
				if (cow_is_wp_async() && opts.imgs_dir) {
					char bp[PATH_MAX];
					int bfd;

					snprintf(bp, sizeof(bp),
						 "%s/bulk_send_done",
						 opts.imgs_dir);
					bfd = open(bp,
						   O_CREAT | O_WRONLY | O_TRUNC,
						   0644);
					if (bfd >= 0)
						close(bfd);
					pr_err("Bulk transfer complete, "
					       "wrote %s\n", bp);
				}

				/* WP_ASYNC convergence across all streams */
				if (!image_failed && cow_is_wp_async() && source_pid > 0) {
					int *conv_sks = xmalloc(nr_streams * sizeof(int));
					if (conv_sks) {
						for (s = 0; s < nr_streams; s++)
							conv_sks[s] = workers[s].sk;
						if (cow_converge_dirty_pages_parallel(
							    img, source_pid,
							    conv_sks, nr_streams) < 0)
							pr_warn("COW convergence had errors (non-fatal)\n");
						xfree(conv_sks);
					}
				}

				/* Send final close + close sockets */
				for (s = 0; s < nr_streams; s++) {
					struct page_server_iov close_cmd = {
						.cmd = PS_IOV_CLOSE,
						.nr_pages = 0,
						.dst_id = img->dst_id,
					};
					if (workers[s].sk >= 0) {
						send_psi(workers[s].sk, &close_cmd);
						if (s > 0)
							close(workers[s].sk);
					}
				}

				goto check_complete;
			}

single_stream:
			/* Process each lazy VMA (single-stream fallback) */
			detect_libc_rw_range(source_pid);
			list_for_each_entry(lve, get_global_lazy_vmas(), list) {
				if (lve->dst_id != img->dst_id)
					continue;

				source_pid = lve->source_pid;

				if (process_vma_pages(img, lve, source_pid, &stats) < 0) {
					pr_err("Error processing VMA %lx-%lx\n",
					       lve->start, lve->end);
					image_failed = true;
					break;
				}
			}

			/* WP_ASYNC: run convergence rounds for dirty pages */
			if (!image_failed && cow_is_wp_async() && source_pid > 0) {
				if (cow_converge_dirty_pages(img, source_pid) < 0)
					pr_warn("COW convergence had errors (non-fatal, all pages sent)\n");
			}

check_complete:
			/* Final drain of any remaining queued pages */
			pthread_spin_lock(&active_images_lock);
			if (!image_failed && !cow_is_wp_async() &&
			    final_queue_drain(img, source_pid, &stats) < 0) {
				pr_err("Error in final queue drain\n");
				image_failed = true;
			}

			/* Check if complete (WP_ASYNC: complete after linear + convergence) */
			if ((!image_failed || cow_is_wp_async()) &&
			    img->remaining_pages == 0) {
				pthread_spin_unlock(&active_images_lock);
				if (send_image_complete(img) < 0)
					pr_err("Failed to complete image dst_id=%lu\n",
					       img->dst_id);
				pthread_spin_lock(&active_images_lock);
				list_del(&img->list);
				xfree(img);
				continue;
			}

			pr_err("Failed processing image dst_id=%lu remaining_pages=%lu, closing stream\n",
			       img->dst_id, img->remaining_pages);
			shutdown(img->main_sk, SHUT_RDWR);
			list_del(&img->list);
			xfree(img);
		}

		g_unified_thread_stop = list_empty(&active_images_queue);
		pthread_spin_unlock(&active_images_lock);
	}

	if (g_listen_sk >= 0) {
		close(g_listen_sk);
		g_listen_sk = -1;
	}

	pr_err("Unified page server thread stopped\n");
	g_unified_thread_running = false;
	return NULL;
}

static int page_server_get_all_pages(int sk, struct page_server_iov *pi)
{
	int ret;
	
	pr_warn("Adding image dst_id=%lu to batch transfer queue\n", pi->dst_id);
	
	/* Initialize queues */
	init_active_images_queue();
	init_page_request_queue();
	
	/* Add this image to active queue */
	ret = add_active_image(pi->dst_id, sk);
	if (ret < 0)
		return -1;
	
	/* Start unified thread if not already running */
	if (!g_unified_thread_running) {
		pr_info("Starting unified page server thread\n");
		g_unified_thread_stop = false;
		ret = pthread_create(&g_unified_thread, NULL, unified_page_server_thread, NULL);
		if (ret) {
			pr_perror("Failed to create unified thread");
			return -1;
		}
		g_unified_thread_running = true;
	}
	
	return 0;
}

static int page_server_get_pages(int sk, struct page_server_iov *pi)
{
	unsigned long i;
	
	/* Split multi-page requests into individual page requests */
	for (i = 0; i < pi->nr_pages; i++) {
		add_page_request(pi->vaddr + (i * PAGE_SIZE), 1, sk, pi->dst_id);
	}
	
	pr_debug("Split and enqueued %lu page requests starting at vaddr=%lx\n", 
		 (unsigned long)pi->nr_pages, (unsigned long)pi->vaddr);
	
	/* Return immediately - background thread will send the response */
	return 0;
}
extern void pstree_switch_state(struct pstree_item *root_item, int st);
static int page_server_serve(int sk)
{
	int ret = -1;
	bool flushed = false;
	bool receiving_pages = !opts.lazy_pages;

	if (receiving_pages) {
		/*
		 * This socket only accepts data except one thing -- it
		 * writes back the has_parent bit from time to time, so
		 * make it NODELAY all the time.
		 */
		tcp_nodelay(sk, true);

		if (pipe(cxfer.p)) {
			pr_perror("Can't make pipe for xfer");
			close(sk);
			return -1;
		}

		cxfer.pipe_size = fcntl(cxfer.p[0], F_GETPIPE_SZ, 0);
		pr_debug("Created xfer pipe size %u\n", cxfer.pipe_size);
	} else {
		pipe_read_dest_init(&pipe_read_dest);
		tcp_cork(sk, true);
	}


	/* Initialize page request queue on first use */
	init_page_request_queue();

	while (1) {
		struct page_server_iov pi;
		u32 cmd;

		ret = __recv(sk, &pi, sizeof(pi), MSG_WAITALL);

		if (!ret)
			break;

		if (ret != sizeof(pi)) {
			/*
			 * After COW bulk transfer completes, the receiver
			 * disconnects.  Treat this as success, not error.
			 */
			if (opts.cow_dump && !g_unified_thread_running) {
				pr_info("Receiver disconnected after bulk transfer\n");
				ret = 0;
			} else {
				pr_perror("Can't read pagemap from socket");
				ret = -1;
			}
			break;
		}

		flushed = false;
		cmd = decode_ps_cmd(pi.cmd);

		/* Check and print stats on each iteration */
		check_and_print_stats();

		switch (cmd) {
		case PS_IOV_OPEN:
			ps_stats.serve_open++;
			ret = page_server_open(-1, &pi);
			break;
		case PS_IOV_OPEN2:
			ps_stats.serve_open2++;
			ret = page_server_open(sk, &pi);
			break;
		case PS_IOV_PARENT:
			ps_stats.serve_parent++;
			ret = page_server_check_parent(sk, &pi);
			break;
		case PS_IOV_ADD_F_COMPRESS:
		case PS_IOV_ADD_F:
		case PS_IOV_ADD_F_PF:
		case PS_IOV_ADD:
		case PS_IOV_HOLE: {
			u32 flags;
			if (cmd == PS_IOV_ADD_F_PF)
			{
				cmd = PS_IOV_ADD_F;
				pr_err("PS_IOV_ADD_F_PF %" PRIx64 " - %" PRIx64 "\n",
		 				pi.vaddr, pi.vaddr + pi.nr_pages * PAGE_SIZE);				
			}
			if (likely(cmd == PS_IOV_ADD_F || cmd == PS_IOV_ADD_F_COMPRESS)) {
				flags = decode_ps_flags(pi.cmd);
				ps_stats.serve_add_f++;
			}
			else if (cmd == PS_IOV_ADD){
				flags = PE_PRESENT;
				ps_stats.serve_add++;
			}
			else /* PS_IOV_HOLE */
			{
				flags = PE_PARENT;
				ps_stats.serve_hole++;
			}

			ret = page_server_add(sk, &pi, flags, cmd == PS_IOV_ADD_F_COMPRESS);
			break;
			}
		case PS_IOV_CLOSE:
		case PS_IOV_FORCE_CLOSE: {
			int32_t status = 0;

			ret = 0;
			
			if (cmd == PS_IOV_CLOSE)
				ps_stats.serve_close++;
			else
				ps_stats.serve_force_close++;

			/*
			 * An answer must be sent back to inform another side,
			 * that all data were received
			 */
			pr_info("Got close; sending completion status\n");
			if (__send(sk, &status, sizeof(status), 0) != sizeof(status)) {
				pr_perror("Can't send the final package");
				ret = -1;
			}

			flushed = true;
			break;
		}
		case PS_IOV_GET:
			ps_stats.serve_get++;
			ret = page_server_get_pages(sk, &pi);
			break;
		case PS_IOV_GET_ALL:
			ps_stats.serve_get++;
			ret = page_server_get_all_pages(sk, &pi);
			break;
		default:
			pr_err("Unknown command %u\n", pi.cmd);
			ps_stats.serve_unknown++;
			ret = -1;
			break;
		}

		if (ret){
			break;
		}
		if (pi.cmd == PS_IOV_CLOSE || pi.cmd == PS_IOV_FORCE_CLOSE){
		
			break;
		}
	}

	if (receiving_pages && !ret && !flushed) {
		pr_err("The data were not flushed\n");
		ret = -1;
	}

	tls_terminate_session(ret != 0);

	if (ret == 0 && opts.ps_socket == -1) {
		char c;

		/*
		 * Wait when a remote side closes the connection
		 * to avoid TIME_WAIT bucket
		 */
		if (read(sk, &c, sizeof(c)) != 0) {
			pr_perror("Unexpected data");
			ret = -1;
		}
	}

	page_server_close();

	pr_info("Session over\n");

	close(sk);
	return ret;
}

static int fill_page_pipe(struct page_read *pr, struct page_pipe *pp)
{
	struct page_pipe_buf *ppb;
	int i, ret;

	pr->reset(pr);

	while (pr->advance(pr)) {
		unsigned long vaddr = pr->pe->vaddr;

		for (i = 0; i < pr->pe->nr_pages; i++, vaddr += PAGE_SIZE) {
			if (pagemap_in_parent(pr->pe))
				ret = page_pipe_add_hole(pp, vaddr, PP_HOLE_PARENT);
			else
				ret = page_pipe_add_page(pp, vaddr, pagemap_lazy(pr->pe) ? PPB_LAZY : 0);
			if (ret) {
				pr_err("Failed adding page at %lx\n", vaddr);
				return -1;
			}
		}
	}

	list_for_each_entry(ppb, &pp->bufs, l) {
		for (i = 0; i < ppb->nr_segs; i++) {
			struct iovec iov = ppb->iov[i];

			if (splice(img_raw_fd(pr->pi), NULL, ppb->p[1], NULL, iov.iov_len, SPLICE_F_MOVE) !=
			    iov.iov_len) {
				pr_perror("Splice failed");
				return -1;
			}
		}
	}

	debug_show_page_pipe(pp);

	return 0;
}

static int page_pipe_from_pagemap(struct page_pipe **pp, int pid)
{
	struct page_read pr;
	unsigned long nr_pages = 0;
	int ret = -1;

	if (open_page_read(pid, &pr, PR_TASK) <= 0) {
		pr_err("Failed to open page read for %d\n", pid);
		return -1;
	}

	while (pr.advance(&pr))
		if (pagemap_present(pr.pe))
			nr_pages += pr.pe->nr_pages;

	*pp = create_page_pipe(nr_pages, NULL, 0);
	if (!*pp) {
		pr_err("Cannot create page pipe for %d\n", pid);
		goto err;
	}

	if (fill_page_pipe(&pr, *pp))
		goto err_pp;

	ret = 0;
err:
	pr.close(&pr);
	return ret;
err_pp:
	destroy_page_pipe(*pp);
	*pp = NULL;
	goto err;
}

static int page_server_init_send(void)
{
	struct pstree_item *pi;
	struct page_pipe *pp;

	BUILD_BUG_ON(sizeof(struct dmp_info) > sizeof(struct rst_info));

	if (prepare_dummy_pstree())
		return -1;

	for_each_pstree_item(pi) {
		if (prepare_dummy_task_state(pi))
			return -1;

		if (!task_alive(pi))
			continue;

		if (page_pipe_from_pagemap(&pp, vpid(pi))) {
			pr_err("%d: failed to open page-read\n", vpid(pi));
			return -1;
		}

		/*
		 * prepare_dummy_pstree presumes 'restore' behaviour,
		 * but page_server_get_pages uses dmpi() to get access
		 * to the page-pipe, so we are faking it here.
		 */
		memset(rsti(pi), 0, sizeof(struct rst_info));
		dmpi(pi)->mem_pp = pp;
	}

	return 0;
}

int cr_page_server(bool daemon_mode, bool lazy_dump, int cfd)
{
	int ask = -1;
	int sk = -1;
	int ret;

	/*
	 * When running inside the dump process (lazy_dump=true), stats are
	 * already initialized by cr_dump_tasks(). Re-initializing them here
	 * would reset counters/timings and make dump stats meaningless.
	 */
	if (!lazy_dump && init_stats(DUMP_STATS))
		return -1;

	if (!opts.lazy_pages)
		up_page_ids_base();
	else if (!lazy_dump)
		if (page_server_init_send())
			return -1;

	if (opts.ps_socket != -1) {
		ask = opts.ps_socket;
		pr_info("Reusing ps socket %d\n", ask);
		goto no_server;
	}

	sk = setup_tcp_server("page", opts.addr, &opts.port);
	if (sk == -1)
		return -1;
	/* Keep listen socket for multi-TCP accepts in COW mode */
	if (opts.cow_dump)
		g_listen_sk = sk;
no_server:

	/* Serve image files in background thread if requested */
	if (opts.serve_images_port > 0) {
		static struct { int port; const char *dir; } img_args;
		pthread_t img_thread;

		img_args.port = opts.serve_images_port;
		img_args.dir = opts.imgs_dir;
		if (pthread_create(&img_thread, NULL,
				   serve_image_files_thread,
				   &img_args) == 0) {
			pthread_detach(img_thread);
			pr_err("image-xfer: serving on :%d\n",
			       opts.serve_images_port);
		}
	}

	if (!daemon_mode && cfd >= 0) {
		struct ps_info info = { .pid = getpid(), .port = opts.port };
		int count;

		count = write(cfd, &info, sizeof(info));
		close_safe(&cfd);
		if (count != sizeof(info)) {
			pr_perror("Unable to write ps_info");
			exit(1);
		}
	}

	ret = run_tcp_server(daemon_mode, &ask, cfd, sk);
	if (ret != 0)
		return ret > 0 ? 0 : -1;

	/* Enlarge socket buffers for throughput */
	if (ask >= 0) {
		int bufsize = 16 * 1024 * 1024;
		setsockopt(ask, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
	}

	if (tls_x509_init(ask, true)) {
		close_safe(&sk);
		return -1;
	}

	if (ask >= 0)
		ret = page_server_serve(ask);

	if (daemon_mode)
		exit(ret);

	return ret;
}

static int connect_to_page_server(void)
{
	if (!opts.use_page_server)
		return 0;

	if (opts.ps_socket != -1) {
		page_server_sk = opts.ps_socket;
		pr_info("Reusing ps socket %d\n", page_server_sk);
		goto out;
	}

	page_server_sk = setup_tcp_client(opts.addr);
	if (page_server_sk == -1)
		return -1;

	/* Enlarge receive buffer for throughput */
	{
		int bufsize = 16 * 1024 * 1024;
		setsockopt(page_server_sk, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
	}

	if (tls_x509_init(page_server_sk, false)) {
		close(page_server_sk);
		return -1;
	}
out:
	/*
	 * CORK the socket at the very beginning. As per ANK
	 * the corked by default socket with sporadic NODELAY-s
	 * on urgent data is the smartest mode ever.
	 */
	tcp_cork(page_server_sk, true);
	return 0;
}

int connect_to_page_server_to_send(void)
{
	return connect_to_page_server();
}

int disconnect_from_page_server(void)
{
	struct page_server_iov pi = {};
	int32_t status = -1;
	int ret = -1;

	if (!opts.use_page_server)
		return 0;

	if (page_server_sk == -1)
		return 0;

	pr_err("Disconnect from the page server\n");

	if (opts.ps_socket != -1)
		/*
		 * The socket might not get closed (held by
		 * the parent process) so we must order the
		 * page-server to terminate itself.
		 */
		pi.cmd = PS_IOV_FORCE_CLOSE;
	else
		pi.cmd = PS_IOV_CLOSE;

	if (send_psi(page_server_sk, &pi))
		goto out;

	if (__recv(page_server_sk, &status, sizeof(status), 0) != sizeof(status)) {
		pr_perror("The page server doesn't answer");
		goto out;
	}

	ret = 0;
out:
	tls_terminate_session(ret != 0);
	close_safe(&page_server_sk);

	return ret ?: status;
}

struct ps_async_read {
	unsigned long rb; /* read bytes */
	unsigned long goal;
	unsigned long nr_pages;

	struct page_server_iov pi;
	void *pages;

	ps_async_read_complete complete;
	void *priv;

	struct list_head l;

	/* Compression support */
	int compressed_size;     /* Size of compressed data (0 = uncompressed) */
	int compressed_rb;       /* Bytes read of compressed data */
	char *compressed_buf;    /* Buffer for compressed data */
	int compress_state;      /* 0=reading header, 1=reading size, 2=reading data */
};

static LIST_HEAD(async_reads);

/*
 * Per-stream state for multi-TCP bulk transfers.  Each stream has its
 * own epoll_rfd, socket, and compression state machine so that the
 * receiver can process N parallel TCP streams independently.
 */
struct bulk_stream {
	struct epoll_rfd rfd;      /* embedded for container_of */
	struct ps_async_read ar;   /* per-stream reader state */
	int sk;                    /* socket fd */
	int id;                    /* stream index for logging */
};

static struct bulk_stream bulk_streams[COW_TRANSFER_STREAMS];

static inline void async_read_set_goal(struct ps_async_read *ar, unsigned long nr_pages)
{
	ar->goal = sizeof(ar->pi) + nr_pages * PAGE_SIZE;
	ar->nr_pages = nr_pages;
}

static void init_ps_async_read(struct ps_async_read *ar, void *buf, unsigned long nr_pages, ps_async_read_complete complete,
			       void *priv)
{
	ar->pages = buf;
	ar->rb = 0;
	ar->complete = complete;
	ar->priv = priv;
	async_read_set_goal(ar, nr_pages);
}

static int page_server_start_async_read(void *buf, unsigned long nr_pages, ps_async_read_complete complete, void *priv)
{
	struct ps_async_read *ar;

	ar = xmalloc(sizeof(*ar));
	if (ar == NULL)
		return -1;

	init_ps_async_read(ar, buf, nr_pages, complete, priv);
	list_add_tail(&ar->l, &async_reads);
	return 0;
}
static struct {
	unsigned long recv_calls;
	unsigned long recv_would_block;
	unsigned long recv_bytes;
	unsigned long recv_wait_time_ns;
	unsigned long pages_completed;
	unsigned long decompress_calls;
	unsigned long decompress_time_ns;
	unsigned long callback_calls;
	time_t last_print_time;
} bulk_stats;

/*
 * Bulk mode continuous stream reader.
 * Processes headers and pages as they arrive without correlation to requests.
 * The server's background thread sends pages continuously.
 * Supports compressed pages (PS_IOV_ADD_F_COMPRESS).
 */
static int page_server_read_bulk_stream(struct ps_async_read *ar, int flags, int sk)
{
	int ret, need;
	void *buf;
	u32 cmd;

	/* Reading header */
	if (ar->compress_state == COMPRESS_STATE_READING_HEADER) {
		if (ar->rb < sizeof(ar->pi)) {
			struct timespec t_recv_start, t_recv_end;

			buf = ((void *)&ar->pi) + ar->rb;
			need = sizeof(ar->pi) - ar->rb;

			clock_gettime(CLOCK_MONOTONIC, &t_recv_start);
			bulk_stats.recv_calls++;
			ret = __recv(sk, buf, need, flags);
			clock_gettime(CLOCK_MONOTONIC, &t_recv_end);
			bulk_stats.recv_wait_time_ns += (t_recv_end.tv_sec - t_recv_start.tv_sec) * 1000000000 +
						       (t_recv_end.tv_nsec - t_recv_start.tv_nsec);
			if (ret < 0) {
				if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR)) {
					bulk_stats.recv_would_block++;
					return BULK_STREAM_WOULD_BLOCK;
				}
				pr_perror("Error reading header from page server");
				return -1;
			}
			bulk_stats.recv_bytes += ret;
			ar->rb += ret;
		}

		if (ar->rb == sizeof(ar->pi)) {
			cmd = decode_ps_cmd(ar->pi.cmd);

			if (ar->pi.nr_pages == 0) {
					g_bulk_streams_closed++;
					pr_err("End-of-stream marker (%d/%d closed)\n",
					       g_bulk_streams_closed,
					       COW_TRANSFER_STREAMS);
					if (g_bulk_streams_closed >= COW_TRANSFER_STREAMS) {
						bulk_stream_done = true;
						/* Signal restore.sh that all
						 * pages have been received */
						if (opts.imgs_dir) {
							char p[PATH_MAX];
							int fd;

							snprintf(p, sizeof(p),
								 "%s/bulk_stream_done",
								 opts.imgs_dir);
							fd = open(p, O_CREAT |
								  O_WRONLY |
								  O_TRUNC, 0644);
							if (fd >= 0)
								close(fd);
						}
					}
					return BULK_STREAM_COMPLETE;
				}

			if (cmd == PS_IOV_ADD_F_COMPRESS) {
				ar->compress_state = COMPRESS_STATE_READING_SIZE;
				ar->compressed_size = 0;
				ar->compressed_rb = 0;
			} else {
				ar->compress_state = COMPRESS_STATE_READING_UNCOMPRESSED;
				ar->goal = sizeof(ar->pi) + ar->pi.nr_pages * PAGE_SIZE;
			}
		}

		return BULK_STREAM_PROGRESS;
	}

	/* Reading compressed size */
	if (ar->compress_state == COMPRESS_STATE_READING_SIZE) {
		struct timespec t_recv_start, t_recv_end;

		need = sizeof(ar->compressed_size) - ar->compressed_rb;
		buf = ((char *)&ar->compressed_size) + ar->compressed_rb;

		clock_gettime(CLOCK_MONOTONIC, &t_recv_start);
		bulk_stats.recv_calls++;
		ret = __recv(sk, buf, need, flags);
		clock_gettime(CLOCK_MONOTONIC, &t_recv_end);
		bulk_stats.recv_wait_time_ns += (t_recv_end.tv_sec - t_recv_start.tv_sec) * 1000000000 +
					       (t_recv_end.tv_nsec - t_recv_start.tv_nsec);
		if (ret < 0) {
			if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR)) {
				bulk_stats.recv_would_block++;
				return BULK_STREAM_WOULD_BLOCK;
			}
			pr_perror("Error reading compressed size");
			return -1;
		}
		bulk_stats.recv_bytes += ret;
		ar->compressed_rb += ret;

		if (ar->compressed_rb == sizeof(ar->compressed_size)) {
			if (ar->compressed_size <= 0 || ar->compressed_size > LZ4_compressBound(PAGE_SIZE)) {
				pr_err("Invalid compressed size: %d\n", ar->compressed_size);
				return -1;
			}

			ar->compressed_buf = xmalloc(ar->compressed_size);
			if (!ar->compressed_buf) {
				pr_err("Failed to allocate compressed buffer\n");
				return -1;
			}

			ar->compressed_rb = 0;
			ar->compress_state = COMPRESS_STATE_READING_COMPRESSED;
		}

		return BULK_STREAM_PROGRESS;
	}

	/* Reading compressed data */
	if (ar->compress_state == COMPRESS_STATE_READING_COMPRESSED) {
		struct timespec t_recv_start, t_recv_end;

		need = ar->compressed_size - ar->compressed_rb;
		buf = ar->compressed_buf + ar->compressed_rb;

		clock_gettime(CLOCK_MONOTONIC, &t_recv_start);
		bulk_stats.recv_calls++;
		ret = __recv(sk, buf, need, flags);
		clock_gettime(CLOCK_MONOTONIC, &t_recv_end);
		bulk_stats.recv_wait_time_ns += (t_recv_end.tv_sec - t_recv_start.tv_sec) * 1000000000 +
					       (t_recv_end.tv_nsec - t_recv_start.tv_nsec);
		if (ret < 0) {
			if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR)) {
				bulk_stats.recv_would_block++;
				return BULK_STREAM_WOULD_BLOCK;
			}
			pr_perror("Error reading compressed data");
			xfree(ar->compressed_buf);
			ar->compressed_buf = NULL;
			return -1;
		}
		bulk_stats.recv_bytes += ret;
		ar->compressed_rb += ret;

		if (ar->compressed_rb == ar->compressed_size) {
			int decomp_ret;
			struct timespec t1, t2;

			clock_gettime(CLOCK_MONOTONIC, &t1);
			decomp_ret = LZ4_decompress_safe(ar->compressed_buf, ar->pages, ar->compressed_size,
							 PAGE_SIZE);
			clock_gettime(CLOCK_MONOTONIC, &t2);
			bulk_stats.decompress_calls++;
			bulk_stats.decompress_time_ns +=
				(t2.tv_sec - t1.tv_sec) * 1000000000 + (t2.tv_nsec - t1.tv_nsec);
			xfree(ar->compressed_buf);
			ar->compressed_buf = NULL;

			if (decomp_ret != PAGE_SIZE) {
				pr_err("LZ4 decompression failed: expected %lu, got %d\n",
				       PAGE_SIZE, decomp_ret);
				return -1;
			}

			bulk_stats.callback_calls++;
			bulk_stats.pages_completed++;
			ret = ar->complete((int)ar->pi.dst_id, (unsigned long)ar->pi.vaddr, (int)ar->pi.nr_pages,
					   ar->priv);
			if (ret < 0)
				return ret;

			ar->rb = 0;
			ar->goal = 0;
			ar->compress_state = COMPRESS_STATE_READING_HEADER;
			ar->compressed_size = 0;
			ar->compressed_rb = 0;
		}

		return BULK_STREAM_PROGRESS;
	}

	/* Reading uncompressed page data */
	if (ar->compress_state == COMPRESS_STATE_READING_UNCOMPRESSED) {
		struct timespec t_recv_start, t_recv_end;

		buf = ar->pages + (ar->rb - sizeof(ar->pi));
		need = ar->goal - ar->rb;

		clock_gettime(CLOCK_MONOTONIC, &t_recv_start);
		bulk_stats.recv_calls++;
		ret = __recv(sk, buf, need, flags);
		clock_gettime(CLOCK_MONOTONIC, &t_recv_end);
		bulk_stats.recv_wait_time_ns += (t_recv_end.tv_sec - t_recv_start.tv_sec) * 1000000000 +
					       (t_recv_end.tv_nsec - t_recv_start.tv_nsec);
		if (ret < 0) {
			if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR)) {
				bulk_stats.recv_would_block++;
				return BULK_STREAM_WOULD_BLOCK;
			}
			pr_perror("Error reading uncompressed page data");
			return -1;
		}
		bulk_stats.recv_bytes += ret;
		ar->rb += ret;

		if (ar->rb == ar->goal) {
			bulk_stats.callback_calls++;
			bulk_stats.pages_completed++;
			ret = ar->complete((int)ar->pi.dst_id, (unsigned long)ar->pi.vaddr, (int)ar->pi.nr_pages,
					   ar->priv);
			if (ret < 0)
				return ret;

			ar->rb = 0;
			ar->goal = 0;
			ar->compress_state = COMPRESS_STATE_READING_HEADER;
		}

		return BULK_STREAM_PROGRESS;
	}

	return BULK_STREAM_PROGRESS;
}

/* Bulk stream statistics */


static void check_and_print_bulk_stats(void)
{
	time_t now = time(NULL);
	
	if (now - bulk_stats.last_print_time >= 1) {
		struct timespec ts;
		struct tm *tm;
		clock_gettime(CLOCK_REALTIME, &ts);
		tm = localtime(&ts.tv_sec);
		pr_debug("[BULK_RECV_STATS] [%02d:%02d:%02d.%03ld] recv=%lu block=%lu bytes=%lu recv_wait_ns=%lu pages=%lu decomp=%lu decomp_ns=%lu cb=%lu\n",
			tm->tm_hour, tm->tm_min, tm->tm_sec, ts.tv_nsec / 1000000,
			bulk_stats.recv_calls,
			bulk_stats.recv_would_block,
			bulk_stats.recv_bytes,
			bulk_stats.recv_wait_time_ns,
			bulk_stats.pages_completed,
			bulk_stats.decompress_calls,
			bulk_stats.decompress_time_ns,
			bulk_stats.callback_calls);
		
		memset(&bulk_stats, 0, sizeof(bulk_stats));
		bulk_stats.last_print_time = now;
	}
}

static int page_server_async_read_bulk(struct epoll_rfd *f)
{
	struct bulk_stream *bs = container_of(f, struct bulk_stream, rfd);
	int ret;

	check_and_print_bulk_stats();
	ret = page_server_read_bulk_stream(&bs->ar, MSG_DONTWAIT, bs->sk);

	if (ret == BULK_STREAM_COMPLETE)
		return 0;
	return (ret < 0) ? -1 : 0;
}

static bool bulk_readers_initialized;

int page_server_init_bulk_readers(void *buf, unsigned long nr_pages,
				  ps_async_read_complete complete, void *priv)
{
	int i;

	/*
	 * Only initialize once.  This function is called for every
	 * page fault, but the bulk readers must not be reset while
	 * they are actively parsing stream data — doing so would
	 * desynchronize the stream state machine.
	 */
	if (bulk_readers_initialized)
		return 0;
	bulk_readers_initialized = true;

	for (i = 0; i < COW_TRANSFER_STREAMS; i++) {
		struct ps_async_read *ar = &bulk_streams[i].ar;

		if (bulk_streams[i].sk <= 0)
			break;

		/*
		 * Each stream needs its own decompress buffer to avoid
		 * overwriting another stream's data between decompress
		 * and the UFFDIO_COPY callback.
		 */
		if (i == 0)
			ar->pages = buf;
		else {
			ar->pages = xmalloc(PAGE_SIZE);
			if (!ar->pages) {
				pr_err("Failed to alloc stream %d buf\n", i);
				return -1;
			}
		}
		ar->rb = 0;
		ar->goal = 0;
		ar->nr_pages = nr_pages;
		ar->complete = complete;
		ar->priv = priv;
		ar->compress_state = COMPRESS_STATE_READING_HEADER;
		ar->compressed_size = 0;
		ar->compressed_rb = 0;
		ar->compressed_buf = NULL;
	}

	return 0;
}

/*
 * There are two possible event types we need to handle:
 * - page info is available as a reply to request_remote_page
 * - page data is available, and it follows page info we've just received
 * Since the on dump side communications are completely synchronous,
 * we can return to epoll right after the reception of page info and
 * for sure the next time socket event will occur we'll get page data
 * related to info we've just received
 */
static int page_server_read(struct ps_async_read *ar, int flags)
{
	int ret, need;
	void *buf;

	if (ar->rb < sizeof(ar->pi)) {
		/* Header */
		buf = ((void *)&ar->pi) + ar->rb;
		need = sizeof(ar->pi) - ar->rb;
	} else {
		/* page-serer may return less pages than we asked for */
		if (ar->pi.nr_pages < ar->nr_pages)
			async_read_set_goal(ar, ar->pi.nr_pages);
		/* Page(s) data itself */
		buf = ar->pages + (ar->rb - sizeof(ar->pi));
		need = ar->goal - ar->rb;
	}

	ret = __recv(page_server_sk, buf, need, flags);
	if (ret < 0) {
		if (flags == MSG_DONTWAIT && (errno == EAGAIN || errno == EINTR)) {
			ret = 0;
		} else {
			pr_perror("Error reading data from page server");
			return -1;
		}
	}

	ar->rb += ret;
	if (ar->rb < ar->goal)
		return 1;

	/*
	 * IO complete -- notify the caller and drop the request
	 */
	BUG_ON(ar->rb > ar->goal);
	return ar->complete((int)ar->pi.dst_id, (unsigned long)ar->pi.vaddr, (int)ar->pi.nr_pages, ar->priv);
}

static int page_server_async_read(struct epoll_rfd *f)
{
	struct ps_async_read *ar;
	int ret;

	BUG_ON(list_empty(&async_reads));
	ar = list_first_entry(&async_reads, struct ps_async_read, l);
	ret = page_server_read(ar, MSG_DONTWAIT);

	if (ret > 0)
		return 0;
	if (!ret) {
		list_del(&ar->l);
		xfree(ar);
	}

	return ret;
}

static int page_server_hangup_event(struct epoll_rfd *rfd)
{
	if (opts.cow_dump) {
		/*
		 * In multi-TCP COW mode, individual streams close
		 * after sending their close marker.  Accept hangups
		 * as long as we've received at least one close marker.
		 */
		if (g_bulk_streams_closed > 0) {
			pr_info("Page server stream closed (%d/%d)\n",
				g_bulk_streams_closed, COW_TRANSFER_STREAMS);
			return 0;
		}
	}
	pr_err("Remote side closed connection\n");
	return -1;
}

int connect_to_page_server_to_recv(int epfd)
{
	int i;

	if (connect_to_page_server())
		return -1;
	bulk_stream_done = false;
	g_bulk_streams_closed = 0;
	bulk_readers_initialized = false;

	memset(bulk_streams, 0, sizeof(bulk_streams));

	/* Stream 0: the main page_server_sk */
	bulk_streams[0].sk = page_server_sk;
	bulk_streams[0].id = 0;
	bulk_streams[0].rfd.fd = page_server_sk;
	if (opts.cow_dump)
		bulk_streams[0].rfd.read_event = page_server_async_read_bulk;
	else
		bulk_streams[0].rfd.read_event = page_server_async_read;
	bulk_streams[0].rfd.hangup_event = page_server_hangup_event;

	if (epoll_add_rfd(epfd, &bulk_streams[0].rfd))
		return -1;

	/* Multi-TCP: additional connections for COW bulk mode */
	if (opts.cow_dump && COW_TRANSFER_STREAMS > 1) {
		for (i = 1; i < COW_TRANSFER_STREAMS; i++) {
			int sk = -1, retry;

			for (retry = 0; retry < 10 && sk < 0; retry++) {
				sk = setup_tcp_client(opts.addr);
				if (sk < 0)
					usleep(100000);
			}
			if (sk < 0) {
				pr_warn("Multi-TCP: stream %d connect failed after %d retries\n",
					i, retry);
				break;
			}
			bulk_streams[i].sk = sk;
			bulk_streams[i].id = i;
			bulk_streams[i].rfd.fd = sk;
			bulk_streams[i].rfd.read_event = page_server_async_read_bulk;
			bulk_streams[i].rfd.hangup_event = page_server_hangup_event;
			if (epoll_add_rfd(epfd, &bulk_streams[i].rfd)) {
				close(sk);
				bulk_streams[i].sk = 0;
				break;
			}
			pr_info("Multi-TCP: connected stream %d\n", i);
		}
	}

	return 0;
}

int request_remote_pages(unsigned long img_id, unsigned long addr, unsigned long nr_pages)
{
	struct page_server_iov pi = {
		.cmd = PS_IOV_GET,
		.nr_pages = nr_pages,
		.vaddr = addr,
		.dst_id = img_id,
	};

	/* XXX: why MSG_DONTWAIT here? */
	if (send_psi_flags(page_server_sk, &pi, MSG_DONTWAIT))
		return -1;

	tcp_nodelay(page_server_sk, true);
	return 0;
}

int request_all_remote_pages(unsigned long img_id)
{
	struct page_server_iov pi = {
		.cmd = PS_IOV_GET_ALL,
		.nr_pages = 0,  /* Not used in batch mode */
		.vaddr = 0,     /* Not used in batch mode */
		.dst_id = img_id,
	};

	pr_info("Requesting all pages for img_id=%lu in batch mode\n", img_id);

	if (send_psi_flags(page_server_sk, &pi, MSG_DONTWAIT))
		return -1;

	tcp_nodelay(page_server_sk, true);
	return 0;
}

static int page_server_start_sync_read(void *buf, unsigned long nr, ps_async_read_complete complete, void *priv)
{
	struct ps_async_read ar;
	int ret = 1;

	init_ps_async_read(&ar, buf, nr, complete, priv);
	while (ret == 1)
		ret = page_server_read(&ar, MSG_WAITALL);
	return ret;
}

int page_server_start_read(void *buf, unsigned long nr, ps_async_read_complete complete, void *priv, unsigned flags)
{
	/* In bulk mode, use continuous stream reader */
	pr_debug("page_server_start_read\n");

	if (opts.cow_dump) {
		if (flags & PR_ASYNC)
			return page_server_init_bulk_readers(buf, nr, complete, priv);
		else {
			pr_err("Bulk mode doesn't support synchronous reads\n");
			return -1;
		}
	}
	
	/* On-demand mode: traditional request/response */
	if (flags & PR_ASYNC)
		return page_server_start_async_read(buf, nr, complete, priv);
	else
		return page_server_start_sync_read(buf, nr, complete, priv);
}
