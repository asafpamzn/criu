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
#include "rst_info.h"
#include "stats.h"
#include "tls.h"
#include "uffd.h"
#include "cow-dump.h"
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

struct vma_diff_entry {
	u64 start;
	u64 end;
	u32 prot;
	u32 pad;
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

	/* Track compression statistics */
	g_compress_uncompressed_bytes += PAGE_SIZE;
	g_compress_compressed_bytes += *compressed_size;

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
	

	/* In COW dump mode, we need to interleave lazy VMA entries with pipe entries */
	if (opts.cow_dump) {
		pr_info("Writing pagemap entries (interleaved mode) for dst_id=%lu\n", 
			(unsigned long)xfer->dst_id);
	}

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

	/* Detect libc rw- range for exclusion */
	detect_libc_rw_range(source_pid);

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

			g_compress_uncompressed_bytes += PAGE_SIZE;
			g_compress_compressed_bytes += (clen < PAGE_SIZE) ? clen : PAGE_SIZE;

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
 * Send a raw RESP command to the local Valkey server to pause
 * or unpause client writes.  Used during convergence to stop
 * the write storm so dirty pages drop to zero.
 */
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
#define CONVERGE_FREEZE_THRESHOLD	4096	/* pages — freeze when below this */
#define CONVERGE_STALL_ROUNDS		3	/* freeze after N rounds with <5% improvement */

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
		usleep(1000);  /* let kernel finish in-flight faults */

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
 * libc rw- exclusion range.  Detected once from /proc/PID/maps.
 * Convergence skips these pages — the bulk transfer sends them
 * from the clean dump state and we never overwrite them.
 * This prevents the glibc main_arena from being corrupted by
 * temporally inconsistent convergence page reads.
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

/* ---- Fork snapshot: ptrace helpers for convergence ---- */

#include <sys/ptrace.h>
#include <linux/elf.h>
#include <sys/syscall.h>

/*
 * Fork injection code: 16 bytes.
 *
 * After clone(), the parent (x0 = child_pid > 0) falls through
 * to BRK which ptrace catches.  The child (x0 = 0) branches to
 * an infinite loop, staying alive for process_vm_readv.
 *
 * aarch64:  SVC #0         → clone syscall
 *           CBZ x0, +8     → child: skip BRK, go to loop
 *           BRK #0         → parent: ptrace trap
 *           B .            → child: infinite loop (SIGSTOP'd later)
 *
 * x86-64:   syscall        → clone
 *           test eax,eax   → check return
 *           jz +1          → child: jump to loop
 *           int3           → parent: ptrace trap
 *           jmp -2         → child: infinite loop
 */
#ifdef __aarch64__
static const unsigned char fork_syscall_insn[16] = {
	0x01, 0x00, 0x00, 0xd4,	/* svc #0           */
	0x40, 0x00, 0x00, 0xb4,	/* cbz x0, +8 (→B.) */
	0x00, 0x00, 0x20, 0xd4,	/* brk #0           */
	0x00, 0x00, 0x00, 0x14		/* b .   (loop)     */
};
#elif defined(__x86_64__)
static const unsigned char fork_syscall_insn[16] = {
	0x0f, 0x05,			/* syscall      */
	0x85, 0xc0,			/* test eax,eax */
	0x74, 0x01,			/* jz +1 (→jmp) */
	0xcc,				/* int3 (parent)*/
	0xeb, 0xfe,			/* jmp -2 (loop)*/
	0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc
};
#endif

static int xfer_get_regs(pid_t pid, user_regs_struct_t *regs)
{
	struct iovec iov = { .iov_base = regs, .iov_len = sizeof(*regs) };

	return ptrace(PTRACE_GETREGSET, pid, NT_PRSTATUS, &iov) ? -1 : 0;
}

static int xfer_set_regs(pid_t pid, user_regs_struct_t *regs)
{
	struct iovec iov = { .iov_base = regs, .iov_len = sizeof(*regs) };

	return ptrace(PTRACE_SETREGSET, pid, NT_PRSTATUS, &iov) ? -1 : 0;
}

/*
 * Fork the source process by injecting clone(SIGCHLD) via ptrace.
 * The source MUST be SIGSTOP'd before calling this.
 * Returns the fork child PID, or -1 on failure.
 * On success, the child is SIGSTOP'd and the source is still stopped
 * (caller must SIGCONT the source after setting up the fork read).
 */
static pid_t fork_source_snapshot(pid_t source_pid)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[16];
	unsigned long pc;
	pid_t fork_pid;
	int status;

	/* Attach via ptrace */
	if (ptrace(PTRACE_SEIZE, source_pid, NULL, 0)) {
		pr_perror("fork_snapshot: PTRACE_SEIZE %d", source_pid);
		return -1;
	}

	if (ptrace(PTRACE_INTERRUPT, source_pid, NULL, NULL)) {
		pr_perror("fork_snapshot: PTRACE_INTERRUPT %d", source_pid);
		goto detach;
	}

	if (waitpid(source_pid, &status, __WALL) != source_pid) {
		pr_perror("fork_snapshot: waitpid after interrupt");
		goto detach;
	}

	if (xfer_get_regs(source_pid, &orig_regs)) {
		pr_err("fork_snapshot: get regs failed\n");
		goto detach;
	}

#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
#else
	pc = (unsigned long)orig_regs.ip;
#endif

	if (ptrace_peek_area(source_pid, orig_code, (void *)pc,
			     sizeof(orig_code))) {
		pr_err("fork_snapshot: peek code failed\n");
		goto detach;
	}

	if (ptrace_poke_area(source_pid, (void *)fork_syscall_insn,
			     (void *)pc, sizeof(fork_syscall_insn))) {
		pr_err("fork_snapshot: poke code failed\n");
		goto restore_code;
	}

	/* Set up clone(SIGCHLD) — equivalent to fork() */
	regs = orig_regs;
#ifdef __aarch64__
	regs.regs[8] = __NR_clone;
	regs.regs[0] = SIGCHLD;
	regs.regs[1] = 0;
	regs.regs[2] = 0;
	regs.regs[3] = 0;
	regs.regs[4] = 0;
	regs.pc = pc;
#else
	regs.ax = __NR_clone;
	regs.di = SIGCHLD;
	regs.si = 0;
	regs.dx = 0;
	regs.r10 = 0;
	regs.r8 = 0;
	regs.ip = pc;
#endif

	if (xfer_set_regs(source_pid, &regs)) {
		pr_err("fork_snapshot: set regs failed\n");
		goto restore_code;
	}

	if (ptrace(PTRACE_CONT, source_pid, NULL, NULL)) {
		pr_perror("fork_snapshot: PTRACE_CONT");
		goto restore_all;
	}

	/*
	 * The source was SIGSTOP'd before PTRACE_SEIZE.  After
	 * PTRACE_CONT, the kernel may re-deliver the pending
	 * group-stop before the clone executes.  If we see
	 * SIGSTOP, suppress it and continue.
	 */
	while (1) {
		if (waitpid(source_pid, &status, __WALL) != source_pid) {
			pr_perror("fork_snapshot: waitpid after clone");
			goto restore_all;
		}

		if (WIFSTOPPED(status) &&
		    WSTOPSIG(status) == SIGTRAP)
			break;  /* clone executed, hit BRK trap */

		if (WIFSTOPPED(status) &&
		    (WSTOPSIG(status) == SIGSTOP ||
		     WSTOPSIG(status) == (SIGTRAP | 0x80))) {
			/* Suppress and retry */
			if (ptrace(PTRACE_CONT, source_pid, NULL, NULL)) {
				pr_perror("fork_snapshot: re-CONT");
				goto restore_all;
			}
			continue;
		}

		pr_err("fork_snapshot: unexpected status %x\n", status);
		goto restore_all;
	}

	/* Read fork PID from return value */
	if (xfer_get_regs(source_pid, &regs)) {
		pr_err("fork_snapshot: get result regs failed\n");
		goto restore_all;
	}

#ifdef __aarch64__
	fork_pid = (pid_t)regs.regs[0];
#else
	fork_pid = (pid_t)regs.ax;
#endif

	if (fork_pid <= 0) {
		pr_err("fork_snapshot: clone returned %d\n", fork_pid);
		goto restore_all;
	}

	/* Stop the child immediately */
	kill(fork_pid, SIGSTOP);
	usleep(1000);

	/* Restore original code and registers */
	if (ptrace_poke_area(source_pid, orig_code, (void *)pc,
			     sizeof(orig_code)))
		pr_err("fork_snapshot: restore code failed\n");
	if (xfer_set_regs(source_pid, &orig_regs))
		pr_err("fork_snapshot: restore regs failed\n");

	ptrace(PTRACE_DETACH, source_pid, NULL, NULL);

	pr_err("COW converge: forked snapshot PID=%d from source %d\n",
	       fork_pid, source_pid);

	return fork_pid;

restore_all:
	xfer_set_regs(source_pid, &orig_regs);
restore_code:
	if (ptrace_poke_area(source_pid, orig_code, (void *)pc,
			     sizeof(orig_code)))
		pr_err("fork_snapshot: restore code failed (cleanup)\n");
detach:
	ptrace(PTRACE_DETACH, source_pid, NULL, NULL);
	return -1;
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

	/* libc rw- pages are now properly re-sent from the fork */

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
static int cow_converge_dirty_pages_parallel(struct active_image *img,
					     pid_t source_pid,
					     int *sockets, int nr_streams,
					     pid_t bulk_fork_pid)
{
	struct converge_region *regions;
	unsigned long iteration, prev_dirty = 0;
	int stall_count = 0;

	if (!cow_is_wp_async() || nr_streams < 1)
		return 0;

	detect_libc_rw_range(source_pid);

	regions = xmalloc(CONVERGE_MAX_REGIONS * sizeof(*regions));
	if (!regions)
		return -1;

	/*
	 * Skip iterative convergence: go straight to the fork-based
	 * final freeze.  Convergence creates cross-round temporal
	 * inconsistency (pages from different time points → corrupted
	 * allocator metadata).  The fork reads ALL dirty-since-dump
	 * pages from a single consistent COW snapshot.
	 */
	pr_err("COW converge: skipping iterative convergence, "
	       "using fork-based consistency snapshot\n");
	iteration = 0;

	for (; 0; iteration++) {
		struct lazy_vma_entry *lve;
		unsigned long total_dirty = 0;
		struct converge_region *all_dirty = NULL;
		int all_dirty_count = 0, all_dirty_cap = 0;

		/* Phase 1: scan all VMAs, collect dirty regions */
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

				/* Accumulate regions */
				if (all_dirty_count + nr_regions > all_dirty_cap) {
					int new_cap = (all_dirty_cap + nr_regions) * 2;
					struct converge_region *tmp;
					tmp = xrealloc(all_dirty,
						       new_cap * sizeof(*tmp));
					if (!tmp)
						goto err;
					all_dirty = tmp;
					all_dirty_cap = new_cap;
				}
				for (r = 0; r < nr_regions; r++) {
					all_dirty[all_dirty_count++] = regions[r];
					total_dirty += (regions[r].end - regions[r].start)
						       / PAGE_SIZE;
				}

				scan_pos = walk_end;
				if (walk_end >= lve->end)
					break;
			}
		}

		pr_err("COW converge iter %lu: %lu dirty pages across %d regions\n",
		       iteration, total_dirty, all_dirty_count);

		if (total_dirty == 0)
			break;

		/* Dynamic freeze trigger: threshold or stall */
		if (total_dirty <= CONVERGE_FREEZE_THRESHOLD) {
			pr_err("COW converge: below freeze threshold (%lu <= %d), "
			       "triggering final freeze\n",
			       total_dirty, CONVERGE_FREEZE_THRESHOLD);
			xfree(all_dirty);
			break;
		}
		if (iteration >= 2 && prev_dirty > 0) {
			long improvement = (long)prev_dirty - (long)total_dirty;
			if (improvement < 0 ||
			    (unsigned long)improvement < prev_dirty / 20) {
				stall_count++;
				pr_err("COW converge: stall %d/%d "
				       "(prev=%lu cur=%lu)\n",
				       stall_count, CONVERGE_STALL_ROUNDS,
				       prev_dirty, total_dirty);
				if (stall_count >= CONVERGE_STALL_ROUNDS) {
					pr_err("COW converge: stall limit hit, "
					       "triggering final freeze\n");
					xfree(all_dirty);
					break;
				}
			} else {
				stall_count = 0;
			}
		}
		prev_dirty = total_dirty;

		/* Phase 2: parallel send — regions round-robin to workers */
		if (all_dirty_count > 0) {
			long sent = converge_dispatch_parallel(
				img, source_pid, sockets, nr_streams,
				all_dirty, all_dirty_count);
			if (sent < 0) {
				xfree(all_dirty);
				goto err;
			}
			pr_err("COW converge iter %lu: sent %ld pages\n",
			       iteration, sent);
		}

		xfree(all_dirty);
	}

	/*
	 * Final freeze: SIGSTOP source, scan remaining dirty pages,
	 * send across all streams, SIGCONT.  The libc rw- exclusion
	 * ensures the glibc arena stays clean from the dump snapshot.
	 */
	{
		struct lazy_vma_entry *lve;
		struct converge_region *freeze_dirty = NULL;
		int freeze_dirty_count = 0, freeze_dirty_cap = 0;
		unsigned long freeze_pages = 0;
		pid_t fork_pid = -1;

		/*
		 * Pre-freeze hook: quiesce application writes so all
		 * allocator locks are released before SIGSTOP.
		 */
		{
			const char *pre_freeze_cmd = getenv("COW_PRE_FREEZE_CMD");
			if (pre_freeze_cmd) {
				int rc = system(pre_freeze_cmd);
				pr_err("COW converge: pre-freeze hook rc=%d\n", rc);
			}
		}

		/*
		 * Wait for ALL threads to be idle (in a wait syscall).
		 * Main thread: epoll_pwait (22) or ppoll (73)
		 * Worker threads: futex (98) = pthread_cond_wait
		 * If any thread is NOT in a wait syscall, it may be
		 * mid-malloc with allocator locks held.
		 */
		{
			char task_dir[64];
			int attempts;

			snprintf(task_dir, sizeof(task_dir),
				 "/proc/%d/task", source_pid);

			for (attempts = 0; attempts < 1000; attempts++) {
				DIR *dir;
				struct dirent *de;
				int all_idle = 1;

				dir = opendir(task_dir);
				if (!dir)
					break;

				while ((de = readdir(dir)) != NULL) {
					char sc_path[PATH_MAX];
					char buf[256];
					int fd, n, sc;

					if (de->d_name[0] == '.')
						continue;

					snprintf(sc_path, sizeof(sc_path),
						 "/proc/%d/task/%s/syscall",
						 source_pid, de->d_name);
					fd = open(sc_path, O_RDONLY);
					if (fd < 0)
						continue;
					n = read(fd, buf, sizeof(buf) - 1);
					close(fd);
					if (n <= 0)
						continue;
					buf[n] = '\0';
					sc = atoi(buf);
					/*
					 * Idle syscalls on aarch64:
					 *   22 = epoll_pwait
					 *   73 = ppoll
					 *   98 = futex (pthread_cond_wait)
					 *  101 = nanosleep
					 *  115 = clock_nanosleep
					 */
					if (sc != 22 && sc != 73 &&
					    sc != 98 && sc != 101 &&
					    sc != 115) {
						all_idle = 0;
						break;
					}
				}
				closedir(dir);

				if (all_idle)
					break;
				usleep(1000); /* 1ms between polls */
			}
			pr_err("COW converge: all threads settled "
			       "after %d checks\n", attempts);
		}

		/* SIGSTOP — source should be idle after hook */
		kill(source_pid, SIGSTOP);
		usleep(1000);

		/* Step 4: Final scan — should be near-zero */
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

				if (freeze_dirty_count + nr_regions > freeze_dirty_cap) {
					int new_cap = (freeze_dirty_cap + nr_regions) * 2;
					struct converge_region *tmp;
					tmp = xrealloc(freeze_dirty,
						       new_cap * sizeof(*tmp));
					if (!tmp)
						break;
					freeze_dirty = tmp;
					freeze_dirty_cap = new_cap;
				}
				for (r = 0; r < nr_regions; r++) {
					freeze_dirty[freeze_dirty_count++] = regions[r];
					freeze_pages += (regions[r].end - regions[r].start)
							/ PAGE_SIZE;
				}

				scan_pos = walk_end;
				if (walk_end >= lve->end)
					break;
			}
		}

		/*
		 * Fork snapshot: create a COW copy of the source for
		 * consistent page reads, then resume the source.
		 * The fork lives only ~2s while we read dirty pages.
		 * COW overhead: ~672 MB (vs 60 GB for bgsave).
		 */
		{
			pid_t read_pid;

			fork_pid = -1;

			if (freeze_dirty_count > 0)
				fork_pid = fork_source_snapshot(source_pid);

			if (fork_pid > 0) {
				/* Resume source immediately — fork has the snapshot */
				kill(source_pid, SIGCONT);
				read_pid = fork_pid;
				pr_err("COW converge: reading %lu dirty pages "
				       "from fork %d (source resumed)\n",
				       freeze_pages, fork_pid);
			} else {
				/* Fork failed — fall back to reading from frozen source */
				read_pid = source_pid;
				if (freeze_dirty_count > 0)
					pr_err("COW converge: fork failed, "
					       "reading from frozen source\n");
			}

			if (freeze_dirty_count > 0) {
				long sent = converge_dispatch_parallel(
					img, read_pid, sockets, nr_streams,
					freeze_dirty, freeze_dirty_count);
				if (sent >= 0)
					freeze_pages = sent;
			}

			/*
			 * Re-send libc rw- pages from fork.
			 * The bulk transfer may have read these
			 * mid-traffic with stale arena state.
			 * Reading from the fork (quiesced snapshot)
			 * gives consistent arena bins/fastbins.
			 */
			if (fork_pid > 0) {
				char lmaps[64];
				FILE *lmfp;

				snprintf(lmaps, sizeof(lmaps),
					 "/proc/%d/maps", fork_pid);
				lmfp = fopen(lmaps, "r");
				if (lmfp) {
					char ll[512];

					while (fgets(ll, sizeof(ll), lmfp)) {
						unsigned long ls, le;
						char lp[8], lpath[256];
						struct converge_region lr;

						lpath[0] = '\0';
						if (sscanf(ll,
							   "%lx-%lx %4s %*s %*s %*s %255[^\n]",
							   &ls, &le, lp,
							   lpath) < 3)
							continue;
						if (lp[0] != 'r' ||
						    lp[1] != 'w' ||
						    !strstr(lpath, "libc.so"))
							continue;
						lr.start = ls;
						lr.end = le;
						lr.categories = 0;
						converge_dispatch_parallel(
							img, fork_pid,
							sockets, nr_streams,
							&lr, 1);
						pr_err("COW converge: "
						       "re-sent libc rw- "
						       "%lx-%lx (%lu pages) "
						       "from fork\n",
						       ls, le,
						       (le - ls) / PAGE_SIZE);
					}
					fclose(lmfp);
				}
			}

			/* Fork kept alive for allocator re-send below */
		}

		pr_err("COW converge fork-snapshot: %lu dirty pages "
		       "across %d streams (%d regions)\n",
		       freeze_pages, nr_streams, freeze_dirty_count);

		/*
		 * Post-fork convergence round: pick up pages dirtied
		 * while we read from the fork (~2s, ~86K pages).
		 * Source is running at this point.
		 */
		if (freeze_dirty_count > 0) {
			struct lazy_vma_entry *lve2;
			struct converge_region *post_dirty = NULL;
			int post_count = 0, post_cap = 0;
			unsigned long post_pages = 0;

			list_for_each_entry(lve2, get_global_lazy_vmas(), list) {
				unsigned long scan_pos;

				if (lve2->dst_id != img->dst_id)
					continue;

				scan_pos = lve2->start;
				while (scan_pos < lve2->end) {
					unsigned long walk_end = 0;
					int nr_r, rr;

					nr_r = cow_scan_dirty_pages(
						source_pid, scan_pos,
						lve2->end, regions,
						CONVERGE_MAX_REGIONS,
						&walk_end);
					if (nr_r <= 0)
						break;

					if (post_count + nr_r > post_cap) {
						int nc = (post_cap + nr_r) * 2;
						struct converge_region *tmp;

						tmp = xrealloc(post_dirty,
							nc * sizeof(*tmp));
						if (!tmp)
							break;
						post_dirty = tmp;
						post_cap = nc;
					}
					for (rr = 0; rr < nr_r; rr++) {
						post_dirty[post_count++] =
							regions[rr];
						post_pages +=
							(regions[rr].end -
							 regions[rr].start) /
							PAGE_SIZE;
					}

					scan_pos = walk_end;
					if (walk_end >= lve2->end)
						break;
				}
			}

			if (post_count > 0) {
				long sent;

				pr_err("COW converge post-fork: %lu dirty "
				       "pages (%d regions)\n",
				       post_pages, post_count);

				sent = converge_dispatch_parallel(
					img, source_pid, sockets,
					nr_streams, post_dirty,
					post_count);
				if (sent >= 0)
					pr_err("COW converge post-fork: "
					       "sent %ld pages\n", sent);
			}
			xfree(post_dirty);
		}

		/*
		 * Second freeze: capture remaining dirty pages from
		 * the post-fork convergence window.
		 */
		kill(source_pid, SIGSTOP);
		usleep(1000);

		{
			struct lazy_vma_entry *lve3;
			struct converge_region *final_dirty = NULL;
			int final_count = 0, final_cap = 0;
			unsigned long final_pages = 0;

			list_for_each_entry(lve3, get_global_lazy_vmas(), list) {
				unsigned long scan_pos;

				if (lve3->dst_id != img->dst_id)
					continue;

				scan_pos = lve3->start;
				while (scan_pos < lve3->end) {
					unsigned long walk_end = 0;
					int nr_r, rr;

					nr_r = cow_scan_dirty_pages(
						source_pid, scan_pos,
						lve3->end, regions,
						CONVERGE_MAX_REGIONS,
						&walk_end);
					if (nr_r <= 0)
						break;

					if (final_count + nr_r > final_cap) {
						int nc = (final_cap + nr_r) * 2;
						struct converge_region *tmp;

						tmp = xrealloc(final_dirty,
							nc * sizeof(*tmp));
						if (!tmp)
							break;
						final_dirty = tmp;
						final_cap = nc;
					}
					for (rr = 0; rr < nr_r; rr++) {
						final_dirty[final_count++] =
							regions[rr];
						final_pages +=
							(regions[rr].end -
							 regions[rr].start) /
							PAGE_SIZE;
					}

					scan_pos = walk_end;
					if (walk_end >= lve3->end)
						break;
				}
			}

			if (final_count > 0) {
				long sent = converge_dispatch_parallel(
					img, source_pid, sockets,
					nr_streams, final_dirty,
					final_count);
				if (sent >= 0)
					pr_err("COW converge second-freeze: "
					       "%ld pages\n", sent);
			}
			xfree(final_dirty);

			pr_err("COW converge second-freeze: %lu dirty "
			       "pages (%d regions)\n",
			       final_pages, final_count);
		}

		/*
		 * Re-send allocator metadata pages from fork +
		 * detect new VMAs for VMA diff.
		 *
		 * Re-send: [heap], file-backed rw- (valkey BSS, libc),
		 *          small anonymous VMAs (thread stacks/TLS).
		 * Skip:    large anonymous VMAs (jemalloc data extents).
		 * This overwrites the arena reset with the fork's
		 * consistent allocator state (~84 MB, ~0.05s).
		 */
		{
			char maps_path[64];
			FILE *mfp;
			struct converge_region *alloc_regions = NULL;
			int alloc_count = 0, alloc_cap = 0;
			unsigned long alloc_pages = 0, skip_pages = 0;
			struct vma_diff_entry *new_vmas = NULL;
			int new_vma_count = 0, new_vma_cap = 0;
			unsigned long new_vma_pages = 0;

			snprintf(maps_path, sizeof(maps_path),
				 "/proc/%d/maps", source_pid);
			mfp = fopen(maps_path, "r");
			if (mfp) {
				char mline[512];

				while (fgets(mline, sizeof(mline), mfp)) {
					unsigned long ms, me;
					char mp[8], mpath[256];
					unsigned long vma_sz;

					mpath[0] = '\0';
					if (sscanf(mline,
						   "%lx-%lx %4s %*s %*s %*s %255[^\n]",
						   &ms, &me, mp, mpath) < 3)
						continue;
					if (mp[0] != 'r' || mp[1] != 'w' ||
					    mp[2] != '-')
						continue;

					vma_sz = me - ms;

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
						new_vma_pages += vma_sz / PAGE_SIZE;
					} else {
						/*
						 * Dump-time VMA: re-send if
						 * allocator metadata (file-backed,
						 * [heap], or small anonymous).
						 * Skip large anonymous (data).
						 */
						int is_alloc = (mpath[0] != '\0')
							|| (vma_sz < 1048576);
						if (is_alloc) {
							if (alloc_count >= alloc_cap) {
								int nc = (alloc_cap + 16) * 2;
								struct converge_region *t;
								t = xrealloc(alloc_regions,
									nc * sizeof(*t));
								if (!t) break;
								alloc_regions = t;
								alloc_cap = nc;
							}
							alloc_regions[alloc_count].start = ms;
							alloc_regions[alloc_count].end = me;
							alloc_regions[alloc_count].categories = 0;
							alloc_count++;
							alloc_pages += vma_sz / PAGE_SIZE;
						} else {
							skip_pages += vma_sz / PAGE_SIZE;
						}
					}
				}
				fclose(mfp);
			}

			/* Re-send allocator pages from BULK fork (T0) */
			{
				pid_t asrc = bulk_fork_pid > 0 ?
					bulk_fork_pid : fork_pid;

				if (alloc_count > 0 && asrc > 0) {
					long re_sent;

					pr_err("COW converge: re-sending "
					       "%lu alloc pages from %s "
					       "fork %d\n",
					       alloc_pages,
					       asrc == bulk_fork_pid ?
					       "bulk" : "converge",
					       asrc);
					re_sent =
						converge_dispatch_parallel(
							img, asrc, sockets,
							nr_streams,
							alloc_regions,
							alloc_count);
					if (re_sent >= 0)
						pr_err("COW converge: "
						       "re-sent %ld\n",
						       re_sent);
				}
			}
			if (fork_pid > 0)
				kill(fork_pid, SIGKILL);
			xfree(alloc_regions);

			/*
			 * Send VMA diff + page data for new VMAs
			 * on stream 0.  The replica will pause when it
			 * receives the diff, inject mmap(MAP_FIXED) via
			 * ptrace, then resume to receive page data.
			 */
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

				pr_err("COW converge: %d new VMAs "
				       "(%lu pages, %.1f MB)\n",
				       new_vma_count, new_vma_pages,
				       (double)(new_vma_pages * PAGE_SIZE) /
				       (1024 * 1024));

				/* Send VMA diff header + payload */
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

				/* Send page data for new VMAs on stream 0 */
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
					pr_err("COW converge: sent %lu "
					       "new-VMA pages\n", total_sent);
				}
				xfree(batch_buf);
				xfree(lio);
			}

			xfree(new_vmas);
		}

		kill(source_pid, SIGCONT);
		xfree(freeze_dirty);
	}

	xfree(regions);
	return 0;

err:
	xfree(regions);
	return -1;
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
	struct vma_range *ranges;
	int nr_ranges;
	bool failed;
	unsigned long pages_sent;
};

static void *stream_worker_func(void *arg)
{
	struct stream_worker *w = arg;
	struct unified_thread_stats stats = { 0 };
	char name[16];
	int i;

	snprintf(name, sizeof(name), "cow-xfer-%d", w->id);
	pthread_setname_np(pthread_self(), name);

	for (i = 0; i < w->nr_ranges; i++) {
		struct vma_range *r = &w->ranges[i];

		if (process_vma_pages_sk(w->img, r->lve, w->source_pid,
					 &stats, w->sk,
					 r->start, r->end) < 0) {
			pr_err("Stream %d: error processing range %lx-%lx\n",
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
				pid_t bulk_fork = -1;
				int nr_vmas = 0, vi = 0, s;
				int nr_ranges = 0;
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
				int ri;

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

				/* Assign ranges to workers round-robin by size */
				ri = 0;
				for (s = 0; s < nr_streams; s++) {
					unsigned long assigned = 0;

					workers[s].id = s;
					workers[s].img = img;
					workers[s].source_pid = source_pid;
					workers[s].ranges = all_ranges + ri;
					workers[s].nr_ranges = 0;

					while (ri < nr_ranges) {
						workers[s].nr_ranges++;
						assigned += (all_ranges[ri].end -
							     all_ranges[ri].start)
							    / PAGE_SIZE;
						ri++;
						if (s < nr_streams - 1 &&
						    assigned >= pages_per_worker)
							break;
					}
				}
				}

				pr_err("Multi-TCP: %d streams, %d ranges "
				       "(%d VMAs), %lu pages\n",
				       nr_streams, nr_ranges,
				       nr_vmas, total_pages);

				/* Fork for consistent bulk snapshot */
				{
				if (cow_is_wp_async())
					bulk_fork = fork_source_snapshot(
							source_pid);
				if (bulk_fork > 0) {
					pr_err("Bulk: fork %d\n", bulk_fork);
					for (s = 0; s < nr_streams; s++)
						workers[s].source_pid =
							bulk_fork;
				}
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
				/* Keep bulk_fork alive for allocator re-send */
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
							    conv_sks, nr_streams,
							    bulk_fork) < 0)
							pr_warn("COW convergence had errors (non-fatal)\n");
						xfree(conv_sks);
					}
				}

				if (bulk_fork > 0)
					kill(bulk_fork, SIGKILL);

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
