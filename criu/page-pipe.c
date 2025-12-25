#include <unistd.h>

#undef LOG_PREFIX
#define LOG_PREFIX "page-pipe: "

#include "common/config.h"
#include "page.h"
#include "util.h"
#include "criu-log.h"
#include "page-pipe.h"
#include "fcntl.h"
#include "stats.h"
#include "cr_options.h"
#include <time.h>
#include "syscall.h"
#include <sys/ioctl.h>  // defines FIONREAD

/* can existing iov accumulate the page? */

/* ============================================================================
 * HASH TABLE FOR O(1) PAGE LOOKUP
 * 
 * Maps: page_address -> (ppb, seg_idx, prefix_len)
 * 
 * This gives O(1) lookup for ALL access patterns:
 * - P1 (COW): Random addresses -> O(1)
 * - P2 (Request): Random addresses -> O(1)  
 * - P3 (Regular): Sequential addresses -> O(1)
 * ============================================================================
 */

/* Hash table size: 2^20 = 1M buckets */
#define PP_HASH_BITS    20
#define PP_HASH_SIZE    (1UL << PP_HASH_BITS)
#define PP_HASH_MASK    (PP_HASH_SIZE - 1)

/* Hash entry: stores location info for one page */
struct pp_hash_entry {
	unsigned long page_addr;        /* Page address (key) */
	struct page_pipe_buf *ppb;      /* Buffer containing this page */
	unsigned int seg_idx;           /* Segment index within buffer */
	unsigned long seg_start;        /* Segment start address */
	unsigned long prefix_len;       /* Sum of segment lengths before this segment */
	struct pp_hash_entry *next;     /* Collision chain */
};

/* Hash table state */
static struct pp_hash_entry **g_hash_buckets = NULL;
static struct page_pipe *g_hash_owner = NULL;
static unsigned long g_hash_num_entries = 0;
static bool g_hash_built = false;

/* Statistics */
static unsigned long g_hash_lookups = 0;
static unsigned long g_hash_hits = 0;
static unsigned long g_hash_misses = 0;
static unsigned long g_hash_chain_walks = 0;
static time_t g_hash_last_log_time = 0;

/* 
 * Hash function: Knuth multiplicative hash
 * Good distribution for page-aligned addresses
 */
static inline unsigned long pp_hash_func(unsigned long addr)
{
	unsigned long page_num = addr >> PAGE_SHIFT;
	return (page_num * 2654435761UL) & PP_HASH_MASK;
}

/*
 * Build hash table from page_pipe
 * Call this AFTER all pages have been added to page_pipe
 * Returns: 0 on success, -1 on failure
 */
int page_pipe_build_hash(struct page_pipe *pp)
{
	struct page_pipe_buf *ppb;
	struct pp_hash_entry *entry;
	struct pp_hash_entry *tmp;
	struct pp_hash_entry *e;
	struct pp_hash_entry *next_e;
	struct iovec *iov;
	unsigned int seg_idx;
	unsigned long bucket;
	unsigned long total_pages = 0;
	unsigned long collisions = 0;
	unsigned long max_chain = 0;
	unsigned long prefix_len;
	unsigned long seg_start;
	unsigned long seg_pages;
	unsigned long page_addr;
	unsigned long chain_len;
	unsigned long p;
	unsigned long i;
	unsigned long bucket_mem;
	unsigned long entry_mem;
	unsigned long total_mem;
	
	pr_info("Building hash table for O(1) page lookup...\n");
	
	/* Free existing hash table if any */
	if (g_hash_buckets) {
		for (i = 0; i < PP_HASH_SIZE; i++) {
			e = g_hash_buckets[i];
			while (e) {
				next_e = e->next;
				xfree(e);
				e = next_e;
			}
		}
		xfree(g_hash_buckets);
		g_hash_buckets = NULL;
	}
	
	/* Allocate bucket array (zeroed) */
	g_hash_buckets = xzalloc(PP_HASH_SIZE * sizeof(struct pp_hash_entry *));
	if (!g_hash_buckets) {
		pr_err("Failed to allocate hash table (%lu bytes)\n", 
		       PP_HASH_SIZE * sizeof(struct pp_hash_entry *));
		return -1;
	}
	
	/* Reset state */
	g_hash_owner = pp;
	g_hash_num_entries = 0;
	g_hash_lookups = 0;
	g_hash_hits = 0;
	g_hash_misses = 0;
	g_hash_chain_walks = 0;
	g_hash_built = false;
	
	/* Iterate through all buffers and segments */
	list_for_each_entry(ppb, &pp->bufs, l) {
		prefix_len = 0;  /* Cumulative length before current segment */
		
		for (seg_idx = 0; seg_idx < ppb->nr_segs; seg_idx++) {
			iov = &ppb->iov[seg_idx];
			seg_start = (unsigned long)iov->iov_base;
			seg_pages = iov->iov_len / PAGE_SIZE;
			
			/* Insert each page in this segment */
			for (p = 0; p < seg_pages; p++) {
				page_addr = seg_start + (p * PAGE_SIZE);
				
				/* Allocate entry */
				entry = xmalloc(sizeof(*entry));
				if (!entry) {
					pr_err("Failed to allocate hash entry (page %lu)\n", total_pages);
					/* Clean up on failure */
					for (i = 0; i < PP_HASH_SIZE; i++) {
						e = g_hash_buckets[i];
						while (e) {
							next_e = e->next;
							xfree(e);
							e = next_e;
						}
					}
					xfree(g_hash_buckets);
					g_hash_buckets = NULL;
					return -1;
				}
				
				/* Fill entry */
				entry->page_addr = page_addr;
				entry->ppb = ppb;
				entry->seg_idx = seg_idx;
				entry->seg_start = seg_start;
				entry->prefix_len = prefix_len;
				
				/* Insert at head of bucket chain */
				bucket = pp_hash_func(page_addr);
				
				/* Count chain length for statistics */
				chain_len = 0;
				if (g_hash_buckets[bucket]) {
					collisions++;
					tmp = g_hash_buckets[bucket];
					while (tmp) {
						chain_len++;
						tmp = tmp->next;
					}
					if (chain_len > max_chain)
						max_chain = chain_len;
				}
				
				entry->next = g_hash_buckets[bucket];
				g_hash_buckets[bucket] = entry;
				
				g_hash_num_entries++;
				total_pages++;
			}
			
			/* Update prefix_len for next segment */
			prefix_len += iov->iov_len;
		}
	}
	
	g_hash_built = true;
	
	/* Calculate memory usage */
	bucket_mem = PP_HASH_SIZE * sizeof(struct pp_hash_entry *);
	entry_mem = total_pages * sizeof(struct pp_hash_entry);
	total_mem = bucket_mem + entry_mem;
	
	pr_info("Hash table built successfully:\n");
	pr_info("  Pages indexed: %lu\n", total_pages);
	pr_info("  Buckets: %lu (%.1f%% load factor)\n", 
	        PP_HASH_SIZE, (double)total_pages * 100.0 / PP_HASH_SIZE);
	pr_info("  Collisions: %lu (%.1f%%)\n",
	        collisions, total_pages > 0 ? (double)collisions * 100.0 / total_pages : 0.0);
	pr_info("  Max chain length: %lu\n", max_chain);
	pr_info("  Memory usage: %.1f MB (buckets: %.1f MB, entries: %.1f MB)\n",
	        total_mem / 1048576.0, bucket_mem / 1048576.0, entry_mem / 1048576.0);
	
	return 0;
}

/*
 * Destroy hash table and free all memory
 */
void page_pipe_destroy_hash(void)
{
	struct pp_hash_entry *e;
	struct pp_hash_entry *next_e;
	unsigned long i;
	
	if (!g_hash_buckets)
		return;
	
	pr_info("Destroying hash table...\n");
	pr_info("  Final stats: %lu lookups, %lu hits (%.2f%%), %lu misses, %lu chain walks\n",
	        g_hash_lookups, g_hash_hits,
	        g_hash_lookups > 0 ? (double)g_hash_hits * 100.0 / g_hash_lookups : 0.0,
	        g_hash_misses, g_hash_chain_walks);
	
	/* Free all entries */
	for (i = 0; i < PP_HASH_SIZE; i++) {
		e = g_hash_buckets[i];
		while (e) {
			next_e = e->next;
			xfree(e);
			e = next_e;
		}
	}
	
	/* Free bucket array */
	xfree(g_hash_buckets);
	g_hash_buckets = NULL;
	g_hash_owner = NULL;
	g_hash_num_entries = 0;
	g_hash_built = false;
}

/*
 * Hash table lookup - O(1) average case
 */
static inline struct pp_hash_entry *pp_hash_lookup(unsigned long addr)
{
	unsigned long bucket;
	struct pp_hash_entry *entry;
	
	bucket = pp_hash_func(addr);
	entry = g_hash_buckets[bucket];
	
	while (entry) {
		if (entry->page_addr == addr) {
			return entry;
		}
		g_hash_chain_walks++;
		entry = entry->next;
	}
	
	return NULL;
}

/*
 * Log hash table statistics every second
 */
static void pp_hash_log_stats(void)
{
	time_t now = time(NULL);
	
	if (now != g_hash_last_log_time) {
		double hit_rate = g_hash_lookups > 0 ? 
			(double)g_hash_hits * 100.0 / g_hash_lookups : 0.0;
		double avg_chain = g_hash_hits > 0 ?
			(double)g_hash_chain_walks / g_hash_hits : 0.0;
		
		pr_warn("[HASH_STATS] lookups=%lu hits=%lu (%.1f%%) misses=%lu avg_chain=%.2f\n",
		        g_hash_lookups, g_hash_hits, hit_rate, g_hash_misses, avg_chain);
		
		g_hash_last_log_time = now;
	}
}

static inline bool iov_grow_page(struct iovec *iov, unsigned long addr)
{
	if ((unsigned long)iov->iov_base + iov->iov_len == addr) {
		iov->iov_len += PAGE_SIZE;
		return true;
	}

	return false;
}

static inline void iov_init(struct iovec *iov, unsigned long addr)
{
	iov->iov_base = (void *)addr;
	iov->iov_len = PAGE_SIZE;
}

static int __ppb_resize_pipe(struct page_pipe_buf *ppb, unsigned long new_size)
{
	int ret;

	ret = fcntl(ppb->p[0], F_SETPIPE_SZ, new_size * PAGE_SIZE);
	if (ret < 0)
		return -1;

	ret /= PAGE_SIZE;
	BUG_ON(ret < ppb->pipe_size);

	pr_debug("Grow pipe %x -> %x\n", ppb->pipe_size, ret);
	ppb->pipe_size = ret;

	return 0;
}

static inline int ppb_resize_pipe(struct page_pipe_buf *ppb)
{
	unsigned long new_size = ppb->pipe_size << 1;
	int ret;

	if (ppb->pages_in + ppb->pipe_off < ppb->pipe_size)
		return 0;

	if (new_size > PIPE_MAX_SIZE) {
		if (ppb->pipe_size < PIPE_MAX_SIZE)
			new_size = PIPE_MAX_SIZE;
		else
			return 1;
	}

	ret = __ppb_resize_pipe(ppb, new_size);
	if (ret < 0)
		return 1; /* need to add another buf */

	return 0;
}

static struct page_pipe_buf *pp_prev_ppb(struct page_pipe *pp, unsigned int ppb_flags)
{
	int type = 0;

	/* don't allow to reuse a pipe in the PP_CHUNK_MODE mode */
	if (pp->flags & PP_CHUNK_MODE)
		return NULL;

	if (list_empty(&pp->bufs))
		return NULL;

	if (ppb_flags & PPB_LAZY && opts.lazy_pages)
		type = 1;

	return pp->prev[type];
}

static void pp_update_prev_ppb(struct page_pipe *pp, struct page_pipe_buf *ppb, unsigned int ppb_flags)
{
	int type = 0;

	if (ppb_flags & PPB_LAZY && opts.lazy_pages)
		type = 1;

	pp->prev[type] = ppb;
}

static struct page_pipe_buf *ppb_alloc(struct page_pipe *pp, unsigned int ppb_flags)
{
	struct page_pipe_buf *prev = pp_prev_ppb(pp, ppb_flags);
	struct page_pipe_buf *ppb;
	int ppb_size = 0;

	ppb = xmalloc(sizeof(*ppb));
	if (!ppb)
		return NULL;
	cnt_add(CNT_PAGE_PIPE_BUFS, 1);

	if (prev && ppb_resize_pipe(prev) == 0) {
		/* The previous pipe isn't full and we can continue to use it. */
		ppb->p[0] = prev->p[0];
		ppb->p[1] = prev->p[1];
		ppb->pipe_off = prev->pages_in + prev->pipe_off;
		ppb->pipe_size = prev->pipe_size;
	} else {
		if (pipe(ppb->p)) {
			xfree(ppb);
			pr_perror("Can't make pipe for page-pipe");
			return NULL;
		}
		cnt_add(CNT_PAGE_PIPES, 1);

		ppb->pipe_off = 0;
		ppb_size = fcntl(ppb->p[0], F_GETPIPE_SZ, 0);
		if (ppb_size < 0) {
			xfree(ppb);
			pr_perror("Can't get pipe size");
			return NULL;
		}
		ppb->pipe_size = ppb_size / PAGE_SIZE;
		pp->nr_pipes++;
	}

	list_add_tail(&ppb->l, &pp->bufs);

	pp_update_prev_ppb(pp, ppb, ppb_flags);

	return ppb;
}

static void ppb_destroy(struct page_pipe_buf *ppb)
{
	/* Check whether a pipe is shared with another ppb */
	if (ppb->pipe_off == 0) {
		close(ppb->p[0]);
		close(ppb->p[1]);
	}
	xfree(ppb);
}

static void ppb_init(struct page_pipe_buf *ppb, unsigned int pages_in, unsigned int nr_segs, unsigned int flags,
		     struct iovec *iov)
{
	ppb->pages_in = pages_in;
	ppb->nr_segs = nr_segs;
	ppb->flags = flags;
	ppb->iov = iov;
}

static int page_pipe_grow(struct page_pipe *pp, unsigned int flags)
{
	struct page_pipe_buf *ppb;
	struct iovec *free_iov;

	pr_debug("Will grow page pipe (iov off is %u)\n", pp->free_iov);

	if (!list_empty(&pp->free_bufs)) {
		ppb = list_first_entry(&pp->free_bufs, struct page_pipe_buf, l);
		list_move_tail(&ppb->l, &pp->bufs);
		goto out;
	}

	if ((pp->flags & PP_CHUNK_MODE) && (pp->nr_pipes == NR_PIPES_PER_CHUNK))
		return -EAGAIN;

	ppb = ppb_alloc(pp, flags);
	if (!ppb)
		return -1;

out:
	free_iov = &pp->iovs[pp->free_iov];
	ppb_init(ppb, 0, 0, flags, free_iov);

	return 0;
}

struct page_pipe *create_page_pipe(unsigned int nr_segs, struct iovec *iovs, unsigned flags)
{
	struct page_pipe *pp;

	pr_debug("Create page pipe for %u segs\n", nr_segs);

	pp = xzalloc(sizeof(*pp));
	if (!pp)
		return NULL;

	INIT_LIST_HEAD(&pp->free_bufs);
	INIT_LIST_HEAD(&pp->bufs);
	pp->nr_iovs = nr_segs;
	pp->flags = flags;

	if (!iovs) {
		iovs = xmalloc(sizeof(*iovs) * nr_segs);
		if (!iovs)
			goto err_free_pp;
		pp->flags |= PP_OWN_IOVS;
	}
	pp->iovs = iovs;

	if (page_pipe_grow(pp, 0))
		goto err_free_iovs;

	return pp;

err_free_iovs:
	if (pp->flags & PP_OWN_IOVS)
		xfree(iovs);
err_free_pp:
	xfree(pp);
	return NULL;
}

void destroy_page_pipe(struct page_pipe *pp)
{
	struct page_pipe_buf *ppb, *n;

	pr_debug("Killing page pipe\n");

	/* Destroy hash table if this page_pipe owns it */
	if (g_hash_owner == pp) {
		page_pipe_destroy_hash();
	}

	list_splice(&pp->free_bufs, &pp->bufs);
	list_for_each_entry_safe(ppb, n, &pp->bufs, l)
		ppb_destroy(ppb);

	if (pp->flags & PP_OWN_IOVS)
		xfree(pp->iovs);
	xfree(pp);
}

void page_pipe_reinit(struct page_pipe *pp)
{
	struct page_pipe_buf *ppb, *n;

	BUG_ON(!(pp->flags & PP_CHUNK_MODE));

	pr_debug("Clean up page pipe\n");

	list_for_each_entry_safe(ppb, n, &pp->bufs, l)
		list_move(&ppb->l, &pp->free_bufs);

	pp->free_hole = 0;

	if (page_pipe_grow(pp, 0))
		BUG(); /* It can't fail, because ppb is in free_bufs */
}

static inline int try_add_page_to(struct page_pipe *pp, struct page_pipe_buf *ppb, unsigned long addr,
				  unsigned int flags)
{
	if (ppb->flags != flags)
		return 1;

	if (ppb_resize_pipe(ppb) == 1)
		return 1;

	if (ppb->nr_segs && iov_grow_page(&ppb->iov[ppb->nr_segs - 1], addr))
		goto out;

	pr_debug("Add iov to page pipe (%u iovs, %u/%u total)\n", ppb->nr_segs, pp->free_iov, pp->nr_iovs);
	iov_init(&ppb->iov[ppb->nr_segs++], addr);
	pp->free_iov++;
	BUG_ON(pp->free_iov > pp->nr_iovs);
out:
	ppb->pages_in++;
	return 0;
}

static inline int try_add_page(struct page_pipe *pp, unsigned long addr, unsigned int flags)
{
	BUG_ON(list_empty(&pp->bufs));
	return try_add_page_to(pp, list_entry(pp->bufs.prev, struct page_pipe_buf, l), addr, flags);
}

int page_pipe_add_page(struct page_pipe *pp, unsigned long addr, unsigned int flags)
{
	int ret;

	ret = try_add_page(pp, addr, flags);
	if (ret <= 0)
		return ret;

	ret = page_pipe_grow(pp, flags);
	if (ret < 0)
		return ret;

	ret = try_add_page(pp, addr, flags);
	BUG_ON(ret > 0);
	return ret;
}

#define PP_HOLES_BATCH 32

int page_pipe_add_hole(struct page_pipe *pp, unsigned long addr, unsigned int flags)
{
	if (pp->free_hole >= pp->nr_holes) {
		size_t new_size = (pp->nr_holes + PP_HOLES_BATCH) * sizeof(struct iovec);
		if (xrealloc_safe(&pp->holes, new_size))
			return -1;

		new_size = (pp->nr_holes + PP_HOLES_BATCH) * sizeof(unsigned int);
		if (xrealloc_safe(&pp->hole_flags, new_size))
			return -1;

		pp->nr_holes += PP_HOLES_BATCH;
	}

	if (pp->free_hole && pp->hole_flags[pp->free_hole - 1] == flags &&
	    iov_grow_page(&pp->holes[pp->free_hole - 1], addr))
		goto out;

	iov_init(&pp->holes[pp->free_hole++], addr);

	pp->hole_flags[pp->free_hole - 1] = flags;

out:
	return 0;
}

/* ============================================================================
 * GET_PPB: O(1) HASH TABLE LOOKUP
 * ============================================================================
 */

/*
 * Get ppb and iov that contain addr and count amount of data between
 * beginning of the pipe belonging to the ppb and addr
 *
 * HASH TABLE VERSION: O(1) for ALL access patterns
 * - P1 (COW random): O(1)
 * - P2 (Request random): O(1)
 * - P3 (Sequential): O(1)
 */
static struct page_pipe_buf *get_ppb(struct page_pipe *pp, unsigned long addr, struct iovec **iov_ret,
				     unsigned long *len)
{
	struct pp_hash_entry *entry;
	
	g_hash_lookups++;
	
	/* Verify hash table is built and belongs to this page_pipe */
	if (!g_hash_built || g_hash_owner != pp) {
		pr_err("Hash table not built or wrong owner! Call page_pipe_build_hash() first.\n");
		g_hash_misses++;
		return NULL;
	}
	
	/* O(1) hash lookup */
	entry = pp_hash_lookup(addr);
	
	if (entry) {
		/* Hit! Compute outputs */
		g_hash_hits++;
		
		/* iov_ret: pointer to the segment */
		*iov_ret = &entry->ppb->iov[entry->seg_idx];
		
		/* len: prefix_len + offset within segment */
		*len = entry->prefix_len + (addr - entry->seg_start);
		
		/* Log stats periodically */
		pp_hash_log_stats();
		
		return entry->ppb;
	}
	
	/* Miss: address not in page_pipe */
	g_hash_misses++;
	pp_hash_log_stats();
	
	return NULL;
}

int pipe_read_dest_init(struct pipe_read_dest *prd)
{
	int ret;

	if (pipe(prd->p)) {
		pr_perror("Cannot create pipe for reading from page-pipe");
		return -1;
	}

	ret = fcntl(prd->p[0], F_SETPIPE_SZ, PIPE_MAX_SIZE * PAGE_SIZE);
	if (ret < 0)
		return -1;

	prd->sink_fd = open("/dev/null", O_WRONLY);
	if (prd->sink_fd < 0) {
		pr_perror("Cannot open sink for reading from page-pipe");
		return -1;
	}

	ret = fcntl(prd->p[0], F_GETPIPE_SZ, 0);
	pr_debug("Created tee pipe size %d\n", ret);

	return 0;
}

int page_pipe_read(struct page_pipe *pp, unsigned long addr, unsigned long int *nr_pages,
		   unsigned int ppb_flags, void **out_buffer, size_t *out_len)
{
	struct page_pipe_buf *ppb;
	struct iovec *iov = NULL;
	unsigned long skip = 0, len;
	ssize_t ret;
	void *temp_buf = NULL;

	/*
	 * Get ppb that contains addr and count length of data between
	 * the beginning of the pipe and addr. If no ppb is found, the
	 * requested page is mapped to zero pfn
	 */
	ppb = get_ppb(pp, addr, &iov, &skip);
	if (!ppb) {
		*nr_pages = 0;
		return 0;
	}

	if (!(ppb->flags & ppb_flags)) {
		pr_err("PPB flags mismatch: %x %x\n", ppb_flags, ppb->flags);
		return -1;
	}

	/* clamp the request if it passes the end of iovec */
	len = min((unsigned long)iov->iov_base + iov->iov_len - addr, *nr_pages * PAGE_SIZE);
	*nr_pages = len / PAGE_SIZE;

	/*
	 * Fast path: Use process_vm_readv if source process is available.
	 * This provides O(1) random access without pipe skip overhead.
	 * Returns allocated buffer to caller.
	 */
	if (pp->source_pid > 0) {
		struct iovec local_iov, remote_iov;

		temp_buf = xmalloc(len);
		if (!temp_buf) {
			pr_perror("Failed to allocate temp buffer for process_vm_readv");
			return -1;
		}

		local_iov.iov_base = temp_buf;
		local_iov.iov_len = len;
		remote_iov.iov_base = (void *)addr;
		remote_iov.iov_len = len;

		ret = process_vm_readv(pp->source_pid, &local_iov, 1, &remote_iov, 1, 0);
		if (ret != len) {
			if (ret >= 0) {
				pr_err("Short read from process_vm_readv: %zd/%lu (pid=%d, addr=%lx)\n",
				       ret, len, pp->source_pid, addr);
			} else {
				pr_perror("process_vm_readv failed (pid=%d, addr=%lx)", pp->source_pid, addr);
			}
			xfree(temp_buf);
			return -1;
		}

		/* Return buffer to caller - they will free it */
		*out_buffer = temp_buf;
		*out_len = len;

		pr_debug("process_vm_readv: read %lu bytes from pid=%d addr=%lx\n", len, pp->source_pid, addr);
		return 0;
	}

	pr_perror("No source_pid set\n");
	exit(1);
	/*
	 * Fallback path: Read from pipe (for compatibility when
	 * source process is not available). Skip unwanted bytes,
	 * then read actual data into buffer.
	 */
	skip += ppb->pipe_off * PAGE_SIZE;
	
	/* Skip unwanted bytes at beginning of pipe */
	if (skip > 0) {
		char *skip_buf = xmalloc(skip);
		if (!skip_buf) {
			pr_perror("Failed to allocate skip buffer");
			return -1;
		}
		
		ret = read(ppb->p[0], skip_buf, skip);
		xfree(skip_buf);
		
		if (ret != skip) {
			pr_perror("Failed to skip %lu bytes from pipe", skip);
			return -1;
		}
	}
	
	/* Read actual data */
	temp_buf = xmalloc(len);
	if (!temp_buf) {
		pr_perror("Failed to allocate buffer for pipe read");
		return -1;
	}
	
	ret = read(ppb->p[0], temp_buf, len);
	if (ret != len) {
		pr_perror("Failed to read %lu bytes from pipe", len);
		xfree(temp_buf);
		return -1;
	}
	
	/* Return buffer to caller */
	*out_buffer = temp_buf;
	*out_len = len;
	
	pr_debug("Pipe read: read %lu bytes (skipped %lu bytes)\n", len, skip);
	return 0;
}

void page_pipe_destroy_ppb(struct page_pipe_buf *ppb)
{
	list_del(&ppb->l);
	ppb_destroy(ppb);
}

void debug_show_page_pipe(struct page_pipe *pp)
{
	struct page_pipe_buf *ppb;
	int i;
	struct iovec *iov;

	if (pr_quelled(LOG_DEBUG))
		return;

	pr_debug("Page pipe:\n");
	pr_debug("* %u pipes %u/%u iovs:\n", pp->nr_pipes, pp->free_iov, pp->nr_iovs);
	list_for_each_entry(ppb, &pp->bufs, l) {
		pr_debug("\tbuf %lx pages, %u iovs, flags: %x pipe_off: %lx :\n", ppb->pages_in, ppb->nr_segs, ppb->flags,
			 ppb->pipe_off);
		for (i = 0; i < ppb->nr_segs; i++) {
			iov = &ppb->iov[i];
			pr_debug("\t\t%p - %p\n", iov->iov_base, iov->iov_base + iov->iov_len);
		}
	}

	pr_debug("* %u holes:\n", pp->free_hole);
	for (i = 0; i < pp->free_hole; i++) {
		iov = &pp->holes[i];
		pr_debug("\t%p - %p\n", iov->iov_base, iov->iov_base + iov->iov_len);
	}
}
