/*
 * COW Process Comparison - Debug tool for cross-host process state comparison
 *
 * Used to compare PRIMARY (source) and REPLICA (restored) processes to verify
 * they are identical before unfreezing. Helps debug crashes after COW migration.
 *
 * Flow:
 *   1. PRIMARY listens on COMPARE_PORT after P3 completion
 *   2. REPLICA connects after drain completes
 *   3. PRIMARY sends VMA list -> REPLICA compares
 *   4. PRIMARY sends page hashes -> REPLICA compares
 *   5. Differences are logged for debugging
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/mman.h>

#include "types.h"
#include "criu-log.h"
#include "page.h"
#include "xmalloc.h"
#include "cow/cow-conf.h"

#undef LOG_PREFIX
#define LOG_PREFIX "cow-compare: "

#define COMPARE_PORT 27020  /* Different from page-server port */

/* Message types */
#define MSG_VMA_LIST      1
#define MSG_VMA_END       2
#define MSG_PAGE_HASH     3
#define MSG_PAGE_HASH_END 4
#define MSG_PAGE_DATA     5
#define MSG_COMPARE_DONE  6

struct compare_msg_hdr {
	uint32_t type;
	uint32_t len;
};

struct vma_info {
	uint64_t start;
	uint64_t end;
	uint32_t prot;      /* PROT_READ | PROT_WRITE | PROT_EXEC */
	uint32_t flags;     /* MAP_PRIVATE | MAP_SHARED etc */
	uint64_t pgoff;
	char name[64];      /* Truncated pathname */
	/* smaps fields for debugging missing VMAs */
	uint64_t rss;       /* Resident Set Size in KB */
	uint64_t pss;       /* Proportional Set Size in KB */
	uint64_t anonymous; /* Anonymous memory in KB */
	uint64_t swap;      /* Swap in KB */
	char vmflags[64];   /* VmFlags string */
};

struct page_hash_info {
	uint64_t vaddr;
	uint32_t crc32;     /* CRC32 of page content */
};

/* Read VMA list from /proc/PID/maps */
static int read_process_vmas(pid_t pid, struct vma_info **out_vmas, int *out_count)
{
	char path[64];
	FILE *f;
	struct vma_info *vmas = NULL;
	int count = 0, capacity = 256;
	char line[512];

	snprintf(path, sizeof(path), "/proc/%d/maps", pid);
	f = fopen(path, "r");
	if (!f) {
		pr_perror("Failed to open %s", path);
		return -1;
	}

	vmas = xmalloc(capacity * sizeof(*vmas));

	while (fgets(line, sizeof(line), f)) {
		struct vma_info *v;
		char perms[8], name[256] = "";
		unsigned long start, end, pgoff;
		int major, minor, inode;

		if (sscanf(line, "%lx-%lx %4s %lx %x:%x %d %255s",
			   &start, &end, perms, &pgoff, &major, &minor, &inode, name) < 7)
			continue;

		/* Skip non-readable or special regions */
		if (perms[0] != 'r')
			continue;
		if (strstr(name, "[vvar]") || strstr(name, "[vsyscall]"))
			continue;

		if (count >= capacity) {
			capacity *= 2;
			vmas = xrealloc(vmas, capacity * sizeof(*vmas));
		}

		v = &vmas[count++];
		v->start = start;
		v->end = end;
		v->prot = 0;
		if (perms[0] == 'r')
			v->prot |= PROT_READ;
		if (perms[1] == 'w')
			v->prot |= PROT_WRITE;
		if (perms[2] == 'x')
			v->prot |= PROT_EXEC;
		v->flags = (perms[3] == 'p') ? MAP_PRIVATE : MAP_SHARED;
		v->pgoff = pgoff;
		strncpy(v->name, name, sizeof(v->name) - 1);
		v->name[sizeof(v->name) - 1] = '\0';
		/* Initialize smaps fields */
		v->rss = 0;
		v->pss = 0;
		v->anonymous = 0;
		v->swap = 0;
		v->vmflags[0] = '\0';
	}

	fclose(f);
	*out_vmas = vmas;
	*out_count = count;
	return 0;
}

/* Read smaps data for VMAs to help debug missing regions */
static void read_vma_smaps(pid_t pid, struct vma_info *vmas, int count)
{
	char path[64];
	FILE *f;
	char line[512];
	int cur_vma = -1;

	snprintf(path, sizeof(path), "/proc/%d/smaps", pid);
	f = fopen(path, "r");
	if (!f)
		return;

	while (fgets(line, sizeof(line), f)) {
		unsigned long start, end;
		char perms[8];
		int i;

		/* Check if this is a VMA header line (address range) */
		if (sscanf(line, "%lx-%lx %4s", &start, &end, perms) == 3) {
			/* Find matching VMA in our list */
			cur_vma = -1;
			for (i = 0; i < count; i++) {
				if (vmas[i].start == start) {
					cur_vma = i;
					break;
				}
			}
			continue;
		}

		/* Parse smaps fields for current VMA */
		if (cur_vma >= 0) {
			unsigned long val;
			char key[32];

			if (sscanf(line, "%31[^:]: %lu kB", key, &val) == 2) {
				if (strcmp(key, "Rss") == 0)
					vmas[cur_vma].rss = val;
				else if (strcmp(key, "Pss") == 0)
					vmas[cur_vma].pss = val;
				else if (strcmp(key, "Anonymous") == 0)
					vmas[cur_vma].anonymous = val;
				else if (strcmp(key, "Swap") == 0)
					vmas[cur_vma].swap = val;
			} else if (strncmp(line, "VmFlags:", 8) == 0) {
				/* Copy VmFlags line (trim "VmFlags: " prefix and newline) */
				char *flags = line + 9;
				char *nl = strchr(flags, '\n');
				if (nl)
					*nl = '\0';
				strncpy(vmas[cur_vma].vmflags, flags,
					sizeof(vmas[cur_vma].vmflags) - 1);
				vmas[cur_vma].vmflags[sizeof(vmas[cur_vma].vmflags) - 1] = '\0';
			}
		}
	}

	fclose(f);
}

#ifdef CONFIG_COW_COMPARE_PAGES
/* Simple CRC32 for page comparison */
static uint32_t crc32_page(const void *data, size_t len)
{
	const uint8_t *p = data;
	uint32_t crc = 0xFFFFFFFF;
	size_t i;
	int j;

	for (i = 0; i < len; i++) {
		crc ^= p[i];
		for (j = 0; j < 8; j++)
			crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
	}
	return ~crc;
}

/* Read page content from /proc/PID/mem */
static int read_page(pid_t pid, uint64_t vaddr, void *buf, bool log_errors)
{
	static int fail_logged = 0;
	char path[64];
	int fd, ret, save_errno;

	snprintf(path, sizeof(path), "/proc/%d/mem", pid);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;

	if (lseek(fd, vaddr, SEEK_SET) < 0) {
		save_errno = errno;
		close(fd);
		if (log_errors && fail_logged++ < 20)
			pr_err("read_page 0x%016lx lseek failed: errno=%d (%s)\n",
			       (unsigned long)vaddr, save_errno, strerror(save_errno));
		return -1;
	}

	ret = read(fd, buf, PAGE_SIZE);
	save_errno = errno;
	close(fd);

	if (ret != PAGE_SIZE) {
		if (log_errors && fail_logged++ < 20)
			pr_err("read_page 0x%016lx read failed: ret=%d errno=%d (%s)\n",
			       (unsigned long)vaddr, ret, save_errno, strerror(save_errno));
		return -1;
	}

	return 0;
}
#endif

/*
 * PRIMARY side: Send process state to replica for comparison
 */
int cow_compare_send_state(int sk, pid_t pid)
{
	struct vma_info *vmas;
	int nr_vmas, i;
	struct compare_msg_hdr hdr;
#ifdef CONFIG_COW_COMPARE_PAGES
	void *page_buf;
	int total_pages = 0, sent_hashes = 0;
#endif

	pr_warn("COMPARE: Starting state send for PID %d\n", pid);

	/* Step 1: Read and send VMA list */
	if (read_process_vmas(pid, &vmas, &nr_vmas) < 0)
		return -1;

	/* Read smaps data for debugging missing VMAs */
	read_vma_smaps(pid, vmas, nr_vmas);

	pr_warn("COMPARE: Sending %d VMAs\n", nr_vmas);

	for (i = 0; i < nr_vmas; i++) {
		hdr.type = MSG_VMA_LIST;
		hdr.len = sizeof(struct vma_info);
		if (send(sk, &hdr, sizeof(hdr), 0) != sizeof(hdr) ||
		    send(sk, &vmas[i], sizeof(vmas[i]), 0) != sizeof(vmas[i])) {
			pr_perror("COMPARE: Failed to send VMA %d", i);
			xfree(vmas);
			return -1;
		}
#ifdef CONFIG_COW_COMPARE_PAGES
		total_pages += (vmas[i].end - vmas[i].start) / PAGE_SIZE;
#endif
	}

	/* End of VMA list */
	hdr.type = MSG_VMA_END;
	hdr.len = 0;
	send(sk, &hdr, sizeof(hdr), 0);

#ifdef CONFIG_COW_COMPARE_PAGES
	pr_warn("COMPARE: Sent %d VMAs, now sending hashes for %d pages\n",
		nr_vmas, total_pages);

	/* Step 2: Send page hashes for each VMA */
	page_buf = xmalloc(PAGE_SIZE);

	for (i = 0; i < nr_vmas; i++) {
		uint64_t addr;

		for (addr = vmas[i].start; addr < vmas[i].end; addr += PAGE_SIZE) {
			struct page_hash_info phi;

			if (read_page(pid, addr, page_buf, false) < 0) {
				/* Unreadable page - send hash of 0 */
				phi.vaddr = addr;
				phi.crc32 = 0;
			} else {
				phi.vaddr = addr;
				phi.crc32 = crc32_page(page_buf, PAGE_SIZE);
			}

			hdr.type = MSG_PAGE_HASH;
			hdr.len = sizeof(phi);
			if (send(sk, &hdr, sizeof(hdr), 0) != sizeof(hdr) ||
			    send(sk, &phi, sizeof(phi), 0) != sizeof(phi)) {
				pr_perror("COMPARE: Failed to send hash for 0x%lx",
					  (unsigned long)addr);
				break;
			}
			sent_hashes++;

			if (sent_hashes % 100000 == 0)
				pr_info("COMPARE: Sent %d page hashes...\n", sent_hashes);
		}
	}

	/* End of hashes */
	hdr.type = MSG_PAGE_HASH_END;
	hdr.len = 0;
	send(sk, &hdr, sizeof(hdr), 0);

	xfree(page_buf);

	pr_warn("COMPARE: Sent %d page hashes, waiting for comparison result\n",
		sent_hashes);
#else
	pr_warn("COMPARE: Sent %d VMAs (page comparison disabled)\n", nr_vmas);
#endif

	xfree(vmas);

	/* Wait for done message */
	if (recv(sk, &hdr, sizeof(hdr), MSG_WAITALL) == sizeof(hdr) &&
	    hdr.type == MSG_COMPARE_DONE) {
		pr_info("COMPARE: Comparison complete\n");
	}

	return 0;
}

/*
 * REPLICA side: Receive and compare process state
 */
int cow_compare_receive_and_verify(int sk, pid_t pid)
{
	struct compare_msg_hdr hdr;
	struct vma_info *local_vmas, *remote_vmas = NULL;
	int local_nr_vmas, remote_nr_vmas = 0, remote_capacity = 256;
	int vma_diffs = 0, replica_only = 0;
	int uncovered_ranges = 0;
	uint64_t total_uncovered = 0;
#ifdef CONFIG_COW_COMPARE_PAGES
	int page_diffs = 0, pages_checked = 0;
	void *page_buf;
#endif
	int i, j;

	pr_warn("COMPARE: Starting state comparison for local PID %d\n", pid);

	/* Read local VMAs */
	if (read_process_vmas(pid, &local_vmas, &local_nr_vmas) < 0)
		return -1;

	/* Read smaps for local VMAs too (for debugging) */
	read_vma_smaps(pid, local_vmas, local_nr_vmas);

	remote_vmas = xmalloc(remote_capacity * sizeof(*remote_vmas));

	/* Step 1: Receive and compare VMA list */
	while (recv(sk, &hdr, sizeof(hdr), MSG_WAITALL) == sizeof(hdr)) {
		if (hdr.type == MSG_VMA_END)
			break;

		if (hdr.type == MSG_VMA_LIST) {
			struct vma_info v;

			if (recv(sk, &v, sizeof(v), MSG_WAITALL) != sizeof(v))
				break;

			if (remote_nr_vmas >= remote_capacity) {
				remote_capacity *= 2;
				remote_vmas = xrealloc(remote_vmas,
						       remote_capacity * sizeof(*remote_vmas));
			}
			remote_vmas[remote_nr_vmas++] = v;
		}
	}

	pr_warn("COMPARE: Received %d remote VMAs, local has %d\n",
		remote_nr_vmas, local_nr_vmas);

	/*
	 * Skip CRIU restorer artifacts - VMAs below this threshold are typically
	 * CRIU's restorer code, /dev/zero mappings, or vdso. Real application
	 * VMAs are at much higher addresses.
	 */
#define CRIU_ARTIFACT_THRESHOLD 0x10000000UL  /* 256MB */

	/* Compare VMA lists (exact boundary match) */
	for (i = 0; i < remote_nr_vmas; i++) {
		bool found = false;

		for (j = 0; j < local_nr_vmas; j++) {
			if (remote_vmas[i].start == local_vmas[j].start &&
			    remote_vmas[i].end == local_vmas[j].end) {
				found = true;
				if (remote_vmas[i].prot != local_vmas[j].prot) {
					pr_err("COMPARE_DIFF: VMA 0x%lx prot: remote=%x local=%x\n",
					       (unsigned long)remote_vmas[i].start,
					       remote_vmas[i].prot, local_vmas[j].prot);
					vma_diffs++;
				}
				break;
			}
		}
		if (!found) {
			/*
			 * Boundary mismatch can be harmless: kernel often merges
			 * adjacent anon VMAs on restore when madvise flags don't
			 * round-trip. The coverage check below is authoritative.
			 */
			pr_err("COMPARE_DIFF: VMA 0x%016lx-0x%016lx exists on PRIMARY but not REPLICA\n",
			       (unsigned long)remote_vmas[i].start,
			       (unsigned long)remote_vmas[i].end);
			pr_err("  name=%s size=%luKB\n",
			       remote_vmas[i].name[0] ? remote_vmas[i].name : "(anon)",
			       (unsigned long)(remote_vmas[i].end - remote_vmas[i].start) / 1024);
			pr_err("  smaps: rss=%luKB pss=%luKB anon=%luKB swap=%luKB\n",
			       (unsigned long)remote_vmas[i].rss,
			       (unsigned long)remote_vmas[i].pss,
			       (unsigned long)remote_vmas[i].anonymous,
			       (unsigned long)remote_vmas[i].swap);
			pr_err("  vmflags: %s\n",
			       remote_vmas[i].vmflags[0] ? remote_vmas[i].vmflags : "(none)");
			vma_diffs++;
		}
	}

	/* Reverse comparison: check for VMAs on REPLICA that don't exist on PRIMARY */
	for (j = 0; j < local_nr_vmas; j++) {
		bool found = false;

		/* Skip CRIU artifacts (low addresses, vdso, etc.) */
		if (local_vmas[j].start < CRIU_ARTIFACT_THRESHOLD ||
		    strstr(local_vmas[j].name, "[vdso]") ||
		    strstr(local_vmas[j].name, "[vvar]") ||
		    strstr(local_vmas[j].name, "/dev/zero")) {
			continue;
		}

		for (i = 0; i < remote_nr_vmas; i++) {
			if (local_vmas[j].start == remote_vmas[i].start &&
			    local_vmas[j].end == remote_vmas[i].end) {
				found = true;
				break;
			}
		}
		if (!found) {
			pr_err("COMPARE_DIFF: VMA 0x%016lx-0x%016lx exists on REPLICA but not PRIMARY\n",
			       (unsigned long)local_vmas[j].start,
			       (unsigned long)local_vmas[j].end);
			pr_err("  name=%s size=%luKB\n",
			       local_vmas[j].name[0] ? local_vmas[j].name : "(anon)",
			       (unsigned long)(local_vmas[j].end - local_vmas[j].start) / 1024);
			pr_err("  smaps: rss=%luKB pss=%luKB anon=%luKB swap=%luKB\n",
			       (unsigned long)local_vmas[j].rss,
			       (unsigned long)local_vmas[j].pss,
			       (unsigned long)local_vmas[j].anonymous,
			       (unsigned long)local_vmas[j].swap);
			pr_err("  vmflags: %s\n",
			       local_vmas[j].vmflags[0] ? local_vmas[j].vmflags : "(none)");
			replica_only++;
		}
	}

	pr_warn("COMPARE: Exact boundary comparison: %d PRIMARY-only, %d REPLICA-only\n",
		vma_diffs, replica_only);

	/*
	 * Coverage check: verify all PRIMARY memory ranges are covered by REPLICA VMAs.
	 * This catches cases where VMAs are merged/split but memory coverage is the same.
	 * Coverage (not exact-boundary match) is the authoritative correctness check.
	 */
	for (i = 0; i < remote_nr_vmas; i++) {
		uint64_t addr = remote_vmas[i].start;
		uint64_t end = remote_vmas[i].end;

		while (addr < end) {
			bool covered = false;
			uint64_t next_check = end;

			/* Find a local VMA that covers this address */
			for (j = 0; j < local_nr_vmas; j++) {
				if (local_vmas[j].start <= addr && addr < local_vmas[j].end) {
					covered = true;
					/* Move to end of this local VMA or end of remote VMA */
					next_check = (local_vmas[j].end < end) ? local_vmas[j].end : end;
					break;
				}
			}

			if (!covered) {
				/* Find next local VMA start to determine gap size */
				uint64_t gap_end = end;
				for (j = 0; j < local_nr_vmas; j++) {
					if (local_vmas[j].start > addr && local_vmas[j].start < gap_end)
						gap_end = local_vmas[j].start;
				}
				if (uncovered_ranges < 10) {
					pr_err("COVERAGE_GAP: PRIMARY 0x%016lx-0x%016lx not covered by REPLICA\n",
					       (unsigned long)addr, (unsigned long)gap_end);
				}
				uncovered_ranges++;
				total_uncovered += gap_end - addr;
				next_check = gap_end;
			}

			addr = next_check;
		}
	}

	if (uncovered_ranges > 0) {
		pr_err("COVERAGE_RESULT: %d PRIMARY ranges (%lu KB) NOT covered by REPLICA\n",
		       uncovered_ranges, (unsigned long)(total_uncovered / 1024));
	} else {
		pr_warn("COVERAGE_RESULT: All PRIMARY memory ranges are covered by REPLICA (VMA merging OK)\n");
	}

#ifdef CONFIG_COW_COMPARE_PAGES
	/* Step 2: Receive and compare page hashes */
	page_buf = xmalloc(PAGE_SIZE);

	while (recv(sk, &hdr, sizeof(hdr), MSG_WAITALL) == sizeof(hdr)) {
		if (hdr.type == MSG_PAGE_HASH_END)
			break;

		if (hdr.type == MSG_PAGE_HASH) {
			struct page_hash_info remote_phi;
			uint32_t local_crc;

			if (recv(sk, &remote_phi, sizeof(remote_phi), MSG_WAITALL) !=
			    sizeof(remote_phi))
				break;

			pages_checked++;

			/* Read local page and compute hash */
			if (read_page(pid, remote_phi.vaddr, page_buf, true) < 0) {
				local_crc = 0;
			} else {
				local_crc = crc32_page(page_buf, PAGE_SIZE);
			}

			if (local_crc != remote_phi.crc32) {
				page_diffs++;
				if (page_diffs <= 100) {  /* Log first 100 */
					pr_err("COMPARE_DIFF: Page 0x%lx hash mismatch: "
					       "remote=%08x local=%08x\n",
					       (unsigned long)remote_phi.vaddr,
					       remote_phi.crc32, local_crc);
				}
			}

			if (pages_checked % 100000 == 0)
				pr_warn("COMPARE: Checked %d pages, %d diffs so far\n",
					pages_checked, page_diffs);
		}
	}

	xfree(page_buf);
#endif

	xfree(local_vmas);
	xfree(remote_vmas);

#ifdef CONFIG_COW_COMPARE_PAGES
	pr_err("COMPARE_RESULT: Checked %d pages, coverage gaps=%d, %d page diffs (exact-boundary: %d PRIMARY-only, %d REPLICA-only)\n",
	       pages_checked, uncovered_ranges, page_diffs, vma_diffs, replica_only);
#else
	pr_err("COMPARE_RESULT: coverage gaps=%d (exact-boundary: %d PRIMARY-only, %d REPLICA-only; page comparison disabled)\n",
	       uncovered_ranges, vma_diffs, replica_only);
#endif

	/* Send done message */
	hdr.type = MSG_COMPARE_DONE;
	hdr.len = 0;
	send(sk, &hdr, sizeof(hdr), 0);

	/*
	 * Success is defined by coverage, not exact-boundary match. Merge/split
	 * of adjacent anon VMAs on restore is normal and harmless as long as
	 * every PRIMARY byte is mapped on REPLICA.
	 */
#ifdef CONFIG_COW_COMPARE_PAGES
	return (uncovered_ranges == 0 && page_diffs == 0) ? 0 : 1;
#else
	return (uncovered_ranges == 0) ? 0 : 1;
#endif
}

/*
 * PRIMARY: Listen for comparison connection
 */
int cow_compare_listen(int *out_sk)
{
	int listen_sk, sk;
	struct sockaddr_in addr;
	int opt = 1;

	listen_sk = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_sk < 0) {
		pr_perror("COMPARE: socket failed");
		return -1;
	}

	setsockopt(listen_sk, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(COMPARE_PORT);

	if (bind(listen_sk, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		pr_perror("COMPARE: bind to port %d failed", COMPARE_PORT);
		close(listen_sk);
		return -1;
	}

	listen(listen_sk, 1);
	pr_info("COMPARE: Listening on port %d for replica connection\n", COMPARE_PORT);

	sk = accept(listen_sk, NULL, NULL);
	close(listen_sk);

	if (sk < 0) {
		pr_perror("COMPARE: accept failed");
		return -1;
	}

	pr_info("COMPARE: Replica connected\n");
	*out_sk = sk;
	return 0;
}

/*
 * REPLICA: Connect to primary for comparison
 */
int cow_compare_connect(const char *primary_addr, int *out_sk)
{
	int sk;
	struct sockaddr_in addr;

	sk = socket(AF_INET, SOCK_STREAM, 0);
	if (sk < 0) {
		pr_perror("COMPARE: socket failed");
		return -1;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(COMPARE_PORT);
	if (inet_pton(AF_INET, primary_addr, &addr.sin_addr) <= 0) {
		pr_err("COMPARE: Invalid address %s\n", primary_addr);
		close(sk);
		return -1;
	}

	pr_info("COMPARE: Connecting to primary at %s:%d\n", primary_addr, COMPARE_PORT);

	if (connect(sk, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		pr_perror("COMPARE: connect failed");
		close(sk);
		return -1;
	}

	pr_info("COMPARE: Connected to primary\n");
	*out_sk = sk;
	return 0;
}
