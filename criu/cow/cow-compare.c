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
	}

	fclose(f);
	*out_vmas = vmas;
	*out_count = count;
	return 0;
}

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
static int read_page(pid_t pid, uint64_t vaddr, void *buf)
{
	char path[64];
	int fd, ret;

	snprintf(path, sizeof(path), "/proc/%d/mem", pid);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;

	if (lseek(fd, vaddr, SEEK_SET) < 0) {
		close(fd);
		return -1;
	}

	ret = read(fd, buf, PAGE_SIZE);
	close(fd);
	return (ret == PAGE_SIZE) ? 0 : -1;
}

/*
 * PRIMARY side: Send process state to replica for comparison
 */
int cow_compare_send_state(int sk, pid_t pid)
{
	struct vma_info *vmas;
	int nr_vmas, i;
	struct compare_msg_hdr hdr;
	void *page_buf;
	int total_pages = 0, sent_hashes = 0;

	pr_warn("COMPARE: Starting state send for PID %d\n", pid);

	/* Step 1: Read and send VMA list */
	if (read_process_vmas(pid, &vmas, &nr_vmas) < 0)
		return -1;

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
		total_pages += (vmas[i].end - vmas[i].start) / PAGE_SIZE;
	}

	/* End of VMA list */
	hdr.type = MSG_VMA_END;
	hdr.len = 0;
	send(sk, &hdr, sizeof(hdr), 0);

	pr_warn("COMPARE: Sent %d VMAs, now sending hashes for %d pages\n",
		nr_vmas, total_pages);

	/* Step 2: Send page hashes for each VMA */
	page_buf = xmalloc(PAGE_SIZE);

	for (i = 0; i < nr_vmas; i++) {
		uint64_t addr;

		for (addr = vmas[i].start; addr < vmas[i].end; addr += PAGE_SIZE) {
			struct page_hash_info phi;

			if (read_page(pid, addr, page_buf) < 0) {
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
	xfree(vmas);

	pr_warn("COMPARE: Sent %d page hashes, waiting for comparison result\n",
		sent_hashes);

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
	int vma_diffs = 0, page_diffs = 0, pages_checked = 0;
	void *page_buf;
	int i, j;

	pr_warn("COMPARE: Starting state comparison for local PID %d\n", pid);

	/* Read local VMAs */
	if (read_process_vmas(pid, &local_vmas, &local_nr_vmas) < 0)
		return -1;

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

	/* Compare VMA lists */
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
			pr_err("COMPARE_DIFF: VMA 0x%lx-0x%lx exists on PRIMARY but not REPLICA\n",
			       (unsigned long)remote_vmas[i].start,
			       (unsigned long)remote_vmas[i].end);
			vma_diffs++;
		}
	}

	pr_warn("COMPARE: VMA comparison done, %d differences\n", vma_diffs);

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
			if (read_page(pid, remote_phi.vaddr, page_buf) < 0) {
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
	xfree(local_vmas);
	xfree(remote_vmas);

	pr_err("COMPARE_RESULT: Checked %d pages, found %d VMA diffs, %d page diffs\n",
	       pages_checked, vma_diffs, page_diffs);

	/* Send done message */
	hdr.type = MSG_COMPARE_DONE;
	hdr.len = 0;
	send(sk, &hdr, sizeof(hdr), 0);

	return (vma_diffs == 0 && page_diffs == 0) ? 0 : 1;
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
