/*
 * page-recv: standalone page receiver for CRIU COW dump migration.
 *
 * Connects to the source CRIU page-server over TCP, receives pages
 * using the existing wire protocol (page_server_iov headers + data),
 * and installs them into a stopped restored process via
 * process_vm_writev.  Bypasses userfaultfd entirely — no UFFDIO_COPY,
 * no futex deadlock on aarch64.
 *
 * Usage:
 *   page-recv --pid PID --address HOST --port PORT \
 *             --images-dir DIR [--streams N]
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <lz4.h>

/* ---- Wire protocol (from criu/page-xfer.c) ---- */

#define PAGE_SIZE 4096

struct page_server_iov {
	unsigned int cmd;
	unsigned long long nr_pages;
	unsigned long long vaddr;
	unsigned long long dst_id;
};

#define PS_IOV_OPEN2		4
#define PS_IOV_ADD_F		6
#define PS_IOV_GET_ALL		8
#define PS_IOV_ADD_F_COMPRESS	10
#define PS_IOV_VMA_DIFF		11
#define PS_IOV_T3_REGS		12

struct vma_diff_entry {
	unsigned long long start;
	unsigned long long end;
	unsigned int prot;
	unsigned int pad;
};

struct t3_thread_regs {
	unsigned long long regs[31];
	unsigned long long sp;
	unsigned long long pc;
	unsigned long long pstate;
	unsigned long long tls;
};
#define PS_CMD_BITS		16
#define PS_CMD_MASK		((1 << PS_CMD_BITS) - 1)
#define PS_TYPE_BITS		8
#define PS_TYPE_PID		1

static inline int decode_ps_cmd(unsigned int cmd)
{
	return cmd & PS_CMD_MASK;
}

static inline unsigned long long encode_pm_pid(unsigned long vpid)
{
	return ((unsigned long long)vpid << PS_TYPE_BITS) | PS_TYPE_PID;
}

/* ---- VMA diff signaling ---- */

static const char *g_images_dir;

static void write_vma_diff_file(struct vma_diff_entry *vmas, int count)
{
	char path[4096];
	int fd;

	snprintf(path, sizeof(path), "%s/new_vmas.dat", g_images_dir);
	fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
	if (fd < 0) {
		perror("open new_vmas.dat");
		return;
	}
	if (write(fd, &count, sizeof(count)) != sizeof(count)) {
		perror("write count");
		goto cleanup;
	}
	if (write(fd, vmas, count * sizeof(*vmas)) !=
	    (ssize_t)(count * sizeof(*vmas))) {
		perror("write vmas");
		goto cleanup;
	}
cleanup:
	close(fd);
}

static void signal_vma_diff_ready(void)
{
	char path[4096];
	int fd;

	snprintf(path, sizeof(path), "%s/vma_diff_ready", g_images_dir);
	fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
	if (fd >= 0)
		close(fd);
}

static void wait_for_vma_created(void)
{
	char path[4096];
	struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 }; /* 1ms */
	int i;

	snprintf(path, sizeof(path), "%s/vma_created", g_images_dir);

	for (i = 0; i < 30000; i++) {
		if (access(path, F_OK) == 0)
			return;
		nanosleep(&ts, NULL);
	}
	fprintf(stderr, "WARN: vma_created not signaled within 30s\n");
}

/* ---- Helpers ---- */

static int recv_full(int sk, void *buf, size_t len)
{
	size_t off = 0;

	while (off < len) {
		ssize_t r = recv(sk, (char *)buf + off, len - off, MSG_WAITALL);
		if (r <= 0) {
			if (r == 0)
				return -1; /* EOF */
			if (errno == EINTR)
				continue;
			perror("recv");
			return -1;
		}
		off += r;
	}
	return 0;
}

static int tcp_connect(const char *host, int port)
{
	struct sockaddr_in addr;
	int sk, one = 1, bufsz = 16 * 1024 * 1024;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
		fprintf(stderr, "Invalid address: %s\n", host);
		return -1;
	}

	sk = socket(AF_INET, SOCK_STREAM, 0);
	if (sk < 0) {
		perror("socket");
		return -1;
	}

	if (connect(sk, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		perror("connect");
		close(sk);
		return -1;
	}

	setsockopt(sk, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));
	setsockopt(sk, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	return sk;
}

/* ---- Protocol handshake ---- */

static int send_full(int sk, const void *buf, size_t len)
{
	size_t off = 0;

	while (off < len) {
		ssize_t w = send(sk, (const char *)buf + off, len - off, 0);
		if (w <= 0) {
			if (w < 0 && errno == EINTR)
				continue;
			perror("send");
			return -1;
		}
		off += w;
	}
	return 0;
}

/*
 * Perform the page-server handshake on stream 0:
 *   1. Send PS_IOV_OPEN2 with encoded dst_id
 *   2. Recv 1-byte has_parent reply
 *   3. Send PS_IOV_GET_ALL to trigger bulk transfer
 */
static int do_handshake(int sk, unsigned long vpid)
{
	struct page_server_iov pi;
	char has_parent;
	int one = 1;

	/* OPEN2 */
	memset(&pi, 0, sizeof(pi));
	pi.cmd = PS_IOV_OPEN2;
	pi.dst_id = encode_pm_pid(vpid);
	if (send_full(sk, &pi, sizeof(pi)) < 0)
		return -1;
	setsockopt(sk, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

	if (recv_full(sk, &has_parent, 1) < 0) {
		fprintf(stderr, "handshake: no OPEN2 reply\n");
		return -1;
	}
	fprintf(stderr, "handshake: OPEN2 ok (has_parent=%d)\n", (int)has_parent);

	/* GET_ALL — triggers unified_page_server_thread on source */
	memset(&pi, 0, sizeof(pi));
	pi.cmd = PS_IOV_GET_ALL;
	pi.dst_id = vpid;
	if (send_full(sk, &pi, sizeof(pi)) < 0)
		return -1;
	setsockopt(sk, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

	fprintf(stderr, "handshake: GET_ALL sent for vpid=%lu\n", vpid);
	return 0;
}

/* ---- Per-stream state ---- */

struct stream_ctx {
	int id;
	int sk;
	pid_t target_pid;
	unsigned long pages_installed;
	unsigned long bytes_received;
	int error;
};

static void *stream_worker(void *arg)
{
	struct stream_ctx *ctx = arg;
	char *data_buf = NULL;
	size_t data_buf_sz = 0;
	char *comp_buf = NULL;
	int comp_buf_sz = 0;
	int max_comp = LZ4_compressBound(PAGE_SIZE);
	unsigned long write_errors = 0;
	int eos_count = 0;

	while (1) {
		struct page_server_iov hdr;
		int cmd;
		unsigned long long npages;
		size_t data_len;

		if (recv_full(ctx->sk, &hdr, sizeof(hdr)) < 0) {
			/*
			 * EOF/reset on header read: source closed the
			 * connection.  If we already received pages, treat
			 * this as a normal end-of-stream (source may have
			 * shut down after sending all data).
			 */
			if (eos_count > 0 || ctx->pages_installed > 0) {
				fprintf(stderr, "stream %d: connection closed "
					"after %lu pages\n",
					ctx->id, ctx->pages_installed);
			} else {
				fprintf(stderr, "stream %d: connection closed "
					"with no data\n", ctx->id);
				ctx->error = 1;
			}
			break;
		}
		__sync_fetch_and_add(&ctx->bytes_received, sizeof(hdr));

		npages = hdr.nr_pages;
		if (npages == 0) {
			eos_count++;
			if (eos_count == 1) {
				/*
				 * First EOS: bulk worker done.  On stream 0,
				 * convergence pages follow.  Keep reading.
				 */
				fprintf(stderr, "stream %d: bulk complete "
					"(%lu pages), waiting for convergence\n",
					ctx->id, ctx->pages_installed);
				continue;
			}
			/*
			 * Second EOS (or first on non-convergence streams):
			 * all pages including convergence are done.
			 */
			fprintf(stderr, "stream %d: end-of-stream "
				"(%lu pages total)\n",
				ctx->id, ctx->pages_installed);
			break;
		}

		cmd = decode_ps_cmd(hdr.cmd);

		/* Handle VMA diff: new VMAs that must be created on replica */
		if (cmd == PS_IOV_VMA_DIFF) {
			int nr_vmas = (int)npages;
			size_t vma_data_sz;

			if (npages > 100000) {
				fprintf(stderr, "stream %d: VMA diff count "
					"too large (%llu), rejecting\n",
					ctx->id, npages);
				ctx->error = 1;
				break;
			}
			vma_data_sz = nr_vmas *
				sizeof(struct vma_diff_entry);
			struct vma_diff_entry *vmas;

			vmas = malloc(vma_data_sz);
			if (!vmas) {
				ctx->error = 1;
				break;
			}
			if (recv_full(ctx->sk, vmas, vma_data_sz) < 0) {
				free(vmas);
				ctx->error = 1;
				break;
			}
			__sync_fetch_and_add(&ctx->bytes_received, vma_data_sz);

			write_vma_diff_file(vmas, nr_vmas);
			signal_vma_diff_ready();

			fprintf(stderr, "stream %d: VMA diff received "
				"(%d new VMAs), waiting for creation\n",
				ctx->id, nr_vmas);

			wait_for_vma_created();

			fprintf(stderr, "stream %d: VMAs created, "
				"resuming\n", ctx->id);

			free(vmas);
			continue;
		}

		/* Handle T3 register data */
		if (cmd == PS_IOV_T3_REGS) {
			int nr_threads = (int)npages;
			size_t regs_sz = nr_threads *
				sizeof(struct t3_thread_regs);
			struct t3_thread_regs *tregs;
			char path[4096];
			int fd;

			if (nr_threads > 1024 || nr_threads <= 0) {
				ctx->error = 1;
				break;
			}
			tregs = malloc(regs_sz);
			if (!tregs) { ctx->error = 1; break; }
			if (recv_full(ctx->sk, tregs, regs_sz) < 0) {
				free(tregs);
				ctx->error = 1;
				break;
			}
			__sync_fetch_and_add(&ctx->bytes_received,
					     regs_sz);
			snprintf(path, sizeof(path),
				 "%s/t3_regs.dat", g_images_dir);
			fd = open(path,
				  O_CREAT | O_WRONLY | O_TRUNC, 0644);
			if (fd >= 0) {
				int cnt = nr_threads;
				ssize_t w1, w2;

				w1 = write(fd, &cnt, sizeof(cnt));
				w2 = write(fd, tregs, regs_sz);
				close(fd);
				if (w1 < 0 || w2 < 0)
					fprintf(stderr, "stream %d: "
						"t3_regs.dat write "
						"error\n", ctx->id);
			}
			fprintf(stderr, "stream %d: T3 regs "
				"received (%d threads)\n",
				ctx->id, nr_threads);
			free(tregs);
			continue;
		}

		data_len = npages * PAGE_SIZE;

		/* Ensure data buffer is large enough */
		if (data_len > data_buf_sz) {
			free(data_buf);
			data_buf_sz = data_len;
			data_buf = malloc(data_buf_sz);
			if (!data_buf) {
				ctx->error = 1;
				break;
			}
		}

		if (cmd == PS_IOV_ADD_F_COMPRESS) {
			int comp_size;

			if (recv_full(ctx->sk, &comp_size, sizeof(comp_size)) < 0) {
				ctx->error = 1;
				break;
			}
			__sync_fetch_and_add(&ctx->bytes_received, sizeof(comp_size));

			if (comp_size <= 0 || comp_size > max_comp) {
				fprintf(stderr, "stream %d: bad compressed size %d\n",
					ctx->id, comp_size);
				ctx->error = 1;
				break;
			}

			if (comp_size > comp_buf_sz) {
				free(comp_buf);
				comp_buf_sz = comp_size + 4096;
				comp_buf = malloc(comp_buf_sz);
				if (!comp_buf) {
					ctx->error = 1;
					break;
				}
			}

			if (recv_full(ctx->sk, comp_buf, comp_size) < 0) {
				ctx->error = 1;
				break;
			}
			__sync_fetch_and_add(&ctx->bytes_received, comp_size);

			int dec = LZ4_decompress_safe(comp_buf, data_buf,
						      comp_size, data_len);
			if (dec != (int)data_len) {
				fprintf(stderr, "stream %d: LZ4 decompress failed "
					"(%d != %zu)\n", ctx->id, dec, data_len);
				ctx->error = 1;
				break;
			}
		} else if (cmd == PS_IOV_ADD_F) {
			if (recv_full(ctx->sk, data_buf, data_len) < 0) {
				ctx->error = 1;
				break;
			}
			__sync_fetch_and_add(&ctx->bytes_received, data_len);
		} else {
			fprintf(stderr, "stream %d: unknown cmd %d, skipping\n",
				ctx->id, cmd);
			continue;
		}

		/* Install pages via process_vm_writev */
		{
			struct iovec local_iov = {
				.iov_base = data_buf,
				.iov_len = data_len,
			};
			struct iovec remote_iov = {
				.iov_base = (void *)hdr.vaddr,
				.iov_len = data_len,
			};
			ssize_t w = process_vm_writev(ctx->target_pid,
						      &local_iov, 1,
						      &remote_iov, 1, 0);
			if (w != (ssize_t)data_len) {
				write_errors++;
				if (write_errors <= 5) {
					fprintf(stderr, "stream %d: process_vm_writev "
						"at %llx: %s (got %zd/%zu)\n",
						ctx->id, hdr.vaddr,
						w < 0 ? strerror(errno) : "short",
						w, data_len);
				}
				if (write_errors >= 50) {
					fprintf(stderr, "stream %d: too many write "
						"errors (%lu), aborting\n",
						ctx->id, write_errors);
					ctx->error = 1;
					break;
				}
			}
		}

		__sync_fetch_and_add(&ctx->pages_installed, npages);
	}

	if (write_errors > 5)
		fprintf(stderr, "stream %d: %lu total write errors (suppressed after first 5)\n",
			ctx->id, write_errors);

	free(data_buf);
	free(comp_buf);
	close(ctx->sk);
	return NULL;
}

/* ---- Main ---- */

static void usage(const char *prog)
{
	fprintf(stderr, "Usage: %s --pid PID --address HOST --port PORT "
		"--images-dir DIR [--vpid VPID] [--streams N]\n", prog);
	exit(1);
}

int main(int argc, char **argv)
{
	pid_t target_pid = 0;
	unsigned long vpid = 0;
	const char *address = NULL;
	int port = 0;
	const char *images_dir = NULL;
	int num_streams = 1;
	int i;

	/* Parse args */
	for (i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "--pid") && i + 1 < argc)
			target_pid = atoi(argv[++i]);
		else if (!strcmp(argv[i], "--vpid") && i + 1 < argc)
			vpid = strtoul(argv[++i], NULL, 10);
		else if (!strcmp(argv[i], "--address") && i + 1 < argc)
			address = argv[++i];
		else if (!strcmp(argv[i], "--port") && i + 1 < argc)
			port = atoi(argv[++i]);
		else if (!strcmp(argv[i], "--images-dir") && i + 1 < argc)
			images_dir = argv[++i];
		else if (!strcmp(argv[i], "--streams") && i + 1 < argc)
			num_streams = atoi(argv[++i]);
		else
			usage(argv[0]);
	}

	if (!target_pid || !address || !port || !images_dir)
		usage(argv[0]);

	g_images_dir = images_dir;

	if (!vpid)
		vpid = target_pid; /* Default: vpid == real pid (no namespaces) */

	if (num_streams < 1 || num_streams > 16)
		num_streams = 1;

	fprintf(stderr, "page-recv: pid=%d vpid=%lu addr=%s port=%d streams=%d\n",
		target_pid, vpid, address, port, num_streams);

	struct timespec t_start;
	clock_gettime(CLOCK_MONOTONIC, &t_start);

	/* Connect stream 0 first — it does the protocol handshake */
	struct stream_ctx *streams = calloc(num_streams, sizeof(*streams));
	pthread_t *threads = calloc(num_streams, sizeof(*threads));
	if (!streams || !threads) {
		perror("calloc");
		return 1;
	}

	{
		int sk = -1, retry;
		for (retry = 0; retry < 10 && sk < 0; retry++) {
			sk = tcp_connect(address, port);
			if (sk < 0)
				usleep(100000);
		}
		if (sk < 0) {
			fprintf(stderr, "stream 0: connect failed\n");
			return 1;
		}
		streams[0].id = 0;
		streams[0].sk = sk;
		streams[0].target_pid = target_pid;
	}

	/* Handshake on stream 0: OPEN2 + GET_ALL triggers bulk transfer */
	if (do_handshake(streams[0].sk, vpid) < 0) {
		fprintf(stderr, "handshake failed\n");
		return 1;
	}

	/* Connect additional streams — the source bulk thread accepts them */
	for (i = 1; i < num_streams; i++) {
		int sk = -1, retry;

		for (retry = 0; retry < 10 && sk < 0; retry++) {
			sk = tcp_connect(address, port);
			if (sk < 0)
				usleep(100000);
		}

		if (sk < 0) {
			fprintf(stderr, "stream %d: connect failed after 10 retries\n", i);
			return 1;
		}

		streams[i].id = i;
		streams[i].sk = sk;
		streams[i].target_pid = target_pid;
	}

	fprintf(stderr, "page-recv: %d streams connected\n", num_streams);

	/* Launch worker threads */
	for (i = 0; i < num_streams; i++) {
		if (i == 0) {
			/* Stream 0 runs in main thread for simplicity */
			continue;
		}
		if (pthread_create(&threads[i], NULL, stream_worker, &streams[i])) {
			perror("pthread_create");
			return 1;
		}
	}

	/* Main thread handles stream 0 */
	stream_worker(&streams[0]);

	/* Join worker threads */
	for (i = 1; i < num_streams; i++)
		pthread_join(threads[i], NULL);

	/* Stats */
	struct timespec t_end;
	clock_gettime(CLOCK_MONOTONIC, &t_end);
	double elapsed = (t_end.tv_sec - t_start.tv_sec) +
			 (t_end.tv_nsec - t_start.tv_nsec) / 1e9;

	unsigned long total_pages = 0, total_bytes = 0;
	int any_error = 0;

	for (i = 0; i < num_streams; i++) {
		total_pages += streams[i].pages_installed;
		total_bytes += streams[i].bytes_received;
		if (streams[i].error)
			any_error = 1;
		fprintf(stderr, "  stream %d: %lu pages, %lu bytes%s\n",
			i, streams[i].pages_installed, streams[i].bytes_received,
			streams[i].error ? " (ERROR)" : "");
	}

	fprintf(stderr, "page-recv: %lu pages (%.1f MB) in %.3fs (%.1f MB/s)%s\n",
		total_pages,
		(double)(total_pages * PAGE_SIZE) / (1024 * 1024),
		elapsed,
		elapsed > 0 ? (double)(total_pages * PAGE_SIZE) / (1024 * 1024) / elapsed : 0,
		any_error ? " WITH ERRORS" : "");

	/* Send STAGED signal to source if configured */
	{
		const char *staged_addr = getenv("PAGE_RECV_STAGED_ADDR");
		const char *staged_port_s = getenv("PAGE_RECV_STAGED_PORT");

		if (staged_addr && staged_port_s) {
			int sp = atoi(staged_port_s);
			struct sockaddr_in sa;
			int ssk, retry;

			memset(&sa, 0, sizeof(sa));
			sa.sin_family = AF_INET;
			sa.sin_port = htons(sp);
			inet_pton(AF_INET, staged_addr, &sa.sin_addr);

			for (retry = 0; retry < 100; retry++) {
				ssk = socket(AF_INET, SOCK_STREAM, 0);
				if (ssk < 0)
					break;
				if (connect(ssk, (struct sockaddr *)&sa,
					    sizeof(sa)) == 0) {
					if (write(ssk, "STAGED\n", 7) < 0)
					perror("staged write");
					close(ssk);
					fprintf(stderr,
						"page-recv: sent STAGED to %s:%d\n",
						staged_addr, sp);
					break;
				}
				close(ssk);
				usleep(100000);
			}
		}
	}

	/* Write completion marker */
	{
		char path[4096];
		int fd;

		snprintf(path, sizeof(path), "%s/bulk_stream_done", images_dir);
		fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
		if (fd >= 0)
			close(fd);
		else
			perror("write bulk_stream_done marker");
	}

	free(streams);
	free(threads);
	return any_error ? 1 : 0;
}
