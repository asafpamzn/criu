#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <endian.h>
#include <pthread.h>

#include "log.h"
#include "image-xfer.h"

static int send_all(int sk, const void *buf, size_t len)
{
	const char *p = buf;
	while (len > 0) {
		ssize_t n = write(sk, p, len);
		if (n <= 0) {
			pr_perror("image-xfer: send");
			return -1;
		}
		p += n;
		len -= n;
	}
	return 0;
}

static int recv_all(int sk, void *buf, size_t len)
{
	char *p = buf;
	while (len > 0) {
		ssize_t n = read(sk, p, len);
		if (n <= 0) {
			pr_perror("image-xfer: recv");
			return -1;
		}
		p += n;
		len -= n;
	}
	return 0;
}

static int should_transfer(const char *name)
{
	size_t len = strlen(name);

	if (len > 4 && strcmp(name + len - 4, ".img") == 0)
		return 1;
	if (strncmp(name, "stats-", 6) == 0)
		return 1;
	return 0;
}

int serve_image_files(int port, const char *imgs_dir)
{
	struct sockaddr_in addr;
	int srv, conn, ret = -1, one = 1;
	DIR *d;
	struct dirent *ent;
	int file_count = 0;
	uint32_t count_be;
	unsigned long total_bytes = 0;

	d = opendir(imgs_dir);
	if (!d) {
		pr_perror("image-xfer: opendir %s", imgs_dir);
		return -1;
	}
	while ((ent = readdir(d)))
		if (should_transfer(ent->d_name))
			file_count++;
	closedir(d);

	srv = socket(AF_INET, SOCK_STREAM, 0);
	if (srv < 0) {
		pr_perror("image-xfer: socket");
		return -1;
	}
	setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(port);
	if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		pr_perror("image-xfer: bind port %d", port);
		close(srv);
		return -1;
	}
	listen(srv, 1);

	conn = accept(srv, NULL, NULL);
	close(srv);
	if (conn < 0) {
		pr_perror("image-xfer: accept");
		return -1;
	}

	count_be = htobe32(file_count);
	if (send_all(conn, &count_be, 4))
		goto out;

	d = opendir(imgs_dir);
	if (!d)
		goto out;

	while ((ent = readdir(d))) {
		char path[PATH_MAX];
		struct stat st;
		uint16_t nlen, name_len_be;
		uint64_t size_be;
		int fd;
		char buf[65536];
		off_t remain;
		ssize_t r;

		if (!should_transfer(ent->d_name))
			continue;

		snprintf(path, sizeof(path), "%s/%s", imgs_dir, ent->d_name);
		if (stat(path, &st) < 0)
			continue;

		nlen = strlen(ent->d_name);
		name_len_be = htobe16(nlen);
		if (send_all(conn, &name_len_be, 2))
			goto out_dir;
		if (send_all(conn, ent->d_name, nlen))
			goto out_dir;

		size_be = htobe64(st.st_size);
		if (send_all(conn, &size_be, 8))
			goto out_dir;

		fd = open(path, O_RDONLY);
		if (fd < 0)
			goto out_dir;

		remain = st.st_size;
		while (remain > 0) {
			r = read(fd, buf,
				 remain < (off_t)sizeof(buf) ?
				 remain : sizeof(buf));
			if (r <= 0) {
				close(fd);
				goto out_dir;
			}
			if (send_all(conn, buf, r)) {
				close(fd);
				goto out_dir;
			}
			remain -= r;
		}
		close(fd);
		total_bytes += st.st_size;
	}

	pr_err("image-xfer: served %d files (%lu bytes)\n",
	       file_count, total_bytes);
	ret = 0;

out_dir:
	closedir(d);
out:
	close(conn);
	return ret;
}

int fetch_image_files(const char *host, int port,
		      const char *imgs_dir, int timeout_s)
{
	struct sockaddr_in addr;
	int sk = -1, attempt;
	uint32_t count_be, file_count;
	unsigned long total_bytes = 0;
	int i;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
		pr_err("image-xfer: bad address %s\n", host);
		return -1;
	}

	for (attempt = 0; attempt < timeout_s * 10; attempt++) {
		sk = socket(AF_INET, SOCK_STREAM, 0);
		if (sk < 0)
			return -1;
		if (connect(sk, (struct sockaddr *)&addr, sizeof(addr)) == 0)
			break;
		close(sk);
		sk = -1;
		usleep(100000);
	}
	if (sk < 0) {
		pr_err("image-xfer: connect to %s:%d failed after %ds\n",
		       host, port, timeout_s);
		return -1;
	}

	if (recv_all(sk, &count_be, 4))
		goto err;
	file_count = be32toh(count_be);

	for (i = 0; i < (int)file_count; i++) {
		uint16_t name_len_be, nlen;
		uint64_t size_be, fsize, remain;
		char name[256];
		char path[PATH_MAX];
		int fd;
		char buf[65536];
		size_t want;

		if (recv_all(sk, &name_len_be, 2))
			goto err;
		nlen = be16toh(name_len_be);
		if (nlen >= sizeof(name))
			goto err;
		if (recv_all(sk, name, nlen))
			goto err;
		name[nlen] = '\0';

		if (recv_all(sk, &size_be, 8))
			goto err;
		fsize = be64toh(size_be);

		snprintf(path, sizeof(path), "%s/%s", imgs_dir, name);
		fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
		if (fd < 0) {
			pr_perror("image-xfer: create %s", path);
			goto err;
		}

		remain = fsize;
		while (remain > 0) {
			want = remain < sizeof(buf) ? remain : sizeof(buf);
			if (recv_all(sk, buf, want)) {
				close(fd);
				goto err;
			}
			if (write(fd, buf, want) != (ssize_t)want) {
				close(fd);
				goto err;
			}
			remain -= want;
		}
		close(fd);
		total_bytes += fsize;
	}

	pr_err("image-xfer: fetched %u files (%lu bytes) from %s:%d\n",
	       file_count, total_bytes, host, port);
	close(sk);
	return 0;

err:
	close(sk);
	return -1;
}

struct serve_args {
	int port;
	const char *dir;
};

void *serve_image_files_thread(void *arg)
{
	struct serve_args *a = arg;

	serve_image_files(a->port, a->dir);
	return NULL;
}
