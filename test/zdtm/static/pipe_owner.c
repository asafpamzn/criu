#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <linux/limits.h>

#include "zdtmtst.h"

const char *test_doc = "Check pipe ownership is preserved after C/R";
const char *test_author = "Ahmed Elaidy <elaidya225@gmail.com>";

#define TEST_STRING "Hello world"

int main(int argc, char **argv)
{
	int pfd[2];
	int ret;
	char path[PATH_MAX];
	struct stat st_before, st_after;
	uid_t expected_uid;
	gid_t expected_gid;
	int proc_fd;
	char buf[sizeof(TEST_STRING)];

	test_init(argc, argv);

	ret = pipe(pfd);
	if (ret) {
		pr_perror("pipe() failed");
		return 1;
	}

	/*
	 * If running as root, change pipe ownership to test that uid/gid
	 * are properly saved and restored across C/R. Otherwise, just
	 * verify the current ownership is preserved.
	 */
	if (getuid() == 0) {
		expected_uid = 13333;
		expected_gid = 44444;
		if (fchown(pfd[0], expected_uid, expected_gid)) {
			pr_perror("fchown(pfd[0]) failed");
			return 1;
		}
		if (fchown(pfd[1], expected_uid, expected_gid)) {
			pr_perror("fchown(pfd[1]) failed");
			return 1;
		}
	} else {
		expected_uid = getuid();
		expected_gid = getgid();
	}

	/* Record ownership before C/R */
	if (fstat(pfd[0], &st_before)) {
		pr_perror("fstat() failed before C/R");
		return 1;
	}

	test_daemon();
	test_waitsig();

	/* Verify ownership is preserved after C/R */
	if (fstat(pfd[0], &st_after)) {
		pr_perror("fstat() failed after C/R");
		return 1;
	}

	if (st_after.st_uid != expected_uid || st_after.st_gid != expected_gid) {
		fail("UID/GID mismatch (got %d/%d but %d/%d expected)",
		     (int)st_after.st_uid, (int)st_after.st_gid,
		     (int)expected_uid, (int)expected_gid);
		return 1;
	}

	/*
	 * Test /proc/self/fd access. This was broken when pipe
	 * ownership wasn't restored (issue #2984).
	 */
	snprintf(path, PATH_MAX, "/proc/self/fd/%d", pfd[1]);
	proc_fd = open(path, O_WRONLY);
	if (proc_fd == -1) {
		fail("open(%s) failed after C/R - ownership not restored?", path);
		return 1;
	}

	/* Verify the pipe still works */
	ret = write(proc_fd, TEST_STRING, sizeof(TEST_STRING));
	if (ret != sizeof(TEST_STRING)) {
		pr_perror("write() failed");
		close(proc_fd);
		return 1;
	}
	close(proc_fd);

	ret = read(pfd[0], buf, sizeof(TEST_STRING));
	if (ret != sizeof(TEST_STRING)) {
		pr_perror("read() failed");
		return 1;
	}

	if (strcmp(TEST_STRING, buf)) {
		fail("data corruption");
		return 1;
	}

	close(pfd[0]);
	close(pfd[1]);

	pass();
	return 0;
}
