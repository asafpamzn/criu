#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/prctl.h>

#include "zdtmtst.h"

const char *test_doc = "Check that timer_slack_ns is preserved across C/R for multiple threads";
const char *test_author = "Ahmed Elaidy <elaidya225@gmail.com>";

#define MAIN_SLACK_NS	 123456UL
#define THREAD_SLACK_NS	 654321UL

static void *thread_fn(void *arg)
{
	long slack;

	(void)arg;

	if (prctl(PR_SET_TIMERSLACK, THREAD_SLACK_NS, 0, 0, 0)) {
		pr_perror("thread: can't set timer_slack_ns");
		return (void *)(uintptr_t)1;
	}

	test_waitsig();

	slack = prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0);
	if (slack < 0) {
		pr_perror("thread: can't get timer_slack_ns");
		return (void *)(uintptr_t)1;
	}

	if (slack != THREAD_SLACK_NS) {
		pr_err("thread: timer_slack_ns changed: expected %lu, got %ld\n",
		       THREAD_SLACK_NS, slack);
		return (void *)(uintptr_t)1;
	}

	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t thread;
	void *thread_ret;
	long slack;
	int ret;

	test_init(argc, argv);

	if (prctl(PR_SET_TIMERSLACK, MAIN_SLACK_NS, 0, 0, 0)) {
		pr_perror("can't set timer_slack_ns");
		return 1;
	}

	ret = pthread_create(&thread, NULL, thread_fn, NULL);
	if (ret) {
		pr_err("pthread_create: %d\n", ret);
		return 1;
	}

	test_daemon();
	test_waitsig();

	slack = prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0);
	if (slack < 0) {
		pr_perror("can't get timer_slack_ns");
		return 1;
	}

	if (slack != MAIN_SLACK_NS) {
		fail("main: timer_slack_ns changed: expected %lu, got %ld",
		     MAIN_SLACK_NS, slack);
		return 1;
	}

	ret = pthread_join(thread, &thread_ret);
	if (ret) {
		pr_err("pthread_join: %d\n", ret);
		return 1;
	}

	if (thread_ret) {
		fail("thread timer_slack_ns was not preserved");
		return 1;
	}

	pass();
	return 0;
}
